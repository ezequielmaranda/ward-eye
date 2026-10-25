"""
Crawl de partidas de otros jugadores, muestreados por tier, para comparar
cómo se juega en cada elo. Pipeline dlt aparte (`lol_crawl`) en el schema
`lol_crawl` del mismo DuckDB: no toca lol_raw ni los marts del jugador.

Dos pasos, los dos idempotentes y retomables:

  sample  Elige candidatos al azar de cada tier/división (League-V4) y los
          guarda en crawl_seeds, etiquetados con una corrida (`--run`, por
          defecto el mes actual). El tier de un jugador es el de HOY, por eso
          las partidas se piden en una ventana corta antes del muestreo.

  run     Lista candidatos en su orden aleatorio (sus últimas N ranked de la
          ventana, en crawl_seed_matches) hasta que cada división tiene su cupo
          de semillas *activas*, y después baja las partidas pendientes en lotes.
          Cada lote se carga apenas termina: si se corta o se acaba el
          presupuesto de tiempo, la próxima corrida sigue donde quedó.

  python -m dlt_pipeline.crawl sample --tiers GOLD --players-per-tier 20 --run 2026-10-test
  python -m dlt_pipeline.crawl run --budget-minutes 60

Qué se guarda de cada partida (timeline recortado):
  - crawl_matches / crawl_participants: igual que lol_raw, con challenges_json.
  - crawl_frames: solo los frames de los minutos 5/10/15/20/25 (el resto de
    los ~35 por partida no se usa en el análisis).
  - crawl_events: kills, estructuras, objetivos y compras de ítems. Sin wards
    ni subidas de nivel/skill (~70% de los eventos); la visión sale de
    challenges.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
import sys
import time
from datetime import datetime, timezone

import dlt
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from extraction.riot_client import RiotAPIError, RiotClient
from extraction.transformers import (
    parse_match,
    parse_participants,
    parse_timeline_events,
    parse_timeline_frames,
)

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RIOT_API_KEY = os.environ["RIOT_API_KEY"].strip()
RIOT_REGION = os.environ.get("RIOT_REGION", "la2").strip()
RIOT_ROUTING = os.environ.get("RIOT_ROUTING", "americas").strip()
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(ROOT_DIR, "warddata.duckdb"))

TIERS = ["IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", "EMERALD", "DIAMOND",
         "MASTER", "GRANDMASTER", "CHALLENGER"]
DIVISIONS = ["I", "II", "III", "IV"]
APEX_TIERS = {"MASTER", "GRANDMASTER", "CHALLENGER"}

RANKED_SOLO = 420
WINDOW_DAYS = 30            # partidas de los 30 días previos al muestreo
MATCHES_PER_SEED = 10
MIN_GAMES = 50              # semillas con historial suficiente en la temporada
# "≥50 partidas y no inactivo" no implica haber jugado en la ventana: en la
# prueba con Oro, 10 de 20 semillas tenían 0 ranked en 30 días. Se muestrean 3×
# candidatos y se listan en su orden aleatorio hasta completar el cupo de activos.
CANDIDATE_FACTOR = 3
PAGES_PER_DIVISION = 3      # páginas al azar de las que se elige la muestra
MATCH_BATCH = 200           # partidas por carga a DuckDB
SEED_BATCH = 50             # semillas listadas por carga
FRAME_MARKS = {5, 10, 15, 20, 25}
KEPT_EVENTS = {"CHAMPION_KILL", "BUILDING_KILL", "ELITE_MONSTER_KILL", "ITEM_PURCHASED"}
# Columnas de parse_timeline_events que solo usan los eventos descartados.
DROPPED_EVENT_COLS = {"ward_type", "skill_slot", "level_up_type"}

# 100 req / 2 min sostenidas (límite de la key), con un margen chico.
MIN_INTERVAL_S = 1.25


# ---------------------------------------------------------------------------
# Recursos dlt
# ---------------------------------------------------------------------------

@dlt.resource(
    name="crawl_seeds",
    write_disposition="merge",
    primary_key=["run", "puuid"],
    # Explícitas: al muestrear valen NULL, y dlt no crea columnas sin un valor
    # del que inferir el tipo (pending_seeds() filtra por listed_at).
    columns={
        "listed_at": {"data_type": "timestamp", "nullable": True},
        "n_matches_listed": {"data_type": "bigint", "nullable": True},
    },
)
def seeds_resource(rows: list[dict]):
    yield from rows


@dlt.resource(name="crawl_seed_matches", write_disposition="merge",
              primary_key=["run", "puuid", "match_id"])
def seed_matches_resource(rows: list[dict]):
    yield from rows


@dlt.resource(name="crawl_matches", write_disposition="merge", primary_key="match_id")
def matches_resource(rows: list[dict]):
    yield from rows


@dlt.resource(
    name="crawl_participants",
    write_disposition="merge",
    primary_key=["match_id", "puuid"],
    columns={
        "lane_minions_first_10min": {"data_type": "bigint", "nullable": True},
        "challenges_json": {"data_type": "text", "nullable": True},
    },
)
def participants_resource(rows: list[dict]):
    yield from rows


@dlt.resource(name="crawl_frames", write_disposition="merge",
              primary_key=["match_id", "participant_id", "timestamp_ms"])
def frames_resource(rows: list[dict]):
    yield from rows


@dlt.resource(
    name="crawl_events",
    write_disposition="merge",
    # Mismo motivo que lol_raw.raw_timeline_events: un evento no tiene clave
    # natural (participant_id es NULL en varios tipos), así que el grano del
    # merge es la partida entera.
    merge_key="match_id",
    columns={"participant_id": {"nullable": True, "data_type": "bigint"}},
)
def events_resource(rows: list[dict]):
    yield from rows


@dlt.resource(name="crawl_failed_matches", write_disposition="merge", primary_key="match_id")
def failed_resource(rows: list[dict]):
    yield from rows


# ---------------------------------------------------------------------------
# Estado (leído de DuckDB, única fuente de verdad)
# ---------------------------------------------------------------------------

def _read(sql: str) -> list[tuple]:
    """Lee en READ_ONLY y cierra enseguida (DuckDB es single-writer)."""
    import duckdb

    if not os.path.exists(DUCKDB_PATH):
        return []
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        return con.execute(sql).fetchall()
    except duckdb.CatalogException:
        return []   # la tabla todavía no existe (primera corrida)
    finally:
        con.close()


SEED_COLS = ["run", "puuid", "population", "tier", "division", "lp", "wins", "losses",
             "hot_streak", "veteran", "fresh_blood", "sampled_at", "quota", "candidate_rank"]


def pending_seeds() -> list[dict]:
    """
    Candidatos sin listar de las divisiones que todavía no llegaron a su cupo
    de semillas activas (≥1 partida en la ventana), en su orden aleatorio.
    Cada fila trae `active`: cuántas activas tiene ya su división.
    """
    cols = ", ".join(f"s.{c}" for c in SEED_COLS)
    rows = _read(f"""
        with g as (
            select run, tier, division,
                   count(*) filter (where n_matches_listed > 0) as active
            from lol_crawl.crawl_seeds group by all
        )
        select {cols}, g.active
        from lol_crawl.crawl_seeds s join g using (run, tier, division)
        where s.listed_at is null and g.active < s.quota
        order by s.run, s.tier, s.division, s.candidate_rank
    """)
    return [dict(zip(SEED_COLS + ["active"], r)) for r in rows]


def pending_match_ids() -> list[str]:
    """Partidas listadas que todavía no están cargadas ni fallaron."""
    loaded = {r[0] for r in _read("select match_id from lol_crawl.crawl_matches")}
    failed = {r[0] for r in _read("select match_id from lol_crawl.crawl_failed_matches")}
    mine = {r[0] for r in _read("select match_id from lol_raw.raw_matches")}
    listed = [r[0] for r in _read(
        "select match_id from lol_crawl.crawl_seed_matches group by 1 order by min(listed_at), 1"
    )]
    return [m for m in listed if m not in loaded | failed | mine]


# ---------------------------------------------------------------------------
# Muestreo de semillas
# ---------------------------------------------------------------------------

def _last_page(client: RiotClient, tier: str, division: str) -> int:
    """Última página no vacía: búsqueda exponencial + binaria (~2·log2 requests)."""
    hi = 1
    while client.get_league_entries(tier, division, hi):
        hi *= 2
    lo = hi // 2           # lo no vacía (o 0 si ni la 1 tiene jugadores)
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if client.get_league_entries(tier, division, mid):
            lo = mid
        else:
            hi = mid
    return lo


def _eligible(e: dict) -> bool:
    return (e.get("wins", 0) + e.get("losses", 0)) >= MIN_GAMES and not e.get("inactive", False)


def sample_seeds(client: RiotClient, tiers: list[str], players_per_tier: int,
                 run: str, rng: random.Random) -> list[dict]:
    sampled_at = datetime.now(timezone.utc)
    seeds: list[dict] = []
    for tier in tiers:
        if tier in APEX_TIERS:
            groups = {"I": [e for e in client.get_apex_league(tier) if _eligible(e)]}
            per_group = players_per_tier
        else:
            groups = {}
            for division in DIVISIONS:
                last = _last_page(client, tier, division)
                if last == 0:
                    groups[division] = []
                    continue
                pages = rng.sample(range(1, last + 1), min(PAGES_PER_DIVISION, last))
                pool = [e for p in pages for e in client.get_league_entries(tier, division, p)]
                groups[division] = [e for e in pool if _eligible(e)]
                logger.info(f"{tier} {division}: {last} páginas, {len(groups[division])} elegibles")
            per_group = math.ceil(players_per_tier / len(DIVISIONS))

        for division, pool in groups.items():
            # rng.sample devuelve en orden aleatorio: ese orden es candidate_rank
            n_candidates = per_group * CANDIDATE_FACTOR
            chosen = rng.sample(pool, min(n_candidates, len(pool)))
            if len(chosen) < n_candidates:
                logger.warning(f"{tier} {division}: solo {len(chosen)} candidatos "
                               f"para un cupo de {per_group}")
            for rank, e in enumerate(chosen):
                seeds.append({
                    "run": run,
                    "puuid": e["puuid"],
                    "population": "tier",
                    "tier": tier,
                    "division": e.get("rank", division),
                    "lp": e.get("leaguePoints"),
                    "wins": e.get("wins"),
                    "losses": e.get("losses"),
                    "hot_streak": e.get("hotStreak"),
                    "veteran": e.get("veteran"),
                    "fresh_blood": e.get("freshBlood"),
                    "sampled_at": sampled_at,
                    "quota": per_group,
                    "candidate_rank": rank,
                    "listed_at": None,
                    "n_matches_listed": None,
                })
    return seeds


# ---------------------------------------------------------------------------
# Listado e ingesta
# ---------------------------------------------------------------------------

def list_seed_matches(client: RiotClient, s: dict) -> tuple[dict, list[dict]]:
    """Últimas MATCHES_PER_SEED ranked de una semilla, en la ventana previa al muestreo."""
    listed_at = datetime.now(timezone.utc)
    sampled_at = s["sampled_at"]
    if sampled_at.tzinfo is None:
        sampled_at = sampled_at.replace(tzinfo=timezone.utc)
    end = int(sampled_at.timestamp())
    ids = client.get_match_ids(
        puuid=s["puuid"], queue=RANKED_SOLO, count=MATCHES_PER_SEED,
        start_time=end - WINDOW_DAYS * 86400, end_time=end,
    )
    links = [{"run": s["run"], "puuid": s["puuid"], "match_id": m, "listed_at": listed_at}
             for m in ids]
    seed = {c: s[c] for c in SEED_COLS} | {"listed_at": listed_at, "n_matches_listed": len(ids)}
    return seed, links


def fetch_matches(client: RiotClient, match_ids: list[str]) -> dict[str, list[dict]]:
    out = {k: [] for k in ("matches", "participants", "frames", "events", "failed")}
    for match_id in match_ids:
        try:
            raw = client.get_match(match_id)
            raw_tl = client.get_match_timeline(match_id)
        except RiotAPIError as e:
            # 404 / datos borrados: se registra para no reintentarla para siempre.
            logger.warning(f"{match_id}: {e}")
            out["failed"].append({"match_id": match_id, "status_code": e.status_code,
                                  "failed_at": datetime.now(timezone.utc)})
            continue

        m = parse_match(raw, my_puuid="")
        m.pop("my_puuid")
        out["matches"].append(m)
        for p in parse_participants(raw, my_puuid=""):
            p.pop("is_me")
            out["participants"].append(p)
        out["frames"] += [f for f in parse_timeline_frames(raw_tl, match_id)
                          if round(f["timestamp_min"]) in FRAME_MARKS]
        out["events"] += [
            {k: v for k, v in e.items() if k not in DROPPED_EVENT_COLS}
            for e in parse_timeline_events(raw_tl, match_id)
            if e["event_type"] in KEPT_EVENTS
        ]
    return out


def build_pipeline():
    return dlt.pipeline(
        pipeline_name="lol_crawl",
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name="lol_crawl",
    )


def make_client() -> RiotClient:
    return RiotClient(api_key=RIOT_API_KEY, platform=RIOT_REGION, routing=RIOT_ROUTING,
                      memoize=False, min_interval=MIN_INTERVAL_S)


def cmd_sample(tiers: list[str], players_per_tier: int, run: str, seed: int | None) -> None:
    client = make_client()
    rng = random.Random(seed)
    seeds = sample_seeds(client, tiers, players_per_tier, run, rng)
    build_pipeline().run([seeds_resource(seeds)])
    logger.info(f"Muestreo '{run}': {len(seeds)} semillas en {len(tiers)} tier(s), "
                f"{client.requests_made} requests")


def cmd_run(budget_minutes: float) -> None:
    client = make_client()
    pipeline = build_pipeline()
    deadline = time.monotonic() + budget_minutes * 60
    t0, matches_done = time.monotonic(), 0

    def out_of_time() -> bool:
        return time.monotonic() >= deadline

    # 1. Listar candidatos hasta completar el cupo de activos de cada división.
    #    Se carga cada SEED_BATCH listados, así un corte pierde poco.
    seed_rows, links = [], []
    active: dict[tuple, int] = {}
    quota: dict[tuple, int] = {}

    def flush():
        if seed_rows:
            pipeline.run([seeds_resource(list(seed_rows)), seed_matches_resource(list(links))])
            seed_rows.clear()
            links.clear()

    candidates = pending_seeds()
    logger.info(f"Candidatos a listar (máx.): {len(candidates)}")
    for s in candidates:
        group = (s["run"], s["tier"], s["division"])
        active.setdefault(group, s["active"])
        quota[group] = s["quota"]
        if active[group] >= s["quota"] or out_of_time():
            continue    # cupo cumplido: el resto de los candidatos queda sin listar
        seed, seed_links = list_seed_matches(client, s)
        seed_rows.append(seed)
        links.extend(seed_links)
        active[group] += seed["n_matches_listed"] > 0
        if len(seed_rows) >= SEED_BATCH:
            flush()
    flush()
    short = {f"{g[1]} {g[2]}": f"{active[g]}/{quota[g]}"
             for g in active if active[g] < quota[g]}
    logger.info(f"Divisiones sin cupo completo: {short or 'ninguna'}")

    # 2. Bajar las partidas pendientes.
    pending = pending_match_ids()
    logger.info(f"Partidas pendientes: {len(pending)}")
    for i in range(0, len(pending), MATCH_BATCH):
        if out_of_time():
            break
        batch = fetch_matches(client, pending[i:i + MATCH_BATCH])
        pipeline.run([
            matches_resource(batch["matches"]),
            participants_resource(batch["participants"]),
            frames_resource(batch["frames"]),
            events_resource(batch["events"]),
            failed_resource(batch["failed"]),
        ])
        matches_done += len(batch["matches"])
        elapsed_h = (time.monotonic() - t0) / 3600
        logger.info(f"Cargadas {matches_done} partidas · {client.requests_made} requests · "
                    f"{client.requests_made / elapsed_h:.0f} req/h")

    left = len(pending_match_ids()) + len(pending_seeds())
    logger.info(f"Fin: {matches_done} partidas en {(time.monotonic() - t0) / 60:.1f} min; "
                f"pendiente para la próxima corrida: {left} (partidas + semillas)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sample", help="muestrear semillas por tier")
    s.add_argument("--tiers", default=",".join(TIERS),
                   help="lista separada por comas (default: los 10)")
    s.add_argument("--players-per-tier", type=int, default=120)
    s.add_argument("--run", default=datetime.now(timezone.utc).strftime("%Y-%m"),
                   help="etiqueta de la muestra (default: mes actual)")
    s.add_argument("--seed", type=int, default=None, help="semilla aleatoria, para reproducir")

    r = sub.add_parser("run", help="listar y bajar lo pendiente")
    r.add_argument("--budget-minutes", type=float, default=360)

    args = parser.parse_args()
    if args.cmd == "sample":
        tiers = [t.strip().upper() for t in args.tiers.split(",")]
        unknown = set(tiers) - set(TIERS)
        if unknown:
            parser.error(f"tiers desconocidos: {', '.join(sorted(unknown))}")
        cmd_sample(tiers, args.players_per_tier, args.run, args.seed)
    else:
        cmd_run(args.budget_minutes)


if __name__ == "__main__":
    main()
