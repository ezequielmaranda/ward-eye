"""
Pipeline dlt: extrae datos de la Riot API y los carga en un archivo DuckDB local.

Estrategia incremental (cursor manual, única fuente de verdad = la propia tabla):
  - Antes de correr leemos de DuckDB el cursor (max game_start_ts ya cargado) y
    los match_id ya presentes. Con eso calculamos UNA sola lista de partidas
    nuevas que compartimos entre TODOS los recursos (matches, participants,
    timelines) → ninguno re-procesa partidas viejas.
  - get_match_ids se pagina con startTime, así no se pierden partidas aunque se
    hayan jugado más de 100 entre corridas.
  - get_match / get_match_timeline están memoizados en el cliente: cada partida
    se descarga una sola vez aunque la usen varios recursos.
  - merge + primary_key en cada recurso → idempotente ante cualquier re-ejecución.

Cómo correr manualmente:
  python -m dlt_pipeline.pipeline
"""

import logging
import os
import sys
from datetime import datetime, timezone

import dlt
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from extraction.riot_client import RiotClient
from extraction.transformers import (
    parse_match,
    parse_participants,
    parse_summoner,
    parse_timeline_events,
    parse_timeline_frames,
)

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config desde .env
# ---------------------------------------------------------------------------
RIOT_API_KEY = os.environ["RIOT_API_KEY"].strip()
RIOT_REGION = os.environ.get("RIOT_REGION", "la2").strip()
RIOT_ROUTING = os.environ.get("RIOT_ROUTING", "americas").strip()
SUMMONER_NAME = os.environ["SUMMONER_NAME"].strip()
SUMMONER_TAG = os.environ["SUMMONER_TAG"].strip()

# Archivo DuckDB local (un único archivo para todo el proyecto).
# Default: <raíz del proyecto>/warddata.duckdb. Override opcional con DUCKDB_PATH.
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(ROOT_DIR, "warddata.duckdb"))

# Cuántas partidas atrás buscar en la primera ejecución (bootstrap)
BOOTSTRAP_MATCH_COUNT = 50

# Tope de partidas a procesar por corrida. Si hubo un parate largo y hay más
# pendientes, se procesan las más viejas primero y el resto se drena en corridas
# siguientes (sin pérdida). Acota memoria y consumo de la API por corrida.
MAX_MATCHES_PER_RUN = 100

# Partidas más viejas que esto no se snapshotean: el rango de hoy ya no dice
# nada sobre el jugador al momento de la partida (backlog, bootstrap).
RANK_SNAPSHOT_MAX_LAG_HOURS = 36


# ---------------------------------------------------------------------------
# Fuentes dlt
# ---------------------------------------------------------------------------


@dlt.resource(
    name="raw_matches",
    write_disposition="merge",
    primary_key="match_id",
)
def matches_resource(client: RiotClient, puuid: str, match_ids: list[str]):
    """
    Detalle de cada partida nueva. La lista `match_ids` ya viene filtrada
    incrementalmente en run_pipeline (solo partidas aún no cargadas).
    `client.get_match` está memoizado: participants_resource reusa esta misma
    descarga sin volver a pegarle a la API.
    """
    for match_id in match_ids:
        logger.info(f"Procesando {match_id}...")
        raw = client.get_match(match_id)
        yield parse_match(raw, puuid)


@dlt.resource(
    name="raw_participants",
    write_disposition="merge",
    primary_key=["match_id", "puuid"],
    # Declarado explícito para que dlt materialice la columna en el destino
    # aunque una corrida no traiga partidas nuevas. Reemplaza a `cs_per_minute`,
    # que era el mismo dato con un nombre que mentía (ver transformers.py y
    # scripts/migrate_lane_minions_rename.py).
    columns={"lane_minions_first_10min": {"data_type": "bigint", "nullable": True}},
)
def participants_resource(client: RiotClient, puuid: str, match_ids: list[str]):
    for match_id in match_ids:
        raw = client.get_match(match_id)
        yield from parse_participants(raw, puuid)


@dlt.resource(
    name="raw_timeline_frames",
    write_disposition="merge",
    primary_key=["match_id", "participant_id", "timestamp_ms"],
)
def timeline_frames_resource(client: RiotClient, match_ids: list[str]):
    for match_id in match_ids:
        logger.info(f"Timeline frames: {match_id}")
        raw_tl = client.get_match_timeline(match_id)
        yield from parse_timeline_frames(raw_tl, match_id)


@dlt.resource(
    name="raw_timeline_events",
    write_disposition="merge",
    # merge_key, NO primary_key. Un evento del timeline no tiene clave natural:
    # `participant_id` es NULL en CHAMPION_KILL, BUILDING_KILL y
    # OBJECTIVE_BOUNTY_PRESTART, y con eso adentro de una primary_key el merge
    # de dlt fallaba de las dos puntas:
    #   - dentro de una carga deduplica con `partition by`, que agrupa los NULL
    #     como iguales y se come eventos simultáneos distintos;
    #   - entre cargas borra con `=`, y `participant_id = NULL` nunca matchea,
    #     así que las filas viejas sobrevivían y los duplicados se acumulaban
    #     (323 filas de más en 50 partidas antes de la limpieza).
    # Con merge_key el grano es la partida: re-ingestar una borra sus eventos y
    # los reinserta enteros. Idempotente y sin dedup por fila.
    merge_key="match_id",
    columns={"participant_id": {"nullable": True, "data_type": "bigint"}},
)
def timeline_events_resource(client: RiotClient, match_ids: list[str]):
    for match_id in match_ids:
        logger.info(f"Timeline events: {match_id}")
        raw_tl = client.get_match_timeline(match_id)
        yield from parse_timeline_events(raw_tl, match_id)


@dlt.resource(
    name="raw_summoners",
    write_disposition="merge",
    primary_key="puuid",
)
def summoner_resource(client: RiotClient, puuid: str):
    summoner = client.get_summoner_by_puuid(puuid)
    ranked = client.get_ranked_info(puuid)
    yield parse_summoner(summoner, puuid, ranked)


@dlt.resource(
    name="raw_player_rank_snapshots",
    write_disposition="merge",
    primary_key=["match_id", "puuid"],
)
def player_rank_snapshots_resource(client: RiotClient, match_ids: list[str]):
    """
    Rango y W/L de SoloQ de los 10 jugadores de cada partida nueva, tomado al
    ingestarla. La API no da el rango previo a la partida: el snapshot ya
    incluye su resultado (se corrige en dbt restando el win/loss) y `lag_hours`
    dice qué tan viejo es respecto del final de la partida.
    `client.get_match` está memoizado: no vuelve a pegarle a Match-V5.
    """
    cache: dict[str, tuple[dict, datetime]] = {}  # un request por jugador por corrida

    for match_id in match_ids:
        info = client.get_match(match_id)["info"]
        match_end_at = datetime.fromtimestamp(info["gameEndTimestamp"] / 1000, tz=timezone.utc)
        lag_now = (datetime.now(timezone.utc) - match_end_at).total_seconds() / 3600
        if lag_now > RANK_SNAPSHOT_MAX_LAG_HOURS:
            logger.info(f"Rank snapshot: salteo {match_id} (terminó hace {lag_now:.0f}h)")
            continue

        logger.info(f"Rank snapshot: {match_id}")
        for p in info["participants"]:
            puuid = p["puuid"]
            if puuid not in cache:
                entries = client.get_ranked_info(puuid)
                solo = next((e for e in entries if e["queueType"] == "RANKED_SOLO_5x5"), {})
                cache[puuid] = (solo, datetime.now(timezone.utc))
            solo, fetched_at = cache[puuid]
            yield {
                "match_id":     match_id,
                "puuid":        puuid,
                "tier":         solo.get("tier"),
                "rank":         solo.get("rank"),
                "lp":           solo.get("leaguePoints"),
                "wins":         solo.get("wins"),
                "losses":       solo.get("losses"),
                "hot_streak":   solo.get("hotStreak"),
                "fresh_blood":  solo.get("freshBlood"),
                "veteran":      solo.get("veteran"),
                "inactive":     solo.get("inactive"),
                "match_end_at": match_end_at,
                "fetched_at":   fetched_at,
                "lag_hours":    round((fetched_at - match_end_at).total_seconds() / 3600, 2),
            }


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------


def build_destination():
    """Destino dlt: archivo DuckDB local (evita el sleep del free tier de Supabase)."""
    return dlt.destinations.duckdb(DUCKDB_PATH)


def read_incremental_state() -> tuple[int | None, set[str]]:
    """
    Estado incremental leído directo de DuckDB (única fuente de verdad):
      - last_ts:    máximo game_start_ts ya cargado (ms), o None si no hay datos
      - loaded_ids: match_id ya presentes, para no reprocesarlos

    Abre el archivo en READ_ONLY y lo cierra enseguida. DuckDB es single-writer,
    pero esta lectura corre ANTES de que dlt abra el archivo para escribir, así
    que no hay conflicto de lock.
    """
    if not os.path.exists(DUCKDB_PATH):
        return None, set()

    import duckdb

    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        last_ts = conn.execute(
            "SELECT max(game_start_ts) FROM lol_raw.raw_matches"
        ).fetchone()[0]
        loaded_ids = {
            r[0]
            for r in conn.execute(
                "SELECT match_id FROM lol_raw.raw_matches"
            ).fetchall()
        }
        return last_ts, loaded_ids
    except duckdb.CatalogException:
        # raw_matches todavía no existe (primera corrida sobre archivo recién creado)
        return None, set()
    finally:
        conn.close()


def fetch_new_match_ids(
    client: RiotClient, puuid: str, last_ts: int | None
) -> list[str]:
    """
    Match IDs a procesar, ordenados de más viejo a más nuevo.
      - Bootstrap (sin datos previos): las BOOTSTRAP_MATCH_COUNT más recientes.
      - Incremental: pagina con startTime hasta agotar, para no perder partidas
        si se jugaron más de 100 desde la última corrida.
    Riot devuelve de más nuevo a más viejo; invertimos para procesar en orden
    cronológico (el cursor avanza parejo y un backlog se drena sin huecos).
    """
    if last_ts is None:
        ids = client.get_match_ids(puuid=puuid, count=BOOTSTRAP_MATCH_COUNT)
        return list(reversed(ids))

    start_time_s = last_ts // 1000
    page = 100  # máximo permitido por Match-V5
    collected: list[str] = []
    start = 0
    while True:
        batch = client.get_match_ids(
            puuid=puuid, start=start, start_time=start_time_s, count=page
        )
        if not batch:
            break
        collected.extend(batch)
        if len(batch) < page:
            break
        start += page
    return list(reversed(collected))


def run_pipeline():
    client = RiotClient(
        api_key=RIOT_API_KEY,
        platform=RIOT_REGION,
        routing=RIOT_ROUTING,
    )

    # 1. Resolvemos el PUUID del jugador
    logger.info(f"Resolviendo PUUID para {SUMMONER_NAME}#{SUMMONER_TAG}...")
    puuid = client.get_puuid(SUMMONER_NAME, SUMMONER_TAG)
    logger.info(f"PUUID: {puuid}")

    # 2. Estado incremental: leemos cursor + IDs ya cargados desde DuckDB y
    #    calculamos UNA sola lista de partidas nuevas, compartida por todos los
    #    recursos. El diff contra loaded_ids descarta la partida del borde (que
    #    startTime, inclusivo en segundos, vuelve a traer) y cualquier solape.
    last_ts, loaded_ids = read_incremental_state()
    fetched = fetch_new_match_ids(client, puuid, last_ts)
    new_ids = [m for m in fetched if m not in loaded_ids]

    if len(new_ids) > MAX_MATCHES_PER_RUN:
        logger.warning(
            f"{len(new_ids)} partidas nuevas; proceso las {MAX_MATCHES_PER_RUN} "
            f"más viejas esta corrida (el resto en la próxima, sin pérdida)."
        )
        new_ids = new_ids[:MAX_MATCHES_PER_RUN]

    logger.info(f"Cursor={last_ts} — partidas nuevas a procesar: {len(new_ids)}")

    # 3. Construimos el pipeline dlt
    pipeline = dlt.pipeline(
        pipeline_name="lol_coach",
        destination=build_destination(),
        dataset_name="lol_raw",  # schema raw en DuckDB
        progress="log",
    )

    # 4. Cargamos todos los recursos en una sola ejecución. get_match y
    #    get_match_timeline están memoizados en el cliente, así que cada partida
    #    se descarga una sola vez aunque la consuman varios recursos.
    #    Si new_ids está vacío (día sin partidas), solo se refresca el summoner.
    load_info = pipeline.run(
        [
            matches_resource(client, puuid, new_ids),
            participants_resource(client, puuid, new_ids),
            timeline_frames_resource(client, new_ids),
            timeline_events_resource(client, new_ids),
            summoner_resource(client, puuid),
            player_rank_snapshots_resource(client, new_ids),
        ]
    )

    logger.info(f"Pipeline completado:\n{load_info}")
    return load_info


if __name__ == "__main__":
    run_pipeline()
