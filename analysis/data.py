"""
Datasets para el EDA de factores de victoria.

Todo sale de warddata.duckdb en READ_ONLY. Dos granos:

  - team_frame(): una fila por partida desde la perspectiva de MI equipo, con
    diferenciales (mi equipo − rival) acumulados hasta cada minuto de corte.
    Solo usa información disponible *en ese minuto* → sirve para predecir sin
    fuga del resultado.
  - participants(): una fila por jugador por partida (10 × partida), con las
    stats de FIN de partida. Sirve para mostrar el problema de la causalidad
    inversa: estas stats son en buena parte consecuencia de ganar.

El dueño de una torre no viene en el evento BUILDING_KILL: se infiere por
posición. El mapa es un cuadrado de ~14870 con la base azul (team 100) en
(0, 0); toda torre azul cumple x + y < 14870 y toda torre roja lo supera
(validado: outer azul máx 12242, outer rojo mín 17465).
"""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(ROOT / "warddata.duckdb"))
CACHE_DIR = Path(__file__).resolve().parent / ".cache"

MINUTES = (5, 10, 15, 20)
MAP_DIAGONAL = 14870


def _q(sql: str) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Grano partida (perspectiva de mi equipo)
# ---------------------------------------------------------------------------

def team_frame() -> pd.DataFrame:
    minutes_sql = ", ".join(f"({m})" for m in MINUTES)
    return _q(f"""
    with me as (
        select match_id, team_id as my_team, win, champion_name as my_champion,
               participant_id as my_pid, team_position as my_position
        from lol_staging.stg_participants where is_me
    ),
    roster as (   -- participant_id → equipo relativo a mí, y si es mi rival directo
        select p.match_id, p.participant_id, p.team_id = me.my_team as mine,
               p.participant_id = me.my_pid
                 or (p.team_id <> me.my_team and p.team_position = me.my_position) as duel
        from lol_staging.stg_participants p join me using (match_id)
    ),
    cuts(minute) as (values {minutes_sql}),

    -- Recursos al minuto de corte (frame más cercano, ya resuelto en dbt)
    res as (
        select s.match_id, s.minute_mark as minute,
               sum(case when r.mine then s.total_gold else -s.total_gold end)        as gold_diff,
               sum(case when r.mine then s.xp else -s.xp end)                        as xp_diff,
               sum(case when r.mine then s.total_cs_at_frame else -s.total_cs_at_frame end) as cs_diff,
               -- partimos el oro en dos: mi duelo (yo vs mi rival directo) y el
               -- resto (4 vs 4). ¿Cuánto pesa cada parte en el resultado?
               sum(case when not r.duel then 0
                        when r.mine then s.total_gold else -s.total_gold end)        as duel_gold_diff,
               sum(case when r.duel then 0
                        when r.mine then s.total_gold else -s.total_gold end)        as rest_gold_diff
        from lol_intermediate.int_frame_snapshots s
        join roster r using (match_id, participant_id)
        where s.minute_mark in (select minute from cuts)
        group by all
    ),

    ev as (
        select e.match_id, e.timestamp_min, e.event_type, e.monster_type,
               e.position_x + e.position_y as pos_sum,
               coalesce(rk.mine, null) as killer_mine,
               rv.mine as victim_mine
        from lol_staging.stg_timeline_events e
        left join roster rk on rk.match_id = e.match_id and rk.participant_id = e.killer_id
        left join roster rv on rv.match_id = e.match_id and rv.participant_id = e.victim_id
        where e.event_type in ('CHAMPION_KILL', 'BUILDING_KILL', 'ELITE_MONSTER_KILL')
    ),
    ev_side as (  -- +1 si el evento favorece a mi equipo, −1 si al rival
        select ev.match_id, ev.timestamp_min, ev.event_type, ev.monster_type,
               case
                 when ev.event_type = 'CHAMPION_KILL' then case when ev.victim_mine then -1 else 1 end
                 when ev.event_type = 'BUILDING_KILL' then
                    -- torre azul (team 100) cae → punto para rojo, y viceversa
                    case when (ev.pos_sum < {MAP_DIAGONAL}) = (me.my_team = 100) then -1 else 1 end
                 else case when ev.killer_mine then 1 when ev.killer_mine = false then -1 end
               end as side
        from ev join me using (match_id)
    ),
    ev_cut as (
        select c.minute, e.match_id,
               sum(e.side) filter (where e.event_type = 'CHAMPION_KILL')                     as kills_diff,
               sum(e.side) filter (where e.event_type = 'BUILDING_KILL')                     as structures_diff,
               sum(e.side) filter (where e.monster_type = 'DRAGON')                          as dragons_diff,
               sum(e.side) filter (where e.monster_type = 'HORDE')                           as grubs_diff,
               sum(e.side) filter (where e.monster_type = 'RIFTHERALD')                      as herald_diff
        from cuts c join ev_side e on e.timestamp_min <= c.minute
        group by all
    ),
    firsts as (
        select match_id,
               arg_min(side, timestamp_min) filter (where event_type = 'CHAMPION_KILL') = 1 as first_blood_mine,
               arg_min(side, timestamp_min) filter (where event_type = 'BUILDING_KILL') = 1 as first_tower_mine,
               arg_min(side, timestamp_min) filter (where monster_type = 'DRAGON')       = 1 as first_dragon_mine,
               min(timestamp_min) filter (where monster_type = 'DRAGON')                     as first_dragon_min
        from ev_side group by 1
    )
    select me.match_id, me.win::int as win, me.my_team, me.my_champion,
           m.game_duration_min, m.game_date, m.game_version,
           res.minute, res.gold_diff, res.xp_diff, res.cs_diff, res.duel_gold_diff, res.rest_gold_diff,
           coalesce(ev_cut.kills_diff, 0)      as kills_diff,
           coalesce(ev_cut.structures_diff, 0) as structures_diff,
           coalesce(ev_cut.dragons_diff, 0)    as dragons_diff,
           coalesce(ev_cut.grubs_diff, 0)      as grubs_diff,
           coalesce(ev_cut.herald_diff, 0)     as herald_diff,
           firsts.first_blood_mine::int  as first_blood_mine,
           firsts.first_tower_mine::int  as first_tower_mine,
           firsts.first_dragon_mine::int as first_dragon_mine,
           firsts.first_dragon_min
    from me
    join lol_staging.stg_matches m using (match_id)
    join res using (match_id)
    left join ev_cut using (match_id, minute)
    left join firsts using (match_id)
    order by me.match_id, res.minute
    """)


def team_at(minute: int) -> pd.DataFrame:
    """team_frame() filtrado a un minuto de corte (una fila por partida)."""
    df = team_frame()
    return df[df["minute"] == minute].reset_index(drop=True)


def gold_diff_before(minutes_col: str = "first_dragon_min") -> pd.DataFrame:
    """
    Diferencial de oro en el último frame ANTERIOR a un evento (por defecto el
    primer dragón): el estado de la partida justo antes del "tratamiento".
    """
    return _q("""
    with me as (
        select match_id, team_id as my_team from lol_staging.stg_participants where is_me
    ),
    fd as (
        select e.match_id, min(e.timestamp_min) as t
        from lol_staging.stg_timeline_events e
        where e.monster_type = 'DRAGON' group by 1
    ),
    g as (
        select f.match_id, f.timestamp_min,
               sum(case when p.team_id = me.my_team then f.total_gold else -f.total_gold end) as gold_diff
        from lol_staging.stg_timeline_frames f
        join lol_staging.stg_participants p using (match_id, participant_id)
        join me using (match_id)
        group by all
    )
    select fd.match_id, fd.t as first_dragon_min,
           arg_max(g.gold_diff, g.timestamp_min) as gold_diff_before_dragon
    from fd join g on g.match_id = fd.match_id and g.timestamp_min < fd.t
    group by all
    """)


# ---------------------------------------------------------------------------
# Mis métricas de jungla (de los marts)
# ---------------------------------------------------------------------------

def my_jungle() -> pd.DataFrame:
    return _q("""
    select j.match_id, j.win::int as win, j.my_champion, j.enemy_champion,
           j.camp_diff_at_10, j.camp_diff_at_15, j.gold_diff_at_10, j.gold_diff_at_15,
           j.deaths_pre15, j.deaths_solo_pre15, j.my_wards_pre15, j.ward_diff,
           e.cs_at_15, e.early_kills, e.early_deaths,
           p.kill_participation, p.vision_per_min, p.deaths, p.kda_ratio,
           p.cs_per_min, p.gold_per_min, p.damage_share, p.game_duration_min
    from lol_marts.mart_jungle_matchup j
    join lol_marts.mart_early_game e using (match_id)
    join lol_marts.mart_match_performance p using (match_id)
    """)


# ---------------------------------------------------------------------------
# Grano jugador (10 por partida, stats de fin de partida)
# ---------------------------------------------------------------------------

def participants() -> pd.DataFrame:
    return _q("""
    select p.match_id, p.is_me, p.team_id, p.team_position, p.champion_name,
           p.win::int as win, p.kills, p.deaths, p.assists, p.kda_ratio,
           p.cs_per_min, p.gold_per_min, p.damage_per_min, p.vision_per_min,
           p.kill_participation, p.total_damage_taken, p.champ_level,
           p.items_json, m.game_duration_min
    from lol_staging.stg_participants p
    join lol_staging.stg_matches m using (match_id)
    """)


# ---------------------------------------------------------------------------
# Data Dragon: clase de cada campeón (CDN público de Riot, sin API key)
# ---------------------------------------------------------------------------

def champion_tags() -> pd.DataFrame:
    """
    Tags (Tank, Fighter, Mage, Assassin, Marksman, Support) y los ratings
    `attack` / `magic` de Data Dragon. Se cachea en analysis/.cache/.
    """
    CACHE_DIR.mkdir(exist_ok=True)
    cache = CACHE_DIR / "champions.json"
    if not cache.exists():
        with urllib.request.urlopen("https://ddragon.leagueoflegends.com/api/versions.json") as r:
            version = json.load(r)[0]
        url = f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
        with urllib.request.urlopen(url) as r:
            cache.write_bytes(r.read())
    data = json.loads(cache.read_text(encoding="utf-8"))["data"]
    return pd.DataFrame(
        [
            {
                # Coincide con championName de Match-V5 salvo mayúsculas
                # (Fiddlesticks vs FiddleSticks): unir por la versión en minúscula.
                "champion_name": key,
                "champion_lower": key.lower(),
                "primary_tag": c["tags"][0],
                "tags": c["tags"],
                "attack": c["info"]["attack"],
                "magic": c["info"]["magic"],
                "defense": c["info"]["defense"],
            }
            for key, c in data.items()
        ]
    )
