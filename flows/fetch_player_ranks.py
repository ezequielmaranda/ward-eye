"""
Baja tier/rank/LP y W/L de SoloQ de los 10 jugadores de tus ultimas N partidas
y lo guarda en lol_raw.raw_player_ranks dentro del mismo DuckDB.

Uso:
    export RIOT_API_KEY=RGAPI-...
    python fetch_player_ranks.py warddata.duckdb 30
"""
import os
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import duckdb
import requests

load_dotenv(override=True)

DB = sys.argv[1] if len(sys.argv) > 1 else "warddata.duckdb"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 30
PLATFORM = "la2"
URL = f"https://{PLATFORM}.api.riotgames.com/lol/league/v4/entries/by-puuid/{{puuid}}"
HEADERS = {"X-Riot-Token": os.environ["RIOT_API_KEY"]}

con = duckdb.connect(DB)
con.execute("""
    create table if not exists lol_raw.raw_player_ranks (
        puuid varchar primary key, tier varchar, rank varchar, lp integer,
        wins integer, losses integer, hot_streak boolean, fetched_at timestamptz
    )
""")

puuids = [r[0] for r in con.execute(f"""
    with last as (
        select match_id from lol_raw.raw_matches
        where game_duration_s > 300
        order by game_start_ts desc limit {N}
    )
    select distinct p.puuid
    from lol_raw.raw_participants p join last using (match_id)
    where p.puuid not in (select puuid from lol_raw.raw_player_ranks)
""").fetchall()]
print(f"{len(puuids)} jugadores a consultar")


def fetch(puuid: str):
    while True:
        r = requests.get(URL.format(puuid=puuid), headers=HEADERS, timeout=15)
        if r.status_code == 429:  # rate limit: esperar lo que pide Riot
            time.sleep(int(r.headers.get("Retry-After", 10)) + 1)
            continue
        r.raise_for_status()
        return next((e for e in r.json() if e["queueType"] == "RANKED_SOLO_5x5"), None)


for i, puuid in enumerate(puuids, 1):
    e = fetch(puuid) or {}
    con.execute(
        "insert or replace into lol_raw.raw_player_ranks values (?,?,?,?,?,?,?,?)",
        [puuid, e.get("tier"), e.get("rank"), e.get("leaguePoints"), e.get("wins"),
         e.get("losses"), e.get("hotStreak"), datetime.now(timezone.utc)],
    )
    if i % 25 == 0:
        print(f"{i}/{len(puuids)}")
    time.sleep(1.3)  # dev key: 100 requests cada 2 minutos

# Resumen: winrate promedio de tus 4 companeros vs los 5 rivales, por partida
print(con.execute(f"""
    with last as (
        select match_id, game_start_ts from lol_raw.raw_matches
        where game_duration_s > 300 order by game_start_ts desc limit {N}
    ), me as (
        select match_id, team_id, win, champion_name from lol_raw.raw_participants where is_me
    )
    select to_timestamp(l.game_start_ts / 1000)::date as fecha, me.champion_name as champ, me.win,
        round(100 * avg(r.wins / nullif(r.wins + r.losses, 0))
              filter (where p.team_id = me.team_id and not p.is_me), 1) as wr_companeros,
        round(100 * avg(r.wins / nullif(r.wins + r.losses, 0))
              filter (where p.team_id <> me.team_id), 1) as wr_rivales,
        count(*) filter (where r.wins is null) as sin_dato
    from last l
    join me using (match_id)
    join lol_raw.raw_participants p using (match_id)
    left join lol_raw.raw_player_ranks r on r.puuid = p.puuid
    group by l.match_id, l.game_start_ts, me.champion_name, me.win
    order by l.game_start_ts desc
""").df().to_string(index=False))
