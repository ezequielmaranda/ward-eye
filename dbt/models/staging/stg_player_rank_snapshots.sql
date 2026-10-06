-- stg_player_rank_snapshots.sql
-- Rango de SoloQ de cada jugador de la partida, corregido a "antes de jugarla".
--
-- La API solo da el rango ACTUAL: el snapshot de dlt se toma al ingestar, o sea
-- después de la partida (y de cualquier otra SoloQ posterior ya incluida en el
-- mismo snapshot). Corregimos lo que se puede:
--   - wins/losses: restamos esta partida y las SoloQ posteriores del mismo
--     jugador que comparten snapshot (mismo puuid + fetched_at). Todas las
--     partidas de una misma corrida de dlt comparten el snapshot de cada
--     jugador (un request por jugador por corrida).
--   - tier/rank/lp: NO se pueden retroceder (el LP depende de la ganancia de
--     cada partida). Quedan como el rango post-snapshot; usalos como
--     aproximación y filtrá por `is_fresh` / `lag_hours`.
-- Solo partidas con `snapshot_is_reliable`: jugadores sin rango (tier null) o
-- snapshots viejos dan wins/losses_before poco confiables.

with snaps as (
    select * from {{ source('lol_raw', 'raw_player_rank_snapshots') }}
),

-- Inner join con stg_matches/stg_participants: hereda el filtro de remakes y
-- trae el resultado (win) y la cola de cada partida.
joined as (
    select
        s.match_id,
        s.puuid,
        s.tier,
        s.rank,
        s.lp,
        s.wins,
        s.losses,
        s.hot_streak,
        s.fresh_blood,
        s.veteran,
        s.inactive,
        s.match_end_at,
        s.fetched_at,
        s.lag_hours,
        m.queue_id,
        p.win,
        p.is_me
    from snaps s
    inner join {{ ref('stg_matches') }} m using (match_id)
    inner join {{ ref('stg_participants') }} p using (match_id, puuid)
),

corrected as (
    select
        *,
        -- Resultado de esta partida y de las SoloQ posteriores incluidas en el
        -- mismo snapshot (ventana: fila actual hasta la última, por jugador).
        sum(case when queue_id = 420 and win     then 1 else 0 end) over w as wins_to_remove,
        sum(case when queue_id = 420 and not win then 1 else 0 end) over w as losses_to_remove
    from joined
    window w as (
        partition by puuid, fetched_at
        order by match_end_at
        rows between current row and unbounded following
    )
)

select
    match_id,
    puuid,
    is_me,
    win,

    -- Rango (post-snapshot, no retrocedible)
    tier,
    rank,
    lp,
    tier is not null                                    as is_ranked,
    hot_streak,
    fresh_blood,
    veteran,
    inactive,

    -- Récord de SoloQ antes de esta partida
    wins                                                as wins_snapshot,
    losses                                              as losses_snapshot,
    wins   - wins_to_remove                             as wins_before,
    losses - losses_to_remove                           as losses_before,
    (wins + losses) - (wins_to_remove + losses_to_remove) as games_before,
    round(
        (wins - wins_to_remove)::numeric
        / nullif((wins + losses) - (wins_to_remove + losses_to_remove), 0),
    3)                                                  as winrate_before,

    -- Calidad del snapshot
    match_end_at,
    fetched_at,
    lag_hours,
    lag_hours <= 36                                     as is_fresh,
    tier is not null and lag_hours <= 36                as snapshot_is_reliable
from corrected
