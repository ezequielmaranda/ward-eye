-- mart_player_trends.sql
-- Tendencias históricas del jugador semana a semana.
-- Permite ver si está mejorando en métricas clave a lo largo del tiempo.

with base as (
    select *
    from {{ ref('mart_match_performance') }}
    where queue_id in (420, 440)   -- solo ranked
),

weekly as (
    select
        date_trunc('week', game_date)::date         as week_start,
        queue_id,

        count(*)                                    as games,
        sum(case when win then 1 else 0 end)        as wins,
        round(avg(case when win then 1.0 else 0.0 end) * 100, 1) as winrate_pct,

        -- KDA semanal
        round(avg(kda_ratio), 2)                    as avg_kda,
        round(avg(kills), 1)                        as avg_kills,
        round(avg(deaths), 1)                       as avg_deaths,
        round(avg(assists), 1)                      as avg_assists,

        -- Farm
        round(avg(cs_per_min), 2)                   as avg_cs_per_min,

        -- Visión
        round(avg(vision_per_min), 2)               as avg_vision_per_min,
        round(avg(control_wards_bought), 1)         as avg_control_wards,

        -- Daño
        round(avg(damage_per_min), 0)               as avg_damage_per_min,
        round(avg(damage_share), 2)                 as avg_damage_share,

        -- Oro
        round(avg(gold_per_min), 0)                 as avg_gold_per_min,

        -- Participación
        round(avg(kill_participation), 2)           as avg_kill_participation,

        -- Campeón más jugado esa semana (mode() nativo de DuckDB)
        mode(champion_name) as top_champion,
        mode(team_position) as top_position

    from base
    group by 1, 2
),

-- Variación respecto a la semana anterior
with_lag as (
    select
        *,
        lag(winrate_pct)       over (partition by queue_id order by week_start) as prev_winrate,
        lag(avg_kda)           over (partition by queue_id order by week_start) as prev_kda,
        lag(avg_cs_per_min)    over (partition by queue_id order by week_start) as prev_cs_per_min,
        lag(avg_vision_per_min)over (partition by queue_id order by week_start) as prev_vision_per_min,
        lag(avg_damage_per_min)over (partition by queue_id order by week_start) as prev_damage_per_min
    from weekly
)

select
    week_start,
    queue_id,
    case queue_id when 420 then 'Ranked Solo' else 'Ranked Flex' end as queue_name,
    games,
    wins,
    winrate_pct,
    round(winrate_pct   - coalesce(prev_winrate, winrate_pct), 1)       as winrate_delta,

    avg_kda,
    round(avg_kda       - coalesce(prev_kda, avg_kda), 2)               as kda_delta,

    avg_kills, avg_deaths, avg_assists,

    avg_cs_per_min,
    round(avg_cs_per_min - coalesce(prev_cs_per_min, avg_cs_per_min), 2) as cs_delta,

    avg_vision_per_min,
    round(avg_vision_per_min - coalesce(prev_vision_per_min, avg_vision_per_min), 2) as vision_delta,

    avg_damage_per_min,
    avg_damage_share,
    avg_gold_per_min,
    avg_kill_participation,
    top_champion,
    top_position

from with_lag
order by week_start desc, queue_id
