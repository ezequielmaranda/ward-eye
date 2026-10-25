-- stg_matches.sql
-- Limpia y enriquece los datos de partidas.
-- Convierte timestamps, calcula duración en minutos, asigna nombre de cola.
-- Descarta remakes (partidas que terminan antes de los 5min por /remake).

with source as (
    select * from {{ source('lol_raw', 'raw_matches') }}
    where game_duration_s >= 300
)

select
    match_id,
    game_version,
    queue_id,
    game_mode,
    game_type,
    platform_id,
    my_puuid,

    -- Duración
    game_duration_s,
    round(game_duration_s / 60.0, 1)                    as game_duration_min,

    -- Timestamps (Riot devuelve ms)
    to_timestamp(game_start_ts / 1000.0)                as game_start_at,
    -- El día calendario depende del huso: explícito para no heredar el TimeZone
    -- de la sesión DuckDB (= el del host, y una Raspberry suele estar en UTC).
    timezone('{{ var("game_timezone") }}', to_timestamp(game_start_ts / 1000.0))::date
                                                        as game_date,
    to_timestamp(game_end_ts   / 1000.0)                as game_end_at,

    -- Nombre legible de la cola
    case queue_id
        when 420 then 'Ranked Solo/Duo'
        when 440 then 'Ranked Flex'
        when 400 then 'Normal Draft'
        when 450 then 'ARAM'
        when 700 then 'Clash'
        else 'Other (' || queue_id::text || ')'
    end                                                 as queue_name

from source
