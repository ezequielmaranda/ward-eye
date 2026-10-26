-- stg_challenges tiene el mismo grano que stg_participants: una fila por
-- (match_id, puuid), sin duplicados y sin huecos. Un participante sin fila
-- de challenges (challenges_json vacío o NULL en raw) es un backfill que no
-- corrió: ver dlt_pipeline/backfill_participants.py.

with c as (
    select match_id, puuid, count(*) as filas
    from {{ ref('stg_challenges') }}
    group by 1, 2
)

select p.match_id, p.puuid, coalesce(c.filas, 0) as filas
from {{ ref('stg_participants') }} p
left join c using (match_id, puuid)
where coalesce(c.filas, 0) <> 1
