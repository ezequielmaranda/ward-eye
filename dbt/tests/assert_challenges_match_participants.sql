-- Centinela de la extracción del JSON: control_wards_bought de
-- stg_participants sale de la misma clave (challenges.controlWardsPlaced) por
-- otro camino (parseada en Python al ingestar). Si difieren, alguno de los dos
-- lee mal el objeto.

select p.match_id, p.puuid, p.control_wards_bought, c.control_wards_placed
from {{ ref('stg_participants') }} p
join {{ ref('stg_challenges') }} c using (match_id, puuid)
where p.control_wards_bought is distinct from c.control_wards_placed
