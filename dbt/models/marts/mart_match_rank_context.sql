-- mart_match_rank_context.sql
-- Una fila por partida con snapshot de rango: qué tan difícil era la partida
-- según el rango de los otros 9 jugadores.
--
-- Por qué existe: un diferencial negativo contra el rival directo no significa
-- lo mismo si el rival era de tu rango o dos divisiones más alto. Este mart da
-- ese contexto para el coaching y (cuando haya muestra) para los KPIs.
--
-- Cobertura: solo partidas ingestadas con snapshot fresco (ver
-- RANK_SNAPSHOT_MAX_LAG_HOURS en dlt_pipeline/pipeline.py). El histórico previo
-- no se puede completar; la tabla crece a medida que se juega.
--
-- rank_score: escala lineal para poder promediar y restar rangos. 400 puntos
-- por tier, 100 por división, + LP (Bronce I 28 LP = 400 + 300 + 28 = 728).
-- Master+ no tiene divisiones: arranca en 2800, que empalma con Diamante I 100 LP.
-- El LP es el del snapshot (post-partida, no retrocedible): ±~25 de ruido.

{{ config(materialized='table') }}

with snaps as (

    select
        s.*,
        p.team_id,
        p.team_position,
        p.champion_name
    from {{ ref('stg_player_rank_snapshots') }} s
    inner join {{ ref('stg_participants') }} p using (match_id, puuid)

),

scored as (

    select
        *,
        case when snapshot_is_reliable then
            (list_position(
                ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'EMERALD',
                 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER'],
                tier
            ) - 1) * 400
            + case
                when tier in ('MASTER', 'GRANDMASTER', 'CHALLENGER') then 0
                else case rank when 'IV' then 0 when 'III' then 100
                               when 'II' then 200 when 'I'   then 300 end
              end
            + lp
        end                                                     as rank_score,
        -- Heurística de cuenta fuera de nivel (smurf o cuenta nueva que sube
        -- rápido): pocas partidas en la season y winrate muy alto. Gruesa a
        -- propósito; sirve como señal de contexto, no como acusación.
        coalesce(
            snapshot_is_reliable
            and games_before between 1 and 59
            and winrate_before >= 0.65,
        false)                                                  as is_smurf_suspect
    from snaps

),

me as (

    select * from scored where is_me

),

-- Rival directo = el del otro equipo en la misma posición (para un jungla, el
-- jungla rival). qualify: team_position no está garantizado único.
opponent as (

    select s.*
    from scored s
    inner join me
        on me.match_id = s.match_id
       and s.team_id != me.team_id
       and s.team_position = me.team_position
    qualify row_number() over (partition by s.match_id order by s.puuid) = 1

),

teams as (

    select
        s.match_id,
        avg(s.rank_score)   filter (where s.team_id  = me.team_id)               as ally_avg_score,
        count(s.rank_score) filter (where s.team_id  = me.team_id)               as ally_ranked_n,
        avg(s.rank_score)   filter (where s.team_id != me.team_id)               as enemy_avg_score,
        count(s.rank_score) filter (where s.team_id != me.team_id)               as enemy_ranked_n,
        avg(s.rank_score)   filter (where not s.is_me)                           as lobby_avg_score,
        count(*) filter (where s.team_id  = me.team_id and not s.is_me and s.is_smurf_suspect)
                                                                                 as ally_smurf_suspects,
        count(*) filter (where s.team_id != me.team_id and s.is_smurf_suspect)   as enemy_smurf_suspects
    from scored s
    inner join me on me.match_id = s.match_id
    group by s.match_id

)

select
    me.match_id,
    me.win,
    me.champion_name                                            as my_champion,
    me.team_position,

    -- yo
    me.tier                                                     as my_tier,
    me.rank                                                     as my_division,
    me.lp                                                       as my_lp,
    me.rank_score                                               as my_rank_score,
    me.wins_before                                              as my_wins_before,
    me.losses_before                                            as my_losses_before,
    me.winrate_before                                           as my_winrate_before,

    -- rival directo (diferencial positivo = el rival era más alto)
    op.champion_name                                            as opponent_champion,
    op.tier                                                     as opponent_tier,
    op.rank                                                     as opponent_division,
    op.lp                                                       as opponent_lp,
    op.rank_score                                               as opponent_rank_score,
    op.rank_score - me.rank_score                               as opponent_rank_gap,
    coalesce(op.is_smurf_suspect, false)                        as opponent_is_smurf_suspect,

    -- equipos (diferencial positivo = el equipo rival era más alto)
    round(t.ally_avg_score, 0)                                  as ally_avg_score,
    round(t.enemy_avg_score, 0)                                 as enemy_avg_score,
    round(t.enemy_avg_score - t.ally_avg_score, 0)              as team_rank_gap,
    t.ally_ranked_n,
    t.enemy_ranked_n,
    t.ally_smurf_suspects,
    t.enemy_smurf_suspects,

    -- lobby completo (sin mí) contra mi rango
    round(t.lobby_avg_score, 0)                                 as lobby_avg_score,
    round(t.lobby_avg_score - me.rank_score, 0)                 as lobby_rank_gap,
    -- ±150 = división y media: menos que eso es ruido de LP y de matchmaking.
    case
        when t.lobby_avg_score is null or me.rank_score is null then null
        when t.lobby_avg_score - me.rank_score >  150 then 'harder'
        when t.lobby_avg_score - me.rank_score < -150 then 'easier'
        else 'even'
    end                                                         as lobby_difficulty

from me
left join opponent op on op.match_id = me.match_id
left join teams    t  on t.match_id  = me.match_id
