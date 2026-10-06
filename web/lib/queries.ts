import "server-only";
import { withRun, type Run } from "./db";
import type {
  ChampionOption,
  ChampionStat,
  DashboardData,
  EarlyGame,
  JungleMatchup,
  MatchPerformance,
  ObjectiveControl,
  PlayerTrend,
  RankContext,
  Summary,
} from "./types";

const MARTS = "lol_marts";

/** Normaliza el filtro de campeón: "" / "all" / undefined cuentan como "sin filtro". */
function normalizeChampion(champion?: string | null): string | null {
  const c = champion?.trim();
  return c && c.toLowerCase() !== "all" ? c : null;
}

/** Resumen global (para las KPI cards), opcionalmente acotado a un campeón. */
async function querySummary(
  run: Run,
  champion?: string | null,
): Promise<Summary | null> {
  const where = champion ? "where champion_name = ?" : "";
  const params = champion ? [champion] : [];
  const rows = await run<Summary>(
    `
    select
      count(*)::int                                   as total_games,
      sum(case when win then 1 else 0 end)::int       as wins,
      round(avg(case when win then 1.0 else 0.0 end) * 100, 1)::double as winrate_pct,
      round(avg(kda_ratio), 2)::double                 as avg_kda,
      round(avg(cs_per_min), 2)::double                as avg_cs_per_min,
      round(avg(vision_per_min), 2)::double            as avg_vision_per_min,
      round(avg(damage_per_min), 0)::double            as avg_damage_per_min
    from ${MARTS}.mart_match_performance
    ${where}
  `,
    params,
  );
  const s = rows[0];
  return s && s.total_games > 0 ? s : null;
}

/** Últimas N partidas (mart central), opcionalmente acotadas a un campeón. */
async function queryMatchPerformance(
  run: Run,
  limit = 30,
  champion?: string | null,
): Promise<MatchPerformance[]> {
  const where = champion ? "where champion_name = ?" : "";
  const params = champion ? [champion, limit] : [limit];
  return run<MatchPerformance>(
    `
    select
      match_id,
      game_date::text                       as game_date,
      game_start_at::text                   as game_start_at,
      queue_name,
      champion_name,
      team_position,
      win,
      game_duration_min::double              as game_duration_min,
      kills::int as kills, deaths::int as deaths, assists::int as assists,
      kda_ratio::double                      as kda_ratio,
      total_cs::int                         as total_cs,
      cs_per_min::double                     as cs_per_min,
      vision_score::int                     as vision_score,
      vision_per_min::double                 as vision_per_min,
      damage_per_min::double                 as damage_per_min,
      damage_share::double                   as damage_share,
      kill_participation::double             as kill_participation,
      gold_per_min::double                   as gold_per_min,
      gold_share::double                     as gold_share,
      control_wards_bought::int             as control_wards_bought,
      total_damage_taken::int               as total_damage_taken,
      solo_kills::int                       as solo_kills,
      dragon_kills::int                     as dragon_kills,
      baron_kills::int                      as baron_kills,
      turret_kills::int                     as turret_kills,
      first_blood_kill,
      first_tower_kill
    from ${MARTS}.mart_match_performance
    ${where}
    order by game_start_at desc
    limit ?
  `,
    params,
  );
}

/** Stats agregadas por campeón (solo ranked, según el mart); opcionalmente 1 sola fila. */
async function queryChampionStats(
  run: Run,
  limit = 15,
  champion?: string | null,
): Promise<ChampionStat[]> {
  const where = champion ? "where champion_name = ?" : "";
  const params = champion ? [champion, limit] : [limit];
  return run<ChampionStat>(
    `
    select
      champion_name,
      games_played::int                     as games_played,
      wins::int                             as wins,
      losses::int                           as losses,
      winrate_pct::double                    as winrate_pct,
      main_position,
      avg_kda::double                        as avg_kda,
      avg_kills::double                      as avg_kills,
      avg_deaths::double                     as avg_deaths,
      avg_assists::double                    as avg_assists,
      avg_cs_per_min::double                 as avg_cs_per_min,
      avg_vision_per_min::double             as avg_vision_per_min,
      avg_damage_per_min::double             as avg_damage_per_min,
      avg_gold_per_min::double               as avg_gold_per_min,
      recent_winrate_pct::double             as recent_winrate_pct,
      winrate_trend::double                  as winrate_trend,
      kda_trend::double                      as kda_trend
    from ${MARTS}.mart_champion_stats
    ${where}
    order by games_played desc
    limit ?
  `,
    params,
  );
}

/** Early game de las últimas N partidas, ordenado cronológicamente. */
async function queryEarlyGame(
  run: Run,
  limit = 30,
  champion?: string | null,
): Promise<EarlyGame[]> {
  const where = champion ? "where e.champion_name = ?" : "";
  const params = champion ? [champion, limit] : [limit];
  return run<EarlyGame>(
    `
    select
      e.match_id,
      e.champion_name,
      e.team_position,
      e.win,
      m.game_start_at::text                 as game_start_at,
      e.cs_at_10::double                     as cs_at_10,
      e.gold_at_10::double                   as gold_at_10,
      e.gold_at_15::double                   as gold_at_15,
      e.cs_per_min_at_10::double             as cs_per_min_at_10,
      e.cs_per_min_at_15::double             as cs_per_min_at_15,
      e.early_kills::int                    as early_kills,
      e.early_deaths::int                   as early_deaths,
      e.early_wards::int                    as early_wards
    from ${MARTS}.mart_early_game e
    left join ${MARTS}.mart_match_performance m using (match_id)
    ${where}
    order by m.game_start_at desc nulls last
    limit ?
  `,
    params,
  );
}

/**
 * Tendencias semanales (ranked), de la más vieja a la más nueva.
 * Sin filtro usa el mart precalculado (`mart_player_trends`, agregado sobre
 * todos los campeones). Con un campeón puntual, ese agregado no sirve —
 * se recalcula al vuelo desde `mart_match_performance` con el mismo criterio
 * (solo ranked, agrupado por semana) para que el gráfico de tendencia siga
 * siendo comparable.
 */
async function queryPlayerTrends(
  run: Run,
  champion?: string | null,
): Promise<PlayerTrend[]> {
  if (!champion) {
    return run<PlayerTrend>(`
      select
        week_start::text                      as week_start,
        queue_id::int                         as queue_id,
        queue_name,
        games::int                            as games,
        wins::int                             as wins,
        winrate_pct::double                    as winrate_pct,
        avg_kda::double                        as avg_kda,
        avg_cs_per_min::double                 as avg_cs_per_min,
        avg_vision_per_min::double             as avg_vision_per_min,
        avg_damage_per_min::double             as avg_damage_per_min
      from ${MARTS}.mart_player_trends
      order by week_start asc, queue_id
    `);
  }
  return run<PlayerTrend>(
    `
    with weekly as (
      select
        date_trunc('week', game_start_at)::date::text          as week_start,
        queue_id::int                                          as queue_id,
        count(*)::int                                          as games,
        sum(case when win then 1 else 0 end)::int              as wins,
        round(avg(case when win then 1.0 else 0.0 end) * 100, 1)::double as winrate_pct,
        round(avg(kda_ratio), 2)::double                        as avg_kda,
        round(avg(cs_per_min), 2)::double                       as avg_cs_per_min,
        round(avg(vision_per_min), 2)::double                   as avg_vision_per_min,
        round(avg(damage_per_min), 0)::double                   as avg_damage_per_min
      from ${MARTS}.mart_match_performance
      where queue_id in (420, 440) and champion_name = ?
      group by 1, 2
    )
    select
      week_start,
      queue_id,
      case queue_id when 420 then 'Ranked Solo' else 'Ranked Flex' end as queue_name,
      games, wins, winrate_pct, avg_kda, avg_cs_per_min, avg_vision_per_min, avg_damage_per_min
    from weekly
    order by week_start asc, queue_id
  `,
    [champion],
  );
}

/**
 * Duelo contra el jungla rival, últimas N partidas de jungla.
 * Ordena por `mart_match_performance` porque el mart de matchup no tiene fecha.
 */
async function queryJungleMatchup(
  run: Run,
  limit = 30,
  champion?: string | null,
): Promise<JungleMatchup[]> {
  const where = champion ? "where j.my_champion = ?" : "";
  const params = champion ? [champion, limit] : [limit];
  return run<JungleMatchup>(
    `
    select
      j.match_id,
      j.my_champion,
      j.enemy_champion,
      j.win,
      m.game_start_at::text                 as game_start_at,
      j.camp_diff_at_10::int                as camp_diff_at_10,
      j.camp_diff_at_15::int                as camp_diff_at_15,
      j.gold_diff_at_10::int                as gold_diff_at_10,
      j.gold_diff_at_15::int                as gold_diff_at_15,
      j.my_wards::int                       as my_wards,
      j.enemy_jungler_wards::int            as enemy_jungler_wards,
      j.ward_diff::int                      as ward_diff,
      j.deaths_total::int                   as deaths_total,
      j.deaths_solo::int                    as deaths_solo,
      j.deaths_solo_pre15::int              as deaths_solo_pre15,
      j.deaths_solo_vs_jungler::int         as deaths_solo_vs_jungler,
      j.had_early_solo_death
    from ${MARTS}.mart_jungle_matchup j
    left join ${MARTS}.mart_match_performance m using (match_id)
    ${where}
    order by m.game_start_at desc nulls last
    limit ?
  `,
    params,
  );
}

/** Objetivos neutrales por bando, últimas N partidas. */
async function queryObjectives(
  run: Run,
  limit = 30,
  champion?: string | null,
): Promise<ObjectiveControl[]> {
  const where = champion ? "where m.champion_name = ?" : "";
  const params = champion ? [champion, limit] : [limit];
  return run<ObjectiveControl>(
    `
    select
      o.match_id,
      o.win,
      m.game_start_at::text                 as game_start_at,
      o.first_dragon_mine,
      o.first_dragon_min::double            as first_dragon_min,
      o.first_herald_mine,
      o.first_baron_mine,
      o.dragons_mine::int                   as dragons_mine,
      o.dragons_enemy::int                  as dragons_enemy,
      o.grubs_mine::int                     as grubs_mine,
      o.grubs_enemy::int                    as grubs_enemy,
      o.dragon_diff::int                    as dragon_diff,
      o.grub_diff::int                      as grub_diff
    from ${MARTS}.mart_objective_control o
    left join ${MARTS}.mart_match_performance m using (match_id)
    ${where}
    order by m.game_start_at desc nulls last
    limit ?
  `,
    params,
  );
}

/**
 * Contexto de rango de las últimas N partidas que tienen snapshot. Cobertura
 * parcial: el histórico previo a la ingesta de rangos no existe, así que puede
 * devolver menos filas que las otras queries (o ninguna).
 */
async function queryRankContext(
  run: Run,
  limit = 30,
  champion?: string | null,
): Promise<RankContext[]> {
  const where = champion ? "where r.my_champion = ?" : "";
  const params = champion ? [champion, limit] : [limit];
  return run<RankContext>(
    `
    select
      r.match_id,
      r.win,
      m.game_start_at::text                 as game_start_at,
      r.my_champion,
      r.team_position,
      r.my_tier,
      r.my_division,
      r.my_lp::int                          as my_lp,
      r.my_rank_score::int                  as my_rank_score,
      r.opponent_champion,
      r.opponent_tier,
      r.opponent_division,
      r.opponent_lp::int                    as opponent_lp,
      r.opponent_rank_gap::int              as opponent_rank_gap,
      r.opponent_is_smurf_suspect,
      r.ally_avg_score::int                 as ally_avg_score,
      r.enemy_avg_score::int                as enemy_avg_score,
      r.team_rank_gap::int                  as team_rank_gap,
      r.ally_smurf_suspects::int            as ally_smurf_suspects,
      r.enemy_smurf_suspects::int           as enemy_smurf_suspects,
      r.lobby_rank_gap::int                 as lobby_rank_gap,
      r.lobby_difficulty
    from ${MARTS}.mart_match_rank_context r
    left join ${MARTS}.mart_match_performance m using (match_id)
    ${where}
    order by m.game_start_at desc nulls last
    limit ?
  `,
    params,
  );
}

/** Lista de todos los campeones jugados (todas las colas), para el picker. */
async function queryChampionList(run: Run): Promise<ChampionOption[]> {
  return run<ChampionOption>(`
    select
      champion_name,
      count(*)::int as games_played
    from ${MARTS}.mart_match_performance
    group by champion_name
    order by games_played desc, champion_name asc
  `);
}

/**
 * Carga todo el dashboard en paralelo; degrada con elegancia si la DB falla.
 * `champion` (opcional) acota todas las métricas a un solo campeón; "" / "all"
 * / undefined equivalen a "todos" (ver `normalizeChampion`).
 */
export async function getDashboardData(
  championFilter?: string | null,
): Promise<DashboardData> {
  const champion = normalizeChampion(championFilter);
  try {
    return await withRun(async (run) => {
      const [
        summary,
        matches,
        champions,
        earlyGame,
        jungleMatchup,
        objectives,
        trends,
        championList,
      ] = await Promise.all([
        querySummary(run, champion),
        queryMatchPerformance(run, 30, champion),
        queryChampionStats(run, 15, champion),
        queryEarlyGame(run, 30, champion),
        queryJungleMatchup(run, 30, champion),
        queryObjectives(run, 30, champion),
        queryPlayerTrends(run, champion),
        queryChampionList(run),
      ]);
      return {
        summary,
        matches,
        champions,
        earlyGame,
        jungleMatchup,
        objectives,
        trends,
        championList,
        error: null,
      };
    });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Error desconocido consultando la DB";
    return {
      summary: null,
      matches: [],
      champions: [],
      earlyGame: [],
      jungleMatchup: [],
      objectives: [],
      trends: [],
      championList: [],
      error: message,
    };
  }
}

/** Datos para el endpoint de coaching (límites más chicos que el dashboard). */
export async function getCoachingData(championFilter?: string | null): Promise<{
  summary: Summary | null;
  matches: MatchPerformance[];
  champions: ChampionStat[];
  earlyGame: EarlyGame[];
  jungleMatchup: JungleMatchup[];
  objectives: ObjectiveControl[];
  trends: PlayerTrend[];
  rankContext: RankContext[];
}> {
  const champion = normalizeChampion(championFilter);
  return withRun(async (run) => {
    const [
      summary,
      matches,
      champions,
      earlyGame,
      jungleMatchup,
      objectives,
      trends,
      rankContext,
    ] = await Promise.all([
      querySummary(run, champion),
      queryMatchPerformance(run, 20, champion),
      queryChampionStats(run, 10, champion),
      queryEarlyGame(run, 20, champion),
      queryJungleMatchup(run, 20, champion),
      queryObjectives(run, 20, champion),
      queryPlayerTrends(run, champion),
      // Mismo horizonte que las demás: el contexto es de las partidas analizadas.
      queryRankContext(run, 20, champion),
    ]);
    return {
      summary,
      matches,
      champions,
      earlyGame,
      jungleMatchup,
      objectives,
      trends,
      rankContext,
    };
  });
}
