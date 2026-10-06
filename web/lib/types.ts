/**
 * Tipos de las filas de los marts (`lol_marts.*`).
 * Solo incluyen las columnas que consume el dashboard.
 * Los numéricos llegan como `number` salvo los DECIMAL/NUMERIC, que el driver
 * de DuckDB devuelve como string: por eso las queries los castean con
 * `::double` (NO `::float` — en DuckDB `FLOAT` es alias de `REAL`, 32 bits,
 * y pierde precisión al volver a un `number` de JS de 64 bits).
 */

export type MatchPerformance = {
  match_id: string;
  game_date: string; // YYYY-MM-DD
  game_start_at: string; // ISO
  queue_name: string | null;
  champion_name: string;
  team_position: string | null;
  win: boolean;
  game_duration_min: number;
  kills: number;
  deaths: number;
  assists: number;
  kda_ratio: number;
  total_cs: number;
  cs_per_min: number;
  vision_score: number;
  vision_per_min: number;
  damage_per_min: number;
  damage_share: number; // 0..1
  kill_participation: number; // 0..1
  gold_per_min: number;
  gold_share: number; // 0..1
  control_wards_bought: number;
  total_damage_taken: number;
  solo_kills: number;
  dragon_kills: number; // objetivos asegurados por el jugador
  baron_kills: number;
  turret_kills: number;
  first_blood_kill: boolean;
  first_tower_kill: boolean;
};

export type ChampionStat = {
  champion_name: string;
  games_played: number;
  wins: number;
  losses: number;
  winrate_pct: number;
  main_position: string | null;
  avg_kda: number;
  avg_kills: number;
  avg_deaths: number;
  avg_assists: number;
  avg_cs_per_min: number;
  avg_vision_per_min: number;
  avg_damage_per_min: number;
  avg_gold_per_min: number;
  recent_winrate_pct: number | null;
  winrate_trend: number | null;
  kda_trend: number | null;
};

export type EarlyGame = {
  match_id: string;
  champion_name: string;
  team_position: string | null;
  win: boolean;
  game_start_at: string | null;
  cs_at_10: number | null;
  gold_at_10: number | null;
  gold_at_15: number | null;
  cs_per_min_at_10: number | null;
  cs_per_min_at_15: number | null;
  early_kills: number;
  early_deaths: number;
  early_wards: number;
};

/**
 * Una partida de jungla contra el jungla rival. Es el único mart que compara
 * contra un oponente: el resto sólo tiene al jugador, así que las métricas
 * absolutas ahí no dicen si el déficit existe.
 * Los `*_diff_at_*` son null si la partida no llegó al hito.
 */
export type JungleMatchup = {
  match_id: string;
  my_champion: string;
  enemy_champion: string;
  win: boolean;
  game_start_at: string | null;
  camp_diff_at_10: number | null;
  camp_diff_at_15: number | null;
  gold_diff_at_10: number | null;
  gold_diff_at_15: number | null;
  my_wards: number;
  enemy_jungler_wards: number;
  ward_diff: number;
  deaths_total: number;
  deaths_solo: number;
  deaths_solo_pre15: number;
  deaths_solo_vs_jungler: number;
  had_early_solo_death: boolean;
};

/**
 * Contexto de rango de una partida (`mart_match_rank_context`). Los `*_score`
 * son rank_score lineal (400/tier, 100/división, + LP); los `*_gap` positivos
 * significan que el rival / equipo rival / lobby era más alto que yo.
 * Solo existe para partidas ingestadas con snapshot fresco.
 */
export type RankContext = {
  match_id: string;
  win: boolean;
  game_start_at: string | null;
  my_champion: string;
  team_position: string | null;
  my_tier: string | null;
  my_division: string | null;
  my_lp: number | null;
  my_rank_score: number | null;
  opponent_champion: string | null;
  opponent_tier: string | null;
  opponent_division: string | null;
  opponent_lp: number | null;
  opponent_rank_gap: number | null;
  opponent_is_smurf_suspect: boolean;
  ally_avg_score: number | null;
  enemy_avg_score: number | null;
  team_rank_gap: number | null;
  ally_smurf_suspects: number;
  enemy_smurf_suspects: number;
  lobby_rank_gap: number | null;
  lobby_difficulty: "harder" | "even" | "easier" | null;
};

/** Objetivos neutrales por bando. `first_*_mine` es null si nunca cayó. */
export type ObjectiveControl = {
  match_id: string;
  win: boolean;
  game_start_at: string | null;
  first_dragon_mine: boolean | null;
  first_dragon_min: number | null;
  first_herald_mine: boolean | null;
  first_baron_mine: boolean | null;
  dragons_mine: number;
  dragons_enemy: number;
  grubs_mine: number;
  grubs_enemy: number;
  dragon_diff: number;
  grub_diff: number;
};

export type PlayerTrend = {
  week_start: string; // YYYY-MM-DD
  queue_id: number;
  queue_name: string;
  games: number;
  wins: number;
  winrate_pct: number;
  avg_kda: number;
  avg_cs_per_min: number;
  avg_vision_per_min: number;
  avg_damage_per_min: number;
};

export type Summary = {
  total_games: number;
  wins: number;
  winrate_pct: number;
  avg_kda: number;
  avg_cs_per_min: number;
  avg_vision_per_min: number;
  avg_damage_per_min: number;
};

/** Opción del picker de campeón: nombre + volumen para ordenar/mostrar. */
export type ChampionOption = {
  champion_name: string;
  games_played: number;
};

export type CoachMeta = {
  player: string | null; // "Nombre#tag"
  region: string | null;
  mainRole: string | null;
  champion: string | null; // campeón filtrado, si el picker tiene uno activo
  totalGames: number | null;
  analyzedGames: number;
  dateFrom: string | null; // YYYY-MM-DD
  dateTo: string | null; // YYYY-MM-DD
  winratePct: number | null;
};

export type DashboardData = {
  summary: Summary | null;
  matches: MatchPerformance[];
  champions: ChampionStat[];
  earlyGame: EarlyGame[];
  jungleMatchup: JungleMatchup[];
  objectives: ObjectiveControl[];
  trends: PlayerTrend[];
  championList: ChampionOption[];
  error: string | null;
};
