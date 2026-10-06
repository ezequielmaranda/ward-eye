import "server-only";
import OpenAI from "openai";
import type { Provider } from "./providers";
import type {
  ChampionStat,
  EarlyGame,
  JungleMatchup,
  MatchPerformance,
  ObjectiveControl,
  PlayerTrend,
  Summary,
} from "./types";

/**
 * Coaching contra un endpoint OpenAI-compatible. Cada proveedor tiene su
 * propio set de env vars (base URL y API key difieren, no solo el modelo):
 *   LLM_GROQ_BASE_URL   / LLM_GROQ_API_KEY   / LLM_GROQ_MODEL   / LLM_GROQ_REASONING_EFFORT
 *   LLM_GEMINI_BASE_URL / LLM_GEMINI_API_KEY / LLM_GEMINI_MODEL / LLM_GEMINI_REASONING_EFFORT
 * El proveedor a usar lo elige el usuario en el selector de `CoachingPanel`.
 */

// reasoning_effort: solo lo usan los modelos de razonamiento (gpt-oss, qwen3,
// deepseek-r1, y Gemini vía su capa OpenAI-compatible). Valores: low | medium | high.
// En modelos no-reasoning conviene dejarlo vacío.
type ReasoningEffort = "low" | "medium" | "high";

function parseReasoningEffort(v: string | undefined): ReasoningEffort | undefined {
  const s = v?.toLowerCase();
  return s === "low" || s === "medium" || s === "high" ? s : undefined;
}

type ProviderConfig = {
  baseUrl: string;
  apiKey: string | undefined;
  apiKeyEnvVar: string;
  model: string;
  reasoningEffort: ReasoningEffort | undefined;
};

function getProviderConfig(provider: Provider): ProviderConfig {
  if (provider === "gemini") {
    return {
      baseUrl:
        process.env.LLM_GEMINI_BASE_URL ??
        "https://generativelanguage.googleapis.com/v1beta/openai/",
      apiKey: process.env.LLM_GEMINI_API_KEY,
      apiKeyEnvVar: "LLM_GEMINI_API_KEY",
      model: process.env.LLM_GEMINI_MODEL ?? "gemini-3.8-flash",
      reasoningEffort: parseReasoningEffort(process.env.LLM_GEMINI_REASONING_EFFORT),
    };
  }
  return {
    baseUrl: process.env.LLM_GROQ_BASE_URL ?? "https://api.groq.com/openai/v1",
    apiKey: process.env.LLM_GROQ_API_KEY,
    apiKeyEnvVar: "LLM_GROQ_API_KEY",
    model: process.env.LLM_GROQ_MODEL ?? "openai/gpt-oss-120b",
    reasoningEffort: parseReasoningEffort(process.env.LLM_GROQ_REASONING_EFFORT),
  };
}

/** Modelo efectivo de un proveedor (env var si está seteada, si no el default). Usado por el selector del dashboard para que el label nunca se desincronice del modelo que realmente se llama. */
export function getProviderModel(provider: Provider): string {
  return getProviderConfig(provider).model;
}

export type CoachInput = {
  summary: Summary | null;
  matches: MatchPerformance[];
  champions: ChampionStat[];
  earlyGame: EarlyGame[];
  jungleMatchup: JungleMatchup[];
  objectives: ObjectiveControl[];
  trends: PlayerTrend[];
  champion?: string | null; // filtro de campeón activo, si lo hay
};

export type CoachResult = {
  report: string;
  model: string;
};

function avg(nums: number[]): number {
  const v = nums.filter((n) => Number.isFinite(n));
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : 0;
}

function mode<T>(items: T[]): T | undefined {
  const counts = new Map<T, number>();
  for (const it of items) counts.set(it, (counts.get(it) ?? 0) + 1);
  let best: T | undefined;
  let bestN = -1;
  for (const [k, n] of counts) if (n > bestN) [best, bestN] = [k, n];
  return best;
}

/** Agrega una línea en blanco después del bloque, salvo que esté vacío. */
function withBlank(block: string[]): string[] {
  return block.length ? [...block, ""] : [];
}

/** Se queda con la cola (queue) con más partidas para no duplicar semanas. */
function dominantQueue(trends: PlayerTrend[]): PlayerTrend[] {
  if (trends.length === 0) return [];
  const games = new Map<number, number>();
  for (const t of trends) games.set(t.queue_id, (games.get(t.queue_id) ?? 0) + t.games);
  const best = [...games.entries()].sort((a, b) => b[1] - a[1])[0][0];
  return trends.filter((t) => t.queue_id === best);
}

function roleHeader(matches: MatchPerformance[]): string {
  const role = mode(matches.map((m) => m.team_position ?? "UNKNOWN")) ?? "UNKNOWN";
  const n = matches.filter((m) => (m.team_position ?? "UNKNOWN") === role).length;
  const pct = matches.length ? Math.round((n / matches.length) * 100) : 0;
  return `Rol principal: ${role} (${pct}% de las partidas recientes). Calibrá los benchmarks a este rol.`;
}

/** El bloque más accionable: qué separa las victorias de las derrotas. */
function winLossBlock(matches: MatchPerformance[], earlyGame: EarlyGame[]): string[] {
  const w = matches.filter((m) => m.win);
  const l = matches.filter((m) => !m.win);
  const egW = earlyGame.filter((e) => e.win);
  const egL = earlyGame.filter((e) => !e.win);
  const row = (label: string, vw: number, vl: number, d = 1) =>
    `- ${label}: Victorias ${vw.toFixed(d)} | Derrotas ${vl.toFixed(d)}`;

  return [
    `Comparación Victorias vs Derrotas (n: ${w.length}V / ${l.length}D):`,
    row("KDA", avg(w.map((m) => m.kda_ratio)), avg(l.map((m) => m.kda_ratio)), 2),
    row("CS/min", avg(w.map((m) => m.cs_per_min)), avg(l.map((m) => m.cs_per_min)), 2),
    row("Visión/min", avg(w.map((m) => m.vision_per_min)), avg(l.map((m) => m.vision_per_min)), 2),
    row("Control wards", avg(w.map((m) => m.control_wards_bought)), avg(l.map((m) => m.control_wards_bought))),
    row("KP %", avg(w.map((m) => m.kill_participation * 100)), avg(l.map((m) => m.kill_participation * 100)), 0),
    row("Dragones propios", avg(w.map((m) => m.dragon_kills)), avg(l.map((m) => m.dragon_kills))),
    row("Muertes ≤15m", avg(egW.map((e) => e.early_deaths)), avg(egL.map((e) => e.early_deaths))),
    row("Kills ≤15m", avg(egW.map((e) => e.early_kills)), avg(egL.map((e) => e.early_kills))),
    row("Duración (min)", avg(w.map((m) => m.game_duration_min)), avg(l.map((m) => m.game_duration_min)), 0),
  ];
}

function earlyGameBlock(earlyGame: EarlyGame[]): string[] {
  if (earlyGame.length === 0) return ["Early game: sin datos."];
  return [
    "Early game (promedios):",
    `- CS/min @10: ${avg(earlyGame.map((e) => e.cs_per_min_at_10 ?? NaN)).toFixed(2)}`,
    `- CS/min @15: ${avg(earlyGame.map((e) => e.cs_per_min_at_15 ?? NaN)).toFixed(2)}`,
    `- Oro @10: ${avg(earlyGame.map((e) => e.gold_at_10 ?? NaN)).toFixed(0)}`,
    `- Oro @15: ${avg(earlyGame.map((e) => e.gold_at_15 ?? NaN)).toFixed(0)}`,
    `- Wards ≤15m: ${avg(earlyGame.map((e) => e.early_wards)).toFixed(1)}`,
  ];
}

/**
 * Duelo contra el jungla rival. Es el bloque con más señal para un jungla: las
 * métricas absolutas pueden verse normales mientras el diferencial contra el
 * rival de esa misma partida es malo.
 */
function jungleMatchupBlock(rows: JungleMatchup[]): string[] {
  if (rows.length === 0) return [];
  const w = rows.filter((r) => r.win);
  const l = rows.filter((r) => !r.win);
  const cmp = (label: string, pick: (r: JungleMatchup) => number, d = 1) =>
    `- ${label}: Victorias ${avg(w.map(pick)).toFixed(d)} | Derrotas ${avg(l.map(pick)).toFixed(d)}`;
  const solo = rows.filter((r) => r.had_early_solo_death);
  const soloWr = solo.length
    ? Math.round((solo.filter((r) => r.win).length / solo.length) * 100)
    : null;

  return [
    `Duelo vs el jungla rival (n: ${rows.length}; valores negativos = vas por detrás):`,
    `- Campamentos @10: ${avg(rows.map((r) => r.camp_diff_at_10 ?? NaN)).toFixed(1)} | @15: ${avg(rows.map((r) => r.camp_diff_at_15 ?? NaN)).toFixed(1)}`,
    `- Oro @10: ${avg(rows.map((r) => r.gold_diff_at_10 ?? NaN)).toFixed(0)} | @15: ${avg(rows.map((r) => r.gold_diff_at_15 ?? NaN)).toFixed(0)}`,
    `- Wards colocadas: vos ${avg(rows.map((r) => r.my_wards)).toFixed(1)} | jungla rival ${avg(rows.map((r) => r.enemy_jungler_wards)).toFixed(1)}`,
    `- Muertes en duelo (sin asistencias del rival): ${avg(rows.map((r) => r.deaths_solo)).toFixed(1)} de ${avg(rows.map((r) => r.deaths_total)).toFixed(1)} totales; ${avg(rows.map((r) => r.deaths_solo_vs_jungler)).toFixed(1)} de ellas contra el jungla rival`,
    cmp("Campamentos @15 (V vs D)", (r) => r.camp_diff_at_15 ?? NaN),
    cmp("Oro @15 (V vs D)", (r) => r.gold_diff_at_15 ?? NaN, 0),
    ...(soloWr !== null
      ? [
          `- Partidas con al menos una muerte en duelo antes del 15: ${solo.length}/${rows.length}, winrate ${soloWr}%`,
        ]
      : []),
  ];
}

/** Objetivos neutrales por bando: el corte con más señal del dataset. */
function objectivesBlock(rows: ObjectiveControl[]): string[] {
  if (rows.length === 0) return [];
  const wr = (subset: ObjectiveControl[]) =>
    subset.length
      ? `${Math.round((subset.filter((r) => r.win).length / subset.length) * 100)}% (${subset.length}g)`
      : "sin datos";
  return [
    "Control de objetivos (winrate según quién se lo llevó):",
    `- Primer dragón a favor: ${wr(rows.filter((r) => r.first_dragon_mine === true))} | en contra: ${wr(rows.filter((r) => r.first_dragon_mine === false))}`,
    `- Primer heraldo a favor: ${wr(rows.filter((r) => r.first_herald_mine === true))} | en contra: ${wr(rows.filter((r) => r.first_herald_mine === false))}`,
    `- Dragones por partida: vos ${avg(rows.map((r) => r.dragons_mine)).toFixed(1)} | rival ${avg(rows.map((r) => r.dragons_enemy)).toFixed(1)}`,
    `- Larvas del vacío (grubs): vos ${avg(rows.map((r) => r.grubs_mine)).toFixed(1)} | rival ${avg(rows.map((r) => r.grubs_enemy)).toFixed(1)}`,
  ];
}

function trendsBlock(trends: PlayerTrend[]): string[] {
  const rows = dominantQueue(trends).slice(-6); // últimas 6 semanas
  if (rows.length === 0) return ["Tendencia semanal: sin datos."];
  return [
    "Tendencia semanal (ranked, de más vieja a más nueva):",
    ...rows.map(
      (t) =>
        `- ${t.week_start}: WR ${t.winrate_pct}% (${t.games}g) | KDA ${t.avg_kda} | CS/min ${t.avg_cs_per_min} | Vis/min ${t.avg_vision_per_min}`,
    ),
  ];
}

function matchLines(matches: MatchPerformance[]): string[] {
  return matches.slice(0, 12).map((m) =>
    [
      `${m.game_date} ${m.win ? "V" : "D"} ${m.champion_name}(${m.team_position ?? "?"})`,
      `KDA ${m.kills}/${m.deaths}/${m.assists}(${m.kda_ratio})`,
      `CS/min ${m.cs_per_min}`,
      `Vis/min ${m.vision_per_min}`,
      `KP ${Math.round(m.kill_participation * 100)}%`,
      `CW ${m.control_wards_bought}`,
      `Drag ${m.dragon_kills}`,
      `FirstTower ${m.first_tower_kill ? "sí" : "no"}`,
      `${Math.round(m.game_duration_min)}min`,
    ].join(" | "),
  );
}

function champsBlock(champions: ChampionStat[]): string[] {
  return [
    "Campeones (histórico ranked):",
    ...champions.slice(0, 8).map((c) =>
      [
        `${c.champion_name}(${c.main_position ?? "?"})`,
        `${c.games_played}g`,
        `WR ${c.winrate_pct}%`,
        `KDA ${c.avg_kda}`,
        `CS/min ${c.avg_cs_per_min}`,
        `Vis/min ${c.avg_vision_per_min}`,
      ].join(" | "),
    ),
  ];
}

function buildPrompt({
  summary,
  matches,
  champions,
  earlyGame,
  jungleMatchup,
  objectives,
  trends,
  champion,
}: CoachInput): string {
  const summaryLine = summary
    ? `Global: ${summary.total_games} partidas, WR ${summary.winrate_pct}%, KDA ${summary.avg_kda}, CS/min ${summary.avg_cs_per_min}, Vis/min ${summary.avg_vision_per_min}, Dmg/min ${Math.round(summary.avg_damage_per_min)}`
    : "Sin resumen global disponible.";
  const filterLine = champion
    ? `Filtro activo: SOLO partidas con ${champion}. Todo el análisis (resumen, fortalezas, áreas de mejora, objetivos) debe ser específico de este campeón.`
    : null;

  return [
    roleHeader(matches),
    ...(filterLine ? [filterLine] : []),
    summaryLine,
    "",
    ...winLossBlock(matches, earlyGame),
    "",
    ...earlyGameBlock(earlyGame),
    "",
    ...withBlank(jungleMatchupBlock(jungleMatchup)),
    ...withBlank(objectivesBlock(objectives)),
    ...trendsBlock(trends),
    "",
    "Partidas recientes (más nuevas primero):",
    ...matchLines(matches),
    "",
    ...champsBlock(champions),
  ].join("\n");
}

const SYSTEM_PROMPT = `Sos un coach de League of Legends especializado en analizar datos de partidas de un jugador.
Hablás en español rioplatense, directo y concreto, sin relleno.
Basate SOLO en las métricas provistas; no inventes datos que no estén.

El jugador tiene un ROL PRINCIPAL indicado en los datos. Calibrá TODO el feedback a ese rol: los benchmarks cambian mucho según la posición.

Referencias soloQ aproximadas por rol:
- JUNGLE: CS/min ~5-6.5 (incluye campamentos), Vis/min ~0.9-1.3, KP alto >55-65% (rol de ganks). Son KPIs clave el control de objetivos (dragón/heraldo/barón), conseguir el first tower y mantener bajas las muertes tempranas (≤15m). Comparar tu tempo de oro/CS @10-@15 importa más que el CS total.
- MID/TOP/ADC (carriles): CS/min >7 es bueno, Vis/min >0.8, KP variable según campeón.
- SUPPORT: CS/min bajo es normal (~1-2), Vis/min >2 muy bueno, control wards alto.

Usá la comparación Victorias vs Derrotas para identificar QUÉ separa tus wins de tus losses: esa es la señal más accionable. Si una métrica es parecida en V y D, no es el problema.

Si hay un bloque "Duelo vs el jungla rival", PRIORIZALO sobre las métricas absolutas. Un CS/min que parece normal puede esconder un déficit grande contra el jungla rival de esa misma partida, y lo que se puede corregir es el diferencial, no el número aislado. Los valores negativos significan que el jugador va por detrás. Distinguí las muertes en duelo (sin asistencias: decisiones propias de pelear) de las muertes en gank (posicionamiento y visión) — se corrigen distinto.

Si hay un bloque "Control de objetivos", usalo para hablar de prioridades de ruta y tempo, no como una estadística suelta.

Si calculás vos algún promedio o ratio a partir de los datos, redondealo SIEMPRE a como máximo 2 decimales antes de citarlo.

Devolvé el análisis en Markdown con EXACTAMENTE estas secciones:
## Resumen
## Puntos fuertes
## Áreas de mejora
## Objetivos próxima sesión
Cada sección con 2-4 bullets accionables. Sé específico citando métricas concretas y, cuando puedas, partidas o campeones puntuales.`;

export async function generateCoaching(
  input: CoachInput,
  provider: Provider = "groq",
): Promise<CoachResult> {
  const config = getProviderConfig(provider);
  if (!config.apiKey) {
    throw new Error(`Falta ${config.apiKeyEnvVar} en web/.env.local.`);
  }
  if (input.matches.length === 0) {
    throw new Error("No hay partidas para analizar todavía.");
  }

  const client = new OpenAI({ baseURL: config.baseUrl, apiKey: config.apiKey });

  const completion = await client.chat.completions.create({
    model: config.model,
    temperature: 0.4,
    // Si reasoningEffort es undefined, el SDK omite el campo en el request.
    reasoning_effort: config.reasoningEffort,
    messages: [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "user", content: buildPrompt(input) },
    ],
  });

  const report = completion.choices[0]?.message?.content?.trim();
  if (!report) {
    throw new Error("El modelo no devolvió contenido.");
  }
  return { report, model: config.model };
}
