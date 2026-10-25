import { NextResponse } from "next/server";
import { generateCoaching } from "@/lib/coach";
import { getCoachingData } from "@/lib/queries";
import type { Provider } from "@/lib/providers";
import type { CoachMeta, MatchPerformance, Summary } from "@/lib/types";

function isProvider(value: unknown): value is Provider {
  return value === "groq" || value === "gemini";
}

// Siempre en runtime: consulta DB + LLM, nunca prerenderizar.
export const dynamic = "force-dynamic";

/** Metadata de contexto para el encabezado del informe / PDF. */
function buildMeta(
  summary: Summary | null,
  matches: MatchPerformance[],
  champion: string | null,
): CoachMeta {
  const name = process.env.SUMMONER_NAME?.trim();
  const tag = process.env.SUMMONER_TAG?.trim();
  const player = name ? (tag ? `${name}#${tag}` : name) : null;
  const region = process.env.RIOT_REGION?.trim().toUpperCase() || null;

  // rol principal = posición más frecuente
  const counts = new Map<string, number>();
  for (const m of matches) {
    const r = m.team_position || "UNKNOWN";
    counts.set(r, (counts.get(r) ?? 0) + 1);
  }
  let mainRole: string | null = null;
  let bestN = -1;
  for (const [r, n] of counts) {
    if (n > bestN) {
      mainRole = r;
      bestN = n;
    }
  }

  const dates = matches.map((m) => m.game_date).sort();

  return {
    player,
    region,
    mainRole,
    champion,
    totalGames: summary?.total_games ?? null,
    analyzedGames: matches.length,
    dateFrom: dates[0] ?? null,
    dateTo: dates[dates.length - 1] ?? null,
    winratePct: summary?.winrate_pct ?? null,
  };
}

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => null);
    const provider: Provider = isProvider(body?.provider) ? body.provider : "groq";
    const champion: string | null =
      typeof body?.champion === "string" && body.champion.trim() ? body.champion : null;

    const {
      summary,
      matches,
      champions,
      earlyGame,
      jungleMatchup,
      objectives,
      trends,
      rankContext,
    } = await getCoachingData(champion);

    const result = await generateCoaching(
      {
        summary,
        matches,
        champions,
        earlyGame,
        jungleMatchup,
        objectives,
        trends,
        rankContext,
        champion,
      },
      provider,
    );
    const meta = buildMeta(summary, matches, champion);
    return NextResponse.json({ ...result, meta });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Error generando el coaching.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
