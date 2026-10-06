import { getDashboardData } from "@/lib/queries";
import { Card } from "@/components/Card";
import { Kpis } from "@/components/Kpis";
import { MatchesTable } from "@/components/MatchesTable";
import { CoachingPanel } from "@/components/CoachingPanel";
import { getProviderModel } from "@/lib/coach";
import { PROVIDER_IDS, providerName } from "@/lib/providers";
import { ChampionPicker } from "@/components/ChampionPicker";
import TrendsChart from "@/components/charts/TrendsChart";
import TrendsResourcesChart from "@/components/charts/TrendsResourcesChart";
import KdaTimelineChart from "@/components/charts/KdaTimelineChart";
import ImpactScatterChart from "@/components/charts/ImpactScatterChart";
import ChampionWinrateChart from "@/components/charts/ChampionWinrateChart";
import EarlyGameChart from "@/components/charts/EarlyGameChart";
import JungleMatchupChart from "@/components/charts/JungleMatchupChart";
import ObjectivesChart from "@/components/charts/ObjectivesChart";

// Consulta DB en cada request; nunca prerenderizar con datos viejos.
export const dynamic = "force-dynamic";

export default async function Home({
  searchParams,
}: {
  searchParams: Promise<{ champion?: string | string[] }>;
}) {
  const sp = await searchParams;
  const championParam = Array.isArray(sp.champion) ? sp.champion[0] : sp.champion;
  const champion =
    championParam && championParam.toLowerCase() !== "all" ? championParam : null;

  const data = await getDashboardData(champion);
  const hasData = data.matches.length > 0;

  return (
    <main className="min-h-screen">
      <div className="mx-auto max-w-7xl px-4 py-10 sm:px-6">
        <header className="mb-8 flex flex-wrap items-end justify-between gap-4 border-b border-line pb-5">
          <div>
            <div className="flex items-baseline gap-3">
              <h1 className="text-3xl font-bold tracking-tight text-primary-glow">
                WardEye
              </h1>
              <span className="hidden text-[11px] uppercase tracking-[0.25em] text-secondary sm:inline">
                analytics &amp; coaching
              </span>
            </div>
            <p className="mt-2 text-sm text-muted">
              Dashboard de partidas de League of Legends · coaching automático
            </p>
          </div>
          {data.championList.length > 0 && (
            <ChampionPicker champions={data.championList} selected={champion} />
          )}
        </header>

        {data.error && (
          <div className="mb-6 rounded-lg border border-danger/50 bg-danger/10 p-4 text-sm">
            <p className="font-semibold uppercase tracking-wide text-danger">
              No se pudieron cargar los datos.
            </p>
            <p className="mt-1 text-muted">{data.error}</p>
            <p className="mt-1 text-xs text-muted/70">
              Verificá que exista el archivo{" "}
              <code className="text-primary">warddata.duckdb</code> (corré el
              pipeline) y, si lo moviste, la variable{" "}
              <code className="text-primary">DUCKDB_PATH</code> en{" "}
              <code className="text-primary">web/.env.local</code>.
            </p>
          </div>
        )}

        {!data.error && !hasData && champion && (
          <div className="surface-card mb-6 rounded-lg p-4 text-sm text-muted">
            No hay partidas registradas con {champion}. Probá con otro
            campeón o volvé a &quot;Todos los campeones&quot;.
          </div>
        )}

        {!data.error && !hasData && !champion && (
          <div className="surface-card mb-6 rounded-lg p-4 text-sm text-muted">
            No hay partidas en los marts todavía. Corré el pipeline (dlt + dbt)
            para poblarlos.
          </div>
        )}

        <Kpis summary={data.summary} />

        <div className="mt-6">
          <Card
            title="Coaching"
            subtitle="Análisis de tus partidas recientes vía LLM (OpenAI-compatible)"
          >
            <CoachingPanel
              key={champion ?? "all"}
              champion={champion}
              providerOptions={PROVIDER_IDS.map((id) => ({
                id,
                label: `${providerName(id)} · ${getProviderModel(id)}`,
              }))}
            />
          </Card>
        </div>

        {hasData && (
          <div className="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
            <Card title="Tendencia semanal" subtitle="Winrate vs KDA (ranked)">
              <TrendsChart trends={data.trends} />
            </Card>
            <Card
              title="Recursos por minuto"
              subtitle="Evolución semanal (ranked)"
            >
              <TrendsResourcesChart trends={data.trends} />
            </Card>
            <Card
              title="Últimas partidas — KDA"
              subtitle="Kills / Deaths / Assists y ratio"
            >
              <KdaTimelineChart matches={data.matches} />
            </Card>
            <Card
              title="Impacto en partida"
              subtitle="Daño share % vs Kill participation %"
            >
              <ImpactScatterChart matches={data.matches} />
            </Card>
            <Card
              title="Winrate por campeón"
              subtitle="Histórico ranked (con nº de partidas)"
            >
              <ChampionWinrateChart champions={data.champions} />
            </Card>
            <Card
              title="Early game"
              subtitle="CS/min a min 10 y 15, muertes ≤15m"
            >
              <EarlyGameChart earlyGame={data.earlyGame} />
            </Card>
            {data.objectives.length > 0 && (
              <Card
                title="Control de objetivos"
                subtitle="Winrate según quién se llevó cada objetivo"
              >
                <ObjectivesChart objectives={data.objectives} />
              </Card>
            )}
          </div>
        )}

        {data.jungleMatchup.length > 0 && (
          <div className="mt-6">
            <Card
              title="Duelo vs el jungla rival"
              subtitle="Diferencial de campamentos al minuto 15 — arriba de 0 vas ganando el duelo"
            >
              <JungleMatchupChart matchup={data.jungleMatchup} />
            </Card>
          </div>
        )}

        {hasData && (
          <div className="mt-6">
            <Card title="Partidas recientes">
              <MatchesTable matches={data.matches} />
            </Card>
          </div>
        )}
      </div>
    </main>
  );
}
