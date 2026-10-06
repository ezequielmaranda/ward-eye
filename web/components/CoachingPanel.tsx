"use client";

import { useState, type ReactNode } from "react";
import { downloadReportPdf } from "@/lib/reportPdf";
import type { Provider } from "@/lib/providers";
import type { CoachMeta } from "@/lib/types";

type CoachResponse = {
  report?: string;
  model?: string;
  meta?: CoachMeta | null;
  error?: string;
};

function inline(text: string): ReactNode {
  return text.split(/(\*\*[^*]+\*\*)/g).map((p, i) =>
    p.startsWith("**") && p.endsWith("**") ? (
      <strong key={i} className="text-primary">
        {p.slice(2, -2)}
      </strong>
    ) : (
      <span key={i}>{p}</span>
    ),
  );
}

/** Render mínimo de Markdown (headings, bullets, bold) sin dependencias. */
function Markdown({ text }: { text: string }) {
  const out: ReactNode[] = [];
  let bullets: string[] = [];

  const flush = (key: string) => {
    if (bullets.length) {
      out.push(
        <ul
          key={key}
          className="mb-2 list-disc space-y-1 pl-5 text-sm text-fg marker:text-primary"
        >
          {bullets.map((b, i) => (
            <li key={i}>{inline(b)}</li>
          ))}
        </ul>,
      );
      bullets = [];
    }
  };

  text.split("\n").forEach((raw, i) => {
    const line = raw.trimEnd();
    if (/^#{1,3}\s/.test(line)) {
      flush(`f${i}`);
      out.push(
        <h3
          key={i}
          className="mt-4 mb-1 text-sm font-semibold uppercase tracking-[0.1em] text-primary"
        >
          {line.replace(/^#{1,3}\s/, "")}
        </h3>,
      );
    } else if (/^\s*[-*]\s/.test(line)) {
      bullets.push(line.replace(/^\s*[-*]\s/, ""));
    } else if (line === "") {
      flush(`f${i}`);
    } else {
      flush(`f${i}`);
      out.push(
        <p key={i} className="mb-2 text-sm text-fg">
          {inline(line)}
        </p>,
      );
    }
  });
  flush("end");
  return <>{out}</>;
}

type ProviderOption = { id: Provider; label: string };

export function CoachingPanel({
  champion = null,
  providerOptions,
}: {
  champion?: string | null;
  providerOptions: ProviderOption[];
}) {
  const [provider, setProvider] = useState<Provider>(providerOptions[0]?.id ?? "groq");
  const [loading, setLoading] = useState(false);
  const [report, setReport] = useState<string | null>(null);
  const [model, setModel] = useState<string | null>(null);
  const [generatedAt, setGeneratedAt] = useState<Date | null>(null);
  const [meta, setMeta] = useState<CoachMeta | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function run() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/coaching", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, champion }),
      });
      const data: CoachResponse = await res.json();
      if (!res.ok || data.error) {
        throw new Error(data.error ?? "Error generando el coaching.");
      }
      setReport(data.report ?? "");
      setModel(data.model ?? null);
      setMeta(data.meta ?? null);
      setGeneratedAt(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error desconocido.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <div className="flex items-center gap-3">
        <select
          value={provider}
          onChange={(e) => setProvider(e.target.value as Provider)}
          disabled={loading}
          className="rounded border border-line bg-surface px-2 py-2 text-sm text-fg disabled:cursor-not-allowed disabled:opacity-60"
        >
          {providerOptions.map((p) => (
            <option key={p.id} value={p.id}>
              {p.label}
            </option>
          ))}
        </select>
        <button
          onClick={run}
          disabled={loading}
          className="btn-primary rounded px-4 py-2 text-sm font-semibold uppercase tracking-wide"
        >
          {loading ? "Analizando…" : "Generar coaching"}
        </button>
        {model && <span className="text-xs text-muted">modelo: {model}</span>}
      </div>
      <p className="mt-2 text-xs text-muted">
        {champion ? `Analizando solo partidas con ${champion}.` : "Analizando todos los campeones."}
      </p>

      {error && (
        <p className="mt-3 rounded-lg border border-danger/50 bg-danger/10 p-3 text-sm text-fg">
          {error}
        </p>
      )}

      {report && (
        <div className="surface-card mt-4 rounded-lg p-4">
          <div className="mb-3 flex items-center justify-between border-b border-line pb-2">
            <span className="text-xs text-muted">
              {generatedAt
                ? `Generado ${generatedAt.toLocaleString()}`
                : "Informe"}
            </span>
            <button
              onClick={() =>
                downloadReportPdf({
                  report,
                  model,
                  meta,
                  generatedAt: generatedAt ?? undefined,
                }).catch((e) =>
                  setError(
                    e instanceof Error ? e.message : "Error generando el PDF.",
                  ),
                )
              }
              className="rounded border border-primary/50 px-3 py-1 text-xs font-medium text-primary transition-colors hover:bg-primary/10"
            >
              Descargar PDF
            </button>
          </div>
          <Markdown text={report} />
        </div>
      )}

      {!report && !error && !loading && (
        <p className="mt-3 text-sm text-muted">
          Genera un análisis de tus partidas recientes con el LLM configurado.
        </p>
      )}
    </div>
  );
}
