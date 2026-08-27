"use client";

import { useMemo, useState } from "react";
import {
  fetchAlerts,
  fetchAnimalProfile,
  fetchDataQuality,
  fetchFarmProfile,
  fetchOverview,
  type Farm,
} from "@/lib/api";
import { Button, Panel, Table } from "@/components/ui";

const REPORT_TYPES = [
  { value: "herd-summary", label: "Herd summary" },
  { value: "farm-comparison", label: "Farm comparison" },
  { value: "individual-animal", label: "Individual animal" },
  { value: "health-alerts", label: "Health alerts" },
  { value: "data-quality", label: "Data quality" },
] as const;

type ReportType = (typeof REPORT_TYPES)[number]["value"];
type ReportResult = { title: string; kpis: { label: string; value: string }[]; rows: Record<string, unknown>[] };

function toCsv(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return "";
  const cols = Object.keys(rows[0]);
  const escape = (v: unknown) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  return [cols.join(","), ...rows.map((r) => cols.map((c) => escape(r[c])).join(","))].join("\n");
}

export function ReportsClient({ farms }: { farms: Farm[] }) {
  const [reportType, setReportType] = useState<ReportType>("herd-summary");
  const [farmId, setFarmId] = useState(farms[0]?.id ?? "");
  const [animalId, setAnimalId] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ReportResult | null>(null);

  async function generate() {
    setLoading(true);
    try {
      if (reportType === "herd-summary") {
        const overview = await fetchOverview(farmId || undefined);
        setResult({
          title: "Herd summary",
          kpis: overview.system_cards.map((c) => ({ label: c.title, value: c.value })),
          rows: (overview.alerts as Record<string, unknown>[] | undefined) ?? [],
        });
      } else if (reportType === "farm-comparison") {
        const profiles = await Promise.all(farms.map((f) => fetchFarmProfile(f.id)));
        const rows = profiles
          .map((p, i) => {
            if (!p) return null;
            const header = p.header as Record<string, unknown>;
            const rating = p.farm_rating as { grade: string; index: number };
            const pressure = p.farm_action_pressure as { score: number; band: string };
            return {
              farm: farms[i].name ?? farms[i].id,
              cow_count: header.cow_count,
              grade: rating.grade,
              rating_index: rating.index,
              pressure_band: pressure.band,
            };
          })
          .filter(Boolean) as Record<string, unknown>[];
        setResult({ title: "Farm comparison", kpis: [{ label: "Farms compared", value: String(rows.length) }], rows });
      } else if (reportType === "individual-animal") {
        if (!animalId) {
          setResult({ title: "Individual animal", kpis: [], rows: [] });
          return;
        }
        const profile = await fetchAnimalProfile(animalId);
        if (!profile) {
          setResult({ title: "Individual animal", kpis: [{ label: "Status", value: "No data" }], rows: [] });
          return;
        }
        const header = profile.header as Record<string, unknown>;
        const scorecard = (profile.current_metric_scorecard as Record<string, unknown>[]) ?? [];
        setResult({
          title: `Animal ${animalId}`,
          kpis: [
            { label: "Records", value: String(header.record_count) },
            { label: "Date range", value: String(header.date_range) },
          ],
          rows: scorecard,
        });
      } else if (reportType === "health-alerts") {
        const alerts = await fetchAlerts({ limit: 200, farmId: farmId || undefined });
        setResult({
          title: "Health alerts",
          kpis: [{ label: "Total alerts", value: String(alerts.length) }],
          rows: alerts as unknown as Record<string, unknown>[],
        });
      } else if (reportType === "data-quality") {
        const dq = await fetchDataQuality();
        setResult({
          title: "Data quality",
          kpis: Object.entries(dq.quality_flags).map(([k, v]) => ({ label: k, value: String(v) })),
          rows: dq.latest_run ? [dq.latest_run] : [],
        });
      }
    } finally {
      setLoading(false);
    }
  }

  function exportCsv() {
    if (!result) return;
    const csv = toCsv(result.rows);
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${result.title.replace(/\s+/g, "_").toLowerCase()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  const columns = useMemo(() => {
    if (!result || result.rows.length === 0) return [];
    return Object.keys(result.rows[0]).map((k) => ({ key: k, label: k.replace(/_/g, " ") }));
  }, [result]);

  return (
    <div className="space-y-6">
      <Panel title="Report builder" className="no-print">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-4">
          <div>
            <label className="mb-1 block text-xs font-medium text-[var(--foreground-muted)]">Report type</label>
            <select
              value={reportType}
              onChange={(e) => setReportType(e.target.value as ReportType)}
              className="w-full rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm"
            >
              {REPORT_TYPES.map((t) => (
                <option key={t.value} value={t.value}>
                  {t.label}
                </option>
              ))}
            </select>
          </div>
          {reportType !== "farm-comparison" && reportType !== "data-quality" && (
            <div>
              <label className="mb-1 block text-xs font-medium text-[var(--foreground-muted)]">Farm</label>
              <select
                value={farmId}
                onChange={(e) => setFarmId(e.target.value)}
                className="w-full rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm"
              >
                <option value="">All farms</option>
                {farms.map((f) => (
                  <option key={f.id} value={f.id}>
                    {f.name ?? f.id}
                  </option>
                ))}
              </select>
            </div>
          )}
          {reportType === "individual-animal" && (
            <div>
              <label className="mb-1 block text-xs font-medium text-[var(--foreground-muted)]">Animal ID</label>
              <input
                value={animalId}
                onChange={(e) => setAnimalId(e.target.value)}
                placeholder="e.g. UNMAPPEDDANONESENSORCOW-10000245"
                className="w-full rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm"
              />
            </div>
          )}
        </div>
        <div className="mt-4">
          <Button onClick={generate} disabled={loading}>
            {loading ? "Generating…" : "Generate report"}
          </Button>
        </div>
      </Panel>

      {result && (
        <div id="report-output" className="space-y-6">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-semibold text-[var(--foreground)]">{result.title}</h2>
            <div className="no-print flex gap-2">
              <Button variant="secondary" onClick={exportCsv}>
                Export CSV
              </Button>
              <Button variant="secondary" onClick={() => window.print()}>
                Export PDF
              </Button>
            </div>
          </div>

          {result.kpis.length > 0 && (
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
              {result.kpis.map((k) => (
                <div key={k.label} className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-4">
                  <div className="text-xs uppercase tracking-wide text-[var(--foreground-muted)]">{k.label}</div>
                  <div className="mt-1 text-xl font-semibold text-[var(--foreground)]">{k.value}</div>
                </div>
              ))}
            </div>
          )}

          <Panel title="Data">
            <Table
              columns={columns}
              rows={result.rows.map((r) => {
                const out: Record<string, React.ReactNode> = {};
                for (const c of columns) out[c.key] = String(r[c.key] ?? "");
                return out;
              })}
            />
          </Panel>
        </div>
      )}
    </div>
  );
}
