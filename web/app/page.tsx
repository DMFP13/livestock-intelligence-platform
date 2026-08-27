import Link from "next/link";
import { fetchAlerts, fetchDataQuality, fetchFarms, fetchObservations } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function PortfolioOverviewPage() {
  const [farms, observations, alerts, quality] = await Promise.all([
    fetchFarms(),
    fetchObservations(500),
    fetchAlerts(200),
    fetchDataQuality(),
  ]);

  const observationsByFarm = new Map<string, number>();
  for (const obs of observations) {
    const farmId = String(obs.farm_id ?? "unknown");
    observationsByFarm.set(farmId, (observationsByFarm.get(farmId) ?? 0) + 1);
  }
  const alertsByFarm = new Map<string, number>();
  for (const alert of alerts) {
    const farmId = String(alert.farm_id ?? "unknown");
    alertsByFarm.set(farmId, (alertsByFarm.get(farmId) ?? 0) + 1);
  }

  const goodFlags = quality.quality_flags?.good ?? 0;
  const totalFlags = Object.values(quality.quality_flags ?? {}).reduce((a, b) => a + b, 0);

  return (
    <main className="mx-auto max-w-6xl px-6 py-10">
      <header className="mb-8">
        <h1 className="text-2xl font-semibold tracking-tight">Portfolio Overview</h1>
        <p className="mt-1 text-sm text-neutral-500">
          Canonical data served from the FastAPI service layer — {observations.length} observations
          across {farms.length} farms.
        </p>
      </header>

      <section className="mb-8 grid grid-cols-1 gap-4 sm:grid-cols-3">
        <StatCard label="Farms" value={farms.length} />
        <StatCard label="Open alerts" value={alerts.length} />
        <StatCard
          label="Data quality (good)"
          value={totalFlags ? `${Math.round((goodFlags / totalFlags) * 100)}%` : "—"}
        />
      </section>

      <section>
        <h2 className="mb-3 text-lg font-medium">Farms</h2>
        {farms.length === 0 ? (
          <p className="text-sm text-neutral-500">
            No farms in the canonical store yet. Run an ingestion job, e.g.:{" "}
            <code className="ml-1 rounded bg-neutral-100 px-1.5 py-0.5 text-xs dark:bg-neutral-800">
              python -m apps.worker.run_ingestion --connector sensor_upload ...
            </code>
          </p>
        ) : (
          <div className="overflow-hidden rounded-lg border border-neutral-200 dark:border-neutral-800">
            <table className="w-full text-sm">
              <thead className="bg-neutral-50 text-left dark:bg-neutral-900">
                <tr>
                  <th className="px-4 py-2 font-medium">Farm</th>
                  <th className="px-4 py-2 font-medium">Observations</th>
                  <th className="px-4 py-2 font-medium">Alerts</th>
                </tr>
              </thead>
              <tbody>
                {farms.map((farm) => (
                  <tr key={farm.id} className="border-t border-neutral-200 dark:border-neutral-800">
                    <td className="px-4 py-2">
                      <Link className="text-blue-600 hover:underline dark:text-blue-400" href={`/farms/${farm.id}`}>
                        {farm.name ?? farm.id}
                      </Link>
                    </td>
                    <td className="px-4 py-2">{observationsByFarm.get(farm.id) ?? 0}</td>
                    <td className="px-4 py-2">{alertsByFarm.get(farm.id) ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border border-neutral-200 p-4 dark:border-neutral-800">
      <div className="text-xs uppercase tracking-wide text-neutral-500">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
    </div>
  );
}
