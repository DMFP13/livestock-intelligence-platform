import Link from "next/link";
import { fetchAlerts, fetchObservations } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function FarmDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const [observations, alerts] = await Promise.all([fetchObservations(1000), fetchAlerts(200)]);

  const farmObservations = observations.filter((o) => String(o.farm_id) === id);
  const farmAlerts = alerts.filter((a) => String(a.farm_id) === id);

  const byMetric = new Map<string, number>();
  for (const obs of farmObservations) {
    const key = String(obs.metric_key ?? "unknown");
    byMetric.set(key, (byMetric.get(key) ?? 0) + 1);
  }

  return (
    <main className="mx-auto max-w-6xl px-6 py-10">
      <Link href="/" className="text-sm text-blue-600 hover:underline dark:text-blue-400">
        &larr; Portfolio
      </Link>
      <h1 className="mt-2 text-2xl font-semibold tracking-tight">{id}</h1>
      <p className="mt-1 text-sm text-neutral-500">
        {farmObservations.length} observations, {farmAlerts.length} alerts.
      </p>

      <section className="mt-8">
        <h2 className="mb-3 text-lg font-medium">Observations by metric</h2>
        {byMetric.size === 0 ? (
          <p className="text-sm text-neutral-500">No observations for this farm yet.</p>
        ) : (
          <ul className="divide-y divide-neutral-200 rounded-lg border border-neutral-200 text-sm dark:divide-neutral-800 dark:border-neutral-800">
            {[...byMetric.entries()].map(([metric, count]) => (
              <li key={metric} className="flex justify-between px-4 py-2">
                <span>{metric}</span>
                <span className="text-neutral-500">{count}</span>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="mt-8">
        <h2 className="mb-3 text-lg font-medium">Alerts</h2>
        {farmAlerts.length === 0 ? (
          <p className="text-sm text-neutral-500">No open alerts.</p>
        ) : (
          <ul className="space-y-2 text-sm">
            {farmAlerts.map((alert) => (
              <li
                key={alert.id}
                className="rounded-lg border border-neutral-200 px-4 py-2 dark:border-neutral-800"
              >
                <span className="mr-2 rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900/40 dark:text-amber-300">
                  {String(alert.severity ?? "info")}
                </span>
                {String(alert.message ?? alert.id)}
              </li>
            ))}
          </ul>
        )}
      </section>
    </main>
  );
}
