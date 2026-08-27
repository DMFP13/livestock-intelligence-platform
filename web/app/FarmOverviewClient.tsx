"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { fetchAlerts, fetchObservations, fetchOverview, fetchFarms, type Farm, type OverviewPayload } from "@/lib/api";
import { Badge, Button, EmptyState, KpiCard, Panel } from "@/components/ui";

type LoadState = "loading" | "ready" | "error";

export function FarmOverviewClient() {
  const [state, setState] = useState<LoadState>("loading");
  const [overview, setOverview] = useState<OverviewPayload | null>(null);
  const [farms, setFarms] = useState<Farm[]>([]);
  const [observationsByFarm, setObservationsByFarm] = useState<Map<string, number>>(new Map());
  const [alertsByFarm, setAlertsByFarm] = useState<Map<string, number>>(new Map());

  const load = useCallback(() => {
    setState("loading");
    Promise.all([
      fetchOverview(),
      fetchFarms(),
      fetchObservations({ limit: 2000 }).catch(() => []),
      fetchAlerts({ limit: 200 }).catch(() => []),
    ])
      .then(([ov, f, observations, alerts]) => {
        const obsMap = new Map<string, number>();
        for (const obs of observations) {
          const farmId = String(obs.farm_id ?? "unknown");
          obsMap.set(farmId, (obsMap.get(farmId) ?? 0) + 1);
        }
        const alertMap = new Map<string, number>();
        for (const alert of alerts) {
          const farmId = String(alert.farm_id ?? "unknown");
          alertMap.set(farmId, (alertMap.get(farmId) ?? 0) + 1);
        }
        setOverview(ov);
        setFarms(f);
        setObservationsByFarm(obsMap);
        setAlertsByFarm(alertMap);
        setState("ready");
      })
      .catch(() => setState("error"));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  if (state === "loading") {
    return (
      <div className="flex items-center gap-3 py-16 text-sm text-[var(--foreground-muted)]">
        <span className="h-4 w-4 animate-spin rounded-full border-2 border-[var(--border)] border-t-[var(--primary)]" />
        Loading portfolio…
      </div>
    );
  }

  if (state === "error" || !overview) {
    return (
      <Panel>
        <EmptyState message="This is taking longer than expected. The API may still be waking up — try again in a moment." />
        <div className="mt-4">
          <Button variant="secondary" onClick={load}>
            Retry
          </Button>
        </div>
      </Panel>
    );
  }

  const insights = (overview.insights as string[] | undefined) ?? [];

  return (
    <div className="space-y-8">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--foreground)]">Farm Overview</h1>
        <p className="mt-1 text-sm text-[var(--foreground-muted)]">
          Portfolio, herd intelligence, and comparative signal across {farms.length} registered farm
          {farms.length === 1 ? "" : "s"}.
        </p>
      </header>

      <section className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
        {overview.system_cards.map((card) => (
          <KpiCard
            key={card.title}
            label={card.title}
            value={card.value}
            status={card.color as "green" | "yellow" | "red" | "neutral"}
          />
        ))}
      </section>

      {insights.length > 0 && (
        <Panel title="Insights">
          <ul className="space-y-1.5 text-sm text-[var(--foreground)]">
            {insights.map((line, i) => (
              <li key={i} className="flex gap-2">
                <span className="text-[var(--primary)]">•</span>
                {line}
              </li>
            ))}
          </ul>
        </Panel>
      )}

      <Panel title="Registered Farms">
        {farms.length === 0 ? (
          <EmptyState message="No farms registered yet — add one from the Upload Data tab." />
        ) : (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {farms.map((farm) => {
              const meta = farm.metadata_json ? JSON.parse(String(farm.metadata_json)) : {};
              return (
                <Link
                  key={farm.id}
                  href={`/farms/${farm.id}`}
                  className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-4 transition-shadow hover:shadow-md"
                >
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <div className="font-semibold text-[var(--foreground)]">{farm.name ?? farm.id}</div>
                      {farm.location_text && (
                        <div className="text-xs text-[var(--foreground-muted)]">{String(farm.location_text)}</div>
                      )}
                    </div>
                    {meta.species && <Badge tone="neutral">{String(meta.species).replace(/_/g, " ")}</Badge>}
                  </div>
                  <div className="mt-3 flex gap-4 text-sm">
                    <div>
                      <span className="font-medium text-[var(--foreground)]">
                        {observationsByFarm.get(farm.id) ?? 0}
                      </span>{" "}
                      <span className="text-[var(--foreground-muted)]">observations</span>
                    </div>
                    <div>
                      <span className="font-medium text-[var(--foreground)]">{alertsByFarm.get(farm.id) ?? 0}</span>{" "}
                      <span className="text-[var(--foreground-muted)]">alerts</span>
                    </div>
                  </div>
                </Link>
              );
            })}
          </div>
        )}
      </Panel>
    </div>
  );
}
