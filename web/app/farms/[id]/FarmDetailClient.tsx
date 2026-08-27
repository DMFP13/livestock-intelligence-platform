"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { fetchFarmProfile, fetchFarms, fetchFeedEnvironment, type Farm } from "@/lib/api";
import { Badge, Button, EmptyState, KpiCard, Panel, Table } from "@/components/ui";

const GRADE_TONE: Record<string, "green" | "yellow" | "red" | "neutral"> = {
  A: "green",
  B: "green",
  C: "yellow",
  D: "yellow",
  E: "red",
};

type LoadState = "loading" | "ready" | "empty" | "error";

export function FarmDetailClient({ id }: { id: string }) {
  const [state, setState] = useState<LoadState>("loading");
  const [profile, setProfile] = useState<Record<string, unknown> | null>(null);
  const [feedEnv, setFeedEnv] = useState<Record<string, unknown> | null>(null);
  const [registeredFarm, setRegisteredFarm] = useState<Farm | undefined>(undefined);

  const load = useCallback(() => {
    setState("loading");
    Promise.all([fetchFarmProfile(id), fetchFeedEnvironment(id).catch(() => null), fetchFarms().catch(() => [])])
      .then(([p, fe, farms]) => {
        setProfile(p);
        setFeedEnv(fe);
        setRegisteredFarm(farms.find((f) => f.id === id));
        setState(p ? "ready" : "empty");
      })
      .catch(() => setState("error"));
  }, [id]);

  useEffect(() => {
    load();
  }, [load]);

  if (state === "loading") {
    return (
      <div className="space-y-4">
        <Link href="/" className="text-sm text-[var(--primary)] hover:underline">
          &larr; Farm Overview
        </Link>
        <Panel>
          <div className="flex items-center gap-3 py-6 text-sm text-[var(--foreground-muted)]">
            <span className="h-4 w-4 animate-spin rounded-full border-2 border-[var(--border)] border-t-[var(--primary)]" />
            Loading farm data — this can take a little while on larger farms.
          </div>
        </Panel>
      </div>
    );
  }

  if (state === "error" || state === "empty" || !profile) {
    return (
      <div className="space-y-4">
        <Link href="/" className="text-sm text-[var(--primary)] hover:underline">
          &larr; Farm Overview
        </Link>
        <Panel>
          <EmptyState
            message={
              state === "error"
                ? "This is taking longer than expected. The data may still be loading in the background — try again in a moment."
                : `No data available yet for ${id}. Upload telemetry from the Upload Data tab.`
            }
          />
          <div className="mt-4">
            <Button variant="secondary" onClick={load}>
              Retry
            </Button>
          </div>
        </Panel>
      </div>
    );
  }

  const header = profile.header as Record<string, unknown>;
  const rating = profile.farm_rating as { grade: string; index: number };
  const pressure = profile.farm_action_pressure as { score: number; band: string };
  const burden = profile.burden_metrics as Record<string, number>;
  const distribution = (profile.rating_distribution_summary as Record<string, unknown>[]) ?? [];
  const topPerformers = (profile.top_performers as Record<string, unknown>[]) ?? [];
  const reviewCows = (profile.top_review_priority_cows as Record<string, unknown>[]) ?? [];
  const drivers = (profile.pressure_drivers as string[]) ?? [];

  return (
    <div className="space-y-8">
      <div>
        <Link href="/" className="text-sm text-[var(--primary)] hover:underline">
          &larr; Farm Overview
        </Link>
        <div className="mt-2 flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight text-[var(--foreground)]">
            {registeredFarm?.name ?? String(header.farm_name ?? id)}
          </h1>
          <Badge tone={GRADE_TONE[rating.grade] ?? "neutral"}>Grade {rating.grade}</Badge>
          <Badge tone={pressure.band === "low" ? "green" : pressure.band === "watch" ? "yellow" : "red"}>
            {pressure.band} pressure
          </Badge>
        </div>
        <p className="mt-1 text-sm text-[var(--foreground-muted)]">
          {String(header.cow_count)} animals tracked · {String(header.records)} records
        </p>
      </div>

      <section className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <KpiCard label="Farm rating index" value={rating.index.toFixed(1)} />
        <KpiCard label="Action pressure score" value={pressure.score.toFixed(1)} />
        <KpiCard
          label="Avg milk yield"
          value={header.avg_milk_yield_l != null ? `${header.avg_milk_yield_l} L` : "n/a"}
        />
        <KpiCard
          label="Anomaly burden"
          value={burden?.anomaly_burden_pct != null ? `${burden.anomaly_burden_pct}%` : "n/a"}
        />
      </section>

      {drivers.length > 0 && (
        <Panel title="Pressure drivers">
          <div className="flex flex-wrap gap-2">
            {drivers.map((d) => (
              <Badge key={d} tone="yellow">
                {d.replace(/_/g, " ")}
              </Badge>
            ))}
          </div>
        </Panel>
      )}

      <Panel title="Cow rating distribution">
        {distribution.length === 0 ? (
          <EmptyState message="No cow-level ratings yet." />
        ) : (
          <div className="flex items-end gap-3">
            {distribution.map((d) => {
              const pct = Number(d.pct ?? 0);
              return (
                <div key={String(d.rating)} className="flex flex-1 flex-col items-center gap-1">
                  <div className="text-xs text-[var(--foreground-muted)]">{String(d.count)}</div>
                  <div
                    className="w-full rounded-t bg-[var(--primary)]"
                    style={{ height: `${Math.max(4, pct * 1.2)}px` }}
                  />
                  <div className="text-xs font-medium">{String(d.rating)}</div>
                </div>
              );
            })}
          </div>
        )}
      </Panel>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <Panel title="Top performers">
          <Table
            columns={[
              { key: "animal_id", label: "Animal" },
              { key: "cow_rating", label: "Rating" },
              { key: "milk_yield_l", label: "Milk (L)" },
              { key: "data_confidence_score", label: "Confidence" },
            ]}
            rows={topPerformers.map((r) => ({
              animal_id: (
                <Link className="text-[var(--primary)] hover:underline" href={`/animals/${r.animal_id}?farm=${id}`}>
                  {String(r.animal_id)}
                </Link>
              ),
              cow_rating: String(r.cow_rating ?? ""),
              milk_yield_l: r.milk_yield_l != null ? String(r.milk_yield_l) : "—",
              data_confidence_score: r.data_confidence_score != null ? String(r.data_confidence_score) : "—",
            }))}
          />
        </Panel>

        <Panel title="Needs review">
          <Table
            columns={[
              { key: "animal_id", label: "Animal" },
              { key: "health_risk_band", label: "Health risk" },
              { key: "review_priority_band", label: "Priority" },
            ]}
            rows={reviewCows.map((r) => ({
              animal_id: (
                <Link className="text-[var(--primary)] hover:underline" href={`/animals/${r.animal_id}?farm=${id}`}>
                  {String(r.animal_id)}
                </Link>
              ),
              health_risk_band: String(r.health_risk_band ?? ""),
              review_priority_band: String(r.review_priority_band ?? ""),
            }))}
          />
        </Panel>
      </div>

      {feedEnv && feedEnv.status !== "empty" && (
        <Panel title="Feed & environment">
          <p className="text-sm text-[var(--foreground-muted)]">
            {(feedEnv.remote_sensing as Record<string, unknown> | undefined)?.message as string}
          </p>
        </Panel>
      )}
    </div>
  );
}
