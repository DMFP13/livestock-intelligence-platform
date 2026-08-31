"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { fetchAnimalProfile, fetchAnimalTimeseries } from "@/lib/api";
import { Badge, Button, EmptyState, KpiCard, Panel, Table } from "@/components/ui";
import { TimeseriesChart } from "@/components/TimeseriesChart";

const BEHAVIOR_METRICS = ["activity_rate", "rumination_min", "eating_min", "resting_min"];

const GRADE_TONE: Record<string, "green" | "yellow" | "red" | "neutral"> = {
  A: "green",
  B: "green",
  C: "yellow",
  D: "yellow",
  E: "red",
};

type LoadState = "loading" | "ready" | "empty" | "error";

export function AnimalDetailClient({ id, farmId }: { id: string; farmId?: string }) {
  const [state, setState] = useState<LoadState>("loading");
  const [profile, setProfile] = useState<Record<string, unknown> | null>(null);
  const [timeseries, setTimeseries] = useState<Record<string, unknown>[]>([]);

  const load = useCallback(() => {
    setState("loading");
    Promise.all([fetchAnimalProfile(id, farmId), fetchAnimalTimeseries(id, BEHAVIOR_METRICS, farmId)])
      .then(([p, ts]) => {
        setProfile(p);
        setTimeseries(ts);
        setState(p ? "ready" : "empty");
      })
      .catch(() => setState("error"));
  }, [id, farmId]);

  useEffect(() => {
    load();
  }, [load]);

  const backHref = farmId ? `/farms/${farmId}` : "/";

  if (state === "loading") {
    return (
      <div className="space-y-4">
        <Link href={backHref} className="text-sm text-[var(--primary)] hover:underline">
          &larr; Back
        </Link>
        <Panel>
          <div className="flex items-center gap-3 py-6 text-sm text-[var(--foreground-muted)]">
            <span className="h-4 w-4 animate-spin rounded-full border-2 border-[var(--border)] border-t-[var(--primary)]" />
            Loading animal data — first load can take a while on a cold start.
          </div>
        </Panel>
      </div>
    );
  }

  if (state === "error" || state === "empty" || !profile) {
    return (
      <div className="space-y-4">
        <Link href={backHref} className="text-sm text-[var(--primary)] hover:underline">
          &larr; Back
        </Link>
        <Panel>
          <EmptyState
            message={
              state === "error"
                ? "Still no response after a while — the query may be slow on a cold start. Give it a moment and retry."
                : `No data available yet for ${id}.`
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
  const rating = profile.cow_rating as { grade: string; index: number };
  const reviewPriority = profile.cow_review_priority as { score: number; band: string };
  const badges = (profile.state_badges as { badge: string; score: number; band: string }[]) ?? [];
  const scorecard = (profile.current_metric_scorecard as Record<string, unknown>[]) ?? [];
  const explanation = (profile.explanations as { text?: string } | undefined)?.text;

  return (
    <div className="space-y-8">
      <div>
        <Link
          href={header.farm_id ? `/farms/${header.farm_id}` : backHref}
          className="text-sm text-[var(--primary)] hover:underline"
        >
          &larr; {String(header.farm_name ?? "Farm")}
        </Link>
        <div className="mt-2 flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight text-[var(--foreground)]">{id}</h1>
          <Badge tone={GRADE_TONE[rating.grade] ?? "neutral"}>Grade {rating.grade}</Badge>
          <Badge tone={reviewPriority.band === "low" ? "green" : reviewPriority.band === "elevated" ? "yellow" : "red"}>
            {reviewPriority.band} priority
          </Badge>
        </div>
        <p className="mt-1 text-sm text-[var(--foreground-muted)]">
          {String(header.record_count)} records · {String(header.date_range)}
        </p>
      </div>

      <section className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <KpiCard label="Rating index" value={rating.index.toFixed(1)} />
        <KpiCard label="Review priority" value={reviewPriority.score.toFixed(1)} />
        <KpiCard
          label="Avg rumination"
          value={header.avg_rumination_min != null ? `${header.avg_rumination_min} min` : "n/a"}
        />
        <KpiCard
          label="Avg activity"
          value={header.avg_activity_rate != null ? String(header.avg_activity_rate) : "n/a"}
        />
      </section>

      {badges.length > 0 && (
        <Panel title="State signals">
          <div className="flex flex-wrap gap-3">
            {badges.map((b) => (
              <div key={b.badge} className="rounded-lg border border-[var(--border)] px-3 py-2 text-sm">
                <span className="font-medium capitalize">{b.badge}</span>{" "}
                <Badge tone={b.band === "low" ? "green" : b.band === "elevated" || b.band === "watch" ? "yellow" : "red"}>
                  {b.band} ({b.score})
                </Badge>
              </div>
            ))}
          </div>
        </Panel>
      )}

      <Panel title="Behaviour over time">
        <TimeseriesChart data={timeseries} xKey="date" series={BEHAVIOR_METRICS} />
      </Panel>

      {explanation && (
        <Panel title="Why this rating">
          <p className="text-sm text-[var(--foreground)]">{explanation}</p>
        </Panel>
      )}

      <Panel title="Current metric scorecard">
        <Table
          columns={[
            { key: "metric", label: "Metric" },
            { key: "value", label: "Value" },
            { key: "rolling_median", label: "Rolling median" },
            { key: "deviation_score", label: "Deviation" },
          ]}
          rows={scorecard.map((r) => ({
            metric: String(r.metric ?? ""),
            value: r.value != null ? String(r.value) : "—",
            rolling_median: r.rolling_median != null ? String(r.rolling_median) : "—",
            deviation_score: r.deviation_score != null ? String(r.deviation_score) : "—",
          }))}
        />
      </Panel>
    </div>
  );
}
