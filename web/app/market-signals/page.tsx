import { fetchMarketFinance } from "@/lib/api";
import { EmptyState, KpiCard, Panel, Table } from "@/components/ui";
import { TimeseriesChart } from "@/components/TimeseriesChart";

export const dynamic = "force-dynamic";

export default async function MarketSignalsPage() {
  const market = await fetchMarketFinance();

  const metrics = (market.profitability_metrics as Record<string, number | null> | undefined) ?? {};
  const chart = (market.milk_vs_feed_chart as Record<string, unknown>[] | undefined) ?? [];
  const summary = (market.summary_df as Record<string, unknown>[] | undefined) ?? [];

  return (
    <div className="space-y-8">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--foreground)]">Market Signals</h1>
        <p className="mt-1 text-sm text-[var(--foreground-muted)]">
          Reference price series and profitability outlook from the market/finance connectors.
        </p>
      </header>

      {market.status === "empty" ? (
        <Panel>
          <EmptyState
            message={
              (market.message as string) ??
              "No reference series loaded yet. Use the prices connector or upload to populate market data."
            }
          />
        </Panel>
      ) : (
        <>
          <section className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <KpiCard label="Milk price" value={metrics.milk_price?.toFixed(2) ?? "n/a"} />
            <KpiCard label="Feed cost" value={metrics.feed_cost?.toFixed(2) ?? "n/a"} />
            <KpiCard label="Fx rate" value={metrics.fx_rate?.toFixed(2) ?? "n/a"} />
            <KpiCard
              label="Estimated margin"
              value={metrics.estimated_margin?.toFixed(2) ?? "n/a"}
              status={
                market.profitability_outlook === "improving"
                  ? "green"
                  : market.profitability_outlook === "declining"
                    ? "red"
                    : "neutral"
              }
              hint={market.profitability_outlook ? String(market.profitability_outlook) : undefined}
            />
          </section>

          <Panel title="Milk price vs feed cost">
            <TimeseriesChart data={chart} xKey="date" series={["milk_price", "feed_cost"]} />
          </Panel>

          <Panel title="Reference series history">
            <Table
              columns={[
                { key: "series_key", label: "Series" },
                { key: "trend", label: "Trend" },
                { key: "change_pct", label: "Change %" },
              ]}
              rows={summary.map((r) => ({
                series_key: String(r.series_key ?? ""),
                trend: String(r.trend ?? ""),
                change_pct: r.change_pct != null ? String(r.change_pct) : "—",
              }))}
            />
          </Panel>
        </>
      )}
    </div>
  );
}
