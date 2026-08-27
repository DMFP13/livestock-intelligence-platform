import { fetchFarms } from "@/lib/api";
import { ReportsClient } from "./ReportsClient";

export const dynamic = "force-dynamic";

export default async function ReportsPage() {
  const farms = await fetchFarms();
  return (
    <div className="space-y-6">
      <header className="no-print">
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--foreground)]">Reports</h1>
        <p className="mt-1 text-sm text-[var(--foreground-muted)]">
          Build a report from live canonical data and export it as CSV or PDF.
        </p>
      </header>
      <ReportsClient farms={farms} />
    </div>
  );
}
