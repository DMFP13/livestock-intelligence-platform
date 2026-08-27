import { fetchFarms } from "@/lib/api";
import { UploadClient } from "./UploadClient";

export const dynamic = "force-dynamic";

export default async function UploadPage() {
  const farms = await fetchFarms();
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--foreground)]">Upload Data</h1>
        <p className="mt-1 text-sm text-[var(--foreground-muted)]">
          Register farms and ingest telemetry or milk production files into the canonical store.
        </p>
      </header>
      <UploadClient farms={farms} />
    </div>
  );
}
