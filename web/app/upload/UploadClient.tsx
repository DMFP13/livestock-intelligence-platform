"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { createFarm, uploadTelemetry, type Farm } from "@/lib/api";
import { Button, Panel } from "@/components/ui";

function UploadPanel({
  title,
  connectorKey,
  sourceSystem,
  farms,
}: {
  title: string;
  connectorKey: string;
  sourceSystem: string;
  farms: Farm[];
}) {
  const [farmId, setFarmId] = useState(farms[0]?.id ?? "");
  const [file, setFile] = useState<File | null>(null);
  const [status, setStatus] = useState<"idle" | "uploading" | "done" | "error">("idle");
  const [message, setMessage] = useState<string>("");
  const [dragOver, setDragOver] = useState(false);
  const router = useRouter();

  async function submit() {
    if (!file || !farmId) {
      setMessage("Choose a farm and a file first.");
      setStatus("error");
      return;
    }
    setStatus("uploading");
    setMessage("");
    try {
      const result = await uploadTelemetry({ file, farmId, connectorKey, sourceSystem });
      setStatus("done");
      setMessage(`Processed ${result.rows_stored ?? result.rows_valid ?? "?"} rows.`);
      router.refresh();
    } catch (err) {
      setStatus("error");
      setMessage(err instanceof Error ? err.message : "Upload failed.");
    }
  }

  return (
    <Panel title={title}>
      <div className="space-y-3">
        <select
          value={farmId}
          onChange={(e) => setFarmId(e.target.value)}
          className="w-full rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm"
        >
          {farms.length === 0 && <option value="">No farms registered</option>}
          {farms.map((f) => (
            <option key={f.id} value={f.id}>
              {f.name ?? f.id}
            </option>
          ))}
        </select>

        <label
          onDragOver={(e) => {
            e.preventDefault();
            setDragOver(true);
          }}
          onDragLeave={() => setDragOver(false)}
          onDrop={(e) => {
            e.preventDefault();
            setDragOver(false);
            const f = e.dataTransfer.files?.[0];
            if (f) setFile(f);
          }}
          className={`flex cursor-pointer flex-col items-center justify-center rounded-lg border-2 border-dashed px-4 py-8 text-center text-sm transition-colors ${
            dragOver ? "border-[var(--primary)] bg-[var(--primary-soft)]" : "border-[var(--border)]"
          }`}
        >
          <input
            type="file"
            accept=".csv,.xlsx,.xls"
            className="hidden"
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          />
          <span className="font-medium text-[var(--foreground)]">
            {file ? file.name : "Drag & drop a .csv or .xlsx file, or click to browse"}
          </span>
        </label>

        <Button onClick={submit} disabled={status === "uploading" || !file || !farmId}>
          {status === "uploading" ? "Uploading…" : "Upload & process"}
        </Button>

        {message && (
          <p className={`text-sm ${status === "error" ? "text-[var(--danger)]" : "text-[var(--ok)]"}`}>{message}</p>
        )}
      </div>
    </Panel>
  );
}

function AddFarmPanel() {
  const [form, setForm] = useState({
    name: "",
    location: "",
    region: "",
    species: "",
    sensorSystem: "",
    contact: "",
    notes: "",
  });
  const [status, setStatus] = useState<"idle" | "saving" | "done" | "error">("idle");
  const [message, setMessage] = useState("");
  const router = useRouter();

  async function submit() {
    if (!form.name.trim()) {
      setStatus("error");
      setMessage("Farm name is required.");
      return;
    }
    setStatus("saving");
    try {
      const farm = await createFarm(form);
      setStatus("done");
      setMessage(`Registered ${farm.name} (${farm.id}).`);
      setForm({ name: "", location: "", region: "", species: "", sensorSystem: "", contact: "", notes: "" });
      router.refresh();
    } catch (err) {
      setStatus("error");
      setMessage(err instanceof Error ? err.message : "Failed to register farm.");
    }
  }

  const field = (key: keyof typeof form, label: string, placeholder?: string) => (
    <div>
      <label className="mb-1 block text-xs font-medium text-[var(--foreground-muted)]">{label}</label>
      <input
        value={form[key]}
        onChange={(e) => setForm((f) => ({ ...f, [key]: e.target.value }))}
        placeholder={placeholder}
        className="w-full rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm"
      />
    </div>
  );

  return (
    <Panel title="Add a farm">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        {field("name", "Name *", "Jos Plateau Dairy")}
        {field("location", "Location", "Jos, Plateau State")}
        {field("region", "Region", "North Central")}
        {field("species", "Species", "dairy_cattle")}
        {field("sensorSystem", "Sensor system", "BODIT")}
        {field("contact", "Contact")}
      </div>
      <div className="mt-3">
        <label className="mb-1 block text-xs font-medium text-[var(--foreground-muted)]">Notes</label>
        <textarea
          value={form.notes}
          onChange={(e) => setForm((f) => ({ ...f, notes: e.target.value }))}
          rows={2}
          className="w-full rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm"
        />
      </div>
      <div className="mt-4">
        <Button onClick={submit} disabled={status === "saving"}>
          {status === "saving" ? "Registering…" : "Register farm"}
        </Button>
        {message && (
          <p className={`mt-2 text-sm ${status === "error" ? "text-[var(--danger)]" : "text-[var(--ok)]"}`}>
            {message}
          </p>
        )}
      </div>
    </Panel>
  );
}

export function UploadClient({ farms }: { farms: Farm[] }) {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <UploadPanel title="BODIT telemetry" connectorKey="sensor_upload" sourceSystem="api_upload_telemetry" farms={farms} />
        <UploadPanel title="Milk production" connectorKey="sensor_upload" sourceSystem="api_upload_milk" farms={farms} />
      </div>
      <AddFarmPanel />
      <Panel title="Expected format">
        <p className="text-sm text-[var(--foreground-muted)]">
          CSV or Excel with a cow/animal identifier column and a timestamp column, plus one column per
          telemetry metric (activity, rumination, eating, resting minutes, milk yield, etc). Unrecognised
          columns are ignored; malformed rows are quarantined rather than silently dropped — check the
          ingestion run summary returned after upload.
        </p>
      </Panel>
    </div>
  );
}
