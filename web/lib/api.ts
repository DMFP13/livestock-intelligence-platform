const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8080";

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, { cache: "no-store" });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`GET ${path} failed (${res.status}): ${body}`);
  }
  return res.json() as Promise<T>;
}

export type Farm = {
  id: string;
  name?: string;
  organization_id?: string;
  [key: string]: unknown;
};

export type Observation = {
  id: string;
  farm_id?: string;
  animal_id?: string | null;
  metric_key?: string;
  value?: number;
  observed_at?: string;
  quality_flag?: string;
  source_system?: string;
  [key: string]: unknown;
};

export type Alert = {
  id: string;
  farm_id?: string;
  animal_id?: string | null;
  severity?: string;
  message?: string;
  [key: string]: unknown;
};

export type DataQualitySummary = {
  latest_run: Record<string, unknown> | null;
  quality_flags: Record<string, number>;
};

export function fetchFarms() {
  return apiGet<{ farms: Farm[] }>("/farms").then((r) => r.farms);
}

export function fetchObservations(limit = 200) {
  return apiGet<{ observations: Observation[] }>(`/observations?limit=${limit}`).then(
    (r) => r.observations,
  );
}

export function fetchAlerts(limit = 200) {
  return apiGet<{ alerts: Alert[] }>(`/alerts?limit=${limit}`).then((r) => r.alerts);
}

export function fetchDataQuality() {
  return apiGet<DataQualitySummary>("/data-quality");
}

export function fetchHealth() {
  return apiGet<{ status: string }>("/health");
}
