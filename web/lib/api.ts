const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8080";
// The backend is on Render's free plan: it sleeps after ~15 min idle and can take 30-60s to
// cold-start on the next request, on top of genuinely slow analytics queries on a cold cache.
// Bound the wait generously enough to cover a cold start rather than aborting mid-wake-up.
const REQUEST_TIMEOUT_MS = 75000;

async function apiGet<T>(path: string): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(`${API_BASE_URL}${path}`, { cache: "no-store", signal: controller.signal });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`GET ${path} failed (${res.status}): ${body}`);
    }
    return (await res.json()) as T;
  } finally {
    clearTimeout(timeout);
  }
}

// Like apiGet, but a genuine 404 (resource not found) resolves to null instead of throwing --
// callers can still tell that apart from a timeout/network failure, which rethrows.
async function apiGetOrNull<T>(path: string): Promise<T | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(`${API_BASE_URL}${path}`, { cache: "no-store", signal: controller.signal });
    if (res.status === 404) return null;
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`GET ${path} failed (${res.status}): ${body}`);
    }
    return (await res.json()) as T;
  } finally {
    clearTimeout(timeout);
  }
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`POST ${path} failed (${res.status}): ${text}`);
  }
  return res.json() as Promise<T>;
}

async function apiPostForm<T>(path: string, form: FormData): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, { method: "POST", body: form });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`POST ${path} failed (${res.status}): ${text}`);
  }
  return res.json() as Promise<T>;
}

export type Farm = {
  id: string;
  name?: string;
  organization_id?: string;
  location_text?: string;
  metadata_json?: string;
  [key: string]: unknown;
};

export type Observation = {
  id: string;
  farm_id?: string;
  animal_id?: string | null;
  metric?: string;
  value_num?: number;
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

export type OverviewPayload = {
  cards: Record<string, string>;
  system_cards: { title: string; value: string; status: string; color: string; icon: string }[];
  [key: string]: unknown;
};

export type MarketFinancePayload = {
  status: string;
  message?: string;
  summary_df?: Record<string, unknown>[];
  milk_vs_feed_chart?: Record<string, unknown>[];
  profitability_metrics?: Record<string, unknown>;
  profitability_outlook?: string;
  [key: string]: unknown;
};

export type FeedEnvironmentPayload = {
  status: string;
  message?: string;
  timeseries?: Record<string, unknown>[];
  current_metrics?: Record<string, unknown>[];
  remote_sensing?: Record<string, unknown>;
  live_weather?: Record<string, unknown>;
  [key: string]: unknown;
};

export type FarmProfilePayload = Record<string, unknown> | null;
export type AnimalProfilePayload = Record<string, unknown> | null;

export function fetchFarms() {
  return apiGet<{ farms: Farm[] }>("/farms").then((r) => r.farms);
}

export function createFarm(input: {
  name: string;
  location?: string;
  region?: string;
  species?: string;
  sensorSystem?: string;
  contact?: string;
  notes?: string;
}) {
  return apiPostJson<Farm>("/farms", input);
}

export function fetchObservations(opts: { limit?: number; farmId?: string } = {}) {
  const params = new URLSearchParams();
  if (opts.limit) params.set("limit", String(opts.limit));
  if (opts.farmId) params.set("farm_id", opts.farmId);
  return apiGet<{ observations: Observation[] }>(`/observations?${params}`).then((r) => r.observations);
}

export function fetchAnimals(opts: { limit?: number; farmId?: string } = {}) {
  const params = new URLSearchParams();
  if (opts.limit) params.set("limit", String(opts.limit));
  if (opts.farmId) params.set("farm_id", opts.farmId);
  return apiGet<{ animals: Record<string, unknown>[] }>(`/animals?${params}`).then((r) => r.animals);
}

export function fetchAlerts(opts: { limit?: number; farmId?: string } = {}) {
  const params = new URLSearchParams();
  if (opts.limit) params.set("limit", String(opts.limit));
  if (opts.farmId) params.set("farm_id", opts.farmId);
  return apiGet<{ alerts: Alert[] }>(`/alerts?${params}`).then((r) => r.alerts);
}

export function fetchDataQuality() {
  return apiGet<DataQualitySummary>("/data-quality");
}

export function fetchOverview(farmId?: string) {
  const q = farmId ? `?farm_id=${encodeURIComponent(farmId)}` : "";
  return apiGet<OverviewPayload>(`/overview${q}`);
}

export function fetchFarmProfile(farmId: string) {
  return apiGetOrNull<FarmProfilePayload>(`/farms/${encodeURIComponent(farmId)}/profile`);
}

export function fetchMarketFinance() {
  return apiGet<MarketFinancePayload>("/market-finance");
}

export function fetchFeedEnvironment(farmId?: string) {
  const q = farmId ? `?farm_id=${encodeURIComponent(farmId)}` : "";
  return apiGet<FeedEnvironmentPayload>(`/feed-environment${q}`);
}

export function fetchAnimalProfile(animalId: string, farmId?: string) {
  const q = farmId ? `?farm_id=${encodeURIComponent(farmId)}` : "";
  return apiGetOrNull<AnimalProfilePayload>(`/animals/${encodeURIComponent(animalId)}/profile${q}`);
}

export function fetchAnimalTimeseries(animalId: string, metrics: string[], farmId?: string) {
  const params = new URLSearchParams();
  if (metrics.length) params.set("metrics", metrics.join(","));
  if (farmId) params.set("farm_id", farmId);
  const q = params.toString() ? `?${params}` : "";
  return apiGet<{ records: Record<string, unknown>[] }>(
    `/animals/${encodeURIComponent(animalId)}/timeseries${q}`,
  ).then((r) => r.records);
}

export function fetchOutcomes() {
  return apiGet<Record<string, unknown>>("/outcomes");
}

export function uploadTelemetry(input: { file: File; farmId: string; connectorKey: string; sourceSystem: string }) {
  const form = new FormData();
  form.set("file", input.file);
  form.set("farm_id", input.farmId);
  form.set("connector_key", input.connectorKey);
  form.set("source_system", input.sourceSystem);
  return apiPostForm<Record<string, unknown>>("/ingestion/upload", form);
}
