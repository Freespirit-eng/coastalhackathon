import type { AlertMessage, Zone } from "./types";

const ZONES_ENDPOINT = import.meta.env.VITE_ZONES_ENDPOINT ?? "http://localhost:9002/api/zones";
const ALERTS_ENDPOINT = import.meta.env.VITE_ALERTS_ENDPOINT ?? "http://localhost:9003/api/alerts";

export async function fetchZones(signal?: AbortSignal): Promise<Zone[]> {
  const response = await fetch(ZONES_ENDPOINT, { signal });
  if (!response.ok) {
    throw new Error(`Failed to load zones: HTTP ${response.status}`);
  }
  const zones = (await response.json()) as Zone[];
  return zones;
}

export async function fetchAlerts(signal?: AbortSignal): Promise<AlertMessage[]> {
  const response = await fetch(ALERTS_ENDPOINT, { signal });
  if (!response.ok) {
    throw new Error(`Failed to load alerts: HTTP ${response.status}`);
  }
  const alerts = (await response.json()) as AlertMessage[];
  return alerts;
}
