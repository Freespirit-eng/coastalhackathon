export type Severity = "LOW" | "MEDIUM" | "HIGH";
export type EventType = "ZONE_ENTER" | "ZONE_HEARTBEAT" | "ZONE_EXIT";

export interface VesselPosition {
  vessel_id: string;
  lat: number;
  lon: number;
  sog: number;
  cog: number;
}

export interface AlertMessage {
  alert_id: string;
  vessel_id: string;
  zone_id: string;
  zone_name: string;
  severity: Severity;
  event_type: EventType;
  lat: number;
  lon: number;
  ts_unix_ms: number;
}

export interface ThroughputStat {
  msgs_per_sec: number;
  active_vessels: number;
  ts_unix_ms: number;
}

export interface Zone {
  zone_id: string;
  name: string;
  severity: Severity;
  polygon: Array<[number, number]>;
}

export type WsEnvelope =
  | { type: "ALERT"; payload: AlertMessage }
  | {
      type: "POSITION_BATCH";
      payload: { vessels: VesselPosition[]; ts_unix_ms: number };
    }
  | { type: "THROUGHPUT_STAT"; payload: ThroughputStat };
