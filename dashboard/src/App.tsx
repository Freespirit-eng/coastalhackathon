import { useEffect, useMemo, useState } from "react";
import DeckGL from "@deck.gl/react";
import { ScatterplotLayer, PolygonLayer, IconLayer } from "@deck.gl/layers";
import type { PickingInfo } from "@deck.gl/core";
import { Map } from "react-map-gl/maplibre";
import { fetchAlerts, fetchZones } from "./api";
import { useDashboardStore } from "./store";
import { connectStream } from "./ws";
import type { AlertMessage, Severity, VesselPosition, Zone } from "./types";

interface Toast {
  id: string;
  alert: AlertMessage;
}

const ALERT_ICON_DATA_URL =
  "data:image/svg+xml;charset=utf-8," +
  encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64"><circle cx="32" cy="32" r="28" fill="#ff5f56" stroke="#ffffff" stroke-width="6"/></svg>`
  );

const MAP_STYLE = "https://demotiles.maplibre.org/style.json";

function isMapState(
  value: unknown
): value is { longitude: number; latitude: number; zoom: number; pitch: number; bearing: number } {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.longitude === "number" &&
    typeof candidate.latitude === "number" &&
    typeof candidate.zoom === "number" &&
    typeof candidate.pitch === "number" &&
    typeof candidate.bearing === "number"
  );
}

export default function App() {
  const zones = useDashboardStore((s) => s.zones);
  const alerts = useDashboardStore((s) => s.alerts);
  const vesselsById = useDashboardStore((s) => s.vesselsById);
  const throughput = useDashboardStore((s) => s.throughput);
  const selectedAlertId = useDashboardStore((s) => s.selectedAlertId);
  const setZones = useDashboardStore((s) => s.setZones);
  const setInitialAlerts = useDashboardStore((s) => s.setInitialAlerts);
  const upsertVessels = useDashboardStore((s) => s.upsertVessels);
  const pushAlert = useDashboardStore((s) => s.pushAlert);
  const setThroughput = useDashboardStore((s) => s.setThroughput);
  const selectAlert = useDashboardStore((s) => s.selectAlert);

  const [viewState, setViewState] = useState({
    longitude: 112,
    latitude: 10,
    zoom: 4.3,
    pitch: 0,
    bearing: 0
  });
  const [connected, setConnected] = useState(false);
  const [zoneFilter, setZoneFilter] = useState("");
  const [vesselFilter, setVesselFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState<"ALL" | Severity>("ALL");
  const [toasts, setToasts] = useState<Toast[]>([]);

  useEffect(() => {
    const ctrl = new AbortController();
    fetchZones(ctrl.signal)
      .then(setZones)
      .catch((error: Error) => console.error(error.message));
    fetchAlerts(ctrl.signal)
      .then(setInitialAlerts)
      .catch((error: Error) => console.error(error.message));

    const zoneRefresh = window.setInterval(() => {
      fetchZones().then(setZones).catch(() => undefined);
    }, 30000);

    return () => {
      ctrl.abort();
      window.clearInterval(zoneRefresh);
    };
  }, [setInitialAlerts, setZones]);

  useEffect(() => {
    return connectStream(
      (message) => {
        if (message.type === "POSITION_BATCH") {
          upsertVessels(message.payload.vessels);
          return;
        }
        if (message.type === "THROUGHPUT_STAT") {
          setThroughput(message.payload);
          return;
        }
        pushAlert(message.payload);
        const toast: Toast = { id: message.payload.alert_id, alert: message.payload };
        setToasts((prev) => [toast, ...prev].slice(0, 6));
        window.setTimeout(() => {
          setToasts((prev) => prev.filter((t) => t.id !== toast.id));
        }, 4500);
      },
      setConnected
    );
  }, [pushAlert, setThroughput, upsertVessels]);

  const vessels = useMemo(() => Object.values(vesselsById), [vesselsById]);

  const filteredAlerts = useMemo(() => {
    return alerts.filter((a) => {
      if (zoneFilter && !a.zone_name.toLowerCase().includes(zoneFilter.toLowerCase())) {
        return false;
      }
      if (vesselFilter && !a.vessel_id.toLowerCase().includes(vesselFilter.toLowerCase())) {
        return false;
      }
      if (severityFilter !== "ALL" && a.severity !== severityFilter) {
        return false;
      }
      return true;
    });
  }, [alerts, zoneFilter, vesselFilter, severityFilter]);

  const filteredVessels = useMemo(() => {
    if (!vesselFilter) {
      return vessels;
    }
    const q = vesselFilter.toLowerCase();
    return vessels.filter((v) => v.vessel_id.toLowerCase().includes(q));
  }, [vesselFilter, vessels]);

  const layers = useMemo(() => {
    const zoneLayer = new PolygonLayer<Zone>({
      id: "zones",
      data: zones,
      getPolygon: (z) => z.polygon.map(([lat, lon]) => [lon, lat]),
      stroked: true,
      filled: true,
      getLineColor: [255, 189, 46, 230],
      getFillColor: [255, 189, 46, 40],
      lineWidthMinPixels: 2
    });

    const vesselLayer = new ScatterplotLayer<VesselPosition>({
      id: "vessels",
      data: filteredVessels,
      getPosition: (v) => [v.lon, v.lat],
      getRadius: 850,
      radiusUnits: "meters",
      getFillColor: [56, 182, 255, 220],
      getLineColor: [214, 238, 255, 255],
      lineWidthMinPixels: 1,
      stroked: true,
      pickable: true
    });

    const recentAlerts = filteredAlerts.slice(0, 50);
    const alertLayer = new IconLayer<AlertMessage>({
      id: "alert-markers",
      data: recentAlerts,
      pickable: true,
      getPosition: (a) => [a.lon, a.lat],
      getIcon: () => ({
        url: ALERT_ICON_DATA_URL,
        width: 64,
        height: 64,
        anchorY: 64
      }),
      getSize: (a) => (a.severity === "HIGH" ? 28 : a.severity === "MEDIUM" ? 22 : 18),
      sizeScale: 1
    });

    return [zoneLayer, vesselLayer, alertLayer];
  }, [zones, filteredVessels, filteredAlerts]);

  const onAlertClick = (alert: AlertMessage) => {
    selectAlert(alert.alert_id);
    setViewState((vs) => ({
      ...vs,
      longitude: alert.lon,
      latitude: alert.lat,
      zoom: Math.max(vs.zoom, 8)
    }));
  };

  const onMapClick = (info: PickingInfo) => {
    const selected = info.object as AlertMessage | undefined;
    if (selected?.alert_id) {
      onAlertClick(selected);
    }
  };

  return (
    <div className="app">
      <DeckGL
        viewState={viewState}
        controller
        layers={layers}
        onViewStateChange={({ viewState: next }) => {
          if (isMapState(next)) {
            setViewState(next);
          }
        }}
        onClick={onMapClick}
      >
        <Map mapStyle={MAP_STYLE} />
      </DeckGL>

      <section className="panel hud">
        <h1>AISentry Live Control Dashboard</h1>
        <div className="stats">
          <div className="metric">
            <div className="label">Msgs/sec</div>
            <div className="value">{throughput?.msgs_per_sec.toLocaleString() ?? "—"}</div>
          </div>
          <div className="metric">
            <div className="label">Active Vessels</div>
            <div className="value">{throughput?.active_vessels.toLocaleString() ?? vessels.length.toLocaleString()}</div>
          </div>
        </div>

        <div className="filters">
          <input value={zoneFilter} onChange={(e) => setZoneFilter(e.target.value)} placeholder="Filter zone..." />
          <input
            value={vesselFilter}
            onChange={(e) => setVesselFilter(e.target.value)}
            placeholder="Filter vessel..."
          />
          <select value={severityFilter} onChange={(e) => setSeverityFilter(e.target.value as "ALL" | Severity)}>
            <option value="ALL">All severities</option>
            <option value="HIGH">HIGH</option>
            <option value="MEDIUM">MEDIUM</option>
            <option value="LOW">LOW</option>
          </select>
        </div>

        <div className="connection">
          <span className={`dot ${connected ? "online" : "offline"}`} />
          {connected ? "WebSocket connected" : "WebSocket disconnected"}
        </div>
      </section>

      <section className="panel alerts">
        <h2>Alert Feed ({filteredAlerts.length})</h2>
        <div className="alerts-list">
          {filteredAlerts.slice(0, 150).map((alert) => (
            <div
              key={alert.alert_id}
              className={`alert-item sev-${alert.severity}`}
              style={alert.alert_id === selectedAlertId ? { borderColor: "#7ab8ff" } : undefined}
              onClick={() => onAlertClick(alert)}
            >
              <div className="row">
                <strong>{alert.zone_name}</strong>
                <span>{alert.severity}</span>
              </div>
              <div className="row">
                <span>{alert.event_type}</span>
                <span>{new Date(alert.ts_unix_ms).toLocaleTimeString()}</span>
              </div>
              <div className="row">
                <span>{alert.vessel_id}</span>
                <span>
                  {alert.lat.toFixed(3)}, {alert.lon.toFixed(3)}
                </span>
              </div>
            </div>
          ))}
        </div>
      </section>

      <div className="toast-stack">
        {toasts.map(({ id, alert }) => (
          <div key={id} className="toast" data-severity={alert.severity}>
            <div className="toast-title">
              {alert.severity} {alert.event_type}
            </div>
            <div className="toast-subtitle">{alert.zone_name}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
