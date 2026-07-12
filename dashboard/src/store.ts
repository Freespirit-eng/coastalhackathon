import { create } from "zustand";
import type { AlertMessage, ThroughputStat, VesselPosition, Zone } from "./types";

type VesselsById = Record<string, VesselPosition>;

interface DashboardState {
  zones: Zone[];
  vesselsById: VesselsById;
  alerts: AlertMessage[];
  throughput: ThroughputStat | null;
  selectedAlertId: string | null;
  setZones: (zones: Zone[]) => void;
  setInitialAlerts: (alerts: AlertMessage[]) => void;
  upsertVessels: (vessels: VesselPosition[]) => void;
  pushAlert: (alert: AlertMessage) => void;
  setThroughput: (stat: ThroughputStat) => void;
  selectAlert: (alertId: string | null) => void;
}

const MAX_ALERTS = 500;

export const useDashboardStore = create<DashboardState>((set) => ({
  zones: [],
  vesselsById: {},
  alerts: [],
  throughput: null,
  selectedAlertId: null,
  setZones: (zones) => set({ zones }),
  setInitialAlerts: (alerts) =>
    set({
      alerts: [...alerts].sort((a, b) => b.ts_unix_ms - a.ts_unix_ms).slice(0, MAX_ALERTS)
    }),
  upsertVessels: (vessels) =>
    set((state) => {
      const next = { ...state.vesselsById };
      for (const vessel of vessels) {
        next[vessel.vessel_id] = vessel;
      }
      return { vesselsById: next };
    }),
  pushAlert: (alert) =>
    set((state) => ({
      alerts: [alert, ...state.alerts].slice(0, MAX_ALERTS)
    })),
  setThroughput: (stat) => set({ throughput: stat }),
  selectAlert: (alertId) => set({ selectedAlertId: alertId })
}));
