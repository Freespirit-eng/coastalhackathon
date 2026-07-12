import fs from "node:fs";
import path from "node:path";
import { WebSocketServer } from "ws";
import { encode } from "@msgpack/msgpack";

const PORT = Number(process.env.MOCK_WS_PORT ?? 9003);
const STREAM_PATH = "/stream";
const tickMs = 100;

const zonesFile = path.resolve(process.cwd(), "..", "contracts", "demo_zones.json");
const zones = JSON.parse(fs.readFileSync(zonesFile, "utf8"));

const vessels = Array.from({ length: 1500 }).map((_, idx) => ({
  vessel_id: `vessel-${String(idx + 1).padStart(5, "0")}`,
  lat: 3 + Math.random() * 14,
  lon: 101 + Math.random() * 18,
  sog: 3 + Math.random() * 17,
  cog: Math.random() * 360
}));

function randomSeverity() {
  const roll = Math.random();
  if (roll > 0.75) return "HIGH";
  if (roll > 0.35) return "MEDIUM";
  return "LOW";
}

function sendFrame(client, frame) {
  client.send(encode(frame));
}

function emitBatches(client) {
  const batchSize = 350;
  const updated = [];
  for (let i = 0; i < batchSize; i += 1) {
    const index = Math.floor(Math.random() * vessels.length);
    const vessel = vessels[index];
    vessel.lat += (Math.random() - 0.5) * 0.04;
    vessel.lon += (Math.random() - 0.5) * 0.04;
    vessel.cog = (vessel.cog + (Math.random() - 0.5) * 8 + 360) % 360;
    updated.push(vessel);
  }

  sendFrame(client, {
    type: "POSITION_BATCH",
    payload: {
      vessels: updated,
      ts_unix_ms: Date.now()
    }
  });

  if (Math.random() > 0.82) {
    const z = zones[Math.floor(Math.random() * zones.length)];
    const p = z.polygon[0];
    const target = updated[Math.floor(Math.random() * updated.length)];
    sendFrame(client, {
      type: "ALERT",
      payload: {
        alert_id: `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`,
        vessel_id: target.vessel_id,
        zone_id: z.zone_id,
        zone_name: z.name,
        severity: randomSeverity(),
        event_type: Math.random() > 0.5 ? "ZONE_ENTER" : "ZONE_HEARTBEAT",
        lat: p[0] + (Math.random() - 0.5) * 0.3,
        lon: p[1] + (Math.random() - 0.5) * 0.3,
        ts_unix_ms: Date.now()
      }
    });
  }
}

const wss = new WebSocketServer({
  port: PORT,
  path: STREAM_PATH
});

wss.on("connection", (socket) => {
  const interval = setInterval(() => {
    if (socket.readyState !== socket.OPEN) {
      clearInterval(interval);
      return;
    }
    emitBatches(socket);
  }, tickMs);

  const throughputInterval = setInterval(() => {
    if (socket.readyState !== socket.OPEN) {
      clearInterval(throughputInterval);
      return;
    }
    sendFrame(socket, {
      type: "THROUGHPUT_STAT",
      payload: {
        msgs_per_sec: 50000 + Math.floor(Math.random() * 6000),
        active_vessels: vessels.length,
        ts_unix_ms: Date.now()
      }
    });
  }, 1000);
});

console.log(`Mock stream server listening on ws://localhost:${PORT}${STREAM_PATH}`);
