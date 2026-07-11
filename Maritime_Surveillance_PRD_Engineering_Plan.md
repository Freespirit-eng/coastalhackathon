# AISentry — Real-Time Maritime Illegal Fishing Detection System
## Product Requirement Document & Engineering Execution Plan

**Version:** 1.0
**Doc Owner:** Principal Architecture / PM
**Status:** Ready for team kickoff

---

## 1. Executive Summary

AISentry ingests simulated AIS (Automatic Identification System) vessel location messages at **50,000+ msgs/sec**, cross-references each position against restricted/illegal-fishing geofences in **single-digit milliseconds**, and streams live vessel movement + alerts to a MarineTraffic-style dashboard — without touching Flink, Spark, or Kafka Streams. The entire system is a **single compact binary cluster** (4 decoupled services) built for one machine or a small fleet of machines, communicating over lightweight, well-typed contracts so four engineers can build in parallel from day one.

---

## 2. Core Architecture Design

### 2.1 Why "No Heavy Pipelines" Is Actually a Performance Win

Flink/Spark/Kafka Streams are built to solve *distributed, fault-tolerant, exactly-once, multi-node* stream processing at internet scale. That machinery (checkpointing, shuffle, JVM GC pressure, serialization overhead, network hops between operators) is precisely what kills tail latency. For a single-region, memory-resident geofencing workload, an **in-process, lock-light, shared-nothing worker pool** written in Go or Rust will comfortably beat a JVM-based distributed pipeline on both throughput and p99 latency, while being radically simpler to run, debug, and hand to four engineers.

### 2.2 Proposed Tech Stack

| Layer | Technology | Rationale |
|---|---|---|
| Simulator | **Go** (goroutines) | Cheap concurrency, fast to generate synthetic AIS tracks at high fan-out |
| Ingestion transport | **UDP or raw TCP with length-prefixed binary frames** (fallback: gRPC streaming) | Avoids HTTP/JSON parsing overhead on the hot path |
| Ingestion + Spatial Engine | **Rust** (tokio + rayon) or **Go** (if team is Go-native) | Zero-GC-pause option (Rust) for the latency-critical core; Go is acceptable if p99 targets are relaxed slightly |
| Spatial index | **In-memory H3 (Uber's hexagonal grid) hash index**, backed by a flat `HashMap<H3Index, Vec<ZoneId>>` | O(1) average lookup vs O(log n) for R-Tree; H3 is purpose-built for point-in-polygon-adjacent geofencing at speed |
| Geofence polygon precision check | **R-Tree (rstar crate / go-geoindex)** for boundary cells only | H3 gives a fast coarse filter; R-Tree + ray-casting only runs on the ~1-3% of points that land in a "boundary" hex, keeping average-case cost near-zero |
| Internal event bus | **In-process lock-free ring buffer / MPSC channel** (Rust: `crossbeam-channel`; Go: buffered channels + worker pool) — **not** an external broker | Removes network hop + serialization between ingestion and alerting; this is the key trick that gets us past 50k/sec on commodity hardware |
| Alert persistence / recent-state store | **Redis** (or embedded **SQLite/RocksDB** if zero external deps is preferred) | Sub-ms writes, TTL-based recent-track cache, alert history |
| UI transport | **WebSockets** (native `ws` in Go/Rust, e.g. `tokio-tungstenite`) with **binary/MessagePack** frames, batched at 10-20Hz | JSON is fine for alerts (low volume); vessel position deltas should be binary-packed and batched to avoid flooding the browser |
| Frontend | **React + deck.gl or MapLibre GL (WebGL)**, Canvas fallback | WebGL is required to render thousands of moving icons at 30-60fps; deck.gl's `ScatterplotLayer`/`IconLayer` with GPU-side interpolation is purpose-built for this |
| Serialization on the wire | **Protobuf or FlatBuffers** for simulator→ingestion; **MessagePack/binary** for ingestion→UI | Avoids JSON parse cost at 50k/sec |

### 2.3 End-to-End Data Flow

```
[Engineer 1: Simulator]                 [Engineer 2: Spatial Engine]
  N goroutines/threads                    Ingestion socket listener
  generate AIS-like packets    ---UDP/gRPC-stream--->  Deserialize (binary)
  (vessel_id, lat, lon, sog,                            |
   cog, ts) at 50k+/sec                                 v
                                          H3 cell lookup (coarse filter)
                                                |
                                    [boundary cell?] --No--> discard/pass-through
                                                |Yes
                                                v
                                 R-Tree precise point-in-polygon check
                                                |
                                    [violation?] --No--> update live-position cache
                                                |Yes
                                                v
                          -----internal lock-free channel----->
                                                            [Engineer 3: Event Broker]
                                                     Alert object built (zone, vessel,
                                                     severity, timestamp, geo)
                                                            |
                                              write-through to Redis/RocksDB
                                              (alert history + last-known-position)
                                                            |
                                            fan-out over WebSocket hub (pub/sub)
                                                            |
                                                            v
                                                [Engineer 4: Dashboard]
                                          WebSocket client -> decode binary frames
                                          -> deck.gl/MapLibre GPU layers
                                          -> alert toast/panel + zone overlay
```

Position updates that do **not** violate a geofence still flow to the dashboard (batched) so the live map shows normal traffic, not just violators — this is what makes the demo visually convincing at scale.

### 2.4 How We Hit 50k msgs/sec on Commodity Hardware

1. **Batch, don't syscall-per-message.** The simulator sends fixed-size binary batches (e.g., 256 AIS records per UDP datagram or gRPC stream chunk) instead of one message per network write. This alone is typically a 10-50x throughput multiplier over naive per-message sends.
2. **Zero-copy / pre-allocated buffers.** Ingestion reads directly into a reusable byte buffer pool (Rust: `bytes::BytesMut` pool; Go: `sync.Pool`) — no per-message heap allocation.
3. **H3 coarse filter before any geometry math.** Point-in-polygon against dozens of restricted zones is expensive if run on every point. Precomputing which H3 cells (resolution ~7-8, ~1-5km hexagons) fully overlap, partially overlap, or never overlap a zone means **>95% of incoming points resolve with a single hashmap lookup** and skip geometry entirely.
4. **Sharded worker pool keyed by vessel_id or H3 cell.** N workers (N = CPU cores) each own a shard, avoiding lock contention; a vessel's messages are routed to the same worker for cache-friendly sequential state updates.
5. **In-process channel, not a network broker.** Passing an alert from the spatial engine to the alerting pipeline via an in-memory channel is nanoseconds vs. milliseconds through Kafka/Redis pub-sub. External stores are used only for *persistence*, never on the synchronous hot path.
6. **Backpressure-aware batched WebSocket fan-out.** The UI doesn't need 50k updates/sec rendered — it needs to *look* real-time. Position deltas are coalesced per vessel and flushed to clients at a fixed 10-20Hz tick, decoupling ingestion throughput from render throughput entirely.

**Expected single-node envelope (8-16 core commodity server):** 50k-150k msgs/sec sustained ingestion + geofence evaluation, p99 ingestion-to-alert-decision latency under 5ms, independent of the (deliberately throttled) UI render rate.

---

## 3. Product Requirement Document

### 3.1 Product Scope & User Stories

**In scope:** simulated AIS ingestion, geofence rule engine, real-time alerting, live map visualization, alert history.
**Out of scope (v1):** real satellite AIS feed integration, multi-region/multi-datacenter deployment, ML-based anomaly detection (dark-vessel prediction), user auth/RBAC (stub only).

**Primary user stories:**

- *As a maritime control-room operator*, I want to see all simulated vessels moving on a live map so I can maintain situational awareness of regional traffic.
- *As an operator*, I want to be alerted within milliseconds when a vessel enters a restricted/illegal-fishing zone, with vessel ID, coordinates, and zone name, so I can dispatch enforcement.
- *As an operator*, I want to see historical alerts for a vessel or zone so I can identify repeat offenders.
- *As a system administrator*, I want to define/edit restricted zones (polygons) without redeploying the spatial engine, so operational rules can change quickly.
- *As an engineer*, I want each module independently runnable against mocked inputs so I can develop and test without waiting on the other three modules.

### 3.2 Functional Requirements

**FR-1 — Simulator**
- Generate configurable N concurrent simulated vessels (target: tunable up to 100k+ virtual vessels) each emitting position updates (lat, lon, speed-over-ground, course-over-ground, vessel_id, mmsi, timestamp) at a configurable rate.
- Support scripted "violation" vessels that deliberately transit through known restricted zones, for demo/testing determinism.
- Support realistic movement models (straight-line + bounded random walk) so tracks look plausible on the map, not teleporting.
- Emit batched binary frames over UDP or gRPC stream to the ingestion endpoint.

**FR-2 — Ingestion & Spatial Engine**
- Accept binary AIS frames on a dedicated socket/stream.
- Deserialize and validate (reject malformed/out-of-range lat-lon) without blocking the hot path (invalid records counted, not exception-thrown).
- Maintain an in-memory H3-indexed geofence zone table, hot-reloadable from a config/API without restart.
- Classify each point in O(1) average time; escalate to R-Tree precision check only for boundary-ambiguous cells.
- Maintain last-known-position cache per vessel for dashboard "current fleet state" queries.
- Publish violation events and (batched) position updates to the internal event bus.

**FR-3 — Event Broker & Alerting Pipeline**
- Consume violation events from the spatial engine over an in-process channel.
- Enrich event (zone metadata, severity tier, dedup/debounce so one vessel loitering in a zone doesn't spam 500 alerts/sec — emit on zone-entry edge + heartbeat every N seconds while inside).
- Persist alert to store (Redis/RocksDB) with TTL-based recent window + durable history log.
- Fan out alerts and batched position deltas to all connected WebSocket clients via pub/sub hub.
- Expose a REST/gRPC query API for historical alert lookup (by vessel, zone, time range).

**FR-4 — Visualization Layer**
- Connect to WebSocket feed; render live vessel positions on a WebGL map (deck.gl/MapLibre), targeting smooth rendering of 5,000-20,000 concurrent icons.
- Render restricted-zone polygons as persistent overlays.
- Surface alerts as toast notifications + a live-updating alert feed panel, with click-to-locate on map.
- Display real-time ingestion throughput counter (msgs/sec) to visually demonstrate the performance claim.
- Support basic filtering (by zone, by alert severity, by vessel search).

### 3.3 Non-Functional Requirements

| Category | Target |
|---|---|
| Sustained ingestion throughput | ≥ 50,000 msgs/sec on an 8-16 core commodity server |
| Ingestion → geofence decision latency (p99) | < 5 ms |
| Ingestion → alert delivered to WebSocket client (p99) | < 50 ms (dominated by intentional UI batching interval, not compute) |
| Geofence rule capacity | ≥ 500 concurrent restricted-zone polygons without measurable throughput degradation |
| Memory footprint | < 2 GB resident for spatial engine + 100k tracked vessels + 500 zones |
| Dashboard render performance | ≥ 30 fps with 10,000 concurrently rendered vessel icons |
| Reliability | Ingestion service restart recovers geofence rules from persisted config in < 2s; no single component crash takes down more than its own module (process isolation between the 4 services) |
| Observability | Every module exposes a `/metrics` (Prometheus-compatible) endpoint: msgs/sec, p50/p95/p99 latency, queue depth, error counts |
| Horizontal headroom | Architecture allows running N spatial-engine shards behind a simple hash-based load splitter if a single node's ceiling is reached — without introducing Flink/Spark |

---

## 4. Four-Person Decoupled Engineering Breakdown

**Decoupling principle:** each engineer owns a **separate OS process** with a **versioned, language-agnostic wire contract** (protobuf/FlatBuffers schema + WebSocket message spec). Every contract below can be mocked with a static replay file or a stub server on day one — nobody waits on anybody else's code, only on the schema, which is finalized in the first planning session and checked into a shared `/contracts` directory.

---

### Engineer 1 — High-Velocity AIS Simulator & Ingestion Transport

**Core Responsibilities**
- Build the multi-goroutine/thread vessel simulator generating configurable-volume synthetic AIS tracks with realistic movement.
- Implement the network transport layer that batches and streams records to the ingestion endpoint at ≥50k msgs/sec.
- Provide a CLI/config surface to control vessel count, message rate, and scripted violation scenarios.
- Provide a standalone **replay/mock mode** that writes the exact wire-format frames to a local file, so Engineer 2 can develop against static fixtures without a live simulator running.

**Technology & Tools**
- Go (goroutines + channels) for simulator logic.
- Protobuf or FlatBuffers for the AIS record schema.
- UDP (preferred for raw throughput) with a gRPC-streaming fallback mode for environments where UDP is blocked.
- `golang.org/x/time/rate` or custom token-bucket for precise rate control.

**Strict Interface Contract (Simulator → Ingestion)**

```protobuf
// contracts/ais_record.proto
syntax = "proto3";
package aisentry;

message AISRecord {
  string vessel_id   = 1;   // stable UUID per simulated vessel
  string mmsi        = 2;   // 9-digit simulated MMSI
  double lat         = 3;   // -90..90
  double lon         = 4;   // -180..180
  float  sog_knots   = 5;   // speed over ground
  float  cog_degrees = 6;   // course over ground, 0-359
  int64  ts_unix_ms  = 7;   // simulator-side send timestamp
  bool   is_scripted_violator = 8; // testing/demo flag
}

message AISBatch {
  repeated AISRecord records = 1;
  int32 batch_seq = 2;
}
```

- **Transport:** UDP datagrams containing one serialized `AISBatch` each (target 128-512 records/batch), OR a gRPC `stream AISBatch` client call.
- **Delivery guarantee:** best-effort (UDP), matching the philosophy that a dropped position ping is acceptable in a surveillance-density simulation; gRPC mode gives at-least-once if required for demos.
- **Mock artifact for Engineer 2:** `fixtures/sample_batches.bin` — 10,000 pre-serialized `AISBatch` messages committed to the repo on day one.

---

### Engineer 2 — Low-Latency Spatial Engine & Geofencer

**Core Responsibilities**
- Implement the ingestion socket listener (consuming Engineer 1's contract) with zero-allocation hot-path parsing.
- Build and maintain the H3-indexed geofence zone table with hot-reload support.
- Implement the two-tier classification: H3 coarse filter → R-Tree/ray-casting precision check on boundary cells only.
- Maintain the last-known-position cache (per-vessel, TTL-based).
- Publish violation events and batched position snapshots onto the internal event bus for Engineer 3.
- Expose a small admin API (REST or gRPC) to add/update/remove restricted zones at runtime.

**Technology & Tools**
- Rust (tokio for async socket I/O, rayon or manual sharded worker pool for CPU-bound classification) — or Go if the team standardizes on Go throughout.
- `h3` bindings (h3o crate in Rust / h3-go) for hex indexing.
- `rstar` (Rust) or `go-geoindex`/`rtreego` (Go) for the boundary precision layer.
- Internal channel: `crossbeam-channel` (Rust) or buffered Go channels — **in-process only, no network hop**.

**Strict Interface Contract**

*Input:* consumes `AISBatch` per Engineer 1's schema, over UDP socket or gRPC stream, at a configurable bind address (`INGEST_ADDR`, default `0.0.0.0:9001`).

*Output — internal event bus, consumed by Engineer 3:*

```protobuf
// contracts/spatial_event.proto
syntax = "proto3";
package aisentry;

message ZoneViolationEvent {
  string vessel_id     = 1;
  string zone_id       = 2;
  string zone_name     = 3;
  double lat           = 4;
  double lon           = 5;
  int64  ts_unix_ms    = 6;
  string event_type    = 7; // "ZONE_ENTER" | "ZONE_HEARTBEAT" | "ZONE_EXIT"
}

message PositionSnapshot {
  string vessel_id  = 1;
  double lat        = 2;
  double lon        = 3;
  float  sog_knots  = 4;
  float  cog_degrees = 5;
  int64  ts_unix_ms = 6;
}

message PositionBatch {
  repeated PositionSnapshot snapshots = 1;
}
```

*Admin API for zone management (consumed by an ops tool / Engineer 4's admin panel if built):*

```
POST   /api/zones          { zone_id, name, polygon: [[lat,lon], ...], severity }
DELETE /api/zones/{zone_id}
GET    /api/zones          -> [ {zone_id, name, polygon, severity}, ... ]
```

**Mock artifact for Engineer 3:** a standalone Go/Rust stub process that replays `ZoneViolationEvent` and `PositionBatch` messages from a fixture file onto the same channel interface at a controllable rate, so Engineer 3 never needs a live spatial engine running to build against.

---

### Engineer 3 — Event Broker, Alerting Pipeline & State Store

**Core Responsibilities**
- Consume `ZoneViolationEvent` and `PositionBatch` from Engineer 2's internal bus.
- Apply dedup/debounce logic (zone-entry edge detection + periodic heartbeat while inside a zone, rather than firing on every single point).
- Enrich alerts (severity tier, zone metadata) and persist to Redis/RocksDB with a durable alert-history log and a TTL'd recent-alerts cache.
- Own the WebSocket server: hub/pub-sub fan-out of alerts and batched position deltas to all connected dashboard clients.
- Expose a query API for historical alert lookup.

**Technology & Tools**
- Same language as Engineer 2 is easiest for the in-process channel handoff (Rust or Go); if Engineer 3 must be a separate process from Engineer 2 for true process isolation, use a **local Unix domain socket or shared-memory ring buffer** instead of a network broker — still no Kafka/Flink, just a fast IPC boundary.
- Redis (or embedded RocksDB/SQLite) for state + history.
- WebSocket server: `tokio-tungstenite` (Rust) or `gorilla/websocket`/`nhooyr.io/websocket` (Go).
- MessagePack or a compact custom binary format for outbound WS frames.

**Strict Interface Contract**

*Input:* `ZoneViolationEvent` / `PositionBatch` per Engineer 2's schema, via IPC channel/socket at `EVENT_BUS_ADDR`.

*Output — WebSocket protocol to Engineer 4 (the only contract Engineer 4 needs to build the entire dashboard against):*

```jsonc
// Message envelope (MessagePack-encoded on the wire; shown here as JSON for readability)
{
  "type": "ALERT",              // "ALERT" | "POSITION_BATCH" | "THROUGHPUT_STAT"
  "payload": {
    // for type = ALERT
    "alert_id": "uuid",
    "vessel_id": "uuid",
    "zone_id": "uuid",
    "zone_name": "Restricted Zone Alpha",
    "severity": "HIGH",          // LOW | MEDIUM | HIGH
    "event_type": "ZONE_ENTER",
    "lat": 12.34, "lon": 56.78,
    "ts_unix_ms": 1731000000000
  }
}

{
  "type": "POSITION_BATCH",
  "payload": {
    "vessels": [
      { "vessel_id": "uuid", "lat": 1.1, "lon": 2.2, "sog": 12.4, "cog": 87.0 }
    ],
    "ts_unix_ms": 1731000000000
  }
}

{
  "type": "THROUGHPUT_STAT",
  "payload": { "msgs_per_sec": 52300, "active_vessels": 48211, "ts_unix_ms": 1731000000000 }
}
```

*Historical query REST API:*

```
GET /api/alerts?vessel_id=&zone_id=&from=&to=&limit=
  -> [ { alert_id, vessel_id, zone_name, severity, event_type, lat, lon, ts_unix_ms }, ... ]
```

**Mock artifact for Engineer 4:** a static WebSocket replay server (a ~50-line script) that streams pre-recorded `ALERT`/`POSITION_BATCH`/`THROUGHPUT_STAT` frames from a fixture file on a loop, letting Engineer 4 build and demo the entire dashboard before Engineers 1-3 have written a line of code.

---

### Engineer 4 — Real-Time Live Map & Control Dashboard

**Core Responsibilities**
- Build the React frontend: WebSocket client, GPU-accelerated map rendering (deck.gl or MapLibre GL) for thousands of concurrently moving vessel icons.
- Render restricted-zone polygon overlays (from the zone list, fetched once via REST + optionally live-updated).
- Build the alert feed panel, toast notifications, click-to-locate, and severity-based color coding.
- Build the live throughput/stats HUD (msgs/sec counter, active vessel count) to visually prove the performance claim.
- Build basic filter/search controls (by zone, vessel, severity).

**Technology & Tools**
- React + TypeScript.
- deck.gl (`ScatterplotLayer` for vessels, `PolygonLayer` for zones, `IconLayer` for alert markers) on top of MapLibre GL base map — chosen over raw Canvas for GPU-side interpolation and easy scaling to tens of thousands of points.
- MessagePack decode library matching Engineer 3's wire format.
- Zustand/Redux (lightweight) for client-side vessel-state and alert-state stores, keyed by `vessel_id` for O(1) update-in-place rendering (never re-render the full list on each frame).

**Strict Interface Contract**

*Input:* consumes exactly the WebSocket protocol defined under Engineer 3 above (`ALERT`, `POSITION_BATCH`, `THROUGHPUT_STAT` frames) at `WS_ENDPOINT` (default `ws://localhost:9003/stream`).

*Input (secondary, low-frequency):* `GET /api/zones` and `GET /api/alerts` REST endpoints per Engineers 2 and 3's contracts, for initial load and history views.

*No outbound contract required to other engineers* — Engineer 4 is a pure consumer, which is precisely why it's the easiest module to develop fully in isolation against the mock WebSocket replay server from day one.

---

## 5. Integration & Milestone Plan

| Week | Milestone |
|---|---|
| 0 | Contracts finalized and checked into `/contracts` (protobuf schemas + WS message spec + REST specs). Each engineer generates their mock/fixture. |
| 1 | All 4 modules run standalone against mocks/fixtures. Engineer 4 has a fully clickable dashboard on canned data. |
| 2 | Engineer 1 ↔ Engineer 2 integrated (live simulator feeding live spatial engine); throughput benchmarking begins. |
| 3 | Engineer 2 ↔ Engineer 3 integrated (live violation events flowing into alert pipeline + Redis persistence). |
| 4 | Full end-to-end integration (1→2→3→4 live); load test to confirm 50k msgs/sec + latency targets; performance tuning pass. |
| 5 | Hardening: hot-reload zones, reconnect/backpressure handling, observability dashboards, demo polish. |

## 6. Key Risks & Mitigations

- **Risk:** UDP packet loss under load skews throughput numbers. **Mitigation:** report both "sent" and "received/processed" counters on the HUD; offer gRPC-stream mode for lossless demos.
- **Risk:** Browser can't actually render 50k points/sec even though the backend can process them. **Mitigation:** this is by design — UI is intentionally decoupled and batched at 10-20Hz; document this clearly so it isn't mistaken for a backend bottleneck.
- **Risk:** R-Tree fallback becomes a hot path if geofence boundaries are drawn too finely relative to H3 resolution. **Mitigation:** tune H3 resolution empirically; benchmark boundary-cell hit rate and adjust resolution (7 vs 8 vs 9) to keep it under ~5% of traffic.
- **Risk:** Single-process internal channel becomes the ceiling. **Mitigation:** architecture already supports sharding the spatial engine by vessel-id hash across multiple processes/cores if a single node's ceiling is reached, without introducing an external broker.
