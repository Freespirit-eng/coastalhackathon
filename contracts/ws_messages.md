# WebSocket Message Specification

**Contract:** Engineer 3 (Event Broker) → Engineer 4 (Dashboard)
**Endpoint:** `ws://localhost:9003/stream`
**Encoding:** MessagePack on the wire (shown as JSON below for readability)

---

## Message Envelope

All messages follow this envelope structure:

```json
{
  "type": "<MESSAGE_TYPE>",
  "payload": { ... }
}
```

## Message Types

### `ALERT`

Sent when a vessel triggers a geofence violation (zone entry, heartbeat while inside, or zone exit).

```json
{
  "type": "ALERT",
  "payload": {
    "alert_id": "uuid-string",
    "vessel_id": "uuid-string",
    "zone_id": "uuid-string",
    "zone_name": "Restricted Zone Alpha",
    "severity": "HIGH",
    "event_type": "ZONE_ENTER",
    "lat": 12.34,
    "lon": 56.78,
    "ts_unix_ms": 1731000000000
  }
}
```

**Severity levels:** `LOW` | `MEDIUM` | `HIGH`
**Event types:** `ZONE_ENTER` | `ZONE_HEARTBEAT` | `ZONE_EXIT`

### `POSITION_BATCH`

Batched vessel position updates sent at 10-20Hz tick rate. Contains the latest known position for vessels that moved since the last batch.

```json
{
  "type": "POSITION_BATCH",
  "payload": {
    "vessels": [
      {
        "vessel_id": "uuid-string",
        "lat": 1.1,
        "lon": 2.2,
        "sog": 12.4,
        "cog": 87.0
      }
    ],
    "ts_unix_ms": 1731000000000
  }
}
```

### `THROUGHPUT_STAT`

Real-time ingestion statistics, sent every 1 second.

```json
{
  "type": "THROUGHPUT_STAT",
  "payload": {
    "msgs_per_sec": 52300,
    "active_vessels": 48211,
    "ts_unix_ms": 1731000000000
  }
}
```

---

## Connection Lifecycle

1. Client opens WebSocket to `ws://<host>:9003/stream`
2. Server immediately begins streaming `POSITION_BATCH` and `THROUGHPUT_STAT` messages
3. `ALERT` messages are sent as they occur (event-driven, not batched)
4. Client may send a `SUBSCRIBE` message to filter by zone/severity (v2 — not required for v1)
5. Server handles backpressure by dropping oldest position batches if client falls behind
