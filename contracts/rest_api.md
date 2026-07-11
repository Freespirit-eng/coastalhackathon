# REST API Specification

---

## Zone Management API (Engineer 2 — Spatial Engine)

Base URL: `http://localhost:9002/api`

### Create/Update Zone

```
POST /api/zones
Content-Type: application/json

{
  "zone_id": "uuid-string",
  "name": "Restricted Zone Alpha",
  "polygon": [
    [12.34, 56.78],
    [12.45, 56.89],
    [12.56, 56.90],
    [12.34, 56.78]
  ],
  "severity": "HIGH"
}
```

**Response:** `201 Created`
```json
{ "zone_id": "uuid-string", "status": "created" }
```

### Delete Zone

```
DELETE /api/zones/{zone_id}
```

**Response:** `200 OK`
```json
{ "zone_id": "uuid-string", "status": "deleted" }
```

### List All Zones

```
GET /api/zones
```

**Response:** `200 OK`
```json
[
  {
    "zone_id": "uuid-string",
    "name": "Restricted Zone Alpha",
    "polygon": [[12.34, 56.78], [12.45, 56.89], ...],
    "severity": "HIGH"
  }
]
```

---

## Alert History API (Engineer 3 — Event Broker)

Base URL: `http://localhost:9003/api`

### Query Alerts

```
GET /api/alerts?vessel_id=<uuid>&zone_id=<uuid>&from=<ts_ms>&to=<ts_ms>&limit=<int>
```

All query parameters are optional. Defaults: `limit=100`, `from=0`, `to=now`.

**Response:** `200 OK`
```json
[
  {
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
]
```
