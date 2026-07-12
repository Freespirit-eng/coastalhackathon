# AISentry Dashboard (Engineer 4)

Real-time React + TypeScript dashboard that consumes the Engineer 3 contract:
- `ALERT`
- `POSITION_BATCH`
- `THROUGHPUT_STAT`

## Run

```bash
npm install
npm run dev
```

Default endpoints:
- `VITE_WS_ENDPOINT=ws://localhost:9003/stream`
- `VITE_ZONES_ENDPOINT=http://localhost:9002/api/zones`
- `VITE_ALERTS_ENDPOINT=http://localhost:9003/api/alerts`

## Mock WebSocket replay (for isolated frontend work)

```bash
npm run mock-stream
```

This starts `ws://localhost:9003/stream` and replays synthetic `POSITION_BATCH`, `ALERT`, and `THROUGHPUT_STAT` MessagePack frames.
