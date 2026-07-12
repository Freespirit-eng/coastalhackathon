import { decode } from "@msgpack/msgpack";
import type { WsEnvelope } from "./types";

const WS_ENDPOINT = import.meta.env.VITE_WS_ENDPOINT ?? "ws://localhost:9003/stream";

function parseEnvelope(event: MessageEvent): WsEnvelope | null {
  if (event.data instanceof ArrayBuffer) {
    return decode(new Uint8Array(event.data)) as WsEnvelope;
  }
  if (event.data instanceof Blob) {
    return null;
  }
  if (typeof event.data === "string") {
    return JSON.parse(event.data) as WsEnvelope;
  }
  return null;
}

export function connectStream(onMessage: (message: WsEnvelope) => void, onStatus: (connected: boolean) => void): () => void {
  const socket = new WebSocket(WS_ENDPOINT);
  socket.binaryType = "arraybuffer";

  socket.onopen = () => onStatus(true);
  socket.onclose = () => onStatus(false);
  socket.onerror = () => onStatus(false);
  socket.onmessage = async (event) => {
    let parsed = parseEnvelope(event);
    if (!parsed && event.data instanceof Blob) {
      const buffer = await event.data.arrayBuffer();
      parsed = decode(new Uint8Array(buffer)) as WsEnvelope;
    }
    if (parsed) {
      onMessage(parsed);
    }
  };

  return () => {
    socket.close();
  };
}
