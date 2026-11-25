// Strike Zone online server.
// Each room code maps to one Durable Object. The room keeps the list of connected players,
// relays every message to the other players, and picks a host (the longest-connected player).
// The host's browser runs the bots and the match rules; everyone else sends their own movement and shots.
import { DurableObject } from "cloudflare:workers";

const MAX_PLAYERS = 12;
const CORS = { "Access-Control-Allow-Origin": "*" };

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("Strike Zone server OK", { headers: CORS });
    }
    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{4,8})$/);
    if (!m) return new Response("Not found", { status: 404, headers: CORS });
    if (req.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected a websocket", { status: 426, headers: CORS });
    }
    const room = env.ROOMS.get(env.ROOMS.idFromName(m[1].toUpperCase()));
    return room.fetch(req);
  },
};

export class Room extends DurableObject {
  peers() {
    return this.ctx.getWebSockets().map((ws) => ({ ws, ...(ws.deserializeAttachment() || {}) })).filter((p) => p.id);
  }
  hostId(peers) {
    peers = peers || this.peers();
    let best = null;
    for (const p of peers) if (!best || p.joined < best.joined) best = p;
    return best ? best.id : null;
  }
  broadcast(text, except) {
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) continue;
      try { ws.send(text); } catch (e) {}
    }
  }
  async fetch(req) {
    const url = new URL(req.url);
    const name = (url.searchParams.get("name") || "Player").replace(/[^\w \-]/g, "").slice(0, 16) || "Player";
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    const peers = this.peers();
    if (peers.length >= MAX_PLAYERS) {
      server.send(JSON.stringify({ t: "full" }));
      server.close(4000, "Room full");
      return new Response(null, { status: 101, webSocket: client });
    }
    const id = crypto.randomUUID().slice(0, 6);
    server.serializeAttachment({ id, name, joined: Date.now() + Math.random() });
    const all = this.peers();
    server.send(JSON.stringify({ t: "welcome", id, host: this.hostId(all), peers: all.filter((p) => p.id !== id).map((p) => ({ id: p.id, name: p.name })) }));
    this.broadcast(JSON.stringify({ t: "join", id, name }), server);
    return new Response(null, { status: 101, webSocket: client });
  }
  async webSocketMessage(ws, msg) {
    // Relay as-is to everyone else. Messages are small JSON strings sent by the game.
    if (typeof msg !== "string" || msg.length > 16000) return;
    this.broadcast(msg, ws);
  }
  async webSocketClose(ws) { this.left(ws); }
  async webSocketError(ws) { this.left(ws); }
  left(ws) {
    const a = ws.deserializeAttachment() || {};
    if (!a.id || a.gone) return;
    ws.serializeAttachment({ ...a, gone: true, id: null });
    const rest = this.peers();
    this.broadcast(JSON.stringify({ t: "leave", id: a.id, host: this.hostId(rest) }), ws);
  }
}
