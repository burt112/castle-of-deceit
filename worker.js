// Castle of Deceit - Cloudflare Worker + Durable Object backend
// Routes:
//   GET/POST /api/create        -> { code }
//   WS  /ws/:CODE?name=Alice    -> join a room (first player becomes host)
// Everything else is served from the /public folder (the game page).

const CFG = {
  MIN_PLAYERS: 3,
  MAX_PLAYERS: 10,
  NIGHT_MS: 5 * 60 * 1000,
  DAY_MS: 90 * 1000,
  KILL_RANGE: 60,
  KILL_COOLDOWN_MS: 20 * 1000,
  REPORT_RANGE: 100,
  MAP_W: 2000,
  MAP_H: 2000,
  MAX_SPEED: 400,
  TASKS_PER_PLAYER: 5,
  TASK_WIN: true,
  HEARTBEAT_TIMEOUT_MS: 30 * 1000,
  FLUSH_MS: 100,
  CHAT_MAX: 200,
  ROOM_TTL_MS: 15 * 60 * 1000, // how long an empty lobby stays joinable
};

const PHASE = { LOBBY: "LOBBY", NIGHT: "NIGHT_PHASE", DAY: "DAY_PHASE", OVER: "GAME_OVER" };
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });

const genCode = () =>
  Array.from({ length: 4 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join("");

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") return new Response(null, { headers: CORS });

    if (url.pathname === "/api/create") {
      for (let i = 0; i < 8; i++) {
        const code = genCode();
        const stub = env.ROOMS.get(env.ROOMS.idFromName(code));
        const res = await stub.fetch("https://room/init", { method: "POST" });
        if (res.ok) return json({ code });
      }
      return json({ error: "Could not allocate a room, try again." }, 503);
    }

    const m = url.pathname.match(/^\/ws\/([A-Za-z0-9]{4})$/);
    if (m) {
      if (request.headers.get("Upgrade") !== "websocket")
        return new Response("Expected WebSocket upgrade", { status: 426 });
      const stub = env.ROOMS.get(env.ROOMS.idFromName(m[1].toUpperCase()));
      return stub.fetch(request);
    }

    // Serve the game page if static assets are configured
    if (env.ASSETS) return env.ASSETS.fetch(request);
    if (url.pathname === "/") return new Response("Castle of Deceit server online", { headers: CORS });
    return new Response("Not found", { status: 404 });
  },
};

export class RoomObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.resetRoom();
  }

  resetRoom() {
    this.created = false;
    this.code = "";
    this.phase = PHASE.LOBBY;
    this.players = new Map();
    this.sockets = new Map();
    this.hostId = null;
    this.bodies = [];
    this.votes = new Map();
    this.chat = [];
    this.phaseEndsAt = 0;
    this.winner = null;
    this.dirty = false;
    this.round = 0;
    if (this.tickTimer) clearInterval(this.tickTimer);
    if (this.flushTimer) clearInterval(this.flushTimer);
    this.tickTimer = null;
    this.flushTimer = null;
  }

  async fetch(request) {
    const url = new URL(request.url);

    // Restore "room exists" flag from storage (survives brief empty periods)
    const stored = await this.state.storage.get("created");
    if (!this.created && stored && Date.now() - stored < CFG.ROOM_TTL_MS) this.created = true;

    if (url.pathname === "/init") {
      if (this.created) return new Response("exists", { status: 409 });
      this.resetRoom();
      this.created = true;
      await this.state.storage.put("created", Date.now());
      return new Response("ok");
    }

    if (!this.created) return new Response("Room not found", { status: 404 });
    if (request.headers.get("Upgrade") !== "websocket")
      return new Response("Expected WebSocket", { status: 426 });
    if (this.phase !== PHASE.LOBBY) return new Response("Game already started", { status: 409 });
    if (this.players.size >= CFG.MAX_PLAYERS) return new Response("Room full", { status: 409 });

    const code = url.pathname.split("/").pop().toUpperCase();
    this.code = code;
    const name = this.cleanName(url.searchParams.get("name"));

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    this.addPlayer(server, name);
    return new Response(null, { status: 101, webSocket: client });
  }

  cleanName(n) {
    const s = String(n || "").replace(/[^\w \-]/g, "").trim().slice(0, 16);
    return s || "Player" + Math.floor(Math.random() * 900 + 100);
  }

  addPlayer(ws, name) {
    const id = crypto.randomUUID().slice(0, 8);
    const player = {
      id, name,
      position: { x: CFG.MAP_W / 2, y: CFG.MAP_H / 2 },
