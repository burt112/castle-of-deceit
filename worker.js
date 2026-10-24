// Castle of Deceit - Cloudflare Worker + Durable Object backend
// Routes:
//   GET/POST /api/create        -> { code }   (reserves a unique 4-char room code)
//   WS  /ws/:CODE?name=Alice    -> join a room (first player becomes host)

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

    if (url.pathname === "/init") {
      if (this.created) return new Response("exists", { status: 409 });
      this.resetRoom();
      this.created = true;
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
      role: "innocent",
      isAlive: true,
      tasksCompleted: 0,
      doneTasks: new Set(),
      lastSeen: Date.now(),
      lastMoveAt: Date.now(),
      lastKillAt: 0,
      lastChatAt: 0,
    };
    this.players.set(id, player);
    this.sockets.set(id, ws);
    if (!this.hostId) this.hostId = id;

    ws.addEventListener("message", (ev) => this.onMessage(id, ev.data));
    ws.addEventListener("close", () => this.onLeave(id));
    ws.addEventListener("error", () => this.onLeave(id));

    if (!this.tickTimer) this.tickTimer = setInterval(() => this.tick(), 1000);

    this.send(id, { t: "welcome", id, code: this.code, hostId: this.hostId, config: this.publicConfig() });
    this.broadcastState();
  }

  onLeave(id) {
    if (!this.players.has(id)) return;
    const wasHost = id === this.hostId;
    try { this.sockets.get(id)?.close(); } catch {}
    this.sockets.delete(id);

    if (this.phase === PHASE.LOBBY) {
      this.players.delete(id);
    } else {
      const p = this.players.get(id);
      p.isAlive = false;
      p.connected = false;
      this.votes.delete(id);
    }

    if (this.sockets.size === 0) { this.resetRoom(); return; }

    if (wasHost) this.hostId = [...this.sockets.keys()][0];
    if (this.phase !== PHASE.LOBBY && this.phase !== PHASE.OVER) {
      if (this.checkWin()) return;
      if (this.phase === PHASE.DAY) this.maybeResolveVotes();
    }
    this.broadcastState();
  }

  send(id, msg) {
    const ws = this.sockets.get(id);
    if (!ws) return;
    try { ws.send(JSON.stringify(msg)); } catch {}
  }

  broadcast(msg) {
    const data = JSON.stringify(msg);
    for (const ws of this.sockets.values()) { try { ws.send(data); } catch {} }
  }

  publicConfig() {
    return {
      mapW: CFG.MAP_W, mapH: CFG.MAP_H, killRange: CFG.KILL_RANGE,
      reportRange: CFG.REPORT_RANGE, tasksPerPlayer: CFG.TASKS_PER_PLAYER,
      killCooldownMs: CFG.KILL_COOLDOWN_MS,
    };
  }

  publicPlayer(p, revealRole = false) {
    return {
      id: p.id, name: p.name, position: p.position, isAlive: p.isAlive,
      tasksCompleted: p.tasksCompleted,
      ...(revealRole ? { role: p.role } : {}),
    };
  }

  broadcastState() {
    const reveal = this.phase === PHASE.OVER;
    this.broadcast({
      t: "state",
      phase: this.phase,
      hostId: this.hostId,
      endsAt: this.phaseEndsAt,
      players: [...this.players.values()].map((p) => this.publicPlayer(p, reveal)),
      bodies: this.bodies,
      winner: this.winner,
      round: this.round,
    });
  }

  onMessage(id, raw) {
    const p = this.players.get(id);
    if (!p || typeof raw !== "string" || raw.length > 1000) return;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    p.lastSeen = Date.now();

    switch (msg.t) {
      case "ping": return this.send(id, { t: "pong", now: Date.now() });
      case "setName": return this.handleSetName(p, msg);
      case "start": return this.handleStart(p);
      case "move": return this.handleMove(p, msg);
      case "kill": return this.handleKill(p, msg);
      case "task": return this.handleTask(p, msg);
      case "report": return this.handleReport(p, msg);
      case "chat": return this.handleChat(p, msg);
      case "vote": return this.handleVote(p, msg);
      case "playAgain": return this.handlePlayAgain(p);
    }
  }

  handleSetName(p, msg) {
    if (this.phase !== PHASE.LOBBY) return;
    p.name = this.cleanName(msg.name);
    this.broadcastState();
  }

  handleStart(p) {
    if (this.phase !== PHASE.LOBBY || p.id !== this.hostId) return;
    if (this.players.size < CFG.MIN_PLAYERS)
      return this.send(p.id, { t: "error", message: `Need at least ${CFG.MIN_PLAYERS} players.` });

    const list = [...this.players.values()];
    const murdererCount = list.length >= 6 ? 2 : 1;

    const ids = list.map((x) => x.id);
    for (let i = ids.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [ids[i], ids[j]] = [ids[j], ids[i]];
    }
    const murdererIds = new Set(ids.slice(0, murdererCount));

    for (const pl of list) {
      pl.role = murdererIds.has(pl.id) ? "murderer" : "innocent";
      pl.isAlive = true;
      pl.tasksCompleted = 0;
      pl.doneTasks = new Set();
      pl.lastKillAt = Date.now();
    }

    for (const pl of list) {
      this.send(pl.id, {
        t: "role",
        role: pl.role,
        partners: pl.role === "murderer" ? [...murdererIds].filter((i) => i !== pl.id) : [],
      });
    }
    this.round = 0;
    this.winner = null;
    this.startNight();
  }

  handlePlayAgain(p) {
    if (this.phase !== PHASE.OVER || p.id !== this.hostId) return;
    for (const [id, pl] of [...this.players]) if (pl.connected === false) this.players.delete(id);
    for (const pl of this.players.values()) {
      pl.role = "innocent"; pl.isAlive = true; pl.tasksCompleted = 0; pl.doneTasks = new Set();
    }
    this.bodies = []; this.votes.clear(); this.chat = []; this.winner = null;
    this.phase = PHASE.LOBBY; this.phaseEndsAt = 0;
    this.stopFlush();
    this.broadcastState();
  }

  startNight() {
    this.phase = PHASE.NIGHT;
    this.round++;
    this.bodies = [];
    this.votes.clear();
    this.phaseEndsAt = Date.now() + CFG.NIGHT_MS;
    const alive = [...this.players.values()].filter((p) => p.isAlive);
    alive.forEach((p, i) => {
      const a = (i / alive.length) * Math.PI * 2;
      p.position = { x: CFG.MAP_W / 2 + Math.cos(a) * 150, y: CFG.MAP_H / 2 + Math.sin(a) * 150 };
      p.lastMoveAt = Date.now();
    });
    this.startFlush();
    this.broadcastState();
  }

  startFlush() {
    if (this.flushTimer) return;
    this.flushTimer = setInterval(() => {
      if (!this.dirty || this.phase !== PHASE.NIGHT) return;
      this.dirty = false;
      const pos = {};
      for (const p of this.players.values()) pos[p.id] = [Math.round(p.position.x), Math.round(p.position.y)];
      this.broadcast({ t: "positions", pos });
    }, CFG.FLUSH_MS);
  }

  stopFlush() {
    if (this.flushTimer) clearInterval(this.flushTimer);
    this.flushTimer = null;
  }

  handleMove(p, msg) {
    if (this.phase !== PHASE.NIGHT) return;
    let { x, y } = msg;
    if (!Number.isFinite(x) || !Number.isFinite(y)) return;
    x = Math.max(0, Math.min(CFG.MAP_W, x));
    y = Math.max(0, Math.min(CFG.MAP_H, y));

    const now = Date.now();
    const dt = Math.max(0.05, (now - p.lastMoveAt) / 1000);
    const maxDist = CFG.MAX_SPEED * Math.min(dt, 1) + 20;
    const dx = x - p.position.x, dy = y - p.position.y;
    const dist = Math.hypot(dx, dy);
    if (dist > maxDist) {
      x = p.position.x + (dx / dist) * maxDist;
      y = p.position.y + (dy / dist) * maxDist;
    }
    p.position = { x, y };
    p.lastMoveAt = now;
    this.dirty = true;
  }

  handleKill(p, msg) {
    if (this.phase !== PHASE.NIGHT || p.role !== "murderer" || !p.isAlive) return;
    const now = Date.now();
    if (now - p.lastKillAt < CFG.KILL_COOLDOWN_MS)
      return this.send(p.id, { t: "killDenied", reason: "cooldown" });

    const victim = this.players.get(msg.targetId);
    if (!victim || !victim.isAlive || victim.id === p.id || victim.role === "murderer")
      return this.send(p.id, { t: "killDenied", reason: "invalid" });

    const d = Math.hypot(victim.position.x - p.position.x, victim.position.y - p.position.y);
    if (d > CFG.KILL_RANGE) return this.send(p.id, { t: "killDenied", reason: "range" });

    victim.isAlive = false;
    p.lastKillAt = now;
    this.bodies.push({ id: victim.id, x: victim.position.x, y: victim.position.y });
    this.send(victim.id, { t: "youDied", by: "murderer" });
    this.send(p.id, { t: "killOk", targetId: victim.id });
    this.broadcast({ t: "bodyFound", id: victim.id, x: victim.position.x, y: victim.position.y, hidden: true });
    if (this.checkWin()) return;
    this.broadcastState();
  }

  handleTask(p, msg) {
    if (this.phase !== PHASE.NIGHT || !p.isAlive || p.role !== "innocent") return;
    const taskId = String(msg.taskId ?? "").slice(0, 32);
    if (!taskId || p.doneTasks.has(taskId)) return;
    if (p.doneTasks.size >= CFG.TASKS_PER_PLAYER) return;
    p.doneTasks.add(taskId);
    p.tasksCompleted = p.doneTasks.size;
    this.broadcast({ t: "taskProgress", id: p.id, tasksCompleted: p.tasksCompleted, total: this.taskProgress() });
    if (this.checkWin()) return;
  }

  taskProgress() {
    const inn = [...this.players.values()].filter((p) => p.role === "innocent");
    const done = inn.reduce((s, p) => s + p.tasksCompleted, 0);
    return { done, needed: inn.length * CFG.TASKS_PER_PLAYER };
  }

  handleReport(p, msg) {
    if (this.phase !== PHASE.NIGHT || !p.isAlive) return;
    const body = this.bodies.find((b) => b.id === msg.bodyId);
    if (!body) return;
    if (Math.hypot(body.x - p.position.x, body.y - p.position.y) > CFG.REPORT_RANGE) return;
    this.startDay({ reportedBy: p.id, bodyId: body.id });
  }

  startDay(info = {}) {
    this.phase = PHASE.DAY;
    this.votes.clear();
    this.chat = [];
    this.stopFlush();
    this.phaseEndsAt = Date.now() + CFG.DAY_MS;
    this.broadcast({ t: "meeting", ...info });
    this.broadcastState();
  }

  handleChat(p, msg) {
    if (this.phase !== PHASE.DAY || !p.isAlive) return;
    const now = Date.now();
    if (now - p.lastChatAt < 700) return;
    p.lastChatAt = now;
    const text = String(msg.text ?? "").trim().slice(0, CFG.CHAT_MAX);
    if (!text) return;
    const entry = { from: p.id, name: p.name, text, at: now };
    this.chat.push(entry);
    if (this.chat.length > 100) this.chat.shift();
    this.broadcast({ t: "chat", ...entry });
  }

  handleVote(p, msg) {
    if (this.phase !== PHASE.DAY || !p.isAlive || this.votes.has(p.id)) return;
    const target = msg.targetId === "skip" ? "skip" : msg.targetId;
    if (target !== "skip") {
      const tp = this.players.get(target);
      if (!tp || !tp.isAlive) return;
    }
    this.votes.set(p.id, target);
    this.broadcast({ t: "voteCast", voterId: p.id });
    this.maybeResolveVotes();
  }

  maybeResolveVotes() {
    if (this.phase !== PHASE.DAY) return;
    const alive = [...this.players.values()].filter((p) => p.isAlive);
    if (alive.every((p) => this.votes.has(p.id))) this.resolveVotes();
  }

  resolveVotes() {
    if (this.phase !== PHASE.DAY) return;
    const tally = new Map();
    for (const target of this.votes.values()) tally.set(target, (tally.get(target) || 0) + 1);

    let top = null, topCount = 0, tie = false;
    for (const [target, count] of tally) {
      if (target === "skip") continue;
      if (count > topCount) { top = target; topCount = count; tie = false; }
      else if (count === topCount) tie = true;
    }
    const skipCount = tally.get("skip") || 0;

    let ejected = null;
    if (top && !tie && topCount > skipCount) {
      const ep = this.players.get(top);
      ep.isAlive = false;
      ejected = { id: ep.id, name: ep.name, wasMurderer: ep.role === "murderer" };
      this.send(ep.id, { t: "youDied", by: "vote" });
    }

    this.broadcast({ t: "voteResult", tally: Object.fromEntries(tally), ejected });
    if (this.checkWin()) return;
    this.startNight();
  }

  checkWin() {
    if (this.phase === PHASE.LOBBY || this.phase === PHASE.OVER) return false;
    const alive = [...this.players.values()].filter((p) => p.isAlive);
    const murderers = alive.filter((p) => p.role === "murderer").length;
    const innocents = alive.length - murderers;

    let winner = null;
    if (murderers === 0) winner = "innocents";
    else if (murderers >= innocents) winner = "murderers";
    else if (CFG.TASK_WIN) {
      const { done, needed } = this.taskProgress();
      if (needed > 0 && done >= needed) winner = "innocents";
    }
    if (!winner) return false;
    this.endGame(winner);
    return true;
  }

  endGame(winner) {
    this.phase = PHASE.OVER;
    this.winner = winner;
    this.phaseEndsAt = 0;
    this.stopFlush();
    this.broadcast({
      t: "gameOver", winner,
      roles: Object.fromEntries([...this.players.values()].map((p) => [p.id, p.role])),
    });
    this.broadcastState();
  }

  tick() {
    const now = Date.now();

    for (const [id, p] of this.players) {
      if (this.sockets.has(id) && now - p.lastSeen > CFG.HEARTBEAT_TIMEOUT_MS) this.onLeave(id);
    }
    if (!this.created || this.players.size === 0) return;

    if (this.phase === PHASE.NIGHT && now >= this.phaseEndsAt) {
      this.startDay({ reason: "nightEnded" });
    } else if (this.phase === PHASE.DAY && now >= this.phaseEndsAt) {
      this.resolveVotes();
    }
  }
}
