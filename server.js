/*
 * ORBITA - Servidor multijugador modular para GameMaker (100% Gratuito)
 * Protocolo propio (WebSocket + JSON) + Portal Desarrolladores Completo + API REST Admin + Plugins por juego (games/*.js)
 */

"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

const PROTOCOL = 1;

// ------------------------------------------------------------------
// Configuracion
// ------------------------------------------------------------------
const PORT = Number(process.env.PORT) || 3000;
const GAME_KEY = process.env.GAME_KEY || "";
const GAME_KEY_MD5 = GAME_KEY ? md5(GAME_KEY) : "";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "orbita_dev_token";
const STRICT_DB_WRITE = process.env.STRICT_DB_WRITE === "true";

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const GAMES_DIR = process.env.GAMES_DIR || path.join(__dirname, "games");
const PUBLIC_DIR = process.env.PUBLIC_DIR || path.join(__dirname, "public");

const MAX_MSG_PER_SEC = Number(process.env.MAX_MSG_PER_SEC) || 240;
const MAX_ROOM_SIZE = Number(process.env.MAX_ROOM_SIZE) || 64;

const AI_PROVIDER = (process.env.AI_PROVIDER || "anthropic").toLowerCase();
const AI_API_KEY = process.env.AI_API_KEY || "";
const AI_MODEL = process.env.AI_MODEL || (AI_PROVIDER === "anthropic" ? "claude-haiku-5-5" : "");
const AI_API_URL =
  process.env.AI_API_URL ||
  (AI_PROVIDER === "anthropic" ? "https://api.anthropic.com/v1/messages" : "https://api.openai.com/v1/chat/completions");
const AI_MAX_TOKENS = Number(process.env.AI_MAX_TOKENS) || 512;
const AI_MAX_PER_MINUTE = Number(process.env.AI_MAX_PER_MINUTE) || 10;

const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN || "";
const DISCORD_GUILD_ID = process.env.DISCORD_GUILD_ID || "";
const DISCORD_MAX_PER_MINUTE = Number(process.env.DISCORD_MAX_PER_MINUTE) || 5;

const FORBIDDEN_KEYS = new Set(["__proto__", "constructor", "prototype"]);

function md5(s) {
  return crypto.createHash("md5").update(s, "utf8").digest("hex");
}
function hashPassword(password, salt) {
  return crypto.scryptSync(String(password), String(salt), 64).toString("hex");
}
function log(...a) {
  console.log(new Date().toISOString(), ...a);
}
function safeJSON(text, fallback) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}
const isObj = (v) => v !== null && typeof v === "object" && !Array.isArray(v);
const str = (v, max = 128) => {
  if (typeof v !== "string" && typeof v !== "number") return "";
  const s = String(v).slice(0, max);
  return FORBIDDEN_KEYS.has(s) ? "" : s;
};
const safeKey = (v, max = 64) => str(v, max).replace(/[^a-zA-Z0-9_-]/g, "");

// ------------------------------------------------------------------
// Datos en disco: db, world, keys UUID y cuentas de desarrolladores
// ------------------------------------------------------------------
fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(GAMES_DIR, { recursive: true });
fs.mkdirSync(PUBLIC_DIR, { recursive: true });

const DB_FILE = path.join(DATA_DIR, "db.json");
const WORLD_FILE = path.join(DATA_DIR, "world.json");
const KEYS_FILE = path.join(DATA_DIR, "keys.json");
const USERS_FILE = path.join(DATA_DIR, "users.json");

const db = safeJSON(fs.existsSync(DB_FILE) ? fs.readFileSync(DB_FILE, "utf8") : "{}", {});
const world = safeJSON(fs.existsSync(WORLD_FILE) ? fs.readFileSync(WORLD_FILE, "utf8") : "{}", {});
const keysRegistry = safeJSON(fs.existsSync(KEYS_FILE) ? fs.readFileSync(KEYS_FILE, "utf8") : "{}", {});
const usersRegistry = safeJSON(fs.existsSync(USERS_FILE) ? fs.readFileSync(USERS_FILE, "utf8") : "{}", {});
const dirty = { db: false, world: false, keys: false, users: false };

// Sesiones activas en memoria de desarrolladores (token -> userId)
const devSessions = new Map();

function flush() {
  for (const [k, file, obj] of [
    ["db", DB_FILE, db],
    ["world", WORLD_FILE, world],
    ["keys", KEYS_FILE, keysRegistry],
    ["users", USERS_FILE, usersRegistry],
  ]) {
    if (!dirty[k]) continue;
    dirty[k] = false;
    try {
      fs.writeFileSync(file + ".tmp", JSON.stringify(obj, null, 2));
      fs.renameSync(file + ".tmp", file);
    } catch (e) {
      log("No se pudo guardar", file, e.message);
    }
  }
}
setInterval(flush, 2000);
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => { flush(); process.exit(0); });

const gameDb = (g) => {
  const id = safeKey(g) || "default";
  return (db[id] ||= {});
};
const gameWorld = (g) => {
  const id = safeKey(g) || "default";
  return (world[id] ||= { next: 1, rooms: {} });
};

function resolveApiKey(clientKeyHash) {
  if (GAME_KEY_MD5 && clientKeyHash === GAME_KEY_MD5) {
    return { keyId: "master", owner: "Admin", active: true };
  }
  for (const [uuid, info] of Object.entries(keysRegistry)) {
    if (info && info.active !== false && (clientKeyHash === md5(uuid) || clientKeyHash === uuid)) {
      return { keyId: uuid, ...info };
    }
  }
  return null;
}

function countActivePlayersByKey(keyId) {
  let count = 0;
  for (const c of clients.values()) {
    if (c.ready && c.apiKeyId === keyId) count++;
  }
  return count;
}

// Verifica si un scopedGameId (ej. k_12345678_MiJuego) pertenece a una clave del usuario
function userOwnsScopedGame(userId, scopedGameId) {
  const userPrefixes = Object.entries(keysRegistry)
    .filter(([, info]) => info.userId === userId)
    .map(([uuid]) => `k_${uuid.slice(0, 8)}_`);
  return userPrefixes.some((prefix) => scopedGameId.startsWith(prefix));
}

function getUserScopedGames(userId) {
  const userPrefixes = Object.entries(keysRegistry)
    .filter(([, info]) => info.userId === userId)
    .map(([uuid]) => `k_${uuid.slice(0, 8)}_`);

  const allKnownIds = new Set([
    ...games.keys(),
    ...Object.keys(db),
    ...Object.keys(world),
  ]);

  return [...allKnownIds].filter((gid) => userPrefixes.some((prefix) => gid.startsWith(prefix)));
}

// ------------------------------------------------------------------
// Estado en memoria
// ------------------------------------------------------------------
let nextId = 1;
const clients = new Map();
const games = new Map();

function getGame(id) {
  const gid = safeKey(id) || "default";
  if (!games.has(gid)) games.set(gid, { id: gid, rooms: new Map(), queues: new Map(), matchCounter: 1 });
  return games.get(gid);
}

function send(c, obj) {
  if (c && c.ws.readyState === WebSocket.OPEN) c.ws.send(JSON.stringify(obj));
}
function sendRoom(game, roomId, obj, exceptId) {
  const room = game.rooms.get(roomId);
  if (!room) return;
  const txt = JSON.stringify(obj);
  for (const id of room.members) {
    if (id === exceptId) continue;
    const c = clients.get(id);
    if (c && c.ws.readyState === WebSocket.OPEN) c.ws.send(txt);
  }
}
function playerInfo(c) {
  return { id: c.id, slot: c.slot, s: c.state, e: c.entities, pd: c.pdata };
}

// ------------------------------------------------------------------
// Cargador Modular de Juegos (games/<gameId>.js + respaldo scripts.js)
// ------------------------------------------------------------------
const loadedModules = new Map();
let legacyScripts = {};
try {
  legacyScripts = require("./scripts.js");
  log("Respaldo legacy scripts.js cargado");
} catch (e) {
  if (e.code !== "MODULE_NOT_FOUND") log("Error cargando scripts.js:", e.message);
}

function createGameApi(gameId) {
  const gid = safeKey(gameId) || "default";
  return {
    gameId: gid,
    log: (...args) => log(`[${gid}]`, ...args),
    player: (id) => {
      const c = clients.get(Number(id));
      return c && c.gameId === gid ? c : undefined;
    },
    playersInRoom(arg1, arg2) {
      const roomId = arg2 !== undefined ? String(arg2) : String(arg1);
      const targetGame = arg2 !== undefined ? String(arg1) : gid;
      const room = getGame(targetGame).rooms.get(roomId);
      return room ? room.members.map((id) => clients.get(id)).filter(Boolean) : [];
    },
    hostOf(arg1, arg2) {
      const roomId = arg2 !== undefined ? String(arg2) : String(arg1);
      const targetGame = arg2 !== undefined ? String(arg1) : gid;
      return hostOf(getGame(targetGame).rooms.get(roomId));
    },
    sendTo(playerId, name, data = {}) {
      const c = clients.get(Number(playerId));
      if (c && c.gameId === gid) send(c, { t: "ev", from: -1, n: String(name), d: data });
    },
    sendToRoom(arg1, arg2, arg3, arg4, arg5) {
      if (typeof arg3 === "string") {
        sendRoom(getGame(arg1), String(arg2), { t: "ev", from: -1, n: arg3, d: arg4 || {} }, arg5);
      } else {
        sendRoom(getGame(gid), String(arg1), { t: "ev", from: -1, n: String(arg2), d: arg3 || {} }, arg4);
      }
    },
    setPlayerData(playerId, data) {
      const c = clients.get(Number(playerId));
      if (!c || c.gameId !== gid || !isObj(data)) return;
      c.pdata = { ...c.pdata, ...data };
      if (c.roomId) sendRoom(c.game, c.roomId, { t: "pd", id: c.id, d: c.pdata });
      else send(c, { t: "pd", id: c.id, d: c.pdata });
    },
    moveToRoom(playerId, roomId, max = 0) {
      const c = clients.get(Number(playerId));
      return c && c.gameId === gid ? joinRoom(c, String(roomId), max) : { ok: false };
    },
    db: {
      get(a, b, c) {
        const [g, col, key] = c !== undefined ? [a, b, c] : [gid, a, b];
        return (gameDb(g)[col] || {})[key];
      },
      set(a, b, c, d) {
        const [g, col, key, val] = d !== undefined ? [a, b, c, d] : [gid, a, b, c];
        if (!col || !key) return;
        (gameDb(g)[col] ||= {})[key] = val;
        dirty.db = true;
      },
      delete(a, b, c) {
        const [g, col, key] = c !== undefined ? [a, b, c] : [gid, a, b];
        if (gameDb(g)[col]) {
          delete gameDb(g)[col][key];
          if (!Object.keys(gameDb(g)[col]).length) delete gameDb(g)[col];
          dirty.db = true;
        }
      },
      list(a, b) {
        const [g, col] = b !== undefined ? [a, b] : [gid, a];
        return col ? Object.keys(gameDb(g)[col] || {}) : Object.keys(gameDb(g));
      },
    },
  };
}

function getGameModule(gameId) {
  const gid = safeKey(gameId) || "default";
  if (loadedModules.has(gid)) return loadedModules.get(gid);

  const files = fs.readdirSync(GAMES_DIR);
  const match = files.find((f) => f.toLowerCase() === `${gid.toLowerCase()}.js`);
  let mod = null;

  if (match) {
    try {
      const fullPath = path.join(GAMES_DIR, match);
      mod = { exports: require(fullPath), api: createGameApi(gid), path: fullPath };
      log(`Módulo cargado para [${gid}]: games/${match}`);
    } catch (e) {
      log(`Error cargando games/${match}:`, e.message);
    }
  } else if (Object.keys(legacyScripts).length > 0) {
    mod = { exports: legacyScripts, api: createGameApi(gid), path: "scripts.js" };
  }

  loadedModules.set(gid, mod);
  return mod;
}

function runScript(gameId, hookName, ...args) {
  const mod = getGameModule(gameId);
  if (!mod || typeof mod.exports[hookName] !== "function") return undefined;
  try {
    return mod.exports[hookName](mod.api, ...args);
  } catch (e) {
    log(`Error en [${gameId}] (${hookName}):`, e.message);
  }
}

// ------------------------------------------------------------------
// Salas y Matchmaking
// ------------------------------------------------------------------
function hostOf(room) {
  return room && room.members.length ? room.members[0] : -1;
}

function leaveRoom(c, notifySelf = true) {
  leaveQueue(c);
  if (!c.roomId) return;
  const game = c.game;
  const room = game.rooms.get(c.roomId);
  const oldRoom = c.roomId;
  c.roomId = "";
  c.slot = -1;
  c.entities = {};
  if (room) {
    const wasHost = hostOf(room) === c.id;
    room.members = room.members.filter((id) => id !== c.id);
    if (room.members.length === 0) game.rooms.delete(room.id);
    else {
      sendRoom(game, room.id, { t: "leave", id: c.id });
      if (wasHost) sendRoom(game, room.id, { t: "host", id: hostOf(room) });
    }
  }
  if (notifySelf) send(c, { t: "room", id: "", host: -1, slot: -1, players: [], world: [] });
  runScript(c.gameId, "onLeaveRoom", c, oldRoom);
}

function joinRoom(c, roomId, max = 0) {
  roomId = str(roomId, 64);
  if (!roomId) return { ok: false, error: "bad_room" };
  const game = c.game;
  let room = game.rooms.get(roomId);
  if (room && room.members.includes(c.id)) return { ok: true };
  if (room && room.max > 0 && room.members.length >= room.max) return { ok: false, error: "room_full" };
  if (room && room.members.length >= MAX_ROOM_SIZE) return { ok: false, error: "room_full" };

  leaveRoom(c, false);
  if (!room) {
    room = { id: roomId, createdAt: Date.now(), members: [], max: Math.max(0, Math.min(MAX_ROOM_SIZE, Number(max) || 0)) };
    game.rooms.set(roomId, room);
  }
  const used = new Set(room.members.map((id) => clients.get(id)?.slot));
  let slot = 0;
  while (used.has(slot)) slot++;

  room.members.push(c.id);
  c.roomId = roomId;
  c.slot = slot;
  c.entities = {};

  const others = room.members.filter((id) => id !== c.id).map((id) => clients.get(id)).filter(Boolean);
  const objs = gameWorld(game.id).rooms[roomId] || {};
  send(c, {
    t: "room",
    id: roomId,
    host: hostOf(room),
    slot,
    players: others.map(playerInfo),
    world: Object.entries(objs).map(([id, p]) => ({ id: Number(id), p })),
  });
  sendRoom(game, roomId, { t: "join", ...playerInfo(c) }, c.id);
  log(`[${game.id}] ${c.id} entró a "${roomId}" (${room.members.length}${room.max ? "/" + room.max : ""}) puesto ${slot}`);
  runScript(c.gameId, "onJoinRoom", c, roomId);
  return { ok: true };
}

function leaveQueue(c) {
  if (!c.queue) return;
  const q = c.game.queues.get(c.queue);
  c.queue = null;
  if (!q) return;
  q.members = q.members.filter((id) => id !== c.id);
  for (const id of q.members) send(clients.get(id), { t: "queue", size: q.size, count: q.members.length });
}

function enqueue(c, size, mode) {
  size = Math.max(1, Math.min(MAX_ROOM_SIZE, Math.floor(Number(size)) || 2));
  mode = str(mode, 32) || "default";
  const key = `${mode}:${size}`;
  const game = c.game;
  leaveRoom(c, true);
  if (!game.queues.has(key)) game.queues.set(key, { size, mode, members: [] });
  const q = game.queues.get(key);
  q.members.push(c.id);
  c.queue = key;
  for (const id of q.members) send(clients.get(id), { t: "queue", size, count: q.members.length });

  while (q.members.length >= size) {
    const picked = q.members.splice(0, size);
    const roomId = `match_${mode}_${game.matchCounter++}`;
    for (const id of picked) {
      const p = clients.get(id);
      if (!p) continue;
      p.queue = null;
      joinRoom(p, roomId, size);
    }
    log(`[${game.id}] Partida encontrada (${mode}, ${size}): ${picked.join(", ")} -> ${roomId}`);
  }
}

// ------------------------------------------------------------------
// IA y Discord
// ------------------------------------------------------------------
function allowRate(c, key, perMinute) {
  const now = Date.now();
  const arr = (c.rate[key] = (c.rate[key] || []).filter((t) => now - t < 60000));
  if (arr.length >= perMinute) return false;
  arr.push(now);
  return true;
}

async function callAI(messages, temperature) {
  if (!AI_API_KEY) throw new Error("ai_disabled");
  if (!AI_MODEL) throw new Error("ai_model_missing");
  messages = (Array.isArray(messages) ? messages : [])
    .filter((m) => isObj(m) && typeof m.content === "string")
    .map((m) => ({ role: ["system", "assistant"].includes(m.role) ? m.role : "user", content: m.content.slice(0, 8000) }))
    .slice(-40);
  if (!messages.some((m) => m.role !== "system")) throw new Error("no_messages");
  const temp = Math.max(0, Math.min(1, Number(temperature) || 0.7));

  if (AI_PROVIDER === "anthropic") {
    const system = messages.filter((m) => m.role === "system").map((m) => m.content).join("\n");
    const res = await fetch(AI_API_URL, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": AI_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: AI_MODEL, max_tokens: AI_MAX_TOKENS, temperature: temp,
        messages: messages.filter((m) => m.role !== "system"), ...(system ? { system } : {}),
      }),
    });
    const j = await res.json();
    if (!res.ok) throw new Error(j?.error?.message || `http_${res.status}`);
    return (j.content || []).filter((b) => b.type === "text").map((b) => b.text).join("");
  }
  const res = await fetch(AI_API_URL, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${AI_API_KEY}` },
    body: JSON.stringify({ model: AI_MODEL, max_tokens: AI_MAX_TOKENS, temperature: temp, messages }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(j?.error?.message || `http_${res.status}`);
  return j.choices?.[0]?.message?.content || "";
}

const channelGuild = new Map();
async function discordSend(channel, text) {
  if (!DISCORD_BOT_TOKEN) throw new Error("discord_disabled");
  channel = str(channel, 32);
  if (!/^\d+$/.test(channel)) throw new Error("bad_channel");
  if (!channelGuild.has(channel)) {
    const r = await fetch(`https://discord.com/api/v10/channels/${channel}`, { headers: { authorization: `Bot ${DISCORD_BOT_TOKEN}` } });
    const j = await r.json();
    channelGuild.set(channel, r.ok ? String(j.guild_id || "") : "");
  }
  if (!DISCORD_GUILD_ID || channelGuild.get(channel) !== DISCORD_GUILD_ID) throw new Error("channel_not_allowed");
  const r = await fetch(`https://discord.com/api/v10/channels/${channel}/messages`, {
    method: "POST",
    headers: { authorization: `Bot ${DISCORD_BOT_TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify({ content: String(text).slice(0, 2000), allowed_mentions: { parse: [] } }),
  });
  if (!r.ok) throw new Error(`http_${r.status}`);
}

function startDiscord() {
  if (!DISCORD_BOT_TOKEN) return;
  let seq = null, hb = null;
  const gw = new WebSocket("wss://gateway.discord.gg/?v=10&encoding=json");
  gw.on("message", (raw) => {
    const p = safeJSON(raw.toString(), null);
    if (!p) return;
    if (p.s != null) seq = p.s;
    if (p.op === 10) {
      const every = p.d.heartbeat_interval;
      setTimeout(() => gw.readyState === 1 && gw.send(JSON.stringify({ op: 1, d: seq })), Math.random() * every);
      hb = setInterval(() => gw.readyState === 1 && gw.send(JSON.stringify({ op: 1, d: seq })), every);
      gw.send(JSON.stringify({ op: 2, d: { token: DISCORD_BOT_TOKEN, intents: (1 << 0) | (1 << 9) | (1 << 15), properties: { os: "linux", browser: "orbita", device: "orbita" } } }));
    } else if (p.op === 1) gw.send(JSON.stringify({ op: 1, d: seq }));
    else if (p.op === 7 || p.op === 9) gw.close();
    else if (p.op === 0 && p.t === "READY") log(`Discord conectado como ${p.d.user?.username}`);
    else if (p.op === 0 && p.t === "MESSAGE_CREATE") {
      const m = p.d;
      if (m.author?.bot || String(m.guild_id) !== DISCORD_GUILD_ID) return;
      const msg = { t: "discord", ch: String(m.channel_id), user: m.author.global_name || m.author.username, user_id: String(m.author.id), msg: m.content || "" };
      for (const c of clients.values()) if (c.ready) send(c, msg);
    }
  });
  gw.on("close", (code) => {
    clearInterval(hb);
    if (code === 4004) return log("Discord: token inválido");
    setTimeout(startDiscord, 5000);
  });
  gw.on("error", () => {});
}

// ------------------------------------------------------------------
// Peticiones con respuesta (orb_db_get, orb_ai, ...)
// ------------------------------------------------------------------
function isProtectedCollection(col) {
  return STRICT_DB_WRITE || col.startsWith("_") || col.startsWith("priv_");
}

async function handleRequest(c, m) {
  const g = c.gameId;
  const col = str(m.c, 64), key = str(m.k, 128);
  switch (m.op) {
    case "join": {
      const roomId = str(m.rm, 64);
      if (!roomId) return { ok: false, d: "bad_room" };
      const r = joinRoom(c, roomId, m.mx);
      return { ok: r.ok, d: r.ok ? roomId : r.error };
    }
    case "leave":
      leaveRoom(c, true);
      return { ok: true, d: "" };
    case "mm":
      enqueue(c, m.size, m.mode);
      return { ok: true, d: "" };
    case "mm_cancel":
      leaveQueue(c);
      return { ok: true, d: "" };
    case "rooms": {
      const list = [...c.game.rooms.values()].map((r) => ({ id: r.id, players: r.members.length, max: r.max }));
      return { ok: true, d: list };
    }
    case "players": {
      const r = c.game.rooms.get(str(m.rm, 64));
      return r ? { ok: true, d: [...r.members] } : { ok: false, d: "no_room" };
    }
    case "time":
      return { ok: true, d: Date.now() };

    case "db_get": {
      if (!col || !key || isProtectedCollection(col)) return { ok: false, d: "forbidden_or_bad_args" };
      const colObj = gameDb(g)[col];
      if (!colObj || !Object.prototype.hasOwnProperty.call(colObj, key)) return { ok: false, d: "not_found" };
      return { ok: true, d: colObj[key] };
    }
    case "db_set":
      if (!col || !key || m.v === undefined) return { ok: false, d: "bad_args" };
      if (isProtectedCollection(col)) return { ok: false, d: "forbidden_collection" };
      (gameDb(g)[col] ||= {})[key] = m.v;
      dirty.db = true;
      return { ok: true, d: "" };
    case "db_merge": {
      if (!col || !key || !isObj(m.v)) return { ok: false, d: "bad_args" };
      if (isProtectedCollection(col)) return { ok: false, d: "forbidden_collection" };
      const colObj = (gameDb(g)[col] ||= {});
      colObj[key] = { ...(isObj(colObj[key]) ? colObj[key] : {}), ...m.v };
      dirty.db = true;
      return { ok: true, d: colObj[key] };
    }
    case "db_delete":
      if (!col || !key) return { ok: false, d: "bad_args" };
      if (isProtectedCollection(col)) return { ok: false, d: "forbidden_collection" };
      if (gameDb(g)[col]) {
        delete gameDb(g)[col][key];
        if (!Object.keys(gameDb(g)[col]).length) delete gameDb(g)[col];
        dirty.db = true;
      }
      return { ok: true, d: "" };
    case "db_list": {
      if (col) {
        if (isProtectedCollection(col)) return { ok: false, d: "forbidden_collection" };
        return { ok: true, d: Object.keys(gameDb(g)[col] || {}) };
      }
      return { ok: true, d: Object.keys(gameDb(g)).filter((k) => !isProtectedCollection(k)) };
    }

    case "world_create": {
      const roomId = str(m.rm, 64) || c.roomId;
      if (!roomId || roomId !== c.roomId) return { ok: false, d: "not_in_room" };
      if (!isObj(m.p)) return { ok: false, d: "bad_args" };
      const w = gameWorld(g);
      const id = w.next++;
      (w.rooms[roomId] ||= {})[id] = m.p;
      dirty.world = true;
      sendRoom(c.game, roomId, { t: "world", id, p: m.p });
      return { ok: true, d: id };
    }

    case "ai": {
      if (!allowRate(c, "ai", AI_MAX_PER_MINUTE)) return { ok: false, d: "rate_limited" };
      try {
        return { ok: true, d: await callAI(m.msgs, m.temp) };
      } catch (e) {
        return { ok: false, d: e.message };
      }
    }
    case "discord": {
      if (!allowRate(c, "discord", DISCORD_MAX_PER_MINUTE)) return { ok: false, d: "rate_limited" };
      try {
        await discordSend(m.ch, m.msg);
        return { ok: true, d: "" };
      } catch (e) {
        return { ok: false, d: e.message };
      }
    }
    case "server": {
      const mod = getGameModule(g);
      if (!mod || typeof mod.exports.onRequest !== "function") return { ok: false, d: "no_handler" };
      try {
        const r = await mod.exports.onRequest(mod.api, c, str(m.n, 64), m.d);
        return { ok: true, d: r === undefined ? "" : r };
      } catch (e) {
        return { ok: false, d: String(e.message || e) };
      }
    }
  }
  return { ok: false, d: "unknown_op" };
}

// ------------------------------------------------------------------
// Mensajes WebSocket
// ------------------------------------------------------------------
function findWorldRoom(g, id) {
  const w = gameWorld(g);
  for (const [roomId, objs] of Object.entries(w.rooms)) if (objs[id] !== undefined) return roomId;
  return null;
}

function onMessage(c, raw) {
  const now = Date.now();
  if (now - c.rateWindow >= 1000) { c.rateWindow = now; c.rateCount = 0; }
  if (++c.rateCount > MAX_MSG_PER_SEC) {
    if (c.rateCount === MAX_MSG_PER_SEC + 1) send(c, { t: "err", code: "rate_limited", msg: "Demasiados mensajes por segundo" });
    return;
  }

  const m = safeJSON(raw.toString("utf8").replace(/\0+$/, ""), null);
  if (!isObj(m) || typeof m.t !== "string") return;

  if (m.t === "hello") {
    if (c.ready) return;
    if (Number(m.proto) !== PROTOCOL) {
      send(c, { t: "err", code: "bad_protocol", msg: `El servidor usa el protocolo ${PROTOCOL}; actualiza Órbita en el juego` });
      return c.ws.close();
    }

    const keyInfo = resolveApiKey(str(m.key, 64));
    if (!keyInfo) {
      send(c, { t: "err", code: "bad_key", msg: "La clave del juego (ORB_GAME_KEY) no es valida o fue desactivada" });
      return c.ws.close();
    }

    const rawGame = safeKey(m.game, 64) || "default";
    const scopedGameId =
      keyInfo.keyId === "master"
        ? rawGame
        : `k_${keyInfo.keyId.slice(0, 8)}_${rawGame}`;

    c.ready = true;
    c.apiKeyId = keyInfo.keyId;
    c.gameId = scopedGameId;
    c.game = getGame(c.gameId);
    getGameModule(c.gameId);
    send(c, { t: "welcome", id: c.id, time: Date.now(), proto: PROTOCOL });
    log(`[${c.gameId}] ${c.id} conectado (${countActivePlayersByKey(keyInfo.keyId)} activos en clave)`);
    runScript(c.gameId, "onConnect", c);
    return;
  }
  if (!c.ready) return;

  switch (m.t) {
    case "ping":
      c.lastPing = now;
      send(c, { t: "pong", c: m.c });
      break;

    case "st": {
      if (!c.roomId) break;
      const out = { t: "st", id: c.id };
      let any = false;
      if (isObj(m.s)) {
        c.state = { ...c.state, ...m.s };
        out.s = m.s;
        any = true;
      }
      if (isObj(m.e)) {
        for (const [k, v] of Object.entries(m.e)) if (isObj(v)) c.entities[k] = v;
        out.e = m.e;
        any = true;
      }
      if (Array.isArray(m.d) && m.d.length) {
        for (const k of m.d) delete c.entities[String(k)];
        out.d = m.d.map(String);
        any = true;
      }
      if (any) sendRoom(c.game, c.roomId, out, c.id);
      break;
    }

    case "ev": {
      const name = str(m.n, 64);
      if (!name) break;
      const msg = { t: "ev", from: c.id, n: name, d: m.d === undefined ? {} : m.d };
      if (m.to === "room") { if (c.roomId) sendRoom(c.game, c.roomId, msg, c.id); }
      else if (m.to === "all") { if (c.roomId) sendRoom(c.game, c.roomId, msg); else send(c, msg); }
      else {
        const target = clients.get(Number(m.to));
        if (target && target.gameId === c.gameId) send(target, msg);
      }
      break;
    }

    case "sev":
      runScript(c.gameId, "onEvent", c, str(m.n, 64), m.d === undefined ? {} : m.d);
      break;

    case "world_edit": {
      if (!c.roomId) break;
      const id = String(Number(m.id));
      const roomId = findWorldRoom(c.gameId, id);
      if (!roomId || roomId !== c.roomId || !isObj(m.p)) break;
      gameWorld(c.gameId).rooms[roomId][id] = m.p;
      dirty.world = true;
      sendRoom(c.game, roomId, { t: "world", id: Number(id), p: m.p });
      break;
    }
    case "world_destroy": {
      if (!c.roomId) break;
      const id = String(Number(m.id));
      const roomId = findWorldRoom(c.gameId, id);
      if (!roomId || roomId !== c.roomId) break;
      const w = gameWorld(c.gameId);
      delete w.rooms[roomId][id];
      if (!Object.keys(w.rooms[roomId]).length) delete w.rooms[roomId];
      dirty.world = true;
      sendRoom(c.game, roomId, { t: "world_del", id: Number(id) });
      break;
    }

    case "kick": {
      const room = c.game.rooms.get(c.roomId);
      const victim = clients.get(Number(m.id));
      if (room && hostOf(room) === c.id && victim && victim !== c && victim.roomId === c.roomId) {
        leaveRoom(victim, true);
        send(victim, { t: "ev", from: -1, n: "orb:kicked", d: { by: c.id } });
      }
      break;
    }

    case "req": {
      const r = m.r;
      handleRequest(c, m)
        .then((res) => send(c, { t: "res", r, ok: !!res.ok, d: res.d }))
        .catch((e) => send(c, { t: "res", r, ok: false, d: String(e.message || e) }));
      break;
    }

    case "bye":
      c.ws.close();
      break;
  }
}

// ------------------------------------------------------------------
// API REST Publica (Desarrolladores) + Admin + Archivos Estaticos
// ------------------------------------------------------------------
const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".gml": "text/plain; charset=utf-8",
  ".zip": "application/zip",
  ".yyz": "application/octet-stream",
};

function sendJson(res, status, data) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  });
  res.end(JSON.stringify(data));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) reject(new Error("payload_too_large"));
    });
    req.on("end", () => resolve(safeJSON(body, {})));
    req.on("error", reject);
  });
}

function getBearerToken(req, urlObj) {
  const authHeader = req.headers.authorization || "";
  if (authHeader.startsWith("Bearer ")) return authHeader.slice(7);
  return urlObj.searchParams.get("token") || "";
}

function isAdminAuthorized(req, urlObj) {
  const token = getBearerToken(req, urlObj);
  return ADMIN_TOKEN && token === ADMIN_TOKEN;
}

function getDevUser(req, urlObj) {
  const token = getBearerToken(req, urlObj);
  const userId = devSessions.get(token);
  if (!userId || !usersRegistry[userId]) return null;
  return { id: userId, ...usersRegistry[userId] };
}

async function handleHttp(req, res) {
  if (req.method === "OPTIONS") return sendJson(res, 204, {});
  const urlObj = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const pathname = urlObj.pathname;

  // Endpoint público de salud
  if (pathname === "/health" || pathname === "/status") {
    const perGame = {};
    for (const c of clients.values()) if (c.ready) perGame[c.gameId] = (perGame[c.gameId] || 0) + 1;
    return sendJson(res, 200, { server: "Orbita", protocol: PROTOCOL, players: clients.size, games: perGame });
  }

  // ------------------------------------------------------------------
  // Portal Publico de Desarrolladores (/api/dev/...)
  // ------------------------------------------------------------------
  if (pathname === "/api/dev/register" && req.method === "POST") {
    const body = await readBody(req);
    const name = str(body.name, 48).trim();
    const email = str(body.email, 96).trim().toLowerCase();
    const password = String(body.password || "");

    if (!name || !email || !email.includes("@")) {
      return sendJson(res, 400, { ok: false, error: "Ingresa un nombre y correo valido." });
    }
    if (password.length < 6) {
      return sendJson(res, 400, { ok: false, error: "La contraseña debe tener al menos 6 caracteres." });
    }
    const exists = Object.values(usersRegistry).some((u) => u.email === email);
    if (exists) {
      return sendJson(res, 409, { ok: false, error: "Ese correo ya esta registrado. Inicia sesion." });
    }

    const userId = crypto.randomUUID();
    const salt = crypto.randomBytes(16).toString("hex");
    usersRegistry[userId] = {
      name,
      email,
      salt,
      passHash: hashPassword(password, salt),
      createdAt: new Date().toISOString(),
    };
    dirty.users = true;

    const sessionToken = crypto.randomBytes(24).toString("hex");
    devSessions.set(sessionToken, userId);
    return sendJson(res, 200, { ok: true, token: sessionToken, user: { id: userId, name, email } });
  }

  if (pathname === "/api/dev/login" && req.method === "POST") {
    const body = await readBody(req);
    const email = str(body.email, 96).trim().toLowerCase();
    const password = String(body.password || "");

    const entry = Object.entries(usersRegistry).find(([, u]) => u.email === email);
    if (!entry) return sendJson(res, 401, { ok: false, error: "Correo o contraseña incorrectos." });

    const [userId, user] = entry;
    if (hashPassword(password, user.salt) !== user.passHash) {
      return sendJson(res, 401, { ok: false, error: "Correo o contraseña incorrectos." });
    }

    const sessionToken = crypto.randomBytes(24).toString("hex");
    devSessions.set(sessionToken, userId);
    return sendJson(res, 200, { ok: true, token: sessionToken, user: { id: userId, name: user.name, email: user.email } });
  }

  if (pathname.startsWith("/api/dev/")) {
    const user = getDevUser(req, urlObj);
    if (!user) return sendJson(res, 401, { ok: false, error: "Sesion expirada. Inicia sesion nuevamente." });

    // GET /api/dev/keys -> Listar keys del desarrollador
    if (pathname === "/api/dev/keys" && req.method === "GET") {
      const myKeys = Object.entries(keysRegistry)
        .filter(([, info]) => info.userId === user.id)
        .map(([uuid, info]) => ({
          uuid,
          prefix: `k_${uuid.slice(0, 8)}_`,
          projectName: info.projectName || "Mi Juego",
          active: info.active !== false,
          onlineNow: countActivePlayersByKey(uuid),
          createdAt: info.createdAt || null,
        }));
      return sendJson(res, 200, { ok: true, user: { name: user.name, email: user.email }, keys: myKeys });
    }

    // POST /api/dev/keys -> Crear nueva key gratuita ilimitada
    if (pathname === "/api/dev/keys" && req.method === "POST") {
      const body = await readBody(req);
      const projectName = str(body.projectName || "Mi Juego", 48).trim() || "Mi Juego";
      const uuid = crypto.randomUUID();
      keysRegistry[uuid] = {
        userId: user.id,
        owner: `${user.name} (${user.email})`,
        projectName,
        active: true,
        createdAt: new Date().toISOString(),
      };
      dirty.keys = true;
      return sendJson(res, 200, { ok: true, uuid, data: keysRegistry[uuid] });
    }

    // DELETE /api/dev/keys/:uuid -> Borrar key propia
    const devKeyMatch = pathname.match(/^\/api\/dev\/keys\/([^/]+)$/);
    if (devKeyMatch && req.method === "DELETE") {
      const uuid = decodeURIComponent(devKeyMatch[1]);
      if (!keysRegistry[uuid] || keysRegistry[uuid].userId !== user.id) {
        return sendJson(res, 404, { ok: false, error: "Key no encontrada" });
      }
      delete keysRegistry[uuid];
      dirty.keys = true;
      return sendJson(res, 200, { ok: true });
    }

    // GET /api/dev/overview -> Salas activas y juegos conocidos únicamente de este desarrollador
    if (pathname === "/api/dev/overview" && req.method === "GET") {
      const myScopedIds = getUserScopedGames(user.id);
      const gameList = {};
      for (const gid of myScopedIds) {
        const g = games.get(gid);
        gameList[gid] = {
          rooms: g
            ? [...g.rooms.values()].map((r) => ({
                id: r.id,
                max: r.max,
                host: hostOf(r),
                players: r.members.map((pid) => {
                  const p = clients.get(pid);
                  return p ? { id: p.id, slot: p.slot, pdata: p.pdata, state: p.state } : { id: pid };
                }),
              }))
            : [],
        };
      }
      return sendJson(res, 200, {
        ok: true,
        scopedGames: myScopedIds,
        games: gameList,
      });
    }

    // POST /api/dev/kick -> Expulsar jugador de una sala de un juego propio
    if (pathname === "/api/dev/kick" && req.method === "POST") {
      const body = await readBody(req);
      const victim = clients.get(Number(body.playerId));
      if (!victim || !userOwnsScopedGame(user.id, victim.gameId)) {
        return sendJson(res, 403, { ok: false, error: "No autorizado para este jugador" });
      }
      leaveRoom(victim, true);
      send(victim, { t: "ev", from: -1, n: "orb:kicked", d: { by: "developer" } });
      victim.ws.close();
      return sendJson(res, 200, { ok: true });
    }

    // CRUD /api/dev/db/:scopedGameId(/:col(/:key)) -> Base de datos aislada del desarrollador
    const devDbMatch = pathname.match(/^\/api\/dev\/db\/([^/]+)(?:\/([^/]+)(?:\/([^/]+))?)?$/);
    if (devDbMatch) {
      const g = safeKey(decodeURIComponent(devDbMatch[1]));
      if (!userOwnsScopedGame(user.id, g)) return sendJson(res, 403, { ok: false, error: "Acceso denegado a este juego" });

      const col = devDbMatch[2] ? str(decodeURIComponent(devDbMatch[2]), 64) : "";
      const key = devDbMatch[3] ? str(decodeURIComponent(devDbMatch[3]), 128) : "";

      if (req.method === "GET") {
        if (!col) return sendJson(res, 200, { ok: true, data: gameDb(g) });
        if (!key) return sendJson(res, 200, { ok: true, data: gameDb(g)[col] || {} });
        return sendJson(res, 200, { ok: true, data: (gameDb(g)[col] || {})[key] ?? null });
      }
      if (req.method === "PUT" && col && key) {
        const body = await readBody(req);
        (gameDb(g)[col] ||= {})[key] = body.value !== undefined ? body.value : body;
        dirty.db = true;
        return sendJson(res, 200, { ok: true });
      }
      if (req.method === "DELETE" && col) {
        if (key && gameDb(g)[col]) {
          delete gameDb(g)[col][key];
          if (!Object.keys(gameDb(g)[col]).length) delete gameDb(g)[col];
        } else {
          delete gameDb(g)[col];
        }
        dirty.db = true;
        return sendJson(res, 200, { ok: true });
      }
    }

    // CRUD /api/dev/world/:scopedGameId(/:roomId(/:objId)) -> Objetos persistentes aislados del desarrollador
    const devWorldMatch = pathname.match(/^\/api\/dev\/world\/([^/]+)(?:\/([^/]+)(?:\/([^/]+))?)?$/);
    if (devWorldMatch) {
      const g = safeKey(decodeURIComponent(devWorldMatch[1]));
      if (!userOwnsScopedGame(user.id, g)) return sendJson(res, 403, { ok: false, error: "Acceso denegado a este juego" });

      const roomId = devWorldMatch[2] ? str(decodeURIComponent(devWorldMatch[2]), 64) : "";
      const objId = devWorldMatch[3] ? String(Number(decodeURIComponent(devWorldMatch[3]))) : "";
      const w = gameWorld(g);

      if (req.method === "GET") {
        if (!roomId) return sendJson(res, 200, { ok: true, data: w.rooms });
        if (!objId) return sendJson(res, 200, { ok: true, data: w.rooms[roomId] || {} });
        return sendJson(res, 200, { ok: true, data: (w.rooms[roomId] || {})[objId] ?? null });
      }
      if (req.method === "PUT" && roomId && objId) {
        const body = await readBody(req);
        const props = body.props !== undefined ? body.props : body;
        if (!isObj(props)) return sendJson(res, 400, { ok: false, error: "bad_props" });
        (w.rooms[roomId] ||= {})[objId] = props;
        dirty.world = true;
        sendRoom(getGame(g), roomId, { t: "world", id: Number(objId), p: props });
        return sendJson(res, 200, { ok: true });
      }
      if (req.method === "DELETE" && roomId && objId) {
        if (w.rooms[roomId]) {
          delete w.rooms[roomId][objId];
          if (!Object.keys(w.rooms[roomId]).length) delete w.rooms[roomId];
          dirty.world = true;
          sendRoom(getGame(g), roomId, { t: "world_del", id: Number(objId) });
        }
        return sendJson(res, 200, { ok: true });
      }
    }
  }

  // ------------------------------------------------------------------
  // Rutas protegidas de administracion (/api/admin/...)
  // ------------------------------------------------------------------
  if (pathname.startsWith("/api/admin")) {
    if (!isAdminAuthorized(req, urlObj)) return sendJson(res, 401, { ok: false, error: "unauthorized" });

    if (pathname === "/api/admin/overview" && req.method === "GET") {
      const gameList = {};
      for (const [gid, g] of games.entries()) {
        gameList[gid] = {
          hasModule: Boolean(loadedModules.get(gid)),
          rooms: [...g.rooms.values()].map((r) => ({
            id: r.id,
            max: r.max,
            host: hostOf(r),
            createdAt: r.createdAt || null,
            players: r.members.map((pid) => {
              const p = clients.get(pid);
              return p ? { id: p.id, slot: p.slot, pdata: p.pdata, state: p.state } : { id: pid };
            }),
          })),
          queues: [...g.queues.values()].map((q) => ({ mode: q.mode, size: q.size, waiting: q.members.length })),
        };
      }
      return sendJson(res, 200, {
        ok: true,
        uptimeSec: Math.floor(process.uptime()),
        totalConnections: clients.size,
        totalDevelopers: Object.keys(usersRegistry).length,
        knownDbGames: Object.keys(db),
        knownWorldGames: Object.keys(world),
        games: gameList,
      });
    }

    if (pathname === "/api/admin/kick" && req.method === "POST") {
      const body = await readBody(req);
      const victim = clients.get(Number(body.playerId));
      if (!victim) return sendJson(res, 404, { ok: false, error: "player_not_found" });
      leaveRoom(victim, true);
      send(victim, { t: "ev", from: -1, n: "orb:kicked", d: { by: "admin" } });
      victim.ws.close();
      return sendJson(res, 200, { ok: true });
    }

    // CRUD /api/admin/keys (Vista global de todas las keys de todos los usuarios)
    if (pathname === "/api/admin/keys") {
      if (req.method === "GET") {
        const list = Object.entries(keysRegistry).map(([uuid, info]) => ({
          uuid,
          owner: info.owner || "Admin",
          projectName: info.projectName || "Sin nombre",
          active: info.active !== false,
          onlineNow: countActivePlayersByKey(uuid),
          createdAt: info.createdAt || null,
        }));
        return sendJson(res, 200, { ok: true, keys: list });
      }
      if (req.method === "POST") {
        const body = await readBody(req);
        const owner = str(body.owner || "Admin", 48) || "Admin";
        const projectName = str(body.projectName || owner, 48) || "Proyecto";
        const uuid = crypto.randomUUID();
        keysRegistry[uuid] = {
          owner,
          projectName,
          active: true,
          createdAt: new Date().toISOString(),
        };
        dirty.keys = true;
        return sendJson(res, 200, { ok: true, uuid, data: keysRegistry[uuid] });
      }
    }

    const keyMatch = pathname.match(/^\/api\/admin\/keys\/([^/]+)$/);
    if (keyMatch) {
      const uuid = decodeURIComponent(keyMatch[1]);
      if (!keysRegistry[uuid]) return sendJson(res, 404, { ok: false, error: "key_not_found" });

      if (req.method === "PUT") {
        const body = await readBody(req);
        if (body.active !== undefined) keysRegistry[uuid].active = Boolean(body.active);
        dirty.keys = true;
        return sendJson(res, 200, { ok: true, data: keysRegistry[uuid] });
      }
      if (req.method === "DELETE") {
        delete keysRegistry[uuid];
        dirty.keys = true;
        return sendJson(res, 200, { ok: true });
      }
    }

    // CRUD /api/admin/db/:gameId(/:col(/:key))
    const dbMatch = pathname.match(/^\/api\/admin\/db\/([^/]+)(?:\/([^/]+)(?:\/([^/]+))?)?$/);
    if (dbMatch) {
      const g = safeKey(decodeURIComponent(dbMatch[1]));
      const col = dbMatch[2] ? str(decodeURIComponent(dbMatch[2]), 64) : "";
      const key = dbMatch[3] ? str(decodeURIComponent(dbMatch[3]), 128) : "";

      if (req.method === "GET") {
        if (!col) return sendJson(res, 200, { ok: true, data: gameDb(g) });
        if (!key) return sendJson(res, 200, { ok: true, data: gameDb(g)[col] || {} });
        return sendJson(res, 200, { ok: true, data: (gameDb(g)[col] || {})[key] ?? null });
      }
      if (req.method === "PUT" && col && key) {
        const body = await readBody(req);
        (gameDb(g)[col] ||= {})[key] = body.value !== undefined ? body.value : body;
        dirty.db = true;
        return sendJson(res, 200, { ok: true });
      }
      if (req.method === "DELETE" && col) {
        if (key && gameDb(g)[col]) {
          delete gameDb(g)[col][key];
          if (!Object.keys(gameDb(g)[col]).length) delete gameDb(g)[col];
        } else {
          delete gameDb(g)[col];
        }
        dirty.db = true;
        return sendJson(res, 200, { ok: true });
      }
    }

    // CRUD /api/admin/world/:gameId(/:roomId(/:objId))
    const worldMatch = pathname.match(/^\/api\/admin\/world\/([^/]+)(?:\/([^/]+)(?:\/([^/]+))?)?$/);
    if (worldMatch) {
      const g = safeKey(decodeURIComponent(worldMatch[1]));
      const roomId = worldMatch[2] ? str(decodeURIComponent(worldMatch[2]), 64) : "";
      const objId = worldMatch[3] ? String(Number(decodeURIComponent(worldMatch[3]))) : "";
      const w = gameWorld(g);

      if (req.method === "GET") {
        if (!roomId) return sendJson(res, 200, { ok: true, data: w.rooms });
        if (!objId) return sendJson(res, 200, { ok: true, data: w.rooms[roomId] || {} });
        return sendJson(res, 200, { ok: true, data: (w.rooms[roomId] || {})[objId] ?? null });
      }
      if (req.method === "PUT" && roomId && objId) {
        const body = await readBody(req);
        const props = body.props !== undefined ? body.props : body;
        if (!isObj(props)) return sendJson(res, 400, { ok: false, error: "bad_props" });
        (w.rooms[roomId] ||= {})[objId] = props;
        dirty.world = true;
        sendRoom(getGame(g), roomId, { t: "world", id: Number(objId), p: props });
        return sendJson(res, 200, { ok: true });
      }
      if (req.method === "DELETE" && roomId && objId) {
        if (w.rooms[roomId]) {
          delete w.rooms[roomId][objId];
          if (!Object.keys(w.rooms[roomId]).length) delete w.rooms[roomId];
          dirty.world = true;
          sendRoom(getGame(g), roomId, { t: "world_del", id: Number(objId) });
        }
        return sendJson(res, 200, { ok: true });
      }
    }

    const gameMatch = pathname.match(/^\/api\/admin\/game\/([^/]+)$/);
    if (gameMatch && req.method === "DELETE") {
      const g = safeKey(decodeURIComponent(gameMatch[1]));
      if (!g) return sendJson(res, 400, { ok: false, error: "bad_game_id" });

      const activeGame = games.get(g);
      if (activeGame && activeGame.rooms.size > 0) {
        return sendJson(res, 409, { ok: false, error: "game_has_active_rooms" });
      }

      if (db[g]) { delete db[g]; dirty.db = true; }
      if (world[g]) { delete world[g]; dirty.world = true; }
      games.delete(g);
      loadedModules.delete(g);
      return sendJson(res, 200, { ok: true });
    }
    return sendJson(res, 404, { ok: false, error: "not_found" });
  }

  // Archivos estáticos del Panel Web (./public)
  let filePath = path.join(PUBLIC_DIR, pathname === "/" ? "index.html" : pathname);
  if (!filePath.startsWith(PUBLIC_DIR)) return sendJson(res, 403, { error: "forbidden" });

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { "Content-Type": MIME_TYPES[ext] || "application/octet-stream" });
    return fs.createReadStream(filePath).pipe(res);
  }

  const perGame = {};
  for (const c of clients.values()) if (c.ready) perGame[c.gameId] = (perGame[c.gameId] || 0) + 1;
  sendJson(res, 200, { server: "Orbita", protocol: PROTOCOL, players: clients.size, games: perGame });
}

const httpServer = http.createServer((req, res) => {
  handleHttp(req, res).catch((e) => {
    log("Error HTTP:", e.message);
    sendJson(res, 500, { ok: false, error: "internal_error" });
  });
});

// ------------------------------------------------------------------
// WebSocket Server
// ------------------------------------------------------------------
const wss = new WebSocketServer({ server: httpServer, perMessageDeflate: false, maxPayload: 256 * 1024 });

wss.on("connection", (ws) => {
  const c = {
    id: nextId++, ws, ready: false, apiKeyId: "", gameId: "", game: null,
    roomId: "", slot: -1, queue: null,
    state: {}, entities: {}, pdata: {},
    lastPing: Date.now(), rate: {}, rateWindow: Date.now(), rateCount: 0,
  };
  clients.set(c.id, c);
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
  ws.on("message", (raw) => {
    try { onMessage(c, raw); } catch (e) { log(`Error con el mensaje de ${c.id}:`, e.message); }
  });
  ws.on("close", () => {
    if (c.ready) {
      leaveRoom(c, false);
      runScript(c.gameId, "onDisconnect", c);
      log(`[${c.gameId}] ${c.id} desconectado`);
    }
    clients.delete(c.id);
  });
  ws.on("error", () => {});
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 10000);

setInterval(() => {
  for (const gid of loadedModules.keys()) {
    runScript(gid, "onTick");
  }
}, 100);

httpServer.listen(PORT, () => {
  log(`Órbita escuchando en el puerto ${PORT} (protocolo ${PROTOCOL})`);
  log(`Módulos de juego en: ${GAMES_DIR} | Panel web en: ${PUBLIC_DIR}`);
  log(`IA: ${AI_API_KEY ? `${AI_PROVIDER} / ${AI_MODEL}` : "desactivada"} | Discord: ${DISCORD_BOT_TOKEN ? "activado" : "desactivado"} | Modo 100% Gratuito`);
  startDiscord();
});