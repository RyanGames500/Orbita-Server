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
// Sin ADMIN_TOKEN el panel de admin queda APAGADO (antes había una clave por defecto que cualquiera podía adivinar).
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";
const STRICT_DB_WRITE = process.env.STRICT_DB_WRITE === "true";
// Detrás de un hosting (Render, Railway, nginx...) la IP real viene en X-Forwarded-For.
const TRUST_PROXY = process.env.TRUST_PROXY ? process.env.TRUST_PROXY === "true" : Boolean(process.env.RENDER);
const CORS_ORIGIN = process.env.CORS_ORIGIN || ""; // vacío = el panel solo funciona desde el mismo servidor

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const GAMES_DIR = process.env.GAMES_DIR || path.join(__dirname, "games");
const PUBLIC_DIR = process.env.PUBLIC_DIR || path.join(__dirname, "public");

const MAX_MSG_PER_SEC = Number(process.env.MAX_MSG_PER_SEC) || 240;
const MAX_ROOM_SIZE = Number(process.env.MAX_ROOM_SIZE) || 64;
const MAX_CONN_PER_IP = Number(process.env.MAX_CONN_PER_IP) || 32;
const MAX_ENTITIES_PER_PLAYER = Number(process.env.MAX_ENTITIES_PER_PLAYER) || 512;
const MAX_STATE_KEYS = Number(process.env.MAX_STATE_KEYS) || 128;
const MAX_DB_VALUE_BYTES = Number(process.env.MAX_DB_VALUE_BYTES) || 64 * 1024;
const MAX_DB_DOCS_PER_COLLECTION = Number(process.env.MAX_DB_DOCS_PER_COLLECTION) || 10000;
const MAX_WORLD_OBJECTS_PER_ROOM = Number(process.env.MAX_WORLD_OBJECTS_PER_ROOM) || 2000;
const MAX_KEYS_PER_DEV = Number(process.env.MAX_KEYS_PER_DEV) || 100;
const MAX_PROJECTS_PER_DEV = Number(process.env.MAX_PROJECTS_PER_DEV) || 20;
const MAX_KEYS_PER_PROJECT = Number(process.env.MAX_KEYS_PER_PROJECT) || 5;
const HELLO_TIMEOUT_MS = 10000;
const DEV_SESSION_TTL_MS = 7 * 24 * 3600 * 1000;
const AUTH_MAX_PER_MINUTE = Number(process.env.AUTH_MAX_PER_MINUTE) || 10;
// Si a alguien se le cae el internet estando en una sala, se le guarda el lugar este tiempo (0 = no guardar)
const RESUME_GRACE_MS = process.env.RESUME_GRACE_MS !== undefined ? Number(process.env.RESUME_GRACE_MS) : 20000;

const AI_PROVIDER = (process.env.AI_PROVIDER || "anthropic").toLowerCase();
const AI_API_KEY = process.env.AI_API_KEY || "";
const AI_MODEL = process.env.AI_MODEL || (AI_PROVIDER === "anthropic" ? "claude-haiku-5-5" : "");
const AI_API_URL =
  process.env.AI_API_URL ||
  (AI_PROVIDER === "anthropic" ? "https://api.anthropic.com/v1/messages" : "https://api.openai.com/v1/chat/completions");
const AI_MAX_TOKENS = Number(process.env.AI_MAX_TOKENS) || 512;
const AI_MAX_PER_MINUTE = Number(process.env.AI_MAX_PER_MINUTE) || 10; // por IP
const AI_KEY_MAX_PER_MINUTE = Number(process.env.AI_KEY_MAX_PER_MINUTE) || 60; // por clave (todos los jugadores de ese juego juntos)
// La IA y Discord gastan TUS créditos / usan TU bot. Las claves de desarrollador no los usan
// a menos que lo actives (para todas con estas variables, o por clave desde la API de admin).
const DEV_KEYS_AI = process.env.DEV_KEYS_AI === "true";
const DEV_KEYS_DISCORD = process.env.DEV_KEYS_DISCORD === "true";

const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN || "";
const DISCORD_GUILD_ID = process.env.DISCORD_GUILD_ID || "";
const DISCORD_MAX_PER_MINUTE = Number(process.env.DISCORD_MAX_PER_MINUTE) || 5;

const FORBIDDEN_KEYS = new Set(["__proto__", "constructor", "prototype"]);

function md5(s) {
  return crypto.createHash("md5").update(s, "utf8").digest("hex");
}
// Async para no congelar el servidor mientras calcula (antes era scryptSync).
function hashPassword(password, salt) {
  return new Promise((resolve, reject) =>
    crypto.scrypt(String(password), String(salt), 64, (err, key) => (err ? reject(err) : resolve(key.toString("hex")))),
  );
}
function safeEqual(a, b) {
  const ba = Buffer.from(String(a)), bb = Buffer.from(String(b));
  return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}
// Quita __proto__ / constructor / prototype de un objeto (para que nadie cambie prototipos).
function cleanKeys(obj, maxKeys = Infinity) {
  const out = {};
  let n = 0;
  for (const [k, v] of Object.entries(obj)) {
    if (FORBIDDEN_KEYS.has(k)) continue;
    if (++n > maxKeys) break;
    out[k] = v;
  }
  return out;
}
function jsonSize(v) {
  try { return Buffer.byteLength(JSON.stringify(v) ?? ""); } catch { return Infinity; }
}
// Límite de veces por minuto compartido (por IP, por clave...). Se limpia solo.
const globalRates = new Map();
function allowGlobalRate(bucket, perMinute) {
  const now = Date.now();
  const arr = (globalRates.get(bucket) || []).filter((t) => now - t < 60000);
  if (arr.length >= perMinute) { globalRates.set(bucket, arr); return false; }
  arr.push(now);
  globalRates.set(bucket, arr);
  return true;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, arr] of globalRates) if (!arr.length || now - arr[arr.length - 1] >= 60000) globalRates.delete(k);
}, 60000).unref();
function clientIp(req) {
  if (TRUST_PROXY) {
    const xff = String(req.headers["x-forwarded-for"] || "").split(",").map((x) => x.trim()).filter(Boolean);
    if (xff.length) return xff[xff.length - 1]; // la última la pone el proxy, no se puede falsificar
  }
  return req.socket.remoteAddress || "?";
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
// Letras, números, "_", "-" y "." (el punto deja usar versiones como "1.0" en ORB_GAME_ID)
const safeKey = (v, max = 64) => {
  const k = str(v, max).replace(/[^a-zA-Z0-9_.-]/g, "");
  return /^\.+$/.test(k) ? "" : k;
};

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
const PROJECTS_FILE = path.join(DATA_DIR, "projects.json");

const db = safeJSON(fs.existsSync(DB_FILE) ? fs.readFileSync(DB_FILE, "utf8") : "{}", {});
const world = safeJSON(fs.existsSync(WORLD_FILE) ? fs.readFileSync(WORLD_FILE, "utf8") : "{}", {});
const keysRegistry = safeJSON(fs.existsSync(KEYS_FILE) ? fs.readFileSync(KEYS_FILE, "utf8") : "{}", {});
const usersRegistry = safeJSON(fs.existsSync(USERS_FILE) ? fs.readFileSync(USERS_FILE, "utf8") : "{}", {});
// Proyectos: un proyecto = un juego. Sus claves solo sirven para entrar; los datos son del proyecto.
const projectsRegistry = safeJSON(fs.existsSync(PROJECTS_FILE) ? fs.readFileSync(PROJECTS_FILE, "utf8") : "{}", {});
// Cuentas de jugador de cada juego/proyecto
const PLAYERS_FILE = path.join(DATA_DIR, "players.json");
const playersReg = safeJSON(fs.existsSync(PLAYERS_FILE) ? fs.readFileSync(PLAYERS_FILE, "utf8") : "{}", {});
const dirty = { db: false, world: false, keys: false, users: false, projects: false, players: false };

// Sesiones activas en memoria de desarrolladores (token -> userId)
const devSessions = new Map();

function flush() {
  for (const [k, file, obj] of [
    ["db", DB_FILE, db],
    ["world", WORLD_FILE, world],
    ["keys", KEYS_FILE, keysRegistry],
    ["users", USERS_FILE, usersRegistry],
    ["projects", PROJECTS_FILE, projectsRegistry],
    ["players", PLAYERS_FILE, playersReg],
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
// Si algo revienta, guarda antes de cerrar para no perder los últimos 2 segundos.
process.on("uncaughtException", (e) => {
  log("Error fatal:", e && e.stack ? e.stack : e);
  for (const k of Object.keys(dirty)) dirty[k] = true;
  flush();
  process.exit(1);
});

const gameDb = (g) => {
  const id = safeKey(g) || "default";
  return (db[id] ||= {});
};
const gameWorld = (g) => {
  const id = safeKey(g) || "default";
  return (world[id] ||= { next: 1, rooms: {} });
};
// Solo leer, sin crear entradas vacías (para los GET del panel)
const peekDb = (g) => db[safeKey(g) || "default"] || {};
const peekWorld = (g) => world[safeKey(g) || "default"] || { next: 1, rooms: {} };

// ------------------------------------------------------------------
// Proyectos
//   Proyecto "p_xxxxxxxxxx" (o "k_xxxxxxxx" si venía de una clave vieja)
//   Base de datos: una por proyecto            -> db["<proyecto>"]
//   Versión / modo (ORB_GAME_ID): salas aparte -> "<proyecto>_<ORB_GAME_ID>"
//   (así jugadores de la 1.0 y la 1.1 no se juntan, pero comparten su progreso)
// ------------------------------------------------------------------
const PROJECT_NS_RE = /^((?:k_[0-9a-f]{8})|(?:p_[0-9a-f]{10}))(?:_(.+))?$/;
function projectOfNs(ns) {
  const m = PROJECT_NS_RE.exec(String(ns || ""));
  return m && projectsRegistry[m[1]] ? m[1] : "";
}
// Dónde vive la base de datos de un juego/canal
function dbNsOf(ns) {
  return projectOfNs(ns) || ns;
}
function newProjectId() {
  for (;;) {
    const id = "p_" + crypto.randomBytes(5).toString("hex");
    if (!projectsRegistry[id]) return id;
  }
}
function projectKeys(pid) {
  return Object.entries(keysRegistry).filter(([, k]) => k && k.projectId === pid);
}
function channelsOfProject(pid) {
  const prefix = pid + "_";
  const set = new Set();
  for (const ns of [...games.keys(), ...Object.keys(world)]) if (ns.startsWith(prefix)) set.add(ns);
  return [...set];
}

// Pasa las claves viejas (una clave = un juego) al modelo de proyectos, sin perder nada.
// Antes de tocar algo hace una copia de la carpeta data en data/respaldo-<fecha>/
(function migrateToProjects() {
  const legacyKeys = Object.entries(keysRegistry).filter(([, info]) => info && !info.projectId);
  const legacyDb = Object.keys(db).filter((ns) => /^k_[0-9a-f]{8}_/.test(ns));
  if (!legacyKeys.length && !legacyDb.length) return;

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const backupDir = path.join(DATA_DIR, `respaldo-${stamp}`);
  try {
    fs.mkdirSync(backupDir, { recursive: true });
    for (const f of [DB_FILE, WORLD_FILE, KEYS_FILE, USERS_FILE, PROJECTS_FILE]) {
      if (fs.existsSync(f)) fs.copyFileSync(f, path.join(backupDir, path.basename(f)));
    }
    log(`Migración a proyectos: respaldo guardado en ${backupDir}`);
  } catch (e) {
    log("No se pudo hacer el respaldo, no migro nada:", e.message);
    return;
  }

  for (const [uuid, info] of legacyKeys) {
    const pid = `k_${uuid.slice(0, 8)}`;
    if (!projectsRegistry[pid]) {
      projectsRegistry[pid] = {
        name: info.projectName || "Mi Juego",
        userId: info.userId || null,
        owner: info.owner || "Admin",
        createdAt: info.createdAt || new Date().toISOString(),
        ...(typeof info.ai === "boolean" ? { ai: info.ai } : {}),
        ...(typeof info.discord === "boolean" ? { discord: info.discord } : {}),
      };
    }
    info.projectId = pid;
    info.label = info.label || "Principal";
  }

  // La base de datos pasa de "k_xxxxxxxx_<juego>" a "k_xxxxxxxx" (una por proyecto)
  for (const ns of legacyDb) {
    const pid = ns.slice(0, 10);
    if (!projectsRegistry[pid]) continue;
    const target = (db[pid] ||= {});
    let conflict = false;
    for (const [col, docs] of Object.entries(db[ns] || {})) {
      if (!isObj(docs)) continue;
      const t = (target[col] ||= {});
      for (const [k, v] of Object.entries(docs)) {
        if (Object.prototype.hasOwnProperty.call(t, k)) { conflict = true; continue; }
        t[k] = v;
      }
    }
    if (conflict) log(`Migración: "${ns}" tenía datos repetidos con otra versión; lo dejo también como estaba`);
    else delete db[ns];
  }
  for (const k of Object.keys(dirty)) dirty[k] = true;
  flush();
  log(`Migración a proyectos lista: ${legacyKeys.length} clave(s) convertidas`);
})();
function createProject(name, user) {
  const pid = newProjectId();
  projectsRegistry[pid] = {
    name,
    userId: user ? user.id : null,
    owner: user ? `${user.name} (${user.email})` : "Admin",
    createdAt: new Date().toISOString(),
  };
  dirty.projects = true;
  return pid;
}
function createKey(pid, label, user) {
  const uuid = newKeyUuid();
  keysRegistry[uuid] = {
    projectId: pid,
    label,
    userId: user ? user.id : projectsRegistry[pid]?.userId || null,
    owner: projectsRegistry[pid]?.owner || "Admin",
    projectName: projectsRegistry[pid]?.name || "",
    active: true,
    createdAt: new Date().toISOString(),
  };
  dirty.keys = true;
  return uuid;
}
function deleteProject(pid) {
  disconnectProject(pid);
  for (const [uuid] of projectKeys(pid)) delete keysRegistry[uuid];
  const prefix = pid + "_";
  for (const ns of new Set([...Object.keys(db), ...Object.keys(world), ...games.keys()])) {
    if (ns !== pid && !ns.startsWith(prefix)) continue;
    delete db[ns];
    delete world[ns];
    games.delete(ns);
    loadedModules.delete(ns);
  }
  delete projectsRegistry[pid];
  if (playersReg[pid]) { delete playersReg[pid]; dirty.players = true; }
  dirty.keys = dirty.db = dirty.world = dirty.projects = true;
}
function projectSummary(pid) {
  const p = projectsRegistry[pid];
  let online = 0;
  for (const c of clients.values()) if (c.ready && c.projectId === pid) online++;
  const channels = channelsOfProject(pid);
  let rooms = 0;
  for (const ns of channels) rooms += games.get(ns)?.rooms.size || 0;
  return {
    id: pid,
    name: p.name,
    owner: p.owner || "",
    createdAt: p.createdAt || null,
    keys: projectKeys(pid).length,
    accounts: Object.keys(playersReg[pid]?.accounts || {}).length,
    online,
    rooms,
    versions: channels.map((ns) => ns.slice(pid.length + 1)),
    ai: typeof p.ai === "boolean" ? p.ai : DEV_KEYS_AI,
    discord: typeof p.discord === "boolean" ? p.discord : DEV_KEYS_DISCORD,
    requests: isObj(p.requests) ? p.requests : {},
  };
}
function projectDetail(pid, withKeys) {
  const out = projectSummary(pid);
  if (withKeys) {
    out.keyList = projectKeys(pid).map(([uuid, k]) => ({
      uuid,
      label: k.label || "Clave",
      active: k.active !== false,
      onlineNow: countActivePlayersByKey(uuid),
      createdAt: k.createdAt || null,
    }));
  }
  out.channels = channelsOfProject(pid).map((ns) => {
    const g = games.get(ns);
    const w = peekWorld(ns);
    return {
      gameId: ns.slice(pid.length + 1),
      ns,
      rooms: g
        ? [...g.rooms.values()].map((r) => ({
            id: r.id,
            max: r.max,
            host: hostOf(r),
            createdAt: r.createdAt || null,
            worldObjects: Object.keys(w.rooms[r.id] || {}).length,
            ...roomPublic(r),
            players: r.members.map((id) => {
              const pl = clients.get(id);
              return pl
                ? { id: pl.id, slot: pl.slot, name: pl.pname || "", uid: pl.uid || "", away: !!pl.ghost, state: pl.state, entities: Object.keys(pl.entities).length, pdata: pl.pdata }
                : { id };
            }),
          }))
        : [],
      queues: g ? [...g.queues.values()].map((q) => ({ mode: q.mode, size: q.size, waiting: q.members.length })) : [],
      worldRooms: Object.entries(w.rooms).map(([id, objs]) => ({ id, objects: Object.keys(objs).length })),
    };
  });
  const pdb = peekDb(pid);
  out.collections = Object.entries(pdb).map(([name, docs]) => ({ name, docs: Object.keys(docs || {}).length }));
  return out;
}


function resolveApiKey(clientKeyHash) {
  if (GAME_KEY_MD5 && clientKeyHash && safeEqual(clientKeyHash, GAME_KEY_MD5)) {
    return { keyId: "master", owner: "Admin", active: true };
  }
  for (const [uuid, info] of Object.entries(keysRegistry)) {
    if (info && info.active !== false && (clientKeyHash === md5(uuid) || clientKeyHash === uuid)) {
      return { keyId: uuid, ...info };
    }
  }
  return null;
}

// Clave nueva cuyo prefijo de 8 letras no choque con otra (los juegos se separan por ese prefijo)
function newKeyUuid() {
  for (;;) {
    const uuid = crypto.randomUUID();
    const prefix = uuid.slice(0, 8);
    if (!Object.keys(keysRegistry).some((k) => k.slice(0, 8) === prefix)) return uuid;
  }
}
// Al desactivar o borrar una clave, saca a los que estaban conectados con ella
function disconnectKey(keyId) {
  for (const c of clients.values()) {
    if (c.ready && c.apiKeyId === keyId) {
      send(c, { t: "err", code: "bad_key", msg: "La clave del juego fue desactivada" });
      dropClient(c);
    }
  }
}
function canUseService(c, service) {
  if (c.apiKeyId === "master") return true;
  const proj = projectsRegistry[c.projectId];
  if (proj && typeof proj[service] === "boolean") return proj[service];
  const info = keysRegistry[c.apiKeyId];
  if (info && typeof info[service] === "boolean") return info[service];
  return service === "ai" ? DEV_KEYS_AI : DEV_KEYS_DISCORD;
}
function disconnectProject(pid) {
  for (const [uuid] of projectKeys(pid)) disconnectKey(uuid);
}

function countActivePlayersByKey(keyId) {
  let count = 0;
  for (const c of clients.values()) {
    if (c.ready && c.apiKeyId === keyId) count++;
  }
  return count;
}

// ¿Este juego/canal (ej. p_0123456789_1.0, o el proyecto mismo) es de este usuario?
function userOwnsScopedGame(userId, scopedGameId) {
  const pid = projectOfNs(scopedGameId);
  return Boolean(pid) && projectsRegistry[pid].userId === userId;
}

function getUserScopedGames(userId) {
  const allKnownIds = new Set([...games.keys(), ...Object.keys(db), ...Object.keys(world)]);
  return [...allKnownIds].filter((gid) => userOwnsScopedGame(userId, gid));
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

// Envíos agrupados: todo lo que se le manda a un jugador en la misma vuelta del servidor sale junto
// en una sola escritura de red (en vez de una por mensaje). Los mensajes son los mismos y llegan en el
// mismo orden; solo se gastan muchas menos llamadas al sistema (era lo que más CPU consumía).
const corked = new Set();
let uncorkScheduled = false;
function flushCorked() {
  uncorkScheduled = false;
  for (const sock of corked) { try { sock.uncork(); } catch {} }
  corked.clear();
}
function rawSend(ws, txt) {
  const sock = ws._socket;
  if (sock && !corked.has(sock)) {
    sock.cork();
    corked.add(sock);
    if (!uncorkScheduled) { uncorkScheduled = true; setImmediate(flushCorked); }
  }
  ws.send(txt);
}
function send(c, obj) {
  if (c && c.ws.readyState === WebSocket.OPEN) rawSend(c.ws, JSON.stringify(obj));
}
function sendRoom(game, roomId, obj, exceptId) {
  const room = game.rooms.get(roomId);
  if (!room) return;
  const txt = JSON.stringify(obj);
  for (const id of room.members) {
    if (id === exceptId) continue;
    const c = clients.get(id);
    if (c && c.ws.readyState === WebSocket.OPEN) rawSend(c.ws, txt);
  }
}

// ------------------------------------------------------------------
// Cuentas de jugador (por proyecto)
//   Invitado: el juego guarda un token en la PC del jugador y con eso vuelve a ser él.
//   Usuario y contraseña: para entrar desde otra PC.
//   Colecciones "mi_…": cada jugador solo lee y escribe lo suyo.
// ------------------------------------------------------------------
const MAX_ACCOUNTS_PER_GAME = Number(process.env.MAX_ACCOUNTS_PER_GAME) || 100000;
const MAX_TOKENS_PER_ACCOUNT = 10;
const sha256 = (s) => crypto.createHash("sha256").update(String(s)).digest("hex");

function pspace(ns) {
  const id = safeKey(ns) || "default";
  const sp = (playersReg[id] ||= { accounts: {}, users: {}, tokens: {} });
  sp.accounts ||= {}; sp.users ||= {}; sp.tokens ||= {};
  return sp;
}
function cleanName(v) {
  return String(v ?? "").replace(/[\u0000-\u001f\u007f<>]/g, "").trim().slice(0, 24);
}
function newAccount(sp, extra = {}) {
  let uid;
  do uid = "u_" + crypto.randomBytes(6).toString("hex"); while (sp.accounts[uid]);
  sp.accounts[uid] = {
    name: "Jugador-" + uid.slice(-4).toUpperCase(),
    guest: true,
    createdAt: new Date().toISOString(),
    lastSeen: new Date().toISOString(),
    tokens: [],
    ...extra,
  };
  dirty.players = true;
  return uid;
}
function issueToken(sp, uid) {
  const token = crypto.randomBytes(24).toString("hex");
  const h = sha256(token);
  const acc = sp.accounts[uid];
  sp.tokens[h] = uid;
  acc.tokens = [...(acc.tokens || []), h];
  while (acc.tokens.length > MAX_TOKENS_PER_ACCOUNT) delete sp.tokens[acc.tokens.shift()];
  dirty.players = true;
  return { token, hash: h };
}
function authAs(c, sp, uid, hash) {
  const acc = sp.accounts[uid];
  c.uid = uid;
  c.pname = acc.name;
  c.tokenHash = hash;
  acc.lastSeen = new Date().toISOString();
  dirty.players = true;
}
function accountReply(c, token) {
  const acc = pspace(c.dbNs).accounts[c.uid];
  return { uid: c.uid, name: acc.name, user: acc.user || "", guest: !acc.user, ...(token ? { token } : {}) };
}
// Revisa un token guardado: devuelve "" si sirve, o el error
function resumeWithToken(c, token) {
  const sp = pspace(c.dbNs);
  const h = sha256(token);
  const uid = sp.tokens[h];
  if (!uid || !sp.accounts[uid]) return "bad_token";
  if (sp.accounts[uid].banned) return "banned";
  authAs(c, sp, uid, h);
  return "";
}
function kickAccount(ns, uid, code = "banned") {
  for (const c of clients.values()) {
    if (c.ready && c.dbNs === ns && c.uid === uid) {
      send(c, { t: "err", code, msg: code === "banned" ? "Esta cuenta fue bloqueada" : "Tu cuenta fue borrada" });
      dropClient(c);
    }
  }
}
function deleteAccount(ns, uid) {
  const sp = pspace(ns);
  const acc = sp.accounts[uid];
  if (!acc) return false;
  kickAccount(ns, uid, "deleted");
  for (const h of acc.tokens || []) delete sp.tokens[h];
  if (acc.user) delete sp.users[acc.user.toLowerCase()];
  delete sp.accounts[uid];
  // Sus datos en colecciones mi_…
  const d = db[safeKey(ns)];
  if (d) for (const [col, docs] of Object.entries(d)) if (col.startsWith("mi_") && docs && docs[uid] !== undefined) { delete docs[uid]; dirty.db = true; }
  dirty.players = true;
  return true;
}
function listAccounts(ns, q = "", limit = 200) {
  const sp = pspace(ns);
  const needle = String(q || "").toLowerCase();
  let online = new Set();
  for (const c of clients.values()) if (c.ready && c.dbNs === ns && c.uid) online.add(c.uid);
  const all = Object.entries(sp.accounts)
    .filter(([uid, a]) => !needle || uid.includes(needle) || (a.name || "").toLowerCase().includes(needle) || (a.user || "").toLowerCase().includes(needle))
    .sort((a, b) => String(b[1].lastSeen).localeCompare(String(a[1].lastSeen)));
  return {
    total: Object.keys(sp.accounts).length,
    registered: Object.values(sp.accounts).filter((a) => a.user).length,
    online: online.size,
    players: all.slice(0, limit).map(([uid, a]) => ({ uid, name: a.name, user: a.user || "", guest: !a.user, banned: !!a.banned, online: online.has(uid), createdAt: a.createdAt, lastSeen: a.lastSeen })),
  };
}

async function handleAuth(c, m) {
  const sp = pspace(c.dbNs);
  switch (m.op) {
    case "auth_guest": {
      if (c.uid) return { ok: true, d: accountReply(c) };
      if (!allowGlobalRate(`guest:${c.ip}`, 30)) return { ok: false, d: "rate_limited" };
      if (Object.keys(sp.accounts).length >= MAX_ACCOUNTS_PER_GAME) return { ok: false, d: "too_many_accounts" };
      const uid = newAccount(sp);
      const t = issueToken(sp, uid);
      authAs(c, sp, uid, t.hash);
      return { ok: true, d: accountReply(c, t.token) };
    }
    case "auth_resume": {
      const err = resumeWithToken(c, str(m.token, 128));
      return err ? { ok: false, d: err } : { ok: true, d: accountReply(c) };
    }
    case "auth_register": {
      if (!allowGlobalRate(`pauth:${c.ip}`, AUTH_MAX_PER_MINUTE)) return { ok: false, d: "rate_limited" };
      const user = String(m.user ?? "").trim();
      const pass = String(m.pass ?? "");
      if (!/^[a-zA-Z0-9_.-]{3,24}$/.test(user)) return { ok: false, d: "bad_user" };
      if (pass.length < 6 || pass.length > 128) return { ok: false, d: "bad_pass" };
      if (sp.users[user.toLowerCase()]) return { ok: false, d: "user_taken" };
      const salt = crypto.randomBytes(16).toString("hex");
      const passHash = await hashPassword(pass, salt);
      if (sp.users[user.toLowerCase()]) return { ok: false, d: "user_taken" }; // por si otro lo tomó mientras tanto
      let uid = c.uid && sp.accounts[c.uid] && !sp.accounts[c.uid].user ? c.uid : ""; // invitado -> se queda con su progreso
      if (!uid) {
        if (Object.keys(sp.accounts).length >= MAX_ACCOUNTS_PER_GAME) return { ok: false, d: "too_many_accounts" };
        uid = newAccount(sp);
      }
      Object.assign(sp.accounts[uid], { user, salt, passHash, guest: false });
      if (sp.accounts[uid].name.startsWith("Jugador-")) sp.accounts[uid].name = user;
      sp.users[user.toLowerCase()] = uid;
      const t = issueToken(sp, uid);
      authAs(c, sp, uid, t.hash);
      return { ok: true, d: accountReply(c, t.token) };
    }
    case "auth_login": {
      if (!allowGlobalRate(`pauth:${c.ip}`, AUTH_MAX_PER_MINUTE)) return { ok: false, d: "rate_limited" };
      const uid = sp.users[String(m.user ?? "").trim().toLowerCase()];
      const acc = uid && sp.accounts[uid];
      const pass = String(m.pass ?? "").slice(0, 128);
      if (!acc || !acc.passHash || !safeEqual(await hashPassword(pass, acc.salt), acc.passHash)) return { ok: false, d: "wrong_login" };
      if (acc.banned) return { ok: false, d: "banned" };
      const t = issueToken(sp, uid);
      authAs(c, sp, uid, t.hash);
      return { ok: true, d: accountReply(c, t.token) };
    }
    case "auth_logout": {
      if (c.uid && c.tokenHash) {
        const acc = sp.accounts[c.uid];
        delete sp.tokens[c.tokenHash];
        if (acc) acc.tokens = (acc.tokens || []).filter((h) => h !== c.tokenHash);
        dirty.players = true;
      }
      c.uid = ""; c.pname = ""; c.tokenHash = "";
      return { ok: true, d: "" };
    }
    case "set_name": {
      if (!c.uid) return { ok: false, d: "not_logged_in" };
      const name = cleanName(m.name);
      if (!name) return { ok: false, d: "bad_name" };
      sp.accounts[c.uid].name = name;
      c.pname = name;
      dirty.players = true;
      if (c.roomId) sendRoom(c.game, c.roomId, { t: "name", id: c.id, name }, c.id);
      return { ok: true, d: name };
    }
  }
  return null;
}

function playerInfo(c) {
  const out = { id: c.id, slot: c.slot, s: c.state, e: c.entities, pd: c.pdata };
  if (c.ghost) out.away = true;
  if (c.uid) { out.uid = c.uid; out.nm = c.pname; }
  return out;
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
  const dbg = dbNsOf(gid); // la base de datos es del proyecto (compartida entre versiones)
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
      return c && c.gameId === gid ? joinRoom(c, String(roomId), max, { force: true }) : { ok: false };
    },
    // Datos de una sala (o undefined) y cambiarlos desde el servidor
    room(roomId) {
      const r = getGame(gid).rooms.get(String(roomId));
      return r ? { id: r.id, max: r.max, members: [...r.members], host: hostOf(r), ...roomPublic(r) } : undefined;
    },
    setRoom(roomId, { props, locked, private: priv, password } = {}) {
      const r = getGame(gid).rooms.get(String(roomId));
      if (!r) return false;
      if (isObj(props)) r.props = cleanProps({ ...(r.props || {}), ...props });
      if (typeof locked === "boolean") r.locked = locked;
      if (typeof priv === "boolean") r.private = priv;
      if (typeof password === "string") setRoomPassword(r, password);
      sendRoom(getGame(gid), r.id, { t: "room_props", ...roomPublic(r) });
      return true;
    },
    db: {
      get(a, b, c) {
        const [g, col, key] = c !== undefined ? [a, b, c] : [dbg, a, b];
        return (peekDb(g)[col] || {})[key];
      },
      set(a, b, c, d) {
        const [g, col, key, val] = d !== undefined ? [a, b, c, d] : [dbg, a, b, c];
        if (!col || !key) return;
        (gameDb(g)[col] ||= {})[key] = val;
        dirty.db = true;
      },
      delete(a, b, c) {
        const [g, col, key] = c !== undefined ? [a, b, c] : [dbg, a, b];
        if (gameDb(g)[col]) {
          delete gameDb(g)[col][key];
          if (!Object.keys(gameDb(g)[col]).length) delete gameDb(g)[col];
          dirty.db = true;
        }
      },
      list(a, b) {
        const [g, col] = b !== undefined ? [a, b] : [dbg, a];
        return col ? Object.keys(peekDb(g)[col] || {}) : Object.keys(peekDb(g));
      },
    },
  };
}

function getGameModule(gameId) {
  const gid = safeKey(gameId) || "default";
  if (loadedModules.has(gid)) return loadedModules.get(gid);

  const files = fs.readdirSync(GAMES_DIR);
  const pid = projectOfNs(gid);
  const match =
    files.find((f) => f.toLowerCase() === `${gid.toLowerCase()}.js`) ||
    (pid ? files.find((f) => f.toLowerCase() === `${pid.toLowerCase()}.js`) : undefined);
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

// ------------------------------------------------------------------
// Datos de sala: nombre, mapa, modo… (los pone el host), contraseña, privada (no sale en la lista)
// y cerrada (nadie más puede entrar, por ejemplo cuando empieza la partida)
// ------------------------------------------------------------------
const MAX_ROOM_PROPS_BYTES = 4096;
const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // sin 0/O ni 1/I para que no se confundan
function newRoomCode(game) {
  for (;;) {
    let code = "";
    for (let i = 0; i < 6; i++) code += ROOM_CODE_CHARS[crypto.randomInt(ROOM_CODE_CHARS.length)];
    if (!game.rooms.has(code)) return code;
  }
}
function cleanProps(v) {
  if (!isObj(v)) return {};
  const p = cleanKeys(v, 32);
  return jsonSize(p) <= MAX_ROOM_PROPS_BYTES ? p : {};
}
function setRoomPassword(room, pw) {
  pw = String(pw ?? "").slice(0, 64);
  if (!pw) { room.passSalt = ""; room.passHash = ""; return; }
  room.passSalt = crypto.randomBytes(8).toString("hex");
  room.passHash = sha256(room.passSalt + pw);
}
function roomPublic(room) {
  return { props: room.props || {}, locked: !!room.locked, private: !!room.private, has_pw: !!room.passHash };
}

// Todo lo de la sala para quien entra (o vuelve después de caerse)
function sendRoomSnapshot(c, room, extra = {}) {
  const others = room.members.filter((id) => id !== c.id).map((id) => clients.get(id)).filter(Boolean);
  const objs = gameWorld(c.game.id).rooms[room.id] || {};
  send(c, {
    t: "room",
    id: room.id,
    host: hostOf(room),
    slot: c.slot,
    players: others.map(playerInfo),
    world: Object.entries(objs).map(([id, p]) => ({ id: Number(id), p })),
    max: room.max,
    ...roomPublic(room),
    ...extra,
  });
}

// ------------------------------------------------------------------
// Volver después de que se cae el internet
//   Al caerse estando en una sala, el jugador queda "ausente" RESUME_GRACE_MS: conserva su id, su puesto,
//   su estado y sus entidades. Si vuelve a tiempo (con el código secreto "rs" que le dimos en welcome),
//   recupera todo aunque la sala esté cerrada. Si no vuelve, ahí sí sale de la sala.
// ------------------------------------------------------------------
function finalizeClient(c) {
  clearTimeout(c.ghostTimer);
  c.ghost = false;
  if (c.ready) {
    leaveRoom(c, false);
    runScript(c.gameId, "onDisconnect", c);
    log(`[${c.gameId}] ${c.id} desconectado`);
  }
  clients.delete(c.id);
}
// Sacar a alguien sin dejarlo volver (clave pausada, cuenta bloqueada…)
function dropClient(c) {
  c.noResume = true;
  if (c.ghost) finalizeClient(c);
  else c.ws.close();
}
function tryResume(c, m, scopedGameId) {
  const r = m.resume;
  if (!isObj(r) || RESUME_GRACE_MS <= 0) return false;
  const old = clients.get(Number(r.id));
  if (!old || old === c || !old.ready || old.noResume || old.gameId !== scopedGameId || !old.roomId) return false;
  if (!old.rs || !safeEqual(old.rs, str(r.rs, 64))) return false;
  // Pasar la conexión nueva al jugador de antes
  clearTimeout(old.ghostTimer);
  const wasGhost = old.ghost;
  const oldWs = old.ws;
  old.ghost = false;
  old.ws = c.ws;
  old.ip = c.ip;
  old.rate = {}; old.rateWindow = Date.now(); old.rateCount = 0;
  c.ws._orbClient = old;
  clearTimeout(c.helloTimer);
  clients.delete(c.id);
  if (oldWs && oldWs !== c.ws && oldWs.readyState === WebSocket.OPEN) { try { oldWs.terminate(); } catch {} }
  old.rs = crypto.randomBytes(16).toString("hex");
  send(old, { t: "welcome", id: old.id, time: Date.now(), proto: PROTOCOL, rs: old.rs, resumed: true, ...(old.uid ? { uid: old.uid, name: old.pname } : {}) });
  const room = old.game.rooms.get(old.roomId);
  if (room) {
    sendRoomSnapshot(old, room, { resumed: true });
    if (wasGhost) sendRoom(old.game, room.id, { t: "back", id: old.id }, old.id);
  }
  log(`[${old.gameId}] ${old.id} volvió a su sala "${old.roomId}"`);
  runScript(old.gameId, "onReconnect", old);
  return true;
}

function joinRoom(c, roomId, max = 0, opts = {}) {
  roomId = str(roomId, 64);
  if (!roomId) return { ok: false, error: "bad_room" };
  const game = c.game;
  let room = game.rooms.get(roomId);
  if (room && room.members.includes(c.id)) return { ok: true };
  if (room && !opts.force) {
    if (room.locked) return { ok: false, error: "room_locked" };
    if (room.passHash) {
      if (!allowRate(c, "room_pw", 10)) return { ok: false, error: "rate_limited" };
      if (!safeEqual(sha256(room.passSalt + String(opts.pw ?? "").slice(0, 64)), room.passHash)) return { ok: false, error: "wrong_password" };
    }
  }
  if (room && room.max > 0 && room.members.length >= room.max) return { ok: false, error: "room_full" };
  if (room && room.members.length >= MAX_ROOM_SIZE) return { ok: false, error: "room_full" };

  leaveRoom(c, false);
  if (!room) {
    room = { id: roomId, createdAt: Date.now(), members: [], max: Math.max(0, Math.min(MAX_ROOM_SIZE, Number(max) || 0)),
      props: cleanProps(opts.props), private: !!opts.private, locked: false, passSalt: "", passHash: "" };
    if (opts.pw) setRoomPassword(room, opts.pw);
    game.rooms.set(roomId, room);
  }
  const used = new Set(room.members.map((id) => clients.get(id)?.slot));
  let slot = 0;
  while (used.has(slot)) slot++;

  room.members.push(c.id);
  c.roomId = roomId;
  c.slot = slot;
  c.entities = {};

  sendRoomSnapshot(c, room);
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
  const t = Number(temperature);
  const temp = Math.max(0, Math.min(1, Number.isFinite(t) ? t : 0.7));

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
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`http_${r.status}`); // no se guarda: se reintenta la próxima vez
    channelGuild.set(channel, String(j.guild_id || ""));
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
      // Solo a los juegos que tienen permiso de Discord (antes le llegaba a TODOS los juegos del servidor)
      for (const c of clients.values()) if (c.ready && canUseService(c, "discord")) send(c, msg);
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
// "_..." y "priv_..." -> solo el servidor (plugins) puede leer y escribir
// "ro_..."  -> los jugadores pueden LEER pero solo el servidor escribe (rankings, tiendas, configuración...)
// STRICT_DB_WRITE=true -> los jugadores solo pueden leer, en todas las colecciones
function isProtectedCollection(col) {
  return col.startsWith("_") || col.startsWith("priv_");
}
function canClientWrite(col) {
  return !STRICT_DB_WRITE && !isProtectedCollection(col) && !col.startsWith("ro_");
}
function dbWriteCheck(g, col, key, value) {
  if (jsonSize(value) > MAX_DB_VALUE_BYTES) return "value_too_large";
  const colObj = peekDb(g)[col];
  const count = colObj ? Object.keys(colObj).length : 0;
  if (count >= MAX_DB_DOCS_PER_COLLECTION && !(colObj && Object.prototype.hasOwnProperty.call(colObj, key))) return "collection_full";
  return "";
}

async function handleRequest(c, m) {
  const g = c.gameId;       // salas y objetos del mundo: por versión
  const dg = c.dbNs || g;   // base de datos: por proyecto
  const col = str(m.c, 64);
  let key = str(m.k, 128);
  if (typeof m.op === "string" && (m.op.startsWith("auth_") || m.op === "set_name")) {
    const r = await handleAuth(c, m);
    if (r) return r;
  }
  // "mi_…": cada jugador solo lee y escribe su propio documento (la clave es su id de cuenta)
  if (typeof m.op === "string" && m.op.startsWith("db_") && col.startsWith("mi_")) {
    if (!c.uid) return { ok: false, d: "not_logged_in" };
    if (m.op === "db_list") return { ok: true, d: Object.prototype.hasOwnProperty.call(peekDb(dg)[col] || {}, c.uid) ? [c.uid] : [] };
    if (key && key !== c.uid) return { ok: false, d: "forbidden_key" };
    key = c.uid;
  }
  switch (m.op) {
    case "join": {
      const roomId = str(m.rm, 64);
      if (!roomId) return { ok: false, d: "bad_room" };
      const r = joinRoom(c, roomId, m.mx, { pw: m.pw });
      return { ok: r.ok, d: r.ok ? roomId : r.error };
    }
    // Crear sala (si no das nombre, se inventa un código de 6 letras). Quien la crea queda de host.
    case "room_create": {
      let roomId = str(m.rm, 64);
      if (roomId && c.game.rooms.has(roomId)) return { ok: false, d: "room_exists" };
      if (!roomId) roomId = newRoomCode(c.game);
      const r = joinRoom(c, roomId, m.mx, { pw: m.pw, private: m.private, props: m.props });
      return { ok: r.ok, d: r.ok ? roomId : r.error };
    }
    // El host cambia los datos de su sala: props (se mezclan), locked, private, pw ("" = sin contraseña)
    case "room_set": {
      const room = c.game.rooms.get(c.roomId);
      if (!room) return { ok: false, d: "not_in_room" };
      if (hostOf(room) !== c.id) return { ok: false, d: "not_host" };
      if (isObj(m.props)) {
        const merged = { ...(room.props || {}), ...cleanKeys(m.props) };
        for (const [k, v] of Object.entries(merged)) if (v === null) delete merged[k]; // null = borrar ese dato
        if (Object.keys(merged).length > 32 || jsonSize(merged) > MAX_ROOM_PROPS_BYTES) return { ok: false, d: "props_too_large" };
        room.props = merged;
      }
      if (typeof m.locked === "boolean") room.locked = m.locked;
      if (typeof m.private === "boolean") room.private = m.private;
      if (typeof m.pw === "string") setRoomPassword(room, m.pw);
      sendRoom(c.game, room.id, { t: "room_props", ...roomPublic(room) });
      return { ok: true, d: roomPublic(room) };
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
      // Las salas privadas no salen en la lista (se entra solo sabiendo el código)
      const list = [...c.game.rooms.values()]
        .filter((r) => !r.private)
        .map((r) => ({ id: r.id, players: r.members.length, max: r.max, props: r.props || {}, locked: !!r.locked, has_pw: !!r.passHash }));
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
      const colObj = peekDb(dg)[col];
      if (!colObj || !Object.prototype.hasOwnProperty.call(colObj, key)) return { ok: false, d: "not_found" };
      return { ok: true, d: colObj[key] };
    }
    case "db_set": {
      if (!col || !key || m.v === undefined) return { ok: false, d: "bad_args" };
      if (!canClientWrite(col)) return { ok: false, d: "forbidden_collection" };
      const err = dbWriteCheck(dg, col, key, m.v);
      if (err) return { ok: false, d: err };
      (gameDb(dg)[col] ||= {})[key] = m.v;
      dirty.db = true;
      return { ok: true, d: "" };
    }
    case "db_merge": {
      if (!col || !key || !isObj(m.v)) return { ok: false, d: "bad_args" };
      if (!canClientWrite(col)) return { ok: false, d: "forbidden_collection" };
      const prev = isObj(peekDb(dg)[col]?.[key]) ? peekDb(dg)[col][key] : {};
      const merged = { ...prev, ...cleanKeys(m.v) };
      const err = dbWriteCheck(dg, col, key, merged);
      if (err) return { ok: false, d: err };
      const colObj = (gameDb(dg)[col] ||= {});
      colObj[key] = merged;
      dirty.db = true;
      return { ok: true, d: colObj[key] };
    }
    case "db_delete":
      if (!col || !key) return { ok: false, d: "bad_args" };
      if (!canClientWrite(col)) return { ok: false, d: "forbidden_collection" };
      if (peekDb(dg)[col]) {
        delete gameDb(dg)[col][key];
        if (!Object.keys(gameDb(dg)[col]).length) delete gameDb(dg)[col];
        dirty.db = true;
      }
      return { ok: true, d: "" };
    case "db_list": {
      if (col) {
        if (isProtectedCollection(col)) return { ok: false, d: "forbidden_collection" };
        return { ok: true, d: Object.keys(peekDb(dg)[col] || {}) };
      }
      return { ok: true, d: Object.keys(peekDb(dg)).filter((k) => !isProtectedCollection(k)) };
    }

    case "world_create": {
      const roomId = str(m.rm, 64) || c.roomId;
      if (!roomId || roomId !== c.roomId) return { ok: false, d: "not_in_room" };
      if (!isObj(m.p)) return { ok: false, d: "bad_args" };
      if (jsonSize(m.p) > MAX_DB_VALUE_BYTES) return { ok: false, d: "value_too_large" };
      if (Object.keys(peekWorld(g).rooms[roomId] || {}).length >= MAX_WORLD_OBJECTS_PER_ROOM) return { ok: false, d: "room_world_full" };
      const w = gameWorld(g);
      const id = w.next++;
      (w.rooms[roomId] ||= {})[id] = m.p;
      dirty.world = true;
      sendRoom(c.game, roomId, { t: "world", id, p: m.p });
      return { ok: true, d: id };
    }

    case "ai": {
      if (!AI_API_KEY) return { ok: false, d: "ai_disabled" };
      if (!canUseService(c, "ai")) return { ok: false, d: "ai_not_allowed" };
      // Límite por IP (reconectar ya no lo salta) y por clave (todo el juego junto)
      if (!allowGlobalRate(`ai_ip:${c.ip}`, AI_MAX_PER_MINUTE)) return { ok: false, d: "rate_limited" };
      if (!allowGlobalRate(`ai_key:${c.apiKeyId}`, AI_KEY_MAX_PER_MINUTE)) return { ok: false, d: "rate_limited" };
      try {
        return { ok: true, d: await callAI(m.msgs, m.temp) };
      } catch (e) {
        return { ok: false, d: e.message };
      }
    }
    case "discord": {
      if (!DISCORD_BOT_TOKEN) return { ok: false, d: "discord_disabled" };
      if (!canUseService(c, "discord")) return { ok: false, d: "discord_not_allowed" };
      if (!allowGlobalRate(`dc_ip:${c.ip}`, DISCORD_MAX_PER_MINUTE)) return { ok: false, d: "rate_limited" };
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
    let scopedGameId = rawGame, projectId = "";
    if (keyInfo.keyId !== "master") {
      projectId = keyInfo.projectId;
      if (!projectId || !projectsRegistry[projectId]) {
        send(c, { t: "err", code: "bad_key", msg: "La clave no pertenece a ningún proyecto" });
        return c.ws.close();
      }
      scopedGameId = `${projectId}_${rawGame}`; // ORB_GAME_ID = versión o modo dentro del proyecto
    }

    if (tryResume(c, m, scopedGameId)) return;

    c.ready = true;
    c.apiKeyId = keyInfo.keyId;
    c.projectId = projectId;
    c.channel = rawGame;
    c.gameId = scopedGameId;
    c.dbNs = projectId || scopedGameId;
    c.game = getGame(c.gameId);
    getGameModule(c.gameId);
    // Si el juego trae el token de la cuenta del jugador, entra ya con su cuenta
    let authErr = "";
    if (typeof m.auth === "string" && m.auth) authErr = resumeWithToken(c, m.auth.slice(0, 128));
    c.rs = crypto.randomBytes(16).toString("hex"); // código secreto para volver si se cae el internet
    send(c, { t: "welcome", id: c.id, time: Date.now(), proto: PROTOCOL, rs: c.rs, ...(c.uid ? { uid: c.uid, name: c.pname } : {}), ...(authErr ? { auth_err: authErr } : {}) });
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
        const s = cleanKeys(m.s, MAX_STATE_KEYS);
        c.state = cleanKeys({ ...c.state, ...s }, MAX_STATE_KEYS);
        out.s = s;
        any = true;
      }
      if (isObj(m.e)) {
        const e = {};
        for (const [k, v] of Object.entries(m.e)) {
          if (FORBIDDEN_KEYS.has(k) || !isObj(v)) continue;
          if (!Object.prototype.hasOwnProperty.call(c.entities, k) && Object.keys(c.entities).length >= MAX_ENTITIES_PER_PLAYER) continue;
          c.entities[k] = v;
          e[k] = v;
        }
        out.e = e;
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
        // Solo a alguien de TU sala (antes podías escribirle a cualquiera del juego adivinando su id)
        const target = clients.get(Number(m.to));
        if (target && target.gameId === c.gameId && c.roomId && target.roomId === c.roomId) send(target, msg);
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
      if (!roomId || roomId !== c.roomId || !isObj(m.p) || jsonSize(m.p) > MAX_DB_VALUE_BYTES) break;
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
      c.noResume = true; // se fue a propósito: no guardarle el lugar
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

// CORS: "*" solo para /health. El panel (/api) se usa desde el mismo servidor, así que no lo necesita,
// salvo que pongas CORS_ORIGIN (ej. "https://mi-panel.com").
function sendJson(res, status, data, cors = CORS_ORIGIN) {
  const headers = { "Content-Type": "application/json; charset=utf-8" };
  if (cors) {
    headers["Access-Control-Allow-Origin"] = cors;
    headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization";
    headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS";
  }
  res.writeHead(status, headers);
  res.end(status === 204 ? undefined : JSON.stringify(data));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    let done = false;
    req.on("data", (chunk) => {
      if (done) return;
      body += chunk;
      if (body.length > 1024 * 1024) { done = true; req.destroy(); reject(new Error("payload_too_large")); }
    });
    req.on("end", () => {
      if (done) return;
      const v = safeJSON(body, {});
      resolve(isObj(v) ? v : {}); // antes "null" reventaba con error 500
    });
    req.on("error", reject);
  });
}

// El token va SOLO en la cabecera Authorization (en la URL se puede quedar guardado en logs)
function getBearerToken(req) {
  const authHeader = req.headers.authorization || "";
  return authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
}

function isAdminAuthorized(req) {
  const token = getBearerToken(req);
  return Boolean(ADMIN_TOKEN) && safeEqual(token, ADMIN_TOKEN);
}

function getDevUser(req) {
  const token = getBearerToken(req);
  const sess = devSessions.get(token);
  if (!sess) return null;
  if (Date.now() > sess.expires || !usersRegistry[sess.userId]) { devSessions.delete(token); return null; }
  return { id: sess.userId, ...usersRegistry[sess.userId] };
}
function newDevSession(userId) {
  const token = crypto.randomBytes(24).toString("hex");
  devSessions.set(token, { userId, expires: Date.now() + DEV_SESSION_TTL_MS });
  return token;
}
setInterval(() => {
  const now = Date.now();
  for (const [t, sess] of devSessions) if (now > sess.expires) devSessions.delete(t);
}, 3600 * 1000).unref();


function playersRoute(req, res, url, ns, uid) {
  return (async () => {
    if (!uid && req.method === "GET") {
      return sendJson(res, 200, { ok: true, ...listAccounts(ns, url.searchParams.get("q") || "", Math.min(1000, Number(url.searchParams.get("limit")) || 200)) });
    }
    const sp = pspace(ns);
    const acc = uid && sp.accounts[uid];
    if (!acc) return sendJson(res, 404, { ok: false, error: "Jugador no encontrado" });
    if (req.method === "PUT") {
      const body = await readBody(req);
      if (typeof body.banned === "boolean") {
        acc.banned = body.banned;
        if (body.banned) kickAccount(ns, uid, "banned");
      }
      if (typeof body.name === "string" && cleanName(body.name)) acc.name = cleanName(body.name);
      dirty.players = true;
      return sendJson(res, 200, { ok: true });
    }
    if (req.method === "DELETE") {
      deleteAccount(ns, uid);
      return sendJson(res, 200, { ok: true });
    }
    return sendJson(res, 404, { ok: false, error: "not_found" });
  })();
}

async function handleHttp(req, res) {
  const urlObj = new URL(req.url, "http://localhost");
  const pathname = urlObj.pathname;
  if (req.method === "OPTIONS") return sendJson(res, 204, {}, pathname === "/health" || pathname === "/status" ? "*" : CORS_ORIGIN);

  // Endpoint público de salud (ya no muestra la lista de juegos: eso solo lo ve el admin)
  if (pathname === "/health" || pathname === "/status") {
    return sendJson(res, 200, { server: "Orbita", protocol: PROTOCOL, players: clients.size }, "*");
  }

  // ------------------------------------------------------------------
  // Portal Publico de Desarrolladores (/api/dev/...)
  // ------------------------------------------------------------------
  const isAuthRoute = (pathname === "/api/dev/register" || pathname === "/api/dev/login") && req.method === "POST";
  if (isAuthRoute && !allowGlobalRate(`auth:${clientIp(req)}`, AUTH_MAX_PER_MINUTE)) {
    return sendJson(res, 429, { ok: false, error: "Demasiados intentos. Espera un minuto." });
  }

  if (pathname === "/api/dev/register" && req.method === "POST") {
    const body = await readBody(req);
    const name = str(body.name, 48).trim();
    const email = str(body.email, 96).trim().toLowerCase();
    const password = String(body.password || "");

    if (!name || !email || !email.includes("@")) {
      return sendJson(res, 400, { ok: false, error: "Ingresa un nombre y correo valido." });
    }
    if (password.length < 6 || password.length > 128) {
      return sendJson(res, 400, { ok: false, error: "La contraseña debe tener entre 6 y 128 caracteres." });
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
      passHash: await hashPassword(password, salt),
      createdAt: new Date().toISOString(),
    };
    dirty.users = true;

    const sessionToken = newDevSession(userId);
    return sendJson(res, 200, { ok: true, token: sessionToken, user: { id: userId, name, email } });
  }

  if (pathname === "/api/dev/login" && req.method === "POST") {
    const body = await readBody(req);
    const email = str(body.email, 96).trim().toLowerCase();
    const password = String(body.password || "").slice(0, 128);

    const entry = Object.entries(usersRegistry).find(([, u]) => u.email === email);
    if (!entry) return sendJson(res, 401, { ok: false, error: "Correo o contraseña incorrectos." });

    const [userId, user] = entry;
    if (!safeEqual(await hashPassword(password, user.salt), user.passHash)) {
      return sendJson(res, 401, { ok: false, error: "Correo o contraseña incorrectos." });
    }

    const sessionToken = newDevSession(userId);
    return sendJson(res, 200, { ok: true, token: sessionToken, user: { id: userId, name: user.name, email: user.email } });
  }

  if (pathname.startsWith("/api/dev/")) {
    const user = getDevUser(req);
    if (!user) return sendJson(res, 401, { ok: false, error: "Sesion expirada. Inicia sesion nuevamente." });

    // ---------------- Proyectos ----------------
    const isMine = (pid) => projectsRegistry[pid] && projectsRegistry[pid].userId === user.id;
    const myProjects = () => Object.keys(projectsRegistry).filter(isMine);

    // GET /api/dev/me
    if (pathname === "/api/dev/me" && req.method === "GET") {
      return sendJson(res, 200, { ok: true, user: { id: user.id, name: user.name, email: user.email } });
    }

    // GET /api/dev/projects -> lista de proyectos con números en vivo
    if (pathname === "/api/dev/projects" && req.method === "GET") {
      const list = myProjects().map((pid) => projectSummary(pid));
      return sendJson(res, 200, { ok: true, user: { name: user.name, email: user.email }, projects: list });
    }

    // POST /api/dev/projects {name} -> crea el proyecto con 2 claves: Desarrollo y Publicado
    if (pathname === "/api/dev/projects" && req.method === "POST") {
      const body = await readBody(req);
      const name = str(body.name || body.projectName || "Mi Juego", 48).trim() || "Mi Juego";
      if (myProjects().length >= MAX_PROJECTS_PER_DEV) {
        return sendJson(res, 400, { ok: false, error: `Máximo ${MAX_PROJECTS_PER_DEV} proyectos por cuenta.` });
      }
      const pid = createProject(name, user);
      createKey(pid, "Desarrollo", user);
      createKey(pid, "Publicado", user);
      return sendJson(res, 200, { ok: true, project: projectDetail(pid, true) });
    }

    // Jugadores del proyecto: GET lista · PUT {banned, name} · DELETE borra la cuenta
    const plMatch = pathname.match(/^\/api\/dev\/projects\/([^/]+)\/players(?:\/([^/]+))?$/);
    if (plMatch) {
      const pid = safeKey(decodeURIComponent(plMatch[1]));
      if (!isMine(pid)) return sendJson(res, 404, { ok: false, error: "Proyecto no encontrado" });
      return playersRoute(req, res, urlObj, pid, plMatch[2] ? decodeURIComponent(plMatch[2]) : "");
    }

    const projMatch = pathname.match(/^\/api\/dev\/projects\/([^/]+)(?:\/([a-z]+))?$/);
    if (projMatch) {
      const pid = safeKey(decodeURIComponent(projMatch[1]));
      const sub = projMatch[2] || "";
      if (!isMine(pid)) return sendJson(res, 404, { ok: false, error: "Proyecto no encontrado" });

      if (!sub && req.method === "GET") return sendJson(res, 200, { ok: true, project: projectDetail(pid, true) });

      if (!sub && req.method === "PUT") {
        const body = await readBody(req);
        const name = str(body.name, 48).trim();
        if (name) { projectsRegistry[pid].name = name; dirty.projects = true; }
        return sendJson(res, 200, { ok: true, project: projectSummary(pid) });
      }

      if (!sub && req.method === "DELETE") {
        deleteProject(pid);
        return sendJson(res, 200, { ok: true });
      }

      // POST /api/dev/projects/:id/keys {label}
      if (sub === "keys" && req.method === "POST") {
        const body = await readBody(req);
        if (projectKeys(pid).length >= MAX_KEYS_PER_PROJECT) {
          return sendJson(res, 400, { ok: false, error: `Máximo ${MAX_KEYS_PER_PROJECT} claves por proyecto.` });
        }
        const uuid = createKey(pid, str(body.label || "Clave", 32).trim() || "Clave", user);
        return sendJson(res, 200, { ok: true, uuid });
      }

      // POST /api/dev/projects/:id/request {service: "ai"|"discord"} -> pedir acceso al admin
      if (sub === "request" && req.method === "POST") {
        const body = await readBody(req);
        const service = body.service === "ai" ? "ai" : body.service === "discord" ? "discord" : "";
        if (!service) return sendJson(res, 400, { ok: false, error: "Servicio inválido" });
        const p = projectsRegistry[pid];
        p.requests = { ...(isObj(p.requests) ? p.requests : {}), [service]: new Date().toISOString() };
        dirty.projects = true;
        return sendJson(res, 200, { ok: true });
      }
      return sendJson(res, 404, { ok: false, error: "not_found" });
    }

    // ---------------- Claves ----------------
    // GET /api/dev/keys -> todas mis claves (compatibilidad con el panel viejo)
    if (pathname === "/api/dev/keys" && req.method === "GET") {
      const myKeys = Object.entries(keysRegistry)
        .filter(([, info]) => isMine(info.projectId))
        .map(([uuid, info]) => ({
          uuid,
          prefix: info.projectId,
          projectId: info.projectId,
          projectName: projectsRegistry[info.projectId].name,
          label: info.label || "Clave",
          active: info.active !== false,
          onlineNow: countActivePlayersByKey(uuid),
          createdAt: info.createdAt || null,
        }));
      return sendJson(res, 200, { ok: true, user: { name: user.name, email: user.email }, keys: myKeys });
    }

    // POST /api/dev/keys {projectName} -> (compatibilidad) crea un proyecto con una clave
    if (pathname === "/api/dev/keys" && req.method === "POST") {
      const body = await readBody(req);
      const name = str(body.projectName || "Mi Juego", 48).trim() || "Mi Juego";
      if (myProjects().length >= MAX_PROJECTS_PER_DEV) {
        return sendJson(res, 400, { ok: false, error: `Máximo ${MAX_PROJECTS_PER_DEV} proyectos por cuenta.` });
      }
      const pid = createProject(name, user);
      const uuid = createKey(pid, "Principal", user);
      return sendJson(res, 200, { ok: true, uuid, projectId: pid, data: keysRegistry[uuid] });
    }

    const devKeyMatch = pathname.match(/^\/api\/dev\/keys\/([^/]+?)(?:\/(rotate))?$/);
    if (devKeyMatch) {
      const uuid = decodeURIComponent(devKeyMatch[1]);
      const info = keysRegistry[uuid];
      if (!info || !isMine(info.projectId)) return sendJson(res, 404, { ok: false, error: "Clave no encontrada" });

      // PUT /api/dev/keys/:uuid {active, label} -> pausar / reanudar / renombrar
      if (!devKeyMatch[2] && req.method === "PUT") {
        const body = await readBody(req);
        if (body.active !== undefined) info.active = Boolean(body.active);
        if (body.label !== undefined) info.label = str(body.label, 32).trim() || info.label;
        dirty.keys = true;
        if (info.active === false) disconnectKey(uuid);
        return sendJson(res, 200, { ok: true });
      }
      // POST /api/dev/keys/:uuid/rotate -> cambia la clave por una nueva (los datos no se pierden)
      if (devKeyMatch[2] === "rotate" && req.method === "POST") {
        const fresh = newKeyUuid();
        keysRegistry[fresh] = { ...info, createdAt: new Date().toISOString() };
        delete keysRegistry[uuid];
        dirty.keys = true;
        disconnectKey(uuid);
        return sendJson(res, 200, { ok: true, uuid: fresh });
      }
      // DELETE /api/dev/keys/:uuid
      if (!devKeyMatch[2] && req.method === "DELETE") {
        delete keysRegistry[uuid];
        dirty.keys = true;
        disconnectKey(uuid);
        return sendJson(res, 200, { ok: true });
      }
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
                ...roomPublic(r),
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
        if (!col) return sendJson(res, 200, { ok: true, data: peekDb(g) });
        if (!key) return sendJson(res, 200, { ok: true, data: peekDb(g)[col] || {} });
        return sendJson(res, 200, { ok: true, data: (peekDb(g)[col] || {})[key] ?? null });
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
      const w = req.method === "GET" ? peekWorld(g) : gameWorld(g);

      if (req.method === "GET") {
        if (!roomId) return sendJson(res, 200, { ok: true, data: w.rooms });
        if (!objId) return sendJson(res, 200, { ok: true, data: w.rooms[roomId] || {} });
        return sendJson(res, 200, { ok: true, data: (w.rooms[roomId] || {})[objId] ?? null });
      }
      if (req.method === "PUT" && roomId && objId) {
        const body = await readBody(req);
        const props = body.props !== undefined ? body.props : body;
        if (!isObj(props)) return sendJson(res, 400, { ok: false, error: "bad_props" });
        if (!Number.isFinite(Number(objId))) return sendJson(res, 400, { ok: false, error: "bad_id" });
        (w.rooms[roomId] ||= {})[objId] = props;
        if (Number(objId) >= w.next) w.next = Number(objId) + 1; // que el juego no repita ese id después
        dirty.world = true;
        sendRoom(getGame(g), roomId, { t: "world", id: Number(objId), p: props });
        return sendJson(res, 200, { ok: true });
      }
      // POST .../world/:juego/:sala -> crea un objeto nuevo con el siguiente id
      if (req.method === "POST" && roomId && !objId) {
        const body = await readBody(req);
        const props = body.props !== undefined ? body.props : body;
        if (!isObj(props)) return sendJson(res, 400, { ok: false, error: "bad_props" });
        const id = w.next++;
        (w.rooms[roomId] ||= {})[id] = props;
        dirty.world = true;
        sendRoom(getGame(g), roomId, { t: "world", id, p: props });
        return sendJson(res, 200, { ok: true, id });
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
    return sendJson(res, 404, { ok: false, error: "not_found" });
  }

  // ------------------------------------------------------------------
  // Rutas protegidas de administracion (/api/admin/...)
  // ------------------------------------------------------------------
  if (pathname.startsWith("/api/admin")) {
    if (!ADMIN_TOKEN) return sendJson(res, 503, { ok: false, error: "admin_disabled: pon la variable ADMIN_TOKEN en el servidor" });
    if (!isAdminAuthorized(req)) return sendJson(res, 401, { ok: false, error: "unauthorized" });

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
            ...roomPublic(r),
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
          owner: projectsRegistry[info.projectId]?.owner || info.owner || "Admin",
          projectId: info.projectId || "",
          projectName: projectsRegistry[info.projectId]?.name || info.projectName || "Sin nombre",
          label: info.label || "",
          active: info.active !== false,
          onlineNow: countActivePlayersByKey(uuid),
          createdAt: info.createdAt || null,
        }));
        return sendJson(res, 200, { ok: true, keys: list });
      }
      if (req.method === "POST") {
        const body = await readBody(req);
        let pid = safeKey(body.projectId);
        if (!pid || !projectsRegistry[pid]) {
          const projectName = str(body.projectName || body.owner || "Proyecto", 48) || "Proyecto";
          pid = createProject(projectName, null);
        }
        const uuid = createKey(pid, str(body.label || "Principal", 32) || "Principal", null);
        return sendJson(res, 200, { ok: true, uuid, projectId: pid, data: keysRegistry[uuid] });
      }
    }

    const admPl = pathname.match(/^\/api\/admin\/players\/([^/]+)(?:\/([^/]+))?$/);
    if (admPl) return playersRoute(req, res, urlObj, safeKey(decodeURIComponent(admPl[1])), admPl[2] ? decodeURIComponent(admPl[2]) : "");

    // GET /api/admin/projects -> todos los proyectos (con pedidos de IA / Discord)
    if (pathname === "/api/admin/projects" && req.method === "GET") {
      return sendJson(res, 200, { ok: true, projects: Object.keys(projectsRegistry).map((pid) => projectSummary(pid)) });
    }
    const admProjMatch = pathname.match(/^\/api\/admin\/projects\/([^/]+)$/);
    if (admProjMatch) {
      const pid = safeKey(decodeURIComponent(admProjMatch[1]));
      if (!projectsRegistry[pid]) return sendJson(res, 404, { ok: false, error: "project_not_found" });
      if (req.method === "GET") return sendJson(res, 200, { ok: true, project: projectDetail(pid, true) });
      if (req.method === "PUT") {
        const body = await readBody(req);
        const p = projectsRegistry[pid];
        for (const service of ["ai", "discord"]) {
          if (typeof body[service] === "boolean") {
            p[service] = body[service];
            if (isObj(p.requests)) delete p.requests[service];
          }
        }
        if (typeof body.name === "string" && body.name.trim()) p.name = str(body.name, 48).trim();
        dirty.projects = true;
        return sendJson(res, 200, { ok: true, project: projectSummary(pid) });
      }
      if (req.method === "DELETE") {
        deleteProject(pid);
        return sendJson(res, 200, { ok: true });
      }
    }

    const keyMatch = pathname.match(/^\/api\/admin\/keys\/([^/]+)$/);
    if (keyMatch) {
      const uuid = decodeURIComponent(keyMatch[1]);
      if (!keysRegistry[uuid]) return sendJson(res, 404, { ok: false, error: "key_not_found" });

      if (req.method === "PUT") {
        const body = await readBody(req);
        if (body.active !== undefined) keysRegistry[uuid].active = Boolean(body.active);
        // Permisos por clave para usar TU IA / TU bot de Discord
        if (typeof body.ai === "boolean") keysRegistry[uuid].ai = body.ai;
        if (typeof body.discord === "boolean") keysRegistry[uuid].discord = body.discord;
        dirty.keys = true;
        if (keysRegistry[uuid].active === false) disconnectKey(uuid);
        return sendJson(res, 200, { ok: true, data: keysRegistry[uuid] });
      }
      if (req.method === "DELETE") {
        delete keysRegistry[uuid];
        dirty.keys = true;
        disconnectKey(uuid);
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
        if (!col) return sendJson(res, 200, { ok: true, data: peekDb(g) });
        if (!key) return sendJson(res, 200, { ok: true, data: peekDb(g)[col] || {} });
        return sendJson(res, 200, { ok: true, data: (peekDb(g)[col] || {})[key] ?? null });
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
      const w = req.method === "GET" ? peekWorld(g) : gameWorld(g);

      if (req.method === "GET") {
        if (!roomId) return sendJson(res, 200, { ok: true, data: w.rooms });
        if (!objId) return sendJson(res, 200, { ok: true, data: w.rooms[roomId] || {} });
        return sendJson(res, 200, { ok: true, data: (w.rooms[roomId] || {})[objId] ?? null });
      }
      if (req.method === "PUT" && roomId && objId) {
        const body = await readBody(req);
        const props = body.props !== undefined ? body.props : body;
        if (!isObj(props)) return sendJson(res, 400, { ok: false, error: "bad_props" });
        if (!Number.isFinite(Number(objId))) return sendJson(res, 400, { ok: false, error: "bad_id" });
        (w.rooms[roomId] ||= {})[objId] = props;
        if (Number(objId) >= w.next) w.next = Number(objId) + 1; // que el juego no repita ese id después
        dirty.world = true;
        sendRoom(getGame(g), roomId, { t: "world", id: Number(objId), p: props });
        return sendJson(res, 200, { ok: true });
      }
      // POST .../world/:juego/:sala -> crea un objeto nuevo con el siguiente id
      if (req.method === "POST" && roomId && !objId) {
        const body = await readBody(req);
        const props = body.props !== undefined ? body.props : body;
        if (!isObj(props)) return sendJson(res, 400, { ok: false, error: "bad_props" });
        const id = w.next++;
        (w.rooms[roomId] ||= {})[id] = props;
        dirty.world = true;
        sendRoom(getGame(g), roomId, { t: "world", id, p: props });
        return sendJson(res, 200, { ok: true, id });
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
      if (playersReg[g]) { delete playersReg[g]; dirty.players = true; }
      if (world[g]) { delete world[g]; dirty.world = true; }
      games.delete(g);
      loadedModules.delete(g);
      return sendJson(res, 200, { ok: true });
    }
    return sendJson(res, 404, { ok: false, error: "not_found" });
  }

  // Archivos estáticos del Panel Web (./public)
  if (pathname.startsWith("/api/")) return sendJson(res, 404, { ok: false, error: "not_found" });
  let relPath;
  try { relPath = decodeURIComponent(pathname === "/" ? "/index.html" : pathname); } catch { return sendJson(res, 400, { error: "bad_path" }); }
  const filePath = path.resolve(PUBLIC_DIR, "." + relPath);
  const rel = path.relative(path.resolve(PUBLIC_DIR), filePath);
  if (!rel || rel.startsWith("..") || path.isAbsolute(rel)) return sendJson(res, 403, { error: "forbidden" });

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath).toLowerCase();
    // no-cache: el navegador siempre pregunta si hay versión nueva del panel (así no se queda con la vieja)
    res.writeHead(200, { "Content-Type": MIME_TYPES[ext] || "application/octet-stream", "Cache-Control": "no-cache" });
    return fs.createReadStream(filePath).pipe(res);
  }

  // Cualquier otra ruta: info básica (sirve como "health check" de los hostings)
  sendJson(res, 200, { server: "Orbita", protocol: PROTOCOL, players: clients.size });
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

const connPerIp = new Map();
wss.on("connection", (ws, req) => {
  const ip = clientIp(req);
  const n = (connPerIp.get(ip) || 0) + 1;
  if (n > MAX_CONN_PER_IP) {
    try { ws.send(JSON.stringify({ t: "err", code: "too_many_connections", msg: "Demasiadas conexiones desde tu red" })); } catch {}
    return ws.close();
  }
  connPerIp.set(ip, n);
  const c = {
    id: nextId++, ws, ip, ready: false, apiKeyId: "", gameId: "", game: null,
    roomId: "", slot: -1, queue: null,
    state: {}, entities: {}, pdata: {},
    lastPing: Date.now(), rate: {}, rateWindow: Date.now(), rateCount: 0, uid: "", pname: "", tokenHash: "",
  };
  clients.set(c.id, c);
  ws._orbClient = c; // puede cambiar si este jugador "vuelve" a su lugar de antes
  // Si no manda "hello" a tiempo, se cierra (para que nadie acapare conexiones sin entrar)
  c.helloTimer = setTimeout(() => { if (!ws._orbClient.ready) ws.close(); }, HELLO_TIMEOUT_MS);
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
  ws.on("message", (raw) => {
    const cc = ws._orbClient;
    try { onMessage(cc, raw); } catch (e) { log(`Error con el mensaje de ${cc.id}:`, e.message); }
  });
  ws.on("close", () => {
    const left = (connPerIp.get(ip) || 1) - 1;
    if (left > 0) connPerIp.set(ip, left); else connPerIp.delete(ip);
    const cc = ws._orbClient;
    clearTimeout(cc.helloTimer);
    if (cc.ws !== ws) return; // esta conexión ya fue reemplazada por una nueva del mismo jugador
    if (cc.ready && cc.roomId && !cc.noResume && RESUME_GRACE_MS > 0) {
      // Se cayó estando en una sala: guardarle el lugar un rato
      cc.ghost = true;
      leaveQueue(cc);
      sendRoom(cc.game, cc.roomId, { t: "away", id: cc.id }, cc.id);
      cc.ghostTimer = setTimeout(() => { if (cc.ghost) finalizeClient(cc); }, RESUME_GRACE_MS);
      log(`[${cc.gameId}] ${cc.id} se cayó; se le guarda el lugar ${Math.round(RESUME_GRACE_MS / 1000)} s`);
      return;
    }
    finalizeClient(cc);
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
  if (!ADMIN_TOKEN) log("AVISO: no hay ADMIN_TOKEN -> el panel de admin está apagado. Ponlo para usarlo.");
  if (!GAME_KEY) log("AVISO: no hay GAME_KEY -> solo entran juegos con claves de desarrollador.");
  log(`IA: ${AI_API_KEY ? `${AI_PROVIDER} / ${AI_MODEL}` : "desactivada"} | Discord: ${DISCORD_BOT_TOKEN ? "activado" : "desactivado"} | Modo 100% Gratuito`);
  startDiscord();
});
// Solo para la prueba de carga (npm run carga): si este servidor lo arrancó ese programa,
// le contesta cuánta CPU y memoria está usando. Funciona igual en Windows, Mac y Linux.
// Cuando lo corres normal (node server.js / Render) esto no hace nada.
if (typeof process.send === "function") {
  process.on("message", (m) => {
    if (m === "orb_stats") process.send({ cpu: process.cpuUsage(), rss: process.memoryUsage().rss, t: Date.now() });
    if (m === "orb_exit") process.exit(0); // salida limpia (así se guarda el perfil de --perfil)
  });
}
