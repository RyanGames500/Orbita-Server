// Pruebas de seguridad del servidor Órbita.  Uso: npm test   (arranca su propio servidor en el puerto 3996)
const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");
const os = require("os");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 3996;
const MASTER = "clave_maestra_prueba";
const md5 = (s) => crypto.createHash("md5").update(s, "utf8").digest("hex");
let pass = 0, fail = 0;
const ok = (c, msg) => { c ? pass++ : fail++; console.log((c ? "  OK   " : "  FALLA") + "  " + msg); };

function client(game = "Prueba", key = md5(MASTER)) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${PORT}`);
    const c = { ws, log: [], id: -1, r: 1, pending: {}, closed: false };
    ws.on("message", (raw) => {
      const m = JSON.parse(raw);
      c.log.push(m);
      if (m.t === "welcome") { c.id = m.id; resolve(c); }
      if (m.t === "res" && c.pending[m.r]) { c.pending[m.r](m); delete c.pending[m.r]; }
      if (m.t === "err") { c.error = m; resolve(c); }
    });
    ws.on("close", () => { c.closed = true; resolve(c); });
    ws.on("error", () => {});
    ws.on("open", () => ws.send(JSON.stringify({ t: "hello", proto: 1, game, key })));
    c.send = (o) => ws.send(JSON.stringify(o));
    c.req = (op, args = {}) => new Promise((res) => { const r = c.r++; c.pending[r] = res; c.send({ t: "req", r, op, ...args }); });
    c.last = (t, f = () => true) => [...c.log].reverse().find((m) => m.t === t && f(m));
  });
}

function httpReq(method, p, { body, token, raw } = {}) {
  return new Promise((resolve) => {
    const data = raw !== undefined ? raw : body !== undefined ? JSON.stringify(body) : undefined;
    const req = http.request({ host: "127.0.0.1", port: PORT, method, path: p, headers: {
      "content-type": "application/json",
      ...(token ? { authorization: "Bearer " + token } : {}),
    } }, (res) => {
      let txt = "";
      res.on("data", (d) => (txt += d));
      res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, text: txt, json: (() => { try { return JSON.parse(txt); } catch { return null; } })() }));
    });
    req.on("error", (e) => resolve({ status: 0, text: String(e) }));
    if (data !== undefined) req.write(data);
    req.end();
  });
}

function startServer(env) {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "orbita-seg-"));
  const srv = spawn(process.execPath, [path.join(__dirname, "..", "server.js")], {
    env: { ...process.env, PORT: String(PORT), DATA_DIR: dataDir, GAME_KEY: MASTER, ADMIN_TOKEN: "", AI_API_KEY: "", DISCORD_BOT_TOKEN: "", ...env },
    stdio: "ignore",
  });
  return srv;
}

(async () => {
  let srv;
  try {
    // ---------- Servidor SIN ADMIN_TOKEN ----------
    console.log("\nPanel de admin");
    srv = startServer({});
    await sleep(700);
    let r = await httpReq("GET", "/api/admin/overview", { token: "orbita_dev_token" });
    ok(r.status === 503, "sin ADMIN_TOKEN el admin está apagado (la clave vieja por defecto ya no entra)");
    srv.kill(); await sleep(300);

    // ---------- Servidor principal ----------
    srv = startServer({ ADMIN_TOKEN: "admin_secreto", AI_API_KEY: "falsa", MAX_DB_VALUE_BYTES: "1000", MAX_ENTITIES_PER_PLAYER: "3" });
    await sleep(700);
    r = await httpReq("GET", "/api/admin/overview", { token: "otra_cosa" });
    ok(r.status === 401, "token de admin incorrecto -> 401");
    r = await httpReq("GET", "/api/admin/overview?token=admin_secreto");
    ok(r.status === 401, "el token en la URL ya no sirve (solo en la cabecera)");
    r = await httpReq("GET", "/api/admin/overview", { token: "admin_secreto" });
    ok(r.status === 200 && r.json.ok, "token de admin correcto -> entra");
    ok(!r.headers["access-control-allow-origin"], "el admin no manda CORS abierto");

    console.log("\nInfo pública");
    r = await httpReq("GET", "/health");
    ok(r.status === 200 && r.json.games === undefined && r.headers["access-control-allow-origin"] === "*", "/health no muestra la lista de juegos");
    r = await httpReq("GET", "/%2e%2e/server.js");
    ok(!r.text.includes("require("), "no se puede leer server.js con ../");
    r = await httpReq("GET", "/api/cualquier_cosa");
    ok(r.status === 404, "ruta /api desconocida -> 404");

    console.log("\nCuentas de desarrollador");
    r = await httpReq("POST", "/api/dev/register", { raw: "null" });
    ok(r.status === 400, "cuerpo 'null' ya no da error 500");
    r = await httpReq("POST", "/api/dev/register", { body: { name: "Dev", email: "dev@x.com", password: "123456" } });
    const devToken = r.json?.token;
    ok(r.status === 200 && devToken, "registrarse funciona");
    r = await httpReq("POST", "/api/dev/login", { body: { email: "dev@x.com", password: "123456" } });
    ok(r.status === 200 && r.json.token, "iniciar sesión funciona");
    r = await httpReq("POST", "/api/dev/login", { body: { email: "dev@x.com", password: "mala" } });
    ok(r.status === 401, "contraseña mala -> 401");
    r = await httpReq("GET", "/api/dev/no_existe", { token: devToken });
    ok(r.status === 404, "ruta /api/dev desconocida -> 404 (antes daba 200)");
    r = await httpReq("POST", "/api/dev/keys", { token: devToken, body: { projectName: "JuegoDev" } });
    const devKey = r.json?.uuid;
    ok(r.status === 200 && devKey, "crear clave de desarrollador");

    console.log("\nIA y Discord con clave de desarrollador");
    const dev = await client("JuegoDev", md5(devKey));
    ok(dev.id > 0, "conecta con clave de desarrollador");
    r = await dev.req("ai", { msgs: [{ role: "user", content: "hola" }] });
    ok(!r.ok && r.d === "ai_not_allowed", "clave de desarrollador no gasta tu IA si no le das permiso");

    console.log("\nRevocar claves");
    r = await httpReq("PUT", `/api/admin/keys/${devKey}`, { token: "admin_secreto", body: { active: false } });
    await sleep(150);
    ok(r.status === 200 && dev.closed, "al desactivar una clave se desconecta a los que la usan");
    const dev2 = await client("JuegoDev", md5(devKey));
    ok(dev2.error?.code === "bad_key", "y ya no puede volver a entrar");

    console.log("\nEventos y entidades");
    const a = await client(), b = await client(), c = await client();
    await a.req("join", { rm: "sala1" }); await b.req("join", { rm: "sala1" }); await c.req("join", { rm: "sala2" });
    a.send({ t: "ev", to: c.id, n: "spam", d: {} });
    a.send({ t: "ev", to: b.id, n: "hola", d: {} });
    await sleep(100);
    ok(!c.last("ev", (m) => m.n === "spam"), "no puedes mandarle eventos a alguien de otra sala");
    ok(b.last("ev", (m) => m.n === "hola"), "a alguien de tu sala sí");
    a.ws.send('{"t":"st","e":{"__proto__":{"x":1,"hack":true},"1":{"x":1}}}');
    await sleep(80);
    const st = b.last("st");
    ok(st && st.e["1"] && !Object.prototype.hasOwnProperty.call(st.e, "__proto__") && st.e.hack === undefined, "entidad '__proto__' se ignora");
    a.send({ t: "st", e: { 2: { x: 1 }, 3: { x: 1 }, 4: { x: 1 }, 5: { x: 1 } } });
    await sleep(80);
    r = await httpReq("GET", "/api/admin/overview", { token: "admin_secreto" });
    const me = r.json.games.Prueba.rooms.find((x) => x.id === "sala1");
    ok(me && r.json.ok, "overview de admin sigue funcionando");
    const late = await client(); await late.req("join", { rm: "sala1" });
    const aInfo = late.last("room").players.find((p) => p.id === a.id);
    ok(Object.keys(aInfo.e).length === 3, "límite de entidades por jugador (3 en esta prueba)");

    console.log("\nBase de datos");
    r = await httpReq("PUT", "/api/admin/db/Prueba/ro_ranking/top", { token: "admin_secreto", body: { value: [1, 2, 3] } });
    r = await a.req("db_get", { c: "ro_ranking", k: "top" });
    ok(r.ok && r.d.length === 3, "colección ro_: los jugadores pueden leer");
    r = await a.req("db_set", { c: "ro_ranking", k: "top", v: [999] });
    ok(!r.ok && r.d === "forbidden_collection", "colección ro_: los jugadores NO pueden escribir");
    r = await a.req("db_set", { c: "perfiles", k: "x", v: "a".repeat(2000) });
    ok(!r.ok && r.d === "value_too_large", "valor demasiado grande se rechaza");
    r = await a.req("db_merge", { c: "perfiles", k: "y", v: JSON.parse('{"__proto__":{"hack":1},"oro":5}') });
    ok(r.ok && r.d.oro === 5 && r.d.hack === undefined, "db_merge ignora '__proto__'");

    console.log("\nLímite de intentos de login");
    let blocked = false;
    for (let i = 0; i < 12; i++) {
      r = await httpReq("POST", "/api/dev/login", { body: { email: "dev@x.com", password: "mala" } });
      if (r.status === 429) { blocked = true; break; }
    }
    ok(blocked, "muchos intentos de login seguidos -> 429");
    srv.kill(); await sleep(300);

    // ---------- Límite de conexiones por IP ----------
    console.log("\nConexiones por IP");
    srv = startServer({ MAX_CONN_PER_IP: "3" });
    await sleep(700);
    const cs = [];
    for (let i = 0; i < 4; i++) cs.push(await client());
    ok(cs[3].error?.code === "too_many_connections" && cs[0].id > 0, "la cuarta conexión desde la misma IP se rechaza (límite 3)");
  } catch (e) {
    fail++; console.log("  FALLA  excepción:", e);
  }
  if (srv) srv.kill();
  console.log(`\n${pass} pruebas OK, ${fail} fallas\n`);
  process.exit(fail ? 1 : 0);
})();
