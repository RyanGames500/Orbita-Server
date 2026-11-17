// Pruebas de cuentas de jugador (invitado, usuario, nombre, colecciones mi_, bloqueo).  Uso: npm test  (puerto 3993)
const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");
const os = require("os");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 3993;
const md5 = (s) => crypto.createHash("md5").update(s, "utf8").digest("hex");
let pass = 0, fail = 0;
const ok = (c, msg) => { c ? pass++ : fail++; console.log((c ? "  OK   " : "  FALLA") + "  " + msg); };

function client(key, auth = "", game = "1.0") {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${PORT}`);
    const c = { ws, log: [], id: -1, r: 1, pending: {}, closed: false };
    ws.on("message", (raw) => {
      const m = JSON.parse(raw);
      c.log.push(m);
      if (m.t === "welcome") { c.id = m.id; c.welcome = m; resolve(c); }
      if (m.t === "res" && c.pending[m.r]) { c.pending[m.r](m); delete c.pending[m.r]; }
      if (m.t === "err") { c.error = m; resolve(c); }
    });
    ws.on("close", () => { c.closed = true; resolve(c); });
    ws.on("error", () => {});
    ws.on("open", () => ws.send(JSON.stringify({ t: "hello", proto: 1, game, key: md5(key), ...(auth ? { auth } : {}) })));
    c.send = (o) => ws.send(JSON.stringify(o));
    c.req = (op, args = {}) => new Promise((res) => { const r = c.r++; c.pending[r] = res; c.send({ t: "req", r, op, ...args }); });
    c.last = (t, f = () => true) => [...c.log].reverse().find((m) => m.t === t && f(m));
  });
}
function httpReq(method, p, { body, token } = {}) {
  return new Promise((resolve) => {
    const data = body !== undefined ? JSON.stringify(body) : undefined;
    const req = http.request({ host: "127.0.0.1", port: PORT, method, path: p, headers: {
      "content-type": "application/json", ...(token ? { authorization: "Bearer " + token } : {}),
    } }, (res) => {
      let txt = "";
      res.on("data", (d) => (txt += d));
      res.on("end", () => { let j = null; try { j = JSON.parse(txt); } catch {} resolve({ status: res.statusCode, json: j }); });
    });
    req.on("error", () => resolve({ status: 0, json: null }));
    if (data !== undefined) req.write(data);
    req.end();
  });
}

(async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "orbita-jug-"));
  const srv = spawn(process.execPath, [path.join(__dirname, "..", "server.js")], {
    env: { ...process.env, PORT: String(PORT), DATA_DIR: dataDir, GAME_KEY: "", ADMIN_TOKEN: "adm" },
    stdio: "ignore",
  });
  await sleep(800);
  try {
    // Desarrollador con dos proyectos
    let r = await httpReq("POST", "/api/dev/register", { body: { name: "Ryan", email: "r@x.com", password: "123456" } });
    const tok = r.json.token;
    const A = (await httpReq("POST", "/api/dev/projects", { token: tok, body: { name: "Juego A" } })).json.project;
    const B = (await httpReq("POST", "/api/dev/projects", { token: tok, body: { name: "Juego B" } })).json.project;
    const keyA = A.keyList[0].uuid, keyB = B.keyList[0].uuid;

    console.log("\nInvitado");
    const ana = await client(keyA);
    r = await ana.req("auth_guest");
    const anaUid = r.d.uid, anaToken = r.d.token;
    ok(r.ok && anaUid.startsWith("u_") && anaToken && r.d.guest && r.d.name.startsWith("Jugador-"), "entrar como invitado da id, token y nombre");
    r = await ana.req("auth_guest");
    ok(r.ok && r.d.uid === anaUid && !r.d.token, "pedirlo otra vez no crea otra cuenta");
    const ana2 = await client(keyA, anaToken);
    ok(ana2.welcome.uid === anaUid, "con el token guardado vuelve a ser el mismo jugador (al conectar)");
    const bad = await client(keyA, "token_falso");
    ok(bad.welcome.auth_err === "bad_token" && !bad.welcome.uid, "token falso -> bad_token");
    const otroJuego = await client(keyB, anaToken);
    ok(otroJuego.welcome.auth_err === "bad_token", "las cuentas de un proyecto no sirven en otro");
    ana2.ws.close();

    console.log("\nNombre");
    const leo = await client(keyA);
    await leo.req("auth_guest");
    await ana.req("join", { rm: "lobby" }); await leo.req("join", { rm: "lobby" });
    r = await ana.req("set_name", { name: "  Ana<script>  " });
    await sleep(80);
    ok(r.ok && r.d === "Anascript", "cambiar nombre (se limpia)");
    ok(leo.last("name")?.id === ana.id && leo.last("name").name === "Anascript", "los de la sala reciben el nombre nuevo");
    const late = await client(keyA); await late.req("join", { rm: "lobby" });
    const snap = late.last("room").players.find((p) => p.id === ana.id);
    ok(snap.uid === anaUid && snap.nm === "Anascript", "quien entra tarde ve el id de cuenta y el nombre");

    console.log("\nColecciones mi_");
    r = await ana.req("db_set", { c: "mi_perfil", k: "", v: { oro: 10 } });
    ok(r.ok, "guardar en mi_perfil (clave vacía = mi cuenta)");
    r = await ana.req("db_get", { c: "mi_perfil", k: "" });
    ok(r.ok && r.d.oro === 10, "leer lo mío");
    r = await leo.req("db_get", { c: "mi_perfil", k: anaUid });
    ok(!r.ok && r.d === "forbidden_key", "otro jugador no puede leer lo mío");
    r = await leo.req("db_set", { c: "mi_perfil", k: anaUid, v: { oro: 999999 } });
    ok(!r.ok && r.d === "forbidden_key", "ni escribirlo");
    r = await late.req("db_get", { c: "mi_perfil", k: "" });
    ok(!r.ok && r.d === "not_logged_in", "sin cuenta no se puede usar mi_");
    r = await leo.req("db_list", { c: "mi_perfil" });
    ok(r.ok && r.d.length === 0, "db_list en mi_ solo muestra lo tuyo");

    console.log("\nUsuario y contraseña");
    r = await ana.req("auth_register", { user: "ana_99", pass: "secreta1" });
    ok(r.ok && r.d.uid === anaUid && r.d.user === "ana_99" && !r.d.guest, "un invitado se registra y se queda con su progreso");
    r = await leo.req("auth_register", { user: "ANA_99", pass: "otra123" });
    ok(!r.ok && r.d === "user_taken", "usuario repetido (sin importar mayúsculas)");
    r = await leo.req("auth_register", { user: "a", pass: "123456" });
    ok(!r.ok && r.d === "bad_user", "usuario muy corto");
    const pc2 = await client(keyA);
    r = await pc2.req("auth_login", { user: "ana_99", pass: "mala" });
    ok(!r.ok && r.d === "wrong_login", "contraseña mala");
    r = await pc2.req("auth_login", { user: "Ana_99", pass: "secreta1" });
    ok(r.ok && r.d.uid === anaUid && r.d.token, "entrar desde otra PC con usuario y contraseña");
    r = await pc2.req("db_get", { c: "mi_perfil", k: "" });
    ok(r.ok && r.d.oro === 10, "y ver su progreso");
    r = await pc2.req("auth_logout");
    r = await pc2.req("db_get", { c: "mi_perfil", k: "" });
    ok(!r.ok && r.d === "not_logged_in", "cerrar sesión");

    console.log("\nPanel: jugadores del proyecto");
    r = await httpReq("GET", `/api/dev/projects/${A.id}/players`, { token: tok });
    ok(r.json.ok && r.json.total === 2 && r.json.registered === 1 && r.json.players.some((p) => p.uid === anaUid && p.user === "ana_99"), "el desarrollador ve sus jugadores");
    r = await httpReq("GET", `/api/dev/projects/${A.id}/players?q=ana`, { token: tok });
    ok(r.json.players.length === 1, "buscar jugador");
    r = await httpReq("POST", "/api/dev/register", { body: { name: "Otro", email: "o@x.com", password: "123456" } });
    r = await httpReq("GET", `/api/dev/projects/${A.id}/players`, { token: r.json.token });
    ok(r.status === 404, "otro desarrollador no los ve");

    console.log("\nBloquear y borrar");
    r = await httpReq("PUT", `/api/dev/projects/${A.id}/players/${anaUid}`, { token: tok, body: { banned: true } });
    await sleep(120);
    ok(r.status === 200 && ana.closed && ana.error?.code === "banned", "bloquear saca al jugador");
    const again = await client(keyA, anaToken);
    ok(again.welcome.auth_err === "banned" && !again.welcome.uid, "y no puede volver a entrar con su cuenta");
    r = await again.req("auth_login", { user: "ana_99", pass: "secreta1" });
    ok(!r.ok && r.d === "banned", "ni con usuario y contraseña");
    await httpReq("PUT", `/api/dev/projects/${A.id}/players/${anaUid}`, { token: tok, body: { banned: false } });
    r = await httpReq("DELETE", `/api/dev/projects/${A.id}/players/${anaUid}`, { token: tok });
    r = await httpReq("GET", `/api/dev/db/${A.id}`, { token: tok });
    ok(!r.json.data.mi_perfil || !r.json.data.mi_perfil[anaUid], "borrar la cuenta borra sus datos mi_");
    const gone = await client(keyA, anaToken);
    ok(gone.welcome.auth_err === "bad_token", "y su token deja de servir");
    r = await httpReq("GET", `/api/admin/players/${A.id}`, { token: "adm" });
    ok(r.json.ok && r.json.total === 1, "el admin también ve los jugadores");

    console.log("\nSe guarda en disco");
    await sleep(2300);
    const saved = JSON.parse(fs.readFileSync(path.join(dataDir, "players.json"), "utf8"));
    const accs = saved[A.id].accounts;
    ok(Object.keys(accs).length === 1 && !JSON.stringify(saved).includes(anaToken), "las cuentas se guardan (los tokens no se guardan tal cual)");
  } catch (e) {
    fail++; console.log("  FALLA  excepción:", e);
  }
  srv.kill();
  console.log(`\n${pass} pruebas OK, ${fail} fallas\n`);
  process.exit(fail ? 1 : 0);
})();
