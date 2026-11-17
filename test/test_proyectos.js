// Pruebas de proyectos, claves por proyecto y versiones (ORB_GAME_ID).  Uso: npm test  (puerto 3995)
const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");
const os = require("os");
const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 3995;
const md5 = (s) => crypto.createHash("md5").update(s, "utf8").digest("hex");
let pass = 0, fail = 0;
const ok = (c, msg) => { c ? pass++ : fail++; console.log((c ? "  OK   " : "  FALLA") + "  " + msg); };

function client(key, game = "MiJuego") {
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
    ws.on("open", () => ws.send(JSON.stringify({ t: "hello", proto: 1, game, key: md5(key) })));
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
    req.on("error", (e) => resolve({ status: 0, json: null }));
    if (data !== undefined) req.write(data);
    req.end();
  });
}

(async () => {
  // ---- Datos "viejos": una clave = un juego ----
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "orbita-proy-"));
  const LEGACY = "aaaaaaaa-1111-4111-8111-111111111111";
  const salt = "sal123";
  const passHash = crypto.scryptSync("123456", salt, 64).toString("hex");
  fs.writeFileSync(path.join(dataDir, "users.json"), JSON.stringify({ u1: { name: "Ryan", email: "ryan@x.com", salt, passHash } }));
  fs.writeFileSync(path.join(dataDir, "keys.json"), JSON.stringify({ [LEGACY]: { userId: "u1", owner: "Ryan", projectName: "Capture The Flag", active: true } }));
  fs.writeFileSync(path.join(dataDir, "db.json"), JSON.stringify({ k_aaaaaaaa_MiJuego: { perfiles: { ana: { oro: 5 } } } }));
  fs.writeFileSync(path.join(dataDir, "world.json"), JSON.stringify({ k_aaaaaaaa_MiJuego: { next: 2, rooms: { lobby: { 1: { tipo: "cofre" } } } } }));

  const srv = spawn(process.execPath, [path.join(__dirname, "..", "server.js")], {
    env: { ...process.env, PORT: String(PORT), DATA_DIR: dataDir, GAME_KEY: "", ADMIN_TOKEN: "adm", AI_API_KEY: "falsa", AI_API_URL: "http://127.0.0.1:1/", DISCORD_BOT_TOKEN: "" },
    stdio: "ignore",
  });
  await sleep(800);
  try {
    console.log("\nMigración de claves viejas");
    ok(fs.readdirSync(dataDir).some((f) => f.startsWith("respaldo-")), "se hace un respaldo de la carpeta data antes de migrar");
    const projs = JSON.parse(fs.readFileSync(path.join(dataDir, "projects.json"), "utf8"));
    ok(projs.k_aaaaaaaa && projs.k_aaaaaaaa.name === "Capture The Flag", "la clave vieja se convierte en un proyecto con su nombre");
    const old = await client(LEGACY);
    ok(old.id > 0, "la clave vieja sigue entrando");
    let r = await old.req("db_get", { c: "perfiles", k: "ana" });
    ok(r.ok && r.d.oro === 5, "la base de datos vieja sigue ahí");
    await old.req("join", { rm: "lobby" });
    ok(old.last("room").world.some((o) => o.p.tipo === "cofre"), "los objetos del mundo viejos siguen ahí");

    console.log("\nProyectos desde el panel");
    r = await httpReq("POST", "/api/dev/login", { body: { email: "ryan@x.com", password: "123456" } });
    const tok = r.json?.token;
    ok(tok, "iniciar sesión (contraseña creada con la versión vieja)");
    r = await httpReq("GET", "/api/dev/projects", { token: tok });
    ok(r.json.projects.some((p) => p.id === "k_aaaaaaaa" && p.online === 1), "el proyecto migrado aparece con 1 jugador en línea");
    r = await httpReq("POST", "/api/dev/projects", { token: tok, body: { name: "Nuevo Juego" } });
    const np = r.json.project;
    ok(np && np.id.startsWith("p_") && np.keyList.length === 2 && np.keyList[0].label === "Desarrollo" && np.keyList[1].label === "Publicado", "crear proyecto -> trae clave de Desarrollo y de Publicado");
    const devKey = np.keyList[0].uuid, pubKey = np.keyList[1].uuid;

    console.log("\nVersiones (ORB_GAME_ID)");
    const v10 = await client(devKey, "1.0"), v11 = await client(pubKey, "1.1"), v11b = await client(devKey, "1.1");
    await v10.req("join", { rm: "lobby" }); await v11.req("join", { rm: "lobby" }); await v11b.req("join", { rm: "lobby" });
    ok(v11b.last("room").players.length === 1 && v11b.last("room").players[0].id === v11.id, "misma versión: se juntan (aunque usen claves distintas)");
    ok(v10.last("room").players.length === 0, "otra versión: no se juntan");
    await v10.req("db_set", { c: "perfiles", k: "leo", v: { nivel: 9 } });
    r = await v11.req("db_get", { c: "perfiles", k: "leo" });
    ok(r.ok && r.d.nivel === 9, "todas las versiones comparten la base de datos del proyecto");
    r = await old.req("db_get", { c: "perfiles", k: "leo" });
    ok(!r.ok, "y no se mezcla con otro proyecto");

    r = await httpReq("GET", `/api/dev/projects/${np.id}`, { token: tok });
    const chans = r.json.project.channels.map((c) => c.gameId).sort();
    ok(chans.join(",") === "1.0,1.1", "el detalle del proyecto muestra las versiones 1.0 y 1.1");
    const c11 = r.json.project.channels.find((c) => c.gameId === "1.1");
    ok(c11.rooms[0].id === "lobby" && c11.rooms[0].players.length === 2, "y sus salas con jugadores");
    ok(r.json.project.collections.some((c) => c.name === "perfiles" && c.docs === 1), "y sus colecciones de la base de datos");

    console.log("\nCambiar y pausar claves");
    r = await httpReq("POST", `/api/dev/keys/${pubKey}/rotate`, { token: tok });
    const pubKey2 = r.json?.uuid;
    await sleep(120);
    ok(pubKey2 && v11.closed, "cambiar la clave desconecta a los que usaban la vieja");
    const again = await client(pubKey);
    ok(again.error?.code === "bad_key", "la clave vieja ya no entra");
    const fresh = await client(pubKey2, "1.1");
    r = await fresh.req("db_get", { c: "perfiles", k: "leo" });
    ok(r.ok && r.d.nivel === 9, "con la clave nueva los datos siguen ahí");
    r = await httpReq("PUT", `/api/dev/keys/${pubKey2}`, { token: tok, body: { active: false } });
    await sleep(120);
    ok(r.status === 200 && fresh.closed, "pausar una clave saca a sus jugadores");

    console.log("\nIA y Discord: pedir acceso");
    r = await v10.req("ai", { msgs: [{ role: "user", content: "hola" }] });
    ok(!r.ok && r.d === "ai_not_allowed", "sin permiso no usa tu IA");
    await httpReq("POST", `/api/dev/projects/${np.id}/request`, { token: tok, body: { service: "ai" } });
    r = await httpReq("GET", "/api/admin/projects", { token: "adm" });
    ok(r.json.projects.find((p) => p.id === np.id)?.requests?.ai, "el admin ve el pedido");
    await httpReq("PUT", `/api/admin/projects/${np.id}`, { token: "adm", body: { ai: true } });
    r = await v10.req("ai", { msgs: [{ role: "user", content: "hola" }] });
    ok(r.d !== "ai_not_allowed", "al aprobarlo, el proyecto ya puede usar la IA");

    console.log("\nCada quien lo suyo");
    r = await httpReq("POST", "/api/dev/register", { body: { name: "Otro", email: "otro@x.com", password: "123456" } });
    const tok2 = r.json.token;
    r = await httpReq("GET", `/api/dev/projects/${np.id}`, { token: tok2 });
    ok(r.status === 404, "otro desarrollador no ve tu proyecto");
    r = await httpReq("GET", `/api/dev/db/${np.id}`, { token: tok2 });
    ok(r.status === 403, "ni tu base de datos");
    r = await httpReq("GET", `/api/dev/world/${np.id}_1.0`, { token: tok });
    ok(r.status === 200 && r.json.ok, "tú sí ves los objetos del mundo de tu versión");

    console.log("\nBorrar proyecto");
    r = await httpReq("DELETE", `/api/dev/projects/${np.id}`, { token: tok });
    await sleep(120);
    ok(r.status === 200 && v10.closed, "borrar el proyecto desconecta a sus jugadores");
    const gone = await client(devKey, "1.0");
    ok(gone.error?.code === "bad_key", "y sus claves dejan de existir");
    r = await httpReq("GET", "/api/dev/projects", { token: tok });
    ok(!r.json.projects.some((p) => p.id === np.id) && r.json.projects.length === 1, "ya no aparece en la lista");
  } catch (e) {
    fail++; console.log("  FALLA  excepción:", e);
  }
  srv.kill();
  console.log(`\n${pass} pruebas OK, ${fail} fallas\n`);
  process.exit(fail ? 1 : 0);
})();
