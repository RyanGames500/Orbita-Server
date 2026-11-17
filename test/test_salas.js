// Pruebas de salas: código, contraseña, privadas, cerradas y datos del host.  Uso: npm test  (puerto 3992)
const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");
const os = require("os");
const crypto = require("crypto");
const WebSocket = require("ws");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 3992;
const KEY = "clave_salas";
const md5 = (s) => crypto.createHash("md5").update(s, "utf8").digest("hex");
let pass = 0, fail = 0;
const ok = (c, msg) => { c ? pass++ : fail++; console.log((c ? "  OK   " : "  FALLA") + "  " + msg); };

function client() {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${PORT}`);
    const c = { ws, log: [], id: -1, r: 1, pending: {} };
    ws.on("message", (raw) => {
      const m = JSON.parse(raw);
      c.log.push(m);
      if (m.t === "welcome") { c.id = m.id; resolve(c); }
      if (m.t === "res" && c.pending[m.r]) { c.pending[m.r](m); delete c.pending[m.r]; }
    });
    ws.on("open", () => ws.send(JSON.stringify({ t: "hello", proto: 1, game: "Salas", key: md5(KEY) })));
    c.send = (o) => ws.send(JSON.stringify(o));
    c.req = (op, args = {}) => new Promise((res) => { const r = c.r++; c.pending[r] = res; c.send({ t: "req", r, op, ...args }); });
    c.last = (t, f = () => true) => [...c.log].reverse().find((m) => m.t === t && f(m));
  });
}

(async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "orbita-salas-"));
  const srv = spawn(process.execPath, [path.join(__dirname, "..", "server.js")], {
    env: { ...process.env, PORT: String(PORT), DATA_DIR: dataDir, GAME_KEY: KEY }, stdio: "ignore",
  });
  await sleep(800);
  try {
    const host = await client(), ana = await client(), leo = await client(), sofi = await client();

    console.log("\nCrear sala con código");
    let r = await host.req("room_create", { mx: 4, pw: "1234", props: { nombre: "Sala de Ryan", mapa: "Bermuda", modo: "4v4" } });
    const code = r.d;
    ok(r.ok && /^[A-HJ-NP-Z2-9]{6}$/.test(code), "crea la sala con un código de 6 letras");
    const snap = host.last("room");
    ok(snap.id === code && snap.host === host.id && snap.has_pw === true && snap.props.mapa === "Bermuda" && snap.max === 4, "quien la crea queda de host y ve los datos de la sala");
    r = await ana.req("room_create", { rm: code });
    ok(!r.ok && r.d === "room_exists", "no se puede crear una sala que ya existe");

    console.log("\nContraseña");
    r = await ana.req("join", { rm: code });
    ok(!r.ok && r.d === "wrong_password", "sin contraseña no entra");
    r = await ana.req("join", { rm: code, pw: "9999" });
    ok(!r.ok && r.d === "wrong_password", "contraseña mala no entra");
    r = await ana.req("join", { rm: code, pw: "1234" });
    ok(r.ok && ana.last("room").id === code && ana.last("room").props.nombre === "Sala de Ryan", "con la contraseña buena entra y ve los datos");
    let blocked = false;
    for (let i = 0; i < 12; i++) { r = await leo.req("join", { rm: code, pw: "x" + i }); if (r.d === "rate_limited") { blocked = true; break; } }
    ok(blocked, "muchos intentos de contraseña seguidos -> rate_limited");

    console.log("\nLista de salas");
    await sofi.req("room_create", { rm: "secreta", private: true, props: { nombre: "Solo amigos" } });
    r = await ana.req("rooms");
    const mine = r.d.find((x) => x.id === code);
    ok(mine && mine.has_pw === true && mine.props.mapa === "Bermuda" && mine.players === 2 && mine.max === 4, "la lista trae datos, jugadores y si tiene contraseña");
    ok(!r.d.some((x) => x.id === "secreta"), "las salas privadas no salen en la lista");
    r = await leo.req("join", { rm: "secreta" });
    ok(r.ok, "pero se puede entrar sabiendo el código");

    console.log("\nEl host cambia la sala");
    host.log = []; ana.log = [];
    r = await ana.req("room_set", { props: { mapa: "Kalahari" } });
    ok(!r.ok && r.d === "not_host", "solo el host puede cambiarla");
    r = await host.req("room_set", { props: { mapa: "Kalahari", modo: null }, pw: "" });
    await sleep(60);
    const ev = ana.last("room_props");
    ok(r.ok && ev && ev.props.mapa === "Kalahari" && ev.props.modo === undefined && ev.props.nombre === "Sala de Ryan" && ev.has_pw === false, "los de la sala reciben los datos nuevos (null borra un dato)");
    r = await leo.req("join", { rm: code });
    ok(r.ok, "sin contraseña ya entra cualquiera");
    await host.req("room_set", { locked: true });
    r = await sofi.req("join", { rm: code });
    ok(!r.ok && r.d === "room_locked", "sala cerrada: nadie más entra (partida empezada)");
    r = await host.req("room_set", { props: { grande: "x".repeat(5000) } });
    ok(!r.ok && r.d === "props_too_large", "datos de sala muy grandes se rechazan");

    console.log("\nSi el host se va");
    await host.req("leave");
    await sleep(60);
    r = await ana.req("room_set", { locked: false, pw: "nueva" });
    ok(r.ok && r.d.has_pw === true && r.d.locked === false, "el nuevo host puede cambiar la sala");
    r = await sofi.req("join", { rm: code, pw: "nueva" });
    ok(r.ok, "y entra con la contraseña nueva");

    console.log("\nCompatibilidad");
    r = await host.req("join", { rm: "lobby", mx: 3 });
    ok(r.ok && host.last("room").has_pw === false && host.last("room").locked === false, "orb_join_room de siempre sigue igual");
  } catch (e) {
    fail++; console.log("  FALLA  excepción:", e);
  }
  srv.kill();
  console.log(`\n${pass} pruebas OK, ${fail} fallas\n`);
  process.exit(fail ? 1 : 0);
})();
