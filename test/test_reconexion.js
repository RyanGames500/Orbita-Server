// Pruebas de volver a tu lugar si se cae el internet.  Uso: npm test  (puerto 3991)
const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");
const os = require("os");
const crypto = require("crypto");
const WebSocket = require("ws");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 3991;
const KEY = "clave_reconexion";
const md5 = (s) => crypto.createHash("md5").update(s, "utf8").digest("hex");
let pass = 0, fail = 0;
const ok = (c, msg) => { c ? pass++ : fail++; console.log((c ? "  OK   " : "  FALLA") + "  " + msg); };

function client(resume) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${PORT}`);
    const c = { ws, log: [], id: -1, r: 1, pending: {}, closed: false };
    ws.on("message", (raw) => {
      const m = JSON.parse(raw);
      c.log.push(m);
      if (m.t === "welcome") { c.id = m.id; c.rs = m.rs; c.welcome = m; resolve(c); }
      if (m.t === "res" && c.pending[m.r]) { c.pending[m.r](m); delete c.pending[m.r]; }
    });
    ws.on("close", () => { c.closed = true; });
    ws.on("error", () => {});
    ws.on("open", () => ws.send(JSON.stringify({ t: "hello", proto: 1, game: "Reco", key: md5(KEY), ...(resume ? { resume } : {}) })));
    c.send = (o) => ws.send(JSON.stringify(o));
    c.req = (op, args = {}) => new Promise((res) => { const r = c.r++; c.pending[r] = res; c.send({ t: "req", r, op, ...args }); });
    c.last = (t, f = () => true) => [...c.log].reverse().find((m) => m.t === t && f(m));
    c.has = (t, f = () => true) => c.log.some((m) => m.t === t && f(m));
  });
}

(async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "orbita-reco-"));
  const srv = spawn(process.execPath, [path.join(__dirname, "..", "server.js")], {
    env: { ...process.env, PORT: String(PORT), DATA_DIR: dataDir, GAME_KEY: KEY, RESUME_GRACE_MS: "1500" }, stdio: "ignore",
  });
  await sleep(800);
  try {
    console.log("\nSe cae y vuelve");
    const a = await client(), b = await client();
    ok(typeof a.rs === "string" && a.rs.length === 32, "welcome trae un código secreto para volver");
    await a.req("join", { rm: "sala" }); await b.req("join", { rm: "sala" });
    await b.req("auth_guest");
    b.send({ t: "st", s: { x: 10, nombre: "Bea" }, e: { 1: { x: 5 } } });
    await sleep(60);
    await a.req("room_set", { locked: true });
    const bId = b.id, bRs = b.rs, bSlot = b.last("room").slot, bUid = b.last("res", (m) => m.d && m.d.uid)?.d.uid;
    a.log = [];
    b.ws.terminate();
    await sleep(150);
    ok(a.has("away", (m) => m.id === bId) && !a.has("leave"), "los demás reciben 'away' (no 'leave')");
    const c = await client(); await c.req("join", { rm: "otra" });
    let r = await a.req("players", { rm: "sala" });
    ok(r.d.includes(bId), "su lugar sigue ocupado en la sala");

    const b2 = await client({ id: bId, rs: bRs });
    await sleep(80);
    ok(b2.welcome.resumed === true && b2.id === bId, "vuelve con el mismo id");
    const snap = b2.last("room");
    ok(snap && snap.id === "sala" && snap.slot === bSlot && snap.resumed === true, "y el mismo puesto, aunque la sala esté cerrada");
    ok(b2.welcome.uid === bUid && bUid, "conserva su cuenta");
    ok(a.has("back", (m) => m.id === bId) && !a.has("join"), "los demás reciben 'back' (no 'join')");
    ok(typeof b2.welcome.rs === "string" && b2.welcome.rs !== bRs, "le dan un código nuevo");
    a.log = [];
    b2.send({ t: "st", s: { x: 99 } });
    await sleep(60);
    ok(a.last("st")?.id === bId && a.last("st").s.x === 99, "y sigue jugando normal");

    console.log("\nCódigo malo o viejo");
    const fake = await client({ id: bId, rs: "0".repeat(32) });
    ok(fake.id !== bId && !fake.welcome.resumed, "con un código falso no puede tomar el lugar de otro");
    const reuse = await client({ id: bId, rs: bRs });
    ok(reuse.id !== bId, "el código viejo ya no sirve");

    console.log("\nNo vuelve a tiempo");
    a.log = [];
    b2.ws.terminate();
    await sleep(1900);
    ok(a.has("away", (m) => m.id === bId) && a.has("leave", (m) => m.id === bId), "pasado el tiempo, sale de la sala");
    const late = await client({ id: bId, rs: b2.welcome.rs });
    ok(late.id !== bId, "y ya no puede volver a ese lugar");

    console.log("\nSe va a propósito");
    const d = await client(); await d.req("room_set"); await d.req("join", { rm: "sala2" });
    const e = await client(); await e.req("join", { rm: "sala2" });
    d.log = [];
    e.send({ t: "bye" });
    await sleep(120);
    ok(d.has("leave", (m) => m.id === e.id) && !d.has("away"), "con 'bye' (orb_disconnect) sale al momento");

    console.log("\nEl servidor no se dio cuenta de la caída");
    const f = await client(); await f.req("join", { rm: "sala2" }); await sleep(30);
    d.log = [];
    const f2 = await client({ id: f.id, rs: f.rs });   // la conexión vieja sigue abierta
    await sleep(120);
    ok(f2.id === f.id && f2.welcome.resumed && f.closed, "la conexión nueva reemplaza a la vieja");
    ok(!d.has("leave") && !d.has("join"), "los demás no ven nada raro");

    console.log("\nEl host se cae");
    const h = await client(), g = await client();
    await h.req("join", { rm: "sala3" }); await g.req("join", { rm: "sala3" });
    h.ws.terminate(); await sleep(120);
    r = await g.req("room_set", { locked: true });
    ok(!r.ok && r.d === "not_host", "mientras vuelve, sigue siendo el host");
    const h2 = await client({ id: h.id, rs: h.rs });
    r = await h2.req("room_set", { locked: true });
    ok(r.ok, "al volver puede seguir manejando la sala");

    console.log("\nExpulsado mientras estaba caído");
    g.ws.terminate(); await sleep(120);
    h2.send({ t: "kick", id: g.id }); await sleep(80);
    const g2 = await client({ id: g.id, rs: g.rs });
    ok(g2.id !== g.id, "si el host lo expulsó, no puede volver a su lugar");
  } catch (e) {
    fail++; console.log("  FALLA  excepción:", e);
  }
  srv.kill();
  console.log(`\n${pass} pruebas OK, ${fail} fallas\n`);
  process.exit(fail ? 1 : 0);
})();
