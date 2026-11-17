// =====================================================================
// PRUEBA DE CARGA de Órbita: conecta muchos "jugadores falsos" que se mueven como en un juego real
// y mide cuánto aguanta el servidor. Funciona en Windows, Mac y Linux.
//
//   npm run carga                              -> arranca su propio servidor en esta PC y lo prueba
//   node test/carga.js --url wss://tu-servidor.onrender.com --key TU_CLAVE --game carga --pasos 20,40
//                                              -> prueba un servidor que ya está corriendo (ej. Render)
//
// Opciones: --sala 8 (jugadores por sala)  --hz 30 (envíos por segundo de cada uno)
//           --pasos 50,100,200,400 (cuántos jugadores probar)  --seg 10 (segundos por paso)
//           --procesos 3 (cuántos procesos de bots; por defecto según tu CPU)
//           --pid 1234 (solo Linux: medir CPU/memoria de un servidor local que ya está corriendo)
//           --perfil (graba en la carpeta servidor/perfil qué partes del servidor gastan la CPU)
//
// Ojo: si el servidor y los bots corren en la MISMA PC, se pelean la CPU. La columna "Bots" te
// avisa si los bots no dieron abasto: en ese caso el número malo es de tu PC, no del servidor.
// Si pruebas tu servidor de Render, sus límites (MAX_CONN_PER_IP) cortan antes: es normal.
// =====================================================================
const { fork, execFileSync } = require("child_process");
const path = require("path");
const fs = require("fs");
const os = require("os");
const crypto = require("crypto");

const arg = (name, def) => {
  const i = process.argv.indexOf("--" + name);
  return i > 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : def;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pct = (arr, p) => { if (!arr.length) return NaN; const a = [...arr].sort((x, y) => x - y); return a[Math.min(a.length - 1, Math.floor(a.length * p))]; };

// =====================================================================
// PROCESO DE BOTS (cada uno maneja una parte de los jugadores falsos)
// =====================================================================
if (process.argv.includes("--bot-worker")) {
  const WebSocket = require("ws");
  const { url, key, game, hz } = JSON.parse(process.env.ORB_CARGA);
  const md5 = (s) => crypto.createHash("md5").update(s, "utf8").digest("hex");
  const KEEP = 4000; // muestras de retraso que guardamos (al azar) para no gastar memoria
  let st;
  const resetStats = () => { st = { lat: [], latSeen: 0, rtt: [], recv: 0, sent: 0, due: 0, errors: {}, lagMax: 0 }; };
  resetStats();
  const sample = (v) => { st.latSeen++; if (st.lat.length < KEEP) st.lat.push(v); else { const j = Math.floor(Math.random() * st.latSeen); if (j < KEEP) st.lat[j] = v; } };
  const bots = [];

  function bot(room) {
    return new Promise((resolve) => {
      const ws = new WebSocket(url, { perMessageDeflate: false });
      const b = { ws, id: -1, ready: false, x: Math.random() * 1000, y: Math.random() * 600, dir: Math.random() * 6.28, ent: 1, r: 1 };
      let settled = false;
      const done = (ok) => { if (settled) return; settled = true; resolve(b); };
      ws.on("open", () => ws.send(JSON.stringify({ t: "hello", proto: 1, game, key: md5(key) })));
      ws.on("message", (raw) => {
        const m = JSON.parse(raw);
        if (m.t === "st") { st.recv++; if (m.s && m.s.t) sample(Date.now() - m.s.t); }
        else if (m.t === "pong") st.rtt.push(Date.now() - m.c);
        else if (m.t === "welcome") { b.id = m.id; ws.send(JSON.stringify({ t: "req", r: b.r++, op: "join", rm: room })); }
        else if (m.t === "room") { b.ready = true; done(true); }
        else if (m.t === "err") { st.errors[m.code] = (st.errors[m.code] || 0) + 1; if (!b.ready) done(false); }
      });
      ws.on("error", () => { st.errors.conexion = (st.errors.conexion || 0) + 1; done(false); });
      ws.on("close", () => { if (b.ready) st.errors.cerrada = (st.errors.cerrada || 0) + 1; b.ready = false; done(false); });
      setTimeout(() => done(false), 10000);
    });
  }

  function sendState(b, now) {
    b.dir += (Math.random() - 0.5) * 0.3;
    b.x += Math.cos(b.dir) * 4; b.y += Math.sin(b.dir) * 4;
    // Lo mismo que manda orb_state_from + algunos datos del juego
    const msg = { t: "st", s: { x: Math.round(b.x), y: Math.round(b.y), sprite_index: 3, image_index: (now / 100) % 8 | 0, image_xscale: 1, image_yscale: 1, image_angle: 0, image_blend: 16777215, image_alpha: 1, visible: true, name: "Bot" + b.id, vida: 100, t: now } };
    // De vez en cuando una bala (entidad) que aparece y desaparece
    if (Math.random() < 0.05) { msg.e = { [b.ent]: { kind: "obj_bala", x: b.x, y: b.y, image_angle: 0, props: { dano: 10 } } }; b.lastEnt = b.ent++; }
    else if (b.lastEnt && Math.random() < 0.1) { msg.d = [b.lastEnt]; b.lastEnt = 0; }
    b.ws.send(JSON.stringify(msg));
    st.sent++;
  }

  // Reloj propio: en Windows los temporizadores saltan de ~15 ms en ~15 ms, así que en vez de
  // confiar en setInterval calculamos cuántos envíos "tocaban" y mandamos los que falten.
  let running = false, startAt = 0, ticksDone = 0, lastLoop = 0;
  function loop() {
    if (!running) return;
    const now = Date.now();
    if (lastLoop) st.lagMax = Math.max(st.lagMax, now - lastLoop);
    lastLoop = now;
    const shouldBe = Math.floor(((now - startAt) / 1000) * hz);
    let n = shouldBe - ticksDone;
    if (n > 3) n = 3; // si vamos muy atrasados no mandamos ráfagas enormes (lo contamos como "no dio abasto")
    for (let k = 0; k < n; k++) {
      for (const b of bots) if (b.ready && b.ws.readyState === 1) { sendState(b, now); }
    }
    ticksDone = shouldBe;
    setTimeout(loop, 4);
  }
  let pinger = null;

  process.on("message", async (m) => {
    if (m.cmd === "grow") {
      for (let i = 0; i < m.rooms.length; i += 20) {
        bots.push(...(await Promise.all(m.rooms.slice(i, i + 20).map((rm) => bot(rm)))));
        await sleep(30);
      }
      process.send({ t: "grown", alive: bots.filter((b) => b.ready).length });
    } else if (m.cmd === "start") {
      running = true; startAt = Date.now(); ticksDone = 0; lastLoop = 0; loop();
      pinger = setInterval(() => { const c = Date.now(); for (const b of bots) if (b.ready && b.ws.readyState === 1) b.ws.send(JSON.stringify({ t: "ping", c })); }, 1000);
    } else if (m.cmd === "reset") {
      const alive = bots.filter((b) => b.ready).length;
      resetStats(); st.t0 = Date.now(); st.alive0 = alive;
    } else if (m.cmd === "report") {
      const secs = (Date.now() - st.t0) / 1000;
      const alive = bots.filter((b) => b.ready).length;
      process.send({ t: "report", lat: st.lat, latSeen: st.latSeen, rtt: st.rtt, sent: st.sent, recv: st.recv, errors: st.errors, lagMax: st.lagMax, expected: alive * hz * secs, alive });
    } else if (m.cmd === "stop") {
      running = false; clearInterval(pinger);
    } else if (m.cmd === "quit") {
      for (const b of bots) try { b.ws.terminate(); } catch {}
      process.exit(0);
    }
  });
  return;
}

// =====================================================================
// PROCESO PRINCIPAL (arranca el servidor, reparte bots y arma la tabla)
// =====================================================================
const URL_ = arg("url", "");
const KEY = arg("key", "clave_carga");
const GAME = arg("game", "carga");
const ROOM_SIZE = Number(arg("sala", 8));
const HZ = Number(arg("hz", 30));
const PERFIL = process.argv.includes("--perfil");
const PERFIL_DIR = path.join(__dirname, "..", "perfil");
const STEPS = String(arg("pasos", PERFIL ? "200" : "50,100,200,400,600,800,1000")).split(",").map(Number);
const SECONDS = Number(arg("seg", PERFIL ? 15 : 10));
const PORT = 3989;
const CORES = os.cpus().length;
const WORKERS = Math.max(1, Number(arg("procesos", Math.min(4, Math.max(1, CORES - 2)))));
const isLinux = process.platform === "linux";
const hasTaskset = isLinux && fs.existsSync("/usr/bin/taskset");

// CPU/memoria de un servidor externo (solo Linux, --pid)
function procStat(pid) {
  try {
    const st = fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ");
    return { cpuUs: ((Number(st[11]) + Number(st[12])) / 100) * 1e6, rss: Number(st[21]) * 4096 };
  } catch { return null; }
}

(async () => {
  let srv = null, url = URL_;
  if (!url) {
    const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "orbita-carga-"));
    srv = fork(path.join(__dirname, "..", "server.js"), [], {
      env: { ...process.env, PORT: String(PORT), DATA_DIR: dataDir, GAME_KEY: KEY, MAX_CONN_PER_IP: "100000", ADMIN_TOKEN: "carga_" + crypto.randomBytes(8).toString("hex") },
      stdio: ["ignore", "ignore", "inherit", "ipc"],
      execArgv: PERFIL ? ["--cpu-prof", "--cpu-prof-dir=" + PERFIL_DIR] : [],
    });
    // En Linux dejamos el servidor solo en un núcleo para medirlo "como si fuera 1 CPU"
    if (hasTaskset) try { execFileSync("taskset", ["-cp", "0", String(srv.pid)], { stdio: "ignore" }); } catch {}
    url = `ws://127.0.0.1:${PORT}`;
    await sleep(1200);
  }

  // Pedir CPU/memoria al servidor
  const extPid = arg("pid", "") ? Number(arg("pid", "")) : null;
  const askStats = () => new Promise((resolve) => {
    if (srv) {
      const t = setTimeout(() => resolve(null), 2000);
      srv.once("message", (m) => { clearTimeout(t); resolve({ cpuUs: m.cpu.user + m.cpu.system, rss: m.rss }); });
      srv.send("orb_stats");
    } else resolve(extPid ? procStat(extPid) : null);
  });

  // Procesos de bots
  const workers = [];
  for (let w = 0; w < WORKERS; w++) {
    const p = fork(__filename, ["--bot-worker"], { env: { ...process.env, ORB_CARGA: JSON.stringify({ url, key: KEY, game: GAME, hz: HZ }) }, stdio: ["ignore", "inherit", "inherit", "ipc"] });
    if (hasTaskset && CORES > 1) try { execFileSync("taskset", ["-cp", `1-${CORES - 1}`, String(p.pid)], { stdio: "ignore" }); } catch {}
    workers.push(p);
  }
  const ask = (p, cmd, extra = {}, reply) => new Promise((resolve) => {
    if (reply) p.once("message", (m) => resolve(m));
    p.send({ cmd, ...extra });
    if (!reply) resolve();
  });

  console.log(`\nPrueba de carga contra ${url}`);
  console.log(`Salas de ${ROOM_SIZE} jugadores, cada uno manda su estado ${HZ} veces por segundo. Bots repartidos en ${WORKERS} proceso(s); tu PC tiene ${CORES} núcleos.`);
  if (srv) console.log(`El servidor corre en esta misma PC${hasTaskset ? " (fijado a 1 núcleo)" : ""}: CPU 100% = un núcleo completo.`);
  console.log("");
  console.log("Jugadores | CPU servidor | Memoria | Mensajes/s (entran → salen) | Retraso p50 / p95 | Ping p95 | Bots      | Errores");
  console.log("----------|--------------|---------|-----------------------------|-------------------|----------|-----------|--------");

  let total = 0;
  const results = [];
  for (const target of STEPS) {
    // Repartir los jugadores nuevos entre los procesos (cada sala tiene su número)
    const per = workers.map(() => []);
    for (let i = total; i < target; i++) per[i % WORKERS].push("carga_" + Math.floor(i / ROOM_SIZE));
    total = Math.max(total, target);
    const grown = await Promise.all(workers.map((p, w) => ask(p, "grow", { rooms: per[w] }, true)));
    const alive = grown.reduce((a, g) => a + g.alive, 0);

    await Promise.all(workers.map((p) => ask(p, "start")));
    await sleep(2000); // calentar
    await Promise.all(workers.map((p) => ask(p, "reset")));
    const s0 = await askStats(); const t0 = Date.now();
    await sleep(SECONDS * 1000);
    const s1 = await askStats(); const secs = (Date.now() - t0) / 1000;
    const reps = await Promise.all(workers.map((p) => ask(p, "report", {}, true)));
    await Promise.all(workers.map((p) => ask(p, "stop")));

    const lat = [], rtt = []; let sent = 0, recv = 0, expected = 0, lagMax = 0, aliveNow = 0; const errors = {};
    for (const r of reps) {
      lat.push(...r.lat); // muestras al azar de cada proceso (todos llevan una carga parecida)
      rtt.push(...r.rtt); sent += r.sent; recv += r.recv; expected += r.expected; lagMax = Math.max(lagMax, r.lagMax); aliveNow += r.alive;
      for (const [k, v] of Object.entries(r.errors)) errors[k] = (errors[k] || 0) + v;
    }
    const botsOk = expected ? sent / expected : 1;
    const res = {
      players: aliveNow, cpu: s0 && s1 ? ((s1.cpuUs - s0.cpuUs) / 1e6 / secs) * 100 : NaN, mem: s1 ? s1.rss / 1048576 : NaN,
      inPerSec: sent / secs, outPerSec: recv / secs, p50: pct(lat, 0.5), p95: pct(lat, 0.95), ping95: pct(rtt, 0.95),
      botsOk, lagMax, errors: Object.entries(errors).map(([k, v]) => `${k}:${v}`).join(" ") || "-",
    };
    results.push(res);
    const f = (n, d = 0) => (Number.isFinite(n) ? n.toFixed(d) : "?");
    const botsTxt = botsOk >= 0.95 ? "bien" : `${f(botsOk * 100)}% ⚠`;
    console.log(`${String(res.players).padStart(9)} | ${(f(res.cpu) + "%").padStart(12)} | ${(f(res.mem) + " MB").padStart(7)} | ${(f(res.inPerSec) + " → " + f(res.outPerSec)).padStart(27)} | ${(f(res.p50) + " / " + f(res.p95) + " ms").padStart(17)} | ${(f(res.ping95) + " ms").padStart(8)} | ${botsTxt.padEnd(9)} | ${res.errors}`);
    await sleep(500);

    if (botsOk < 0.9) {
      console.log("\nLos bots no dieron abasto: tu PC no puede simular más jugadores mientras corre el servidor.");
      console.log("De aquí en adelante el retraso que se mide es de la PC, no del servidor. Paro aquí.");
      break;
    }
    if (res.p95 > 250 || res.cpu > 95 || aliveNow < target * 0.95) { console.log("\nEl servidor llegó a su límite (retraso alto, CPU al máximo o jugadores que no entraron). Paro aquí."); break; }
  }

  for (const p of workers) try { p.send({ cmd: "quit" }); } catch {}
  await sleep(300);
  if (srv) {
    // Le pedimos que se cierre solo (para que guarde el perfil); si no, lo cerramos a la fuerza
    try { srv.send("orb_exit"); } catch {}
    await new Promise((r) => { const t = setTimeout(r, 4000); srv.once("exit", () => { clearTimeout(t); r(); }); });
    try { srv.kill(); } catch {}
  }
  if (PERFIL) console.log("\nPerfil guardado en: " + PERFIL_DIR + "  (mándaselo a Claude para ver qué gasta la CPU)");
  fs.writeFileSync(path.join(os.tmpdir(), "orbita-carga-resultado.json"), JSON.stringify(results, null, 2));
  console.log("\nListo. (Resultados guardados en " + path.join(os.tmpdir(), "orbita-carga-resultado.json") + ")");
  process.exit(0);
})();
