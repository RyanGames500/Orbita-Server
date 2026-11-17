/*
 * ÓRBITA - Plugin de prueba
 * Solo lo usan las pruebas automáticas (npm test), que se conectan con el juego "Prueba".
 * No afecta a ningún otro juego.
 */
module.exports = {
  onEvent(api, p, name, data) {
    if (name === "tirar_dado") {
      const valor = 1 + Math.floor(Math.random() * 6);
      api.sendToRoom(p.gameId, p.roomId, "resultado_dado", { valor, quien: p.id });
    }
  },

  async onRequest(api, p, name, data) {
    if (name === "sumar") return Number(data?.a || 0) + Number(data?.b || 0);
    throw new Error("pregunta_desconocida");
  },
};
