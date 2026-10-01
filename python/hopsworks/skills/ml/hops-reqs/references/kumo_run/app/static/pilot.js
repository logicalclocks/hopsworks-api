// Kumo Tabular flies the page when the URL asks for it (?pilot=kumo). game.js enters
// pilot mode when the page defines jevworksDecide and jevworksFinished before it loads:
// every move is asked of the app's api/decide, which puts the game state to the Kumo
// Tabular deployment, and every crash posts the run to the board as the kumo pilot.
// A classic script, not a module, so it runs before game.js.

(() => {
  const flying = new URLSearchParams(location.search).get("pilot") === "kumo";
  const link = document.getElementById("switch");
  if (!flying) return;
  link.textContent = "Fly it yourself";
  link.href = "./";

  async function post(path, body) {
    const res = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const reply = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    if (!res.ok || reply.error) throw new Error(reply.error || reply.detail || `HTTP ${res.status}`);
    return reply;
  }

  // State in ({lane, airborne, ahead}), {pilot, moves, probabilities, forwardMs, model} out.
  window.jevworksDecide = (state) => post("api/decide", state);
  // The run as game.js records it at the crash; the board comes back rendered.
  window.jevworksFinished = (run) => post("api/runs", { ...run, name: "Kumo Tabular", pilot: "kumo" });
})();
