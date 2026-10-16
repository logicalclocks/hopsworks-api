// Card fraud console: polls api/snapshot, draws it, and asks the deployment about one card.

const REFRESH_MS = 5000;
const STATUS_MS = 15000;
const $ = (id) => document.getElementById(id);
const nf = new Intl.NumberFormat();
const eur = new Intl.NumberFormat(undefined, { style: "currency", currency: "EUR", maximumFractionDigits: 0 });

let threshold = null; // null: the model's own boundary
let modelThreshold = 0.8;
let paused = false;
let selected = null;
let lastUpdate = 0;
let lastHistogram = null;
// The rows are not redrawn under the pointer, so a click lands on the row it aimed at.
let pointerOnTable = false;
let pendingTop = null;

async function api(path, options) {
  const response = await fetch(path, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || `${response.status} ${response.statusText}`);
  return body;
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  node.append(...children.filter((c) => c !== null && c !== undefined));
  return node;
}

const svg = (tag, attrs = {}) => {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  return node;
};

const level = (risk) => (risk >= (threshold ?? modelThreshold) ? "high" : risk >= 0.5 ? "mid" : "");
const masked = (pan) => `${pan.slice(0, 4)} •••• •••• ${pan.slice(-4)}`;
const fixed = (v, d = 0) => (v === null || v === undefined ? "–" : Number(v).toFixed(d));

function riskBar(risk) {
  return el("div", { class: "bar" },
    el("div", { class: "track" }, el("div", { class: `fill ${level(risk)}`, style: `width:${Math.round(risk * 100)}%` })),
    el("span", { class: "num" }, `${Math.round(risk * 100)}%`));
}

function drawHistogram(counts) {
  const chart = $("histogram");
  // Drawn at the chart's own width, so the labels keep their shape.
  const width = Math.max(320, chart.clientWidth), height = 180, gap = 4;
  chart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  const max = Math.max(1, ...counts);
  const step = width / counts.length;
  const cut = threshold ?? modelThreshold;
  chart.replaceChildren();
  counts.forEach((count, i) => {
    // A log scale: a few anomalous cards stay visible beside thousands of normal ones.
    const h = count ? Math.max(3, (Math.log1p(count) / Math.log1p(max)) * (height - 24)) : 0;
    const over = (i + 1) / counts.length > cut;
    const bar = svg("rect", { x: i * step + gap / 2, y: height - h, width: step - gap, height: h, rx: 3, class: over ? "over" : "ok" });
    bar.append(svg("title"));
    bar.firstChild.textContent = `${nf.format(count)} cards with risk ${(i / counts.length).toFixed(2)}–${((i + 1) / counts.length).toFixed(2)}`;
    chart.append(bar);
    if (count) {
      const label = svg("text", { x: i * step + step / 2, y: height - h - 6, "text-anchor": "middle" });
      label.textContent = nf.format(count);
      chart.append(label);
    }
  });
  chart.append(svg("line", { x1: cut * width, x2: cut * width, y1: 0, y2: height, class: "cut" }));
}

function drawTop(cards) {
  if (pointerOnTable) {
    pendingTop = cards;
    return;
  }
  const body = $("top");
  if (!cards.length) {
    body.replaceChildren(el("tr", {}, el("td", { colspan: 7, class: "empty" }, "No card has an open window yet: start the transaction stream and the streaming features job.")));
    return;
  }
  body.replaceChildren(...cards.map((card, i) => {
    const times = card.avg_daily_amount_eur_30d ? card.amount_sum_eur / card.avg_daily_amount_eur_30d : null;
    const row = el("tr", { class: card.card_number === selected ? "selected" : "", title: card.card_number, onclick: () => select(card.card_number) },
      el("td", { class: "rank" }, String(i + 1)),
      el("td", { class: "pan" }, masked(card.card_number)),
      el("td", {}, riskBar(card.risk)),
      el("td", { class: "num" }, fixed(card.txn_count)),
      el("td", { class: "num" }, eur.format(card.amount_sum_eur ?? 0)),
      el("td", { class: "num" }, times === null ? "–" : `${times.toFixed(1)}×`),
      el("td", { class: "num" }, nf.format(Math.round(card.max_merchant_distance_km ?? 0))));
    return row;
  }));
}

function draw(snap) {
  modelThreshold = snap.model_threshold;
  if (threshold === null) {
    $("threshold").value = modelThreshold.toFixed(2);
    $("threshold-value").value = modelThreshold.toFixed(2);
  }
  $("cards").textContent = nf.format(snap.cards);
  $("baseline").textContent = snap.with_baseline === undefined ? "" : `${Math.round(snap.with_baseline * 100)}% with a dbt baseline`;
  $("flagged").textContent = nf.format(snap.flagged);
  $("flagged-pct").textContent = snap.cards ? `${((100 * snap.flagged) / snap.cards).toFixed(1)}% of the cards` : "";
  $("auths").textContent = nf.format(snap.authorisations);
  $("window").textContent = snap.window_end ? `window ending ${snap.window_end.slice(11, 16)} UTC` : "";
  $("read").textContent = `${Math.round(snap.read_ms)} ms`;
  $("scoring").textContent = `${snap.score_ms.toFixed(1)} ms`;
  $("model").textContent = `model v${snap.model.version}, ${nf.format(snap.model.training_rows)} cards`;
  $("top-note").textContent = `top ${snap.top.length} of ${nf.format(snap.cards)}`;
  lastHistogram = snap.histogram;
  drawHistogram(snap.histogram);
  drawTop(snap.top);
  lastUpdate = Date.now();
  $("updated").textContent = "updated just now";
  const pulse = $("pulse");
  pulse.classList.remove("beat");
  void pulse.offsetWidth; // restart the animation
  pulse.classList.add("beat");
}

async function refresh() {
  if (paused || document.hidden) return;
  const table = $("top");
  table.setAttribute("aria-busy", "true");
  try {
    const query = threshold === null ? "" : `?threshold=${threshold}`;
    draw(await api(`api/snapshot${query}`));
  } catch (error) {
    $("updated").textContent = error.message;
  } finally {
    table.removeAttribute("aria-busy");
  }
}

async function refreshStatus() {
  try {
    const s = await api("api/status");
    const [stream, windows] = Object.values(s.jobs);
    const set = (key, state) => {
      const chip = document.querySelector(`.chip[data-key="${key}"]`);
      chip.className = `chip ${state === "running" ? "ok" : ["missing", "failed", "killed", "stopped"].includes(state) ? "bad" : "wait"}`;
      chip.title = state;
    };
    set("deployment", s.deployment);
    set("stream", stream);
    set("windows", windows);
  } catch {
    // The chips keep their last state.
  }
}

function gauge(risk) {
  const r = 52, c = Math.PI * r; // a half circle
  const g = svg("svg", { viewBox: "0 0 130 80", class: "gauge", width: "100%" });
  g.append(svg("path", { d: "M13 72 A52 52 0 0 1 117 72", fill: "none", "stroke-width": 12, "stroke-linecap": "round", class: "track" }));
  const colour = level(risk) === "high" ? "var(--danger)" : level(risk) === "mid" ? "var(--warn)" : "var(--accent)";
  g.append(svg("path", { d: "M13 72 A52 52 0 0 1 117 72", fill: "none", stroke: colour, "stroke-width": 12, "stroke-linecap": "round", "stroke-dasharray": `${risk * c} ${c}` }));
  const text = svg("text", { x: 65, y: 70, "text-anchor": "middle" });
  text.textContent = `${Math.round(risk * 100)}%`;
  g.append(text);
  return g;
}

function compare(label, now, base, format) {
  // The bar is the window, the tick the baseline, on a scale of the larger of the two.
  const scale = Math.max(now ?? 0, base ?? 0, 1e-9) * 1.15;
  const over = base ? now / base : null;
  return el("div", { class: "row" },
    el("div", { class: "labels" }, el("span", {}, label),
      el("span", {}, `${format(now)} vs ${format(base)}${over === null ? "" : ` · ${over.toFixed(1)}×`}`)),
    el("div", { class: "pair" },
      el("div", { class: `now ${over !== null && over > 2 ? "high" : ""}`, style: `width:${(100 * (now ?? 0)) / scale}%` }),
      base === null || base === undefined ? null : el("div", { class: "base", style: `left:${(100 * base) / scale}%` })));
}

function drawDetail(result) {
  const p = result.prediction;
  const card = result.card;
  const badge = p.fraud ? el("span", { class: "badge high" }, "Flagged by the model") : el("span", { class: "badge" }, "Looks normal");
  const facts = el("dl", {},
    el("dt", {}, "Deployment"), el("dd", {}, `${result.round_trip_ms} ms round trip`),
    el("dt", {}, "Anomaly score"), el("dd", {}, p.anomaly_score.toFixed(3)),
    el("dt", {}, "Features"), el("dd", {}, [p.has_window ? "live window" : "no window", p.has_baseline ? "baseline" : "no baseline"].join(" · ")));
  const parts = [el("div", { class: "verdict" }, gauge(p.risk),
    el("div", {}, el("div", { class: "pan" }, p.card_number), badge, facts))];
  if (card) {
    const n0 = (v) => nf.format(Math.round(v ?? 0));
    parts.push(el("div", { class: "compare" },
      compare("Spend this hour vs a usual day", card.amount_sum_eur, card.avg_daily_amount_eur_30d, (v) => (v === null ? "–" : eur.format(v))),
      compare("Authorisations vs a usual day", card.txn_count, card.avg_daily_txn_count_7d, (v) => fixed(v, 1)),
      compare("Furthest merchant vs 30 days (km)", card.max_merchant_distance_km, card.max_merchant_distance_km_30d, n0),
      compare("Decline ratio vs 30 days", card.decline_ratio, card.decline_ratio_30d, (v) => (v === null ? "–" : `${Math.round(v * 100)}%`)),
      compare("Highest issuer score vs a risky 500", card.max_issuer_fraud_score, 500, n0)));
    parts.push(el("p", { class: "legend" }, "Bar: this card's open window from Spark ·", el("b"), "tick: its baseline from dbt"));
  } else {
    parts.push(el("p", { class: "muted" }, "This card has no open window: the deployment scored it on the training medians."));
  }
  $("detail").replaceChildren(...parts);
}

async function select(cardNumber) {
  selected = cardNumber;
  $("card").value = cardNumber;
  document.querySelectorAll("#top tr").forEach((tr) => tr.classList.toggle("selected", tr.title === cardNumber));
  const detail = $("detail");
  if (!detail.querySelector(".verdict")) detail.replaceChildren(el("div", { class: "loading" }), el("div", { class: "loading" }));
  detail.setAttribute("aria-busy", "true");
  try {
    drawDetail(await api("api/score", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ card_number: cardNumber }) }));
  } catch (error) {
    detail.replaceChildren(el("p", { class: "error" }, error.message));
  } finally {
    detail.removeAttribute("aria-busy");
  }
}

$("lookup").addEventListener("submit", (event) => {
  event.preventDefault();
  const value = $("card").value.replace(/\s/g, "");
  if (/^\d{12,19}$/.test(value)) select(value);
});
$("threshold").addEventListener("input", (event) => {
  threshold = Number(event.target.value);
  $("threshold-value").value = threshold.toFixed(2);
});
$("threshold").addEventListener("change", refresh);
$("reset").addEventListener("click", () => { threshold = null; refresh(); });
$("pause").addEventListener("click", (event) => {
  paused = !paused;
  event.target.setAttribute("aria-pressed", String(paused));
  event.target.textContent = paused ? "Resume" : "Pause";
  $("pulse").classList.toggle("paused", paused);
  if (!paused) refresh();
});
document.addEventListener("visibilitychange", refresh);
$("top").addEventListener("pointerenter", () => { pointerOnTable = true; });
$("top").addEventListener("pointerleave", () => {
  pointerOnTable = false;
  if (pendingTop) drawTop(pendingTop);
  pendingTop = null;
});
window.addEventListener("resize", () => lastHistogram && drawHistogram(lastHistogram));

setInterval(() => {
  if (!lastUpdate) return;
  const seconds = Math.round((Date.now() - lastUpdate) / 1000);
  $("updated").textContent = paused ? "paused" : seconds < 2 ? "updated just now" : `updated ${seconds}s ago`;
}, 1000);
setInterval(refresh, REFRESH_MS);
setInterval(refreshStatus, STATUS_MS);
refresh();
refreshStatus();
