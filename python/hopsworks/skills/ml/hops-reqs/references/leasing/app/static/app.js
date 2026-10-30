// Leasing decisions: one order or a batch scored by both models, then simulate outcomes and retrain.

const STATUS_MS = 10000;
const $ = (id) => document.getElementById(id);
const nf = new Intl.NumberFormat();
const pct = (x) => `${Math.round(x * 100)}%`;
const eur = new Intl.NumberFormat(undefined, { style: "currency", currency: "EUR", maximumFractionDigits: 0 });

let customers = new Map(); // "Name (#id)" -> id
let modelVersion = null;
let retraining = false;

async function api(path, options) {
  const response = await fetch(path, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || `${response.status} ${response.statusText}`);
  return body;
}
const post = (path, body) =>
  api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body ?? {}) });

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else node.setAttribute(k, v);
  }
  node.append(...children.filter((c) => c !== null && c !== undefined));
  return node;
}

async function busy(button, work) {
  button.disabled = true;
  try {
    return await work();
  } finally {
    button.disabled = false;
  }
}

function chip(id, state, text) {
  const node = $(id);
  node.className = `chip ${state}`;
  node.lastChild.textContent = text;
}

// region Status

async function refreshStatus() {
  let status;
  try {
    status = await api("api/status");
  } catch (error) {
    chip("chip-xgb", "bad", "XGBoost: unreachable");
    return;
  }
  const banner = $("banner");
  banner.hidden = status.loaded && !status.error;
  banner.textContent = status.error ? `The models are not ready: ${status.error}` : "Loading the models…";
  if (status.xgboost) {
    const auc = status.xgboost.metrics.roc_auc;
    chip("chip-xgb", "ok", `XGBoost v${status.xgboost.version}${auc ? ` · ROC AUC ${auc.toFixed(3)}` : ""}`);
    chip("chip-kumo", "ok", `Kumo Tabular ${status.kumo.size} · ${nf.format(status.kumo.context_rows)} context rows`);
    if (modelVersion !== null && status.xgboost.version !== modelVersion) {
      $("retrained").replaceChildren(
        el("div", { class: "result" }, `Now deciding with leasing_xgboost v${status.xgboost.version}, ROC AUC ${auc.toFixed(3)}, trained on ${nf.format(status.xgboost.metrics.training_orders)} orders.`)
      );
    }
    modelVersion = status.xgboost.version;
  } else {
    chip("chip-xgb", "wait", "XGBoost: loading");
    chip("chip-kumo", "wait", "Kumo Tabular: loading");
  }
  const job = status.train_job;
  const running = ["initializing", "submitted", "running", "accepted", "new", "pending"].includes(job.state);
  chip("chip-job", running ? "wait" : job.final_status === "failed" ? "bad" : "ok", `Training job: ${job.state}`);
  if (retraining && !running) {
    retraining = false;
    $("retrain").disabled = false;
  }
  if (status.loaded && customers.size === 0) loadCatalogue();
}

// endregion

// region Catalogue and one order

async function loadCatalogue() {
  const catalogue = await api("api/catalogue");
  customers = new Map();
  const list = $("customers");
  list.replaceChildren();
  for (const c of catalogue.customers) {
    const label = `${c.name} (#${c.id})`;
    customers.set(label, c.id);
    list.append(el("option", { value: label }, `${c.type}, credit score ${c.credit_score}`));
  }
  const byCategory = new Map();
  for (const m of catalogue.equipment) {
    if (!byCategory.has(m.category)) byCategory.set(m.category, el("optgroup", { label: m.category }));
    byCategory.get(m.category).append(el("option", { value: m.id }, `${m.name} · ${eur.format(m.monthly_price)}/month`));
  }
  $("equipment").replaceChildren(...byCategory.values());
  $("months").replaceChildren(...catalogue.lease_months.map((n) => el("option", { value: n }, `${n} month${n === 1 ? "" : "s"}`)));
  $("months").value = catalogue.lease_months[1] ?? catalogue.lease_months[0];
}

function customerId(text) {
  if (customers.has(text)) return customers.get(text);
  const id = Number(text.replace(/[^0-9]/g, ""));
  return Number.isInteger(id) && id > 0 ? id : null;
}

function verdict(name, probability, leased, cls) {
  return el(
    "div",
    { class: `verdict ${cls} ${leased ? "yes" : "no"}` },
    el("div", { class: "model" }, el("span", {}, name), el("span", {}, `P(lease) ${probability.toFixed(3)}`)),
    el("div", { class: "call" }, leased ? "Lease" : "Decline"),
    el("div", { class: "meter" }, el("span", { style: `width: ${Math.round(probability * 100)}%` }))
  );
}

const FACTS = [
  ["credit_score", "Credit score", (v) => v],
  ["monthly_income", "Monthly income", (v) => eur.format(v)],
  ["total_debt", "Total debt", (v) => eur.format(v)],
  ["delinquencies_12m", "Delinquencies (12 months)", (v) => v],
  ["monthly_price", "Monthly price", (v) => eur.format(v)],
  ["replacement_value", "Replacement value", (v) => eur.format(v)],
  ["prev_outcome", "Last order", (v) => v ?? "none"],
  ["prev_problem_free", "Last lease problem-free", (v) => (v === null || v === undefined ? "–" : v ? "yes" : "no")],
];

async function decide(event) {
  event.preventDefault();
  const id = customerId($("customer").value);
  if (!id) {
    $("decision").replaceChildren(el("p", { class: "error" }, "Pick a customer from the list, or type their id."));
    return;
  }
  const order = { customer_id: id, equipment_id: Number($("equipment").value), lease_months: Number($("months").value) };
  const target = $("decision");
  target.setAttribute("aria-busy", "true");
  try {
    const result = await busy($("decide-button"), () => post("api/decide", order));
    const d = result.decision;
    const facts = el("dl", { class: "facts" });
    for (const [key, label, format] of FACTS) {
      const value = result.features[key];
      facts.append(el("div", {}, el("dt", {}, label), el("dd", {}, value === null || value === undefined ? (key === "prev_outcome" ? "none" : "–") : String(format(value)))));
    }
    target.replaceChildren(
      el("p", {}, el("strong", {}, d.customer), ` leases a `, el("strong", {}, d.machine), ` (${d.category}) for ${order.lease_months} month${order.lease_months === 1 ? "" : "s"}.`),
      el("div", { class: "verdicts" }, verdict("XGBoost", d.xgboost_probability, d.leased, "xgb"), verdict("Kumo Tabular (in-context)", d.kumo_probability, d.kumo_leased, "kumo")),
      facts
    );
    const t = result.timings_ms;
    $("decide-time").textContent = `transform ${t.transform} ms · XGBoost ${t.xgboost} ms · Kumo ${t.kumo} ms · logged`;
  } catch (error) {
    target.replaceChildren(el("p", { class: "error" }, error.message));
  } finally {
    target.removeAttribute("aria-busy");
  }
}

// endregion

// region Batch, simulation and retraining

const badge = (leased, probability) => el("span", { class: `badge ${leased ? "yes" : "no"}` }, `${leased ? "lease" : "decline"} ${probability.toFixed(2)}`);

async function scoreBatch(button) {
  const n = Number(button.dataset.n);
  $("batch-table").setAttribute("aria-busy", "true");
  try {
    const result = await busy(button, () => post("api/batch", { n }));
    $("batch-tiles").hidden = false;
    $("batch-table").hidden = false;
    $("b-orders").textContent = nf.format(result.orders);
    $("b-time").textContent = `${(result.timings_ms.total / 1000).toFixed(1)} s, Kumo ${(result.timings_ms.kumo / 1000).toFixed(1)} s · logged`;
    $("b-xgb").textContent = pct(result.xgboost_leased);
    $("b-kumo").textContent = pct(result.kumo_leased);
    $("b-agree").textContent = pct(result.agreement);
    $("batch-rows").replaceChildren(
      ...result.rows.map((r) =>
        el(
          "tr",
          {},
          el("td", {}, r.customer, el("span", { class: "sub" }, `#${r.customer_id}`)),
          el("td", {}, r.machine, el("span", { class: "sub" }, r.category)),
          el("td", { class: "num" }, String(r.lease_months)),
          el("td", {}, badge(r.leased, r.xgboost_probability)),
          el("td", {}, badge(r.kumo_leased, r.kumo_probability))
        )
      )
    );
  } catch (error) {
    $("batch-rows").replaceChildren(el("tr", {}, el("td", { colspan: 5, class: "error" }, error.message)));
    $("batch-table").hidden = false;
  } finally {
    $("batch-table").removeAttribute("aria-busy");
  }
}

async function simulate() {
  const target = $("simulated");
  try {
    const r = await busy($("simulate"), () => post("api/simulate"));
    if (!r.orders) {
      target.replaceChildren(el("div", { class: "result" }, "No new logged decisions: decide or score a batch first."));
      return;
    }
    const outcomes = Object.entries(r.outcomes).sort((a, b) => b[1] - a[1]);
    target.replaceChildren(
      el(
        "div",
        { class: "result" },
        `${nf.format(r.orders)} orders written, ${nf.format(r.leased)} leased, in ${r.seconds} s. The underwriter agreed with XGBoost on ${pct(r.xgboost_agreed)} and with Kumo on ${pct(r.kumo_agreed)}.`,
        el("ul", {}, ...outcomes.map(([k, v]) => el("li", {}, `${k.replace("_", " ")}: ${nf.format(v)}`)))
      )
    );
  } catch (error) {
    target.replaceChildren(el("div", { class: "result error" }, error.message));
  }
}

async function retrain() {
  const button = $("retrain");
  button.disabled = true;
  try {
    const r = await post("api/retrain");
    retraining = true;
    $("retrained").replaceChildren(el("div", { class: "result" }, `Started ${r.job} (execution ${r.execution}). The app switches to the new model when it is registered.`));
    refreshStatus();
  } catch (error) {
    button.disabled = false;
    $("retrained").replaceChildren(el("div", { class: "result error" }, error.message));
  }
}

// endregion

$("decide").addEventListener("submit", decide);
for (const button of document.querySelectorAll("[data-n]")) button.addEventListener("click", () => scoreBatch(button));
$("simulate").addEventListener("click", simulate);
$("retrain").addEventListener("click", retrain);
refreshStatus();
setInterval(refreshStatus, STATUS_MS);
