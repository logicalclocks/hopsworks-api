// Ask the shop: the chat, its charts and tables, the query editor and the catalog. No framework, no CDN.

const GRAINS = ["day", "week", "month", "quarter", "year"];
const PALETTE = ["#1eb182", "#3b82f6", "#e0a100", "#d6455d", "#8b5cf6", "#14b8a6", "#f97316", "#64748b"];
const MAX_SERIES = 8;
const MAX_BARS = 25;
const MAX_ROWS = 500;
const FILTER = /^(\S+)\s+(NOT IN|IN|!=|>=|<=|=|>|<|LIKE)\s+(.+)$/i;
const state = { catalog: null, history: [], busy: false };
const $ = (id) => document.getElementById(id);

async function api(path, body) {
  const init = body === undefined ? {} : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
  const res = await fetch(path, init);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : `${res.status} ${res.statusText}`);
  return data;
}

function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === undefined || v === null || v === false) continue;
    if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k === "class") el.className = v;
    else if (k === "value") el.value = v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat()) {
    if (c !== null && c !== undefined && c !== false) el.append(c instanceof Node ? c : String(c));
  }
  return el;
}

function s(tag, attrs = {}, ...children) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  for (const c of children.flat()) if (c !== null && c !== undefined) el.append(c instanceof Node ? c : String(c));
  return el;
}

// region Formatting

const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
const cents = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2 });
const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });
const plain = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });
const percent = new Intl.NumberFormat("en-US", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });

function fmt(value, unit, short = false) {
  if (value === null || value === undefined || value === "") return "–";
  if (typeof value !== "number") return String(value);
  if (unit === "share") return percent.format(value);
  if (unit === "usd") {
    if (short) return "$" + compact.format(value);
    return Math.abs(value) < 1000 ? cents.format(value) : money.format(value);
  }
  return short ? compact.format(value) : plain.format(value);
}

const grainOf = (column) => GRAINS.find((g) => column.endsWith("__" + g));

function label(name) {
  const catalog = state.catalog;
  const metric = catalog?.metrics.find((m) => m.name === name);
  if (metric) return metric.label;
  const grain = grainOf(name);
  const base = grain ? name.slice(0, -(grain.length + 2)) : name;
  const dim = catalog?.dimensions.find((d) => d.name === base);
  const text = base === "metric_time" ? "Time" : dim?.label ?? base;
  return grain ? `${text} (${grain})` : text;
}

function timeText(value, grain) {
  const day = String(value).slice(0, 10);
  if (grain === "year") return day.slice(0, 4);
  if (grain === "quarter") return `${day.slice(0, 4)} Q${Math.floor((Number(day.slice(5, 7)) - 1) / 3) + 1}`;
  if (grain === "month") return day.slice(0, 7);
  return day;
}

function cell(answer, column, value) {
  if (answer.query.metrics.includes(column)) return fmt(value, answer.units[column]);
  const grain = grainOf(column);
  if (grain && value) return timeText(value, grain);
  return value ?? "–";
}

// endregion

// region Charts

function ticks(lo, hi, n = 5) {
  if (lo === hi) hi = lo + 1;
  const span = hi - lo;
  const magnitude = 10 ** Math.floor(Math.log10(span / n));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((st) => span / st <= n);
  const out = [];
  for (let t = Math.floor(lo / step) * step; t <= Math.ceil(hi / step) * step + step / 2; t += step) {
    out.push(Number(t.toFixed(10)));
  }
  return out;
}

function legend(names) {
  return h("div", { class: "legend" },
    names.map((name, i) => h("span", {}, h("i", { style: `background:${PALETTE[i % PALETTE.length]}` }), name)));
}

function lineChart(answer, timeCol, categoryCols) {
  const { query, columns, rows, units } = answer;
  const at = (c) => columns.indexOf(c);
  const metric = query.metrics[0];
  const unit = units[metric];
  const grain = grainOf(timeCol);
  const xs = [...new Set(rows.map((r) => r[at(timeCol)]))].sort();
  let series;
  if (categoryCols.length) {
    const byKey = new Map();
    for (const r of rows) {
      const key = categoryCols.map((c) => r[at(c)]).join(" · ");
      if (!byKey.has(key)) byKey.set(key, new Map());
      byKey.get(key).set(r[at(timeCol)], r[at(metric)]);
    }
    series = [...byKey.entries()]
      .map(([name, values]) => ({ name, values, total: [...values.values()].reduce((a, b) => a + (b ?? 0), 0) }))
      .sort((a, b) => b.total - a.total)
      .slice(0, MAX_SERIES);
  } else {
    series = query.metrics
      .filter((m) => units[m] === unit)
      .map((m) => ({ name: label(m), values: new Map(rows.map((r) => [r[at(timeCol)], r[at(m)]])) }));
  }
  const values = series.flatMap((x) => [...x.values.values()]).filter((v) => typeof v === "number");
  const yTicks = ticks(Math.min(0, ...values), Math.max(0, ...values));
  const [W, H, left, right, top, bottom] = [760, 280, 64, 16, 12, 36];
  const x = (i) => left + (xs.length === 1 ? (W - left - right) / 2 : (i / (xs.length - 1)) * (W - left - right));
  const lo = yTicks[0];
  const hi = yTicks[yTicks.length - 1];
  const y = (v) => top + (1 - (v - lo) / (hi - lo)) * (H - top - bottom);
  const every = Math.max(1, Math.ceil(xs.length / 8));
  const svgEl = s("svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": `${label(metric)} by ${label(timeCol)}` },
    s("g", { class: "grid" }, yTicks.map((t) => s("line", { x1: left, x2: W - right, y1: y(t), y2: y(t) }))),
    yTicks.map((t) => s("text", { x: left - 8, y: y(t) + 4, "text-anchor": "end" }, fmt(t, unit, true))),
    xs.map((v, i) => (i % every === 0 || i === xs.length - 1)
      ? s("text", { x: x(i), y: H - 12, "text-anchor": "middle" }, timeText(v, grain)) : null),
    series.map((line, k) => {
      const color = PALETTE[k % PALETTE.length];
      const points = xs.map((v, i) => [i, line.values.get(v)]).filter(([, v]) => typeof v === "number");
      const d = points.map(([i, v], j) => `${j ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("");
      return s("g", {},
        s("path", { d, fill: "none", stroke: color, "stroke-width": 2, "stroke-linejoin": "round" }),
        xs.length <= 60 ? points.map(([i, v]) => s("circle", { cx: x(i), cy: y(v), r: 3, fill: color },
          s("title", {}, `${line.name}, ${timeText(xs[i], grain)}: ${fmt(v, unit)}`))) : null);
    }));
  return [series.length > 1 ? legend(series.map((x) => x.name)) : null, svgEl];
}

function barChart(answer, categoryCols) {
  const { query, columns, rows, units } = answer;
  const at = (c) => columns.indexOf(c);
  const metric = query.metrics[0];
  const unit = units[metric];
  const bars = rows.slice(0, MAX_BARS).map((r) => ({
    name: categoryCols.map((c) => r[at(c)] ?? "–").join(" · "),
    value: r[at(metric)] ?? 0,
  }));
  const max = Math.max(0, ...bars.map((b) => b.value));
  const min = Math.min(0, ...bars.map((b) => b.value));
  const [W, rowH, labelW, valueW] = [760, 26, 200, 96];
  const scale = (v) => ((v - min) / ((max - min) || 1)) * (W - labelW - valueW);
  const H = bars.length * rowH + 8;
  return [s("svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": `${label(metric)} by ${categoryCols.map(label).join(", ")}` },
    bars.map((b, i) => s("g", {},
      s("text", { x: labelW - 8, y: i * rowH + 17, "text-anchor": "end" }, b.name.length > 30 ? b.name.slice(0, 29) + "…" : b.name),
      s("rect", {
        x: labelW + scale(Math.min(0, b.value)), y: i * rowH + 5, height: rowH - 10, rx: 4,
        width: Math.max(1, Math.abs(scale(b.value) - scale(0))), fill: PALETTE[0],
      }, s("title", {}, `${b.name}: ${fmt(b.value, unit)}`)),
      s("text", { x: labelW + scale(Math.max(0, b.value)) + 6, y: i * rowH + 17 }, fmt(b.value, unit, true)))))];
}

function chart(answer) {
  const { query, columns, rows, units } = answer;
  if (!rows.length) return [h("p", { class: "empty" }, "No rows match this query.")];
  const timeCol = query.group_by.find((g) => grainOf(g));
  const categoryCols = query.group_by.filter((g) => g !== timeCol);
  let kind = query.chart;
  if (!query.group_by.length) kind = "number";
  else if (timeCol && kind !== "table") kind = "line";
  else if (kind === "line" || kind === "number") kind = "bar";
  if (kind === "number") {
    return [h("div", { class: "tiles" }, query.metrics.map((m) => h("div", { class: "card stat" },
      h("div", { class: "label" }, label(m)),
      h("div", { class: "value accent" }, fmt(rows[0][columns.indexOf(m)], units[m])))))];
  }
  if (kind === "line") return lineChart(answer, timeCol, categoryCols);
  if (kind === "bar") return barChart(answer, categoryCols);
  return [];
}

function table(answer) {
  const { columns, rows } = answer;
  return h("div", { class: "table-wrap" }, h("table", {},
    h("thead", {}, h("tr", {}, columns.map((c) => h("th", {}, label(c))))),
    h("tbody", {}, rows.slice(0, MAX_ROWS).map((r) => h("tr", {}, r.map((v, i) => h("td",
      { class: answer.query.metrics.includes(columns[i]) ? "num" : undefined }, cell(answer, columns[i], v))))))));
}

// endregion

// region Answers

const tag = (name, value) => h("span", { class: "tag" }, h("b", {}, name), value);

function queryTags(q) {
  const out = [
    ...q.metrics.map((m) => tag("metric", label(m))),
    ...q.group_by.map((g) => tag("by", label(g))),
    ...q.filters.map((f) => tag("where", `${label(f.dimension)} ${f.op.toLowerCase()} ${[f.value].flat().join(", ")}`)),
  ];
  if (q.time_range) out.push(tag("time", `${q.time_range.start ?? "the start"} to ${q.time_range.end ?? "today"}`));
  if (q.order_by.length) {
    out.push(tag("sort", q.order_by.map((o) => `${label(o.replace(/^-/, ""))} ${o.startsWith("-") ? "descending" : "ascending"}`).join(", ")));
  }
  out.push(tag("limit", q.limit));
  return h("div", { class: "meta" }, out);
}

function renderAnswer(card, answer) {
  const q = answer.query;
  const editorSlot = h("div");
  const toggle = h("button", {
    type: "button", class: "link",
    onclick: () => {
      if (editorSlot.firstChild) editorSlot.replaceChildren();
      else editorSlot.replaceChildren(editor(q, (edited) => runEdited(edited)));
    },
  }, "Edit query");
  card.className = "card answer";
  card.replaceChildren(
    h("p", { class: "lead" }, q.explanation || "What the query returned:"),
    queryTags(q),
    ...chart(answer).filter(Boolean),
    table(answer),
    h("details", {}, h("summary", {}, "SQL"), h("pre", {}, answer.sql)),
    h("div", { class: "actions" }, toggle,
      answer.attempts > 1 ? h("span", { class: "muted" }, "The first query was refused and corrected.") : null,
      answer.rows.length >= q.limit ? h("span", { class: "muted" }, `Showing the first ${q.limit} rows.`) : null),
    editorSlot);
}

// To the top of the screen, not "nearest": the sticky question box would cover a turn scrolled
// to the bottom edge. At the end of the page the scroll stops early and the box sits below it.
const reveal = (card) => card.parentElement.scrollIntoView({ behavior: "smooth", block: "start" });

function newTurn(questionText) {
  $("welcome").hidden = true;
  const card = h("div", { class: "card answer", "aria-busy": "true" }, h("div", { class: "loading" }), h("div", { class: "loading" }));
  const turn = h("div", { class: "turn" }, questionText ? h("div", { class: "question" }, questionText) : null, card);
  $("thread").append(turn);
  reveal(card);
  return card;
}

function failed(card, message) {
  card.className = "card answer failed";
  card.replaceChildren(h("p", { class: "error" }, message));
}

async function busy(work) {
  if (state.busy) return;
  state.busy = true;
  $("send").disabled = true;
  try {
    await work();
  } finally {
    state.busy = false;
    $("send").disabled = false;
  }
}

function ask(question) {
  return busy(async () => {
    const card = newTurn(question);
    try {
      const answer = await api("api/ask", { question, history: state.history.slice(-4) });
      if (answer.kind === "clarify") {
        card.className = "card answer clarify";
        card.replaceChildren(h("p", {}, answer.message));
      } else if (answer.kind === "error") {
        failed(card, `No valid query came out of that question: ${answer.message}`);
      } else {
        state.history.push({ question, query: answer.query });
        renderAnswer(card, answer);
      }
    } catch (err) {
      failed(card, err.message);
    } finally {
      card.removeAttribute("aria-busy");
      reveal(card);
    }
  });
}

function runEdited(query) {
  return busy(async () => {
    const card = newTurn("Edited query");
    try {
      const answer = await api("api/run", { query });
      state.history.push({ question: "Show this query", query: answer.query });
      renderAnswer(card, answer);
    } catch (err) {
      failed(card, err.message);
    } finally {
      card.removeAttribute("aria-busy");
      reveal(card);
    }
  });
}

// endregion

// region Query editor

function parseFilter(line) {
  const m = line.match(FILTER);
  if (!m) return { dimension: line, op: "?", value: "" };
  const op = m[2].toUpperCase();
  const value = op.endsWith("IN") ? m[3].split(",").map((v) => v.trim()).filter(Boolean) : m[3].trim();
  return { dimension: m[1], op, value };
}

function editor(q, onRun) {
  const catalog = state.catalog;
  const groupOptions = [
    ...GRAINS.map((g) => `metric_time__${g}`),
    ...catalog.dimensions.filter((d) => d.name !== "metric_time").flatMap((d) => (d.type === "TIME"
      ? GRAINS.filter((g) => !d.grains?.length || d.grains.includes(g)).map((g) => `${d.name}__${g}`)
      : [d.name])),
  ];
  const metrics = h("select", { multiple: true },
    catalog.metrics.map((m) => h("option", { value: m.name, selected: q?.metrics.includes(m.name) }, m.label)));
  const groupBy = h("select", { multiple: true },
    groupOptions.map((g) => h("option", { value: g, selected: q?.group_by.includes(g) }, `${label(g)} (${g})`)));
  const filters = h("textarea", { rows: 3, placeholder: "One per line, such as: customer__region IN Europe, Asia Pacific" });
  filters.value = (q?.filters ?? []).map((f) => `${f.dimension} ${f.op} ${[f.value].flat().join(", ")}`).join("\n");
  const start = h("input", { type: "date", value: q?.time_range?.start ?? "" });
  const end = h("input", { type: "date", value: q?.time_range?.end ?? "" });
  const order = h("input", { type: "text", value: (q?.order_by ?? []).join(", "), placeholder: "-revenue" });
  const limit = h("input", { type: "number", min: 1, max: 500, value: q?.limit ?? 200 });
  const chartKind = h("select", {}, ["line", "bar", "table", "number"].map((k) => h("option", { value: k, selected: (q?.chart ?? "line") === k }, k)));
  const read = () => ({
    metrics: [...metrics.selectedOptions].map((o) => o.value),
    group_by: [...groupBy.selectedOptions].map((o) => o.value),
    filters: filters.value.split("\n").map((l) => l.trim()).filter(Boolean).map(parseFilter),
    time_range: start.value || end.value ? { start: start.value || null, end: end.value || null } : null,
    order_by: order.value.split(",").map((o) => o.trim()).filter(Boolean),
    limit: Number(limit.value) || 200,
    chart: chartKind.value,
    explanation: "",
  });
  return h("form", { class: "editor", onsubmit: (e) => { e.preventDefault(); onRun(read()); } },
    h("label", {}, "Metrics", metrics),
    h("label", {}, "Group by", groupBy),
    h("label", { class: "wide" }, "Filters", filters),
    h("label", {}, "From", start),
    h("label", {}, "To", end),
    h("label", {}, "Sort, a leading - for descending", order),
    h("label", {}, "Limit", limit),
    h("label", {}, "Chart", chartKind),
    h("div", { class: "wide" }, h("button", { type: "submit" }, "Run query")));
}

function buildQuery() {
  $("welcome").hidden = true;
  const start = { metrics: ["revenue"], group_by: ["metric_time__month"], filters: [], time_range: null, order_by: [], limit: 200, chart: "line" };
  const card = h("div", { class: "card answer" }, h("h2", {}, "Build a query"), editor(start, (q) => runEdited(q)));
  $("thread").append(h("div", { class: "turn" }, card));
  reveal(card);
}

// endregion

// region Catalog

function renderCatalog() {
  const term = $("catalog-filter").value.trim().toLowerCase();
  const match = (x) => !term || `${x.name} ${x.label} ${x.description}`.toLowerCase().includes(term);
  $("metric-list").replaceChildren(...state.catalog.metrics.filter(match).map((m) => h("li", {},
    h("div", {}, h("strong", {}, m.label), " ", h("code", {}, m.name)),
    h("div", { class: "desc" }, m.description))));
  $("dimension-list").replaceChildren(...state.catalog.dimensions.filter((d) => d.name !== "metric_time").filter(match).map((d) => h("li", {},
    h("div", {}, h("strong", {}, d.label), " ", h("code", {}, d.name)),
    h("div", { class: "desc" }, [d.description, d.values?.length ? `Values: ${d.values.join(", ")}` : ""].filter(Boolean).join(" ")))));
}

async function start() {
  const question = $("question");
  $("ask").addEventListener("submit", (e) => {
    e.preventDefault();
    const text = question.value.trim();
    if (!text) return;
    question.value = "";
    ask(text);
  });
  question.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      $("ask").requestSubmit();
    }
  });
  $("catalog-toggle").addEventListener("click", () => {
    const open = $("catalog").hidden;
    $("catalog").hidden = !open;
    $("catalog").parentElement.classList.toggle("with-catalog", open);
    $("catalog-toggle").setAttribute("aria-expanded", String(open));
  });
  $("catalog-filter").addEventListener("input", renderCatalog);
  $("build").addEventListener("click", () => state.catalog && buildQuery());
  try {
    state.catalog = await api("api/catalog");
  } catch (err) {
    $("notice").hidden = false;
    $("notice").textContent = `The semantic layer cannot be read: ${err.message}`;
    return;
  }
  $("subtitle").textContent = `Semantic project ${state.catalog.semantic_project}, ${state.catalog.metrics.length} metrics`
    + (state.catalog.llm.model ? `, questions translated by ${state.catalog.llm.model}` : "");
  if (!state.catalog.llm.configured) {
    $("notice").hidden = false;
    $("notice").textContent = "No LLM is configured, so questions cannot be translated: set LLM_URL, LLM_MODEL and LLM_API_KEY in your account settings (Environment variables) and restart the app. Build a query runs queries without one.";
  }
  $("examples").replaceChildren(...state.catalog.examples.map((text) => h("button", {
    type: "button", class: "chip", onclick: () => ask(text), disabled: !state.catalog.llm.configured,
  }, text)));
  renderCatalog();
}

start();
