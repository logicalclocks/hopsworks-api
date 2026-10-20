// No build step and no CDN: the app runs air-gapped. Every URL is relative so
// the Hopsworks proxy mount works without the app knowing its prefix. Charts are
// Chart.js (static/vendor/), styled once by static/chart-theme.js.

const { Chart, hopsChart } = window;

async function getJSON(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

const percent = (value) => `${(value * 100).toFixed(1)}%`;

// A stat shows its figure on one line; the title keeps it whole when it is cut.
function setStat(id, text) {
  const node = document.getElementById(id);
  node.textContent = text;
  node.title = text;
}
const count = new Intl.NumberFormat();
const day = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" });

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function scoreBar(score) {
  const level = score >= 0.6 ? "high" : score >= 0.3 ? "mid" : "";
  const fill = el("div", `fill ${level}`);
  fill.style.width = percent(score);
  const track = el("div", "track");
  track.append(fill);
  const bar = el("div", "bar");
  bar.append(track, el("span", "num", percent(score)));
  return bar;
}

function message(className, text, columns = 2) {
  const td = el("td", className, text);
  td.colSpan = columns;
  const tr = el("tr");
  tr.append(td);
  return tr;
}

// A reload keeps the rows on screen, dimmed by aria-busy, and replaces them
// only when the new ones arrive, so nothing on the page moves.
async function loadTop() {
  const body = document.querySelector("#top tbody");
  body.setAttribute("aria-busy", "true");
  try {
    const top = await getJSON("api/top");
    if (!top.length) {
      body.replaceChildren(message("empty", "No predictions yet."));
      return;
    }
    body.replaceChildren(
      ...top.map((c) => {
        const tr = el("tr");
        const risk = el("td");
        risk.append(scoreBar(c.score));
        tr.append(el("td", "id", String(c.customer_id)), risk);
        return tr;
      }),
    );
    const mean = top.reduce((sum, c) => sum + c.score, 0) / top.length;

    setStat("avg", percent(mean));
    setStat("max", percent(top[0].score));
  } catch (error) {
    body.replaceChildren(message("error", `Could not load: ${error.message}`));
  } finally {
    body.removeAttribute("aria-busy");
  }
}

document.querySelector("#lookup").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.target;
  const id = new FormData(form).get("customer");
  const result = document.querySelector("#result");
  const button = form.querySelector("button");
  button.disabled = true;
  result.replaceChildren(el("div", "loading"));
  try {
    const c = await getJSON(`api/customers/${encodeURIComponent(id)}`);
    const when = new Date(c.predicted_at).toLocaleString();
    const line = el("p");
    line.append(
      el("strong", "", `Customer ${String(c.customer_id)} `),
      el("span", c.score >= 0.6 ? "status high" : "status", percent(c.score)),
      el("span", "muted", ` as of ${when}`),
    );
    result.replaceChildren(line);
  } catch (error) {
    result.replaceChildren(el("p", "error", error.message));
  } finally {
    button.disabled = false;
  }
});

// One chart per canvas, created on the first load and updated in place after.
const charts = {};

function chartMessage(canvas, className, text) {
  const box = canvas.parentElement;
  box.querySelector(".empty, .error")?.remove();
  if (text) box.append(el("div", className, text));
}

function draw(id, config) {
  const canvas = document.getElementById(id);
  const empty = !config.data.labels.length;
  chartMessage(canvas, "empty", empty ? "No predictions yet." : "");
  if (charts[id]) {
    charts[id].data = config.data;
    charts[id].update();
  } else {
    charts[id] = new Chart(canvas, config);
  }
}

async function loadCharts() {
  try {
    const [trend, bands] = await Promise.all([getJSON("api/trend"), getJSON("api/bands")]);
    draw("trend", {
      type: "line",
      data: {
        labels: trend.map((d) => day.format(new Date(d.day))),
        datasets: [{ label: "Average risk", data: trend.map((d) => d.score * 100), fill: true }],
      },
      options: {
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "Average risk (%)" } },
        },
        plugins: { tooltip: { callbacks: { label: (c) => `${c.parsed.y.toFixed(1)}%` } } },
      },
    });
    draw("bands", {
      type: "bar",
      data: {
        labels: bands.map((b) => b.band),
        datasets: [
          {
            label: "Customers",
            data: bands.map((b) => b.customers),
            // The band that needs action in the accent, the others for context.
            backgroundColor: bands.map((b) =>
              b.band === "high" ? hopsChart.accent : hopsChart.context[0],
            ),
          },
        ],
      },
      options: {
        scales: { y: { beginAtZero: true, title: { display: true, text: "Customers" } } },
      },
    });
    const scored = bands.reduce((sum, b) => sum + b.customers, 0);
    setStat("scored", count.format(scored));
  } catch (error) {
    for (const id of ["trend", "bands"]) {
      chartMessage(document.getElementById(id), "error", `Could not load: ${error.message}`);
    }
  }
}

loadTop();
loadCharts();
