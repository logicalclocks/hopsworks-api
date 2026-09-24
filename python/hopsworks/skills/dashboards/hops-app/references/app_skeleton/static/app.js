// No build step and no CDN: the app runs air-gapped. Every URL is relative so
// the Hopsworks proxy mount works without the app knowing its prefix.

async function getJSON(path) {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

const percent = (value) => `${(value * 100).toFixed(1)}%`;

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

async function loadTop() {
  const body = document.querySelector("#top tbody");
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
        tr.append(el("td", "num", String(c.customer_id)), risk);
        return tr;
      }),
    );
    const mean = top.reduce((sum, c) => sum + c.score, 0) / top.length;
    document.querySelector("#avg").textContent = percent(mean);
    document.querySelector("#max").textContent = percent(top[0].score);
  } catch (error) {
    body.replaceChildren(message("error", `Could not load: ${error.message}`));
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
      el("span", c.score >= 0.6 ? "badge high" : "badge", percent(c.score)),
      el("span", "muted", ` as of ${when}`),
    );
    result.replaceChildren(line);
  } catch (error) {
    result.replaceChildren(el("p", "error", error.message));
  } finally {
    button.disabled = false;
  }
});

loadTop();
