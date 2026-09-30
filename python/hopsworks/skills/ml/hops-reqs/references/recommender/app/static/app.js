// No build step and no CDN: the app runs air-gapped. Every URL is relative so
// the Hopsworks proxy mount works without the app knowing its prefix, except the
// product pictures, which are the absolute URLs the feature pipeline wrote.

const LABELS = { 0: "Ignored", 1: "Clicked", 2: "Bought" };
const state = { customer: "", items: [], acted: new Set(), last: "START", counts: { shown: 0, clicked: 0, bought: 0, ignored: 0 } };

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function picture(url, alt) {
  const img = el("img");
  img.loading = "lazy";
  img.alt = alt || "";
  if (url) img.src = url;
  // A missing picture leaves the tinted frame rather than a broken-image icon.
  img.addEventListener("error", () => img.remove());
  return img;
}

function showCounts() {
  for (const [key, value] of Object.entries(state.counts)) {
    document.querySelector(`#${key}`).textContent = String(value);
  }
}

function record(kind, articleIds) {
  return request("api/interactions", {
    method: "POST",
    body: JSON.stringify({
      customer_id: state.customer,
      kind,
      article_ids: articleIds,
      prev_article_id: state.last,
    }),
  });
}

async function act(item, kind, card) {
  card.querySelectorAll("button").forEach((b) => (b.disabled = true));
  try {
    await record(kind, [item.article_id]);
    state.acted.add(item.article_id);
    state.last = item.article_id;
    card.classList.add(kind === "buy" ? "bought" : "clicked");
    state.counts[kind === "buy" ? "bought" : "clicked"] += 1;
    showCounts();
    loadHistory();
  } catch (error) {
    card.append(el("div", "error", error.message));
  } finally {
    // A clicked card can still be bought; a bought one is done.
    if (kind === "click") card.querySelector(".buy").disabled = false;
  }
}

function productCard(item) {
  const card = el("article", "product");
  const frame = el("div", "picture");
  frame.append(picture(item.image_url, item.prod_name));
  const body = el("div", "body");
  body.append(
    el("div", "name", item.prod_name || item.article_id),
    el("div", "meta", [item.product_type_name, item.colour_group_name].filter(Boolean).join(" · ")),
    el("div", "meta", item.index_group_name || ""),
  );
  const bar = el("div", "bar");
  const track = el("div", "track");
  const fill = el("div", "fill");
  fill.style.width = `${Math.round(item.score * 100)}%`;
  track.append(fill);
  bar.append(track, el("span", "num muted", item.score.toFixed(2)));
  body.append(bar);
  const actions = el("div", "actions");
  const click = el("button", "secondary", "Click");
  click.type = "button";
  const buy = el("button", "buy", "Buy");
  buy.type = "button";
  click.addEventListener("click", () => act(item, "click", card));
  buy.addEventListener("click", () => act(item, "buy", card));
  actions.append(click, buy);
  card.append(frame, body, actions);
  return card;
}

async function ignoreUntouched() {
  const untouched = state.items.map((i) => i.article_id).filter((id) => !state.acted.has(id));
  if (!state.customer || !untouched.length) return;
  await record("ignore", untouched);
  state.counts.ignored += untouched.length;
  showCounts();
}

async function recommend() {
  const products = document.querySelector("#products");
  const button = document.querySelector("#recommend");
  button.disabled = true;
  products.setAttribute("aria-busy", "true");
  const started = performance.now();
  try {
    const reply = await request("api/recommend", {
      method: "POST",
      body: JSON.stringify({ customer_id: state.customer, k: 12 }),
    });
    document.querySelector("#latency").textContent = `${Math.round(performance.now() - started)} ms`;
    state.items = reply.items || [];
    state.acted = new Set();
    state.counts.shown += state.items.length;
    showCounts();
    const timings = Object.entries(reply.timings_ms || {}).map(([stage, ms]) => el("span", "badge", `${stage} ${ms} ms`));
    const found = el("span", "", `${reply.retrieved ?? 0} retrieved, ${reply.already_bought ?? 0} already bought`);
    document.querySelector("#timings").replaceChildren(found, ...timings);
    products.replaceChildren(
      ...(state.items.length ? state.items.map(productCard) : [el("p", "empty", reply.error || "No recommendations.")]),
    );
    document.querySelector("#refresh").disabled = !state.items.length;
  } catch (error) {
    products.replaceChildren(el("p", "error", error.message));
  } finally {
    button.disabled = false;
    products.removeAttribute("aria-busy");
  }
}

async function loadHistory() {
  const list = document.querySelector("#history");
  list.setAttribute("aria-busy", "true");
  try {
    const events = await request(`api/history/${encodeURIComponent(state.customer)}`);
    list.replaceChildren(
      ...(events.length
        ? events.map((e) => {
            const item = el("li");
            const text = el("div");
            text.append(
              el("div", "what", `${LABELS[e.interaction_score] || "Seen"}: ${e.prod_name || e.article_id}`),
              el("div", "when", e.t_dat.slice(0, 16).replace("T", " ")),
            );
            item.append(picture(e.image_url, e.prod_name), text);
            return item;
          })
        : [el("li", "muted", "No interactions yet.")]),
    );
  } catch (error) {
    list.replaceChildren(el("li", "error", error.message));
  } finally {
    list.removeAttribute("aria-busy");
  }
}

async function loadCustomers() {
  const select = document.querySelector("#customer");
  try {
    const customers = await request("api/customers");
    select.replaceChildren(
      ...customers.map((c) => {
        const age = c.age == null ? "" : `, age ${Math.round(c.age)}`;
        const option = el("option", "", `${c.customer_id.slice(0, 12)}… (${c.purchases} purchases${age})`);
        option.value = c.customer_id;
        return option;
      }),
    );
    if (!customers.length) select.replaceChildren(el("option", "", "No customers yet"));
  } catch (error) {
    select.replaceChildren(el("option", "", `Could not load customers: ${error.message}`));
  }
}

document.querySelector("#pick").addEventListener("submit", async (event) => {
  event.preventDefault();
  const chosen = document.querySelector("#customer").value;
  if (!chosen) return;
  if (chosen !== state.customer) {
    state.customer = chosen;
    state.last = "START";
    state.items = [];
  }
  await Promise.all([recommend(), loadHistory()]);
});

document.querySelector("#refresh").addEventListener("click", async () => {
  try {
    await ignoreUntouched();
  } catch (error) {
    document.querySelector("#timings").replaceChildren(el("span", "error", error.message));
  }
  await Promise.all([recommend(), loadHistory()]);
});

loadCustomers();
