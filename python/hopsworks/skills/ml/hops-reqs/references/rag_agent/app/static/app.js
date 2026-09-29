// No build step and no CDN: the app runs air-gapped. Every URL is relative so
// the Hopsworks proxy mount works without the app knowing its prefix, except the
// document links, which the ingestion job wrote relative to the UI's origin.

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

function row(...cells) {
  const tr = el("tr");
  cells.forEach((c) => tr.append(el("td", "", c)));
  return tr;
}

async function loadUsers() {
  const select = document.querySelector("#user");
  try {
    const users = await request("api/users");
    select.replaceChildren(
      ...users.map((id) => {
        const option = el("option", "", String(id));
        option.value = String(id);
        return option;
      }),
    );
    if (!users.length) select.replaceChildren(el("option", "", "No users with events"));
  } catch (error) {
    select.replaceChildren(el("option", "", `Could not load users: ${error.message}`));
  }
}

function showSources(sources) {
  const list = document.querySelector("#sources");
  list.replaceChildren(
    ...sources.map((s) => {
      const item = el("li");
      const where = el("div", "where");
      const link = el("a", "", s.doc_name);
      link.href = new URL(s.url, location.origin).href;
      link.target = "_blank";
      link.rel = "noreferrer";
      where.append(
        link,
        el("span", "muted", `page ${s.page} · paragraph ${s.offset + 1}`),
        el("span", "badge", s.score.toFixed(3)),
      );
      item.append(where, el("p", "", s.text));
      return item;
    }),
  );
}

function showEvents(events) {
  const body = document.querySelector("#events tbody");
  if (!events.length) {
    body.replaceChildren(row("No recent events", "", ""));
    return;
  }
  body.replaceChildren(
    ...events.map((e) =>
      row(new Date(e.event_time).toLocaleString(), e.event_type, e.product_name ?? ""),
    ),
  );
}

document.querySelector("#ask").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = new FormData(event.target);
  const button = document.querySelector("#send");
  const answer = document.querySelector("#answer");
  button.disabled = true;
  answer.setAttribute("aria-busy", "true");
  answer.replaceChildren(el("div", "loading"), el("div", "loading"));
  try {
    const reply = await request("api/ask", {
      method: "POST",
      body: JSON.stringify({ user_id: Number(form.get("user_id")), query: form.get("query") }),
    });
    if (reply.error) throw new Error(reply.error);
    answer.replaceChildren(el("p", "answer", reply.answer));
    showSources(reply.sources ?? []);
    showEvents(reply.events ?? []);
  } catch (error) {
    answer.replaceChildren(el("p", "error", error.message));
  } finally {
    button.disabled = false;
    answer.removeAttribute("aria-busy");
  }
});

loadUsers();
