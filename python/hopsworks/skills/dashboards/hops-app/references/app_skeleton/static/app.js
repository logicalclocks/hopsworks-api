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

function row(cells) {
  const tr = document.createElement("tr");
  for (const value of cells) {
    const td = document.createElement("td");
    td.textContent = value;
    tr.append(td);
  }
  return tr;
}

async function loadTop() {
  const body = document.querySelector("#top tbody");
  try {
    const top = await getJSON("api/top");
    body.replaceChildren(...top.map((c) => row([c.customer_id, c.score.toFixed(3)])));
  } catch (error) {
    body.replaceChildren(row([`Could not load: ${error.message}`, ""]));
  }
}

document.querySelector("#lookup").addEventListener("submit", async (event) => {
  event.preventDefault();
  const id = new FormData(event.target).get("customer");
  const result = document.querySelector("#result");
  result.textContent = "Looking up…";
  try {
    const c = await getJSON(`api/customers/${encodeURIComponent(id)}`);
    result.textContent = `Customer ${c.customer_id}: score ${c.score.toFixed(3)} (as of ${c.predicted_at})`;
  } catch (error) {
    result.textContent = error.message;
  }
});

loadTop();
