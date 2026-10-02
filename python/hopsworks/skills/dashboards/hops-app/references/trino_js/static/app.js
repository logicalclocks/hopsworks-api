// The page asks the app's own server, which queries Trino; a relative URL keeps
// the Hopsworks proxy mount.
const response = await fetch("api/rows?limit=100", { headers: { Accept: "application/json" } });
const body = await response.json();
const table = document.getElementById("rows");

if (!response.ok) {
  table.tBodies[0].replaceChildren();
  document.getElementById("error").textContent = body.detail || response.statusText;
} else {
  const head = document.createElement("tr");
  for (const name of body.columns) head.append(Object.assign(document.createElement("th"), { textContent: name }));
  table.tHead.replaceChildren(head);
  table.tBodies[0].replaceChildren(
    ...body.rows.map((row) => {
      const tr = document.createElement("tr");
      for (const value of row) tr.append(Object.assign(document.createElement("td"), { textContent: value ?? "" }));
      return tr;
    }),
  );
}
