// Leaflet is vendored under static/vendor, so the page loads no script from a CDN;
// the map's tiles come from Esri World Imagery, which serves them with CORS, so
// the page may draw them into a canvas and send that image to the model.

const MIN_ZOOM = 15; // below this aircraft and vehicles are a few pixels and the model finds none
// One colour per kind the app shows, as the legend lists them.
const COLOURS = {
  plane: "#ff3d3d",
  helicopter: "#ff9f1a",
  ship: "#1ab3ff",
  harbor: "#2f6bff",
  "storage tank": "#d43ad4",
  bridge: "#ffd400",
  "large vehicle": "#20d37a",
};
const LABELS = { harbor: "harbour" };
const SETTLE_MS = 400; // the wait after a move ends before the screen is read
const SWEDEN = { center: [62.5, 16.5], zoom: 5 };

const map = L.map("map", { zoomControl: true, preferCanvas: true }).setView(SWEDEN.center, SWEDEN.zoom);
const imagery = L.tileLayer(
  "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
  {
    maxZoom: 19,
    maxNativeZoom: 19,
    crossOrigin: "anonymous",
    attribution: "Imagery &copy; Esri, Maxar, Earthstar Geographics, and the GIS User Community",
  },
).addTo(map);
const boxes = L.layerGroup().addTo(map);

const $ = (selector) => document.querySelector(selector);
let latest = 0;
let settle;

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

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

// The map as it is on screen: every loaded imagery tile, drawn where it shows.
function screenImage() {
  const frame = map.getContainer().getBoundingClientRect();
  const canvas = document.createElement("canvas");
  canvas.width = Math.round(frame.width);
  canvas.height = Math.round(frame.height);
  const context = canvas.getContext("2d");
  for (const tile of imagery.getContainer().querySelectorAll("img.leaflet-tile-loaded")) {
    const at = tile.getBoundingClientRect();
    context.drawImage(tile, at.left - frame.left, at.top - frame.top, at.width, at.height);
  }
  return canvas.toDataURL("image/jpeg", 0.9);
}

function tilesLoaded() {
  return new Promise((resolve) => (imagery.isLoading() ? imagery.once("load", resolve) : resolve()));
}

async function detect() {
  const call = ++latest;
  if (map.getZoom() < MIN_ZOOM) {
    boxes.clearLayers();
    showLegend({});
    $("#hint").hidden = false;
    $("#status").textContent = `Zoom in to level ${MIN_ZOOM} or closer to find objects.`;
    return;
  }
  $("#hint").hidden = true;
  await tilesLoaded();
  if (call !== latest) return;
  $("#status").textContent = "Finding objects…";
  const started = performance.now();
  try {
    const reply = await request("api/detect", {
      method: "POST",
      body: JSON.stringify({ image: screenImage(), threshold: Number($("#threshold").value) }),
    });
    // A later move made this answer stale: its boxes would land on the wrong place.
    if (call !== latest) return;
    boxes.clearLayers();
    const counts = {};
    for (const object of reply.objects) {
      const colour = COLOURS[object.label] ?? "#ffffff";
      const outline = object.corners.map((point) => map.containerPointToLatLng(point));
      L.polygon(outline, { color: colour, weight: 2, fill: false })
        .bindTooltip(`${LABELS[object.label] ?? object.label}, ${Math.round(object.score * 100)}%`)
        .addTo(boxes);
      counts[object.label] = (counts[object.label] ?? 0) + 1;
    }
    showLegend(counts);
    $("#count").textContent = String(reply.objects.length);
    $("#model-time").textContent = `${Math.round(reply.timings_ms.detect)} ms`;
    $("#round-trip").textContent = `${Math.round(performance.now() - started)} ms`;
    $("#image-size").textContent = `${reply.width} × ${reply.height}`;
    $("#detector").textContent = `${reply.model.name} v${reply.model.version}`;
    $("#status").textContent = reply.objects.length
      ? "Hover an outline for its kind and the model's confidence."
      : "Nothing found here; try another place or lower the confidence.";
  } catch (error) {
    if (call === latest) $("#status").textContent = `Could not find objects: ${error.message}`;
  }
}

function showLegend(counts) {
  $("#legend").replaceChildren(
    ...Object.entries(COLOURS).map(([label, colour]) => {
      const item = el("li");
      const swatch = el("span", "swatch");
      swatch.style.borderColor = colour;
      item.append(swatch, el("span", "", LABELS[label] ?? label), el("span", "n", String(counts[label] ?? 0)));
      return item;
    }),
  );
}

function scheduleDetect() {
  clearTimeout(settle);
  settle = setTimeout(detect, SETTLE_MS);
}

async function loadPlaces() {
  const list = $("#places");
  try {
    const places = await request("api/locations");
    list.replaceChildren(
      ...places.map((place) => {
        const button = el("button", "", place.name);
        button.type = "button";
        button.addEventListener("click", () => {
          list.querySelectorAll("button").forEach((b) => b.setAttribute("aria-pressed", "false"));
          button.setAttribute("aria-pressed", "true");
          map.setView([place.lat, place.lon], place.zoom);
        });
        return button;
      }),
    );
  } catch (error) {
    list.replaceChildren(el("span", "error", `Could not load places: ${error.message}`));
  }
}

map.on("movestart", () => clearTimeout(settle));
map.on("moveend", scheduleDetect);
$("#threshold").addEventListener("input", (event) => {
  $("#threshold-value").textContent = Number(event.target.value).toFixed(2);
  scheduleDetect();
});
loadPlaces();
detect();
