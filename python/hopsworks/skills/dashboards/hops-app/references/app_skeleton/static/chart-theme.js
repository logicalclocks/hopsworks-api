// The hops-design look for Chart.js (static/vendor/chart.umd.min.js, loaded before
// this file). Set once here; a chart sets only its data, labels and axis titles.
(() => {
  const { Chart } = window;
  const tokens = {
    accent: "#1eb182",
    accentFill: "rgba(30, 177, 130, 0.12)",
    context: ["#a0a0a0", "#cbcbcb"],
    danger: "#e7000b",
    muted: "#737373",
    grid: "#e5e5e5",
    text: "#252525",
  };

  Chart.defaults.font.family = 'Inter, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';
  Chart.defaults.font.size = 12;
  Chart.defaults.color = tokens.muted;
  Chart.defaults.borderColor = tokens.grid;
  // The chart fills its container, which sets the height (`.chart` in app.css).
  Chart.defaults.responsive = true;
  Chart.defaults.maintainAspectRatio = false;
  // Room on the right, so the last tick label is never clipped.
  Chart.defaults.layout.padding = { top: 4, right: 12, bottom: 0, left: 0 };
  Chart.defaults.animation = false;

  Chart.defaults.elements.line.borderColor = tokens.accent;
  Chart.defaults.elements.line.backgroundColor = tokens.accentFill;
  Chart.defaults.elements.line.borderWidth = 2;
  Chart.defaults.elements.line.tension = 0.25;
  Chart.defaults.elements.point.radius = 0;
  Chart.defaults.elements.point.hoverRadius = 4;
  Chart.defaults.elements.point.backgroundColor = tokens.accent;
  Chart.defaults.elements.bar.backgroundColor = tokens.accent;
  Chart.defaults.elements.bar.borderRadius = 4;
  Chart.defaults.datasets.bar.maxBarThickness = 40;

  for (const type of ["category", "linear", "logarithmic", "time", "timeseries"]) {
    const scale = Chart.defaults.scales[type];
    if (!scale) continue;
    scale.border = { display: false };
    scale.grid = { color: tokens.grid, drawTicks: false };
    scale.ticks = { ...scale.ticks, padding: 8, autoSkip: true, autoSkipPadding: 12, maxRotation: 0 };
    scale.title = { ...scale.title, color: tokens.muted, font: { size: 12 } };
  }
  // Light gridlines on the value axis only.
  Chart.defaults.scales.category.grid = { display: false };

  Chart.defaults.plugins.tooltip.backgroundColor = tokens.text;
  Chart.defaults.plugins.tooltip.padding = 8;
  Chart.defaults.plugins.tooltip.cornerRadius = 8;
  Chart.defaults.plugins.tooltip.displayColors = false;
  // No legend for a single series: a chart with several sets
  // `options.plugins.legend.display: true`.
  Chart.defaults.plugins.legend.display = false;
  Chart.defaults.plugins.legend.labels.boxWidth = 10;
  Chart.defaults.plugins.legend.labels.boxHeight = 10;

  // Colors for a chart's own series: the primary in the accent, the rest in greys.
  window.hopsChart = tokens;
})();
