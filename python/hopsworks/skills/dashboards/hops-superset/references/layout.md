# Dashboard layout

Part of **hops-superset**: the `position_json` a dashboard needs, the look, a layout builder and an idempotent create or update.

## Layout primer

Superset uses a 12-column grid. Row heights are in ~25px units. Every node
has `id`, `type`, `children`, `parents`, and `meta`. Widths must sum to 12
within each row.

Node types used in `position_json`:

| Type | Purpose | `meta` fields |
|---|---|---|
| `ROOT` | Always present, single child `GRID_ID` | - |
| `GRID` | The 12-col grid, children are rows | - |
| `HEADER` | Dashboard title header | `text` |
| `ROW` | Horizontal row, children sum to 12 cols | `background` |
| `CHART` | A chart cell | `width`, `height`, `chartId`, `sliceName` |
| `MARKDOWN` | Static markdown cell | `width`, `height`, `code` |
| `COLUMN` | Vertical column container | `width`, `background` |
| `TABS` / `TAB` | Tabbed sections | `text` (on TAB) |

## Look

Follow [hops-design](../../hops-design/SKILL.md): flat, light, the Hopsworks green
for the primary series only.

- One `HEADER` with the dashboard's name. A `MARKDOWN` cell only for a one-line
  note the charts cannot show (a data caveat, a link); no intro text.
- Key figures first, as one row of `big_number_total` charts with a short
  `subheader`; then the charts that answer the dashboard's questions; tables last.
- Title each chart with the question it answers. `show_legend` only when a chart has
  more than one series.
- Series colors in the dashboard's `json_metadata`: `label_colors` maps the primary
  metric to `#1EB182` and comparison series to greys (`#A0A0A0`, `#CBCBCB`); a
  threshold breach is the only red.
- `css` on the dashboard (`create_dashboard(..., css=...)`) for anything the params
  do not cover, kept short: `.dashboard-component { box-shadow: none; }` removes
  the card shadows.

## Layout builder

```python
import json

def build_position_json(chart_ids, chart_slices, title):
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": [],                # filled below
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }

    def chart(key, width, height):
        nid = f"CHART-{key}"
        layout[nid] = {
            "type": "CHART", "id": nid, "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"width": width, "height": height,
                     "chartId": chart_ids[key], "sliceName": chart_slices[key]},
        }
        return nid

    def row(row_id, children):
        layout[row_id] = {
            "type": "ROW", "id": row_id, "children": children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for c in children:
            layout[c]["parents"] = ["ROOT_ID", "GRID_ID", row_id]
        layout["GRID_ID"]["children"].append(row_id)

    row("ROW-1", [chart("a", 6, 50), chart("b", 6, 50)])    # two half-width charts
    row("ROW-2", [chart("c", 12, 50)])                      # one full-width chart
    return json.dumps(layout)
```

## Create / update idempotently

```python
def ensure_dashboard(api, title, chart_ids, chart_slices):
    position_json = build_position_json(chart_ids, chart_slices, title)

    dashboard_id = next(
        (d["id"] for d in list_all(api, "dashboard")
         if d.get("dashboard_title") == title),
        None,
    )

    if dashboard_id is None:
        dashboard_id = api.create_dashboard(
            dashboard_title=title, published=True, position_json=position_json,
        )["id"]
    else:
        api.update_dashboard(
            dashboard_id, dashboard_title=title, published=True,
            position_json=position_json,
        )

    # Persist the chart -> dashboard relation explicitly.
    for cid in chart_ids.values():
        api.update_chart(cid, dashboards=[dashboard_id])

    return dashboard_id
```

Dashboard URL:
```
https://<hopsworks-host>/hopsworks-api/superset/superset/dashboard/<id>/
```
