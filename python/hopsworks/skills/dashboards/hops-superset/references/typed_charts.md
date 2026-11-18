# Charts and dashboard edits without Python

On Superset 7.0, `hops superset chart generate` creates a chart from a short typed config.
Superset's MCP service compiles it into the viz params and checks every column and metric against the dataset before it saves anything, so no `params` JSON (SKILL.md §4) is needed.
Prefer it to `create_chart` and to `chart create --params`.

```bash
hops superset chart types                    # chart types and their required fields
hops superset chart types xy                 # one type's full config schema, with examples
hops superset chart generate --dataset 12 --type xy --x event_time \
  --y 'SUM(amount)' --group-by category --time-grain P1D --name 'Spend per day'
hops superset chart generate --dataset 12 --type big_number --metric 'COUNT(*)'
hops superset chart generate --dataset 12 --type table --columns 'category,SUM(amount)' \
  --filter 'country IN SE,NO' --filter 'amount > 100'
hops superset chart generate --dataset 12 --config @chart.json   # any type, full schema
hops superset chart generate --dataset 12 --type pie --dimension category --metric 'SUM(amount)' --preview
```

- `--y` and `--metric`: `SUM(col)` (also COUNT, AVG, MIN, MAX, COUNT_DISTINCT, MEDIAN) aggregates a column, a bare name is a saved metric of the dataset, anything else (`COUNT(*)`) is a SQL expression.
- `--preview` leaves the chart unsaved and prints an Explore link; without it the chart is saved and its ID and link printed.
- A rejected config fails with Superset's message, such as the column it could not find; fix the flag and rerun.
- Superset runs the chart's query before saving it, so a saved chart is one whose query worked.
- Superset 7.0.0rc2 saves a generated chart without a query context: it renders in Explore and on dashboards, but `GET /api/v1/chart/{id}/data/` answers 400 `Chart has no query context saved` until the chart is opened and saved once in Superset.

`hops superset dashboard update ID` changes only the fields given and leaves the rest:

```bash
hops superset dashboard update 7 --title 'Fraud overview' --published --refresh 300
hops superset dashboard update 7 --metadata '{"color_scheme": "supersetColors"}'   # merged into json_metadata
hops superset dashboard update 7 --css @print.css --filter-bar horizontal --cross-filters
```

Put a generated chart on a dashboard with `api.update_chart(chart_id, dashboards=[dashboard_id])` (SKILL.md §1), then lay it out as SKILL.md §5 describes.
Both commands run only inside the cluster (a Hopsworks terminal or job), like the rest of `hops superset`.
