# Superset viz_type reference

The registered keys Superset's server accepts, and the legacy keys that fail with `Item with key "X" is not registered`. Always use a modern key.

### Complete registered viz_type enum

From Superset's `VizType` enum — these are the only keys the server accepts.

| Category | Viz name | viz_type key |
|---|---|---|
| Big number | Big Number | `big_number` |
| Big number | Big Number Total | `big_number_total` |
| Big number | Period-over-period KPI | `pop_kpi` |
| Categorical | Pie | `pie` |
| Categorical | Funnel | `funnel` |
| Categorical | Radar | `radar` |
| Categorical | Rose | `rose` |
| Categorical | Chord | `chord` |
| Categorical | Sankey | `sankey_v2` |
| Categorical | Sunburst | `sunburst_v2` |
| Categorical | Treemap | `treemap_v2` |
| Categorical | Partition | `partition` |
| Distribution | Histogram | `histogram_v2` |
| Distribution | Box Plot | `box_plot` |
| Time series | Bar (categorical or time) | `echarts_timeseries_bar` |
| Time series | Line | `echarts_timeseries_line` |
| Time series | Smooth Line | `echarts_timeseries_smooth` |
| Time series | Step | `echarts_timeseries_step` |
| Time series | Area | `echarts_area` |
| Time series | Scatter | `echarts_timeseries_scatter` |
| Time series | Generic time series | `echarts_timeseries` |
| Time series | Mixed time series | `mixed_timeseries` |
| Time series | Waterfall | `waterfall` |
| Time series | Compare | `compare` |
| Time series | Time Pivot | `time_pivot` |
| Time series | Time Table | `time_table` |
| Correlation | Heatmap | `heatmap_v2` |
| Correlation | Bubble | `bubble_v2` |
| Correlation | Parallel Coords | `para` |
| Correlation | Paired t-test | `paired_ttest` |
| Tabular | Table | `table` |
| Tabular | Table (AG Grid) | `ag-grid-table` |
| Tabular | Pivot Table | `pivot_table_v2` |
| Gauge/other | Gauge | `gauge_chart` |
| Gauge/other | Bullet | `bullet` |
| Gauge/other | Calendar heatmap | `cal_heatmap` |
| Maps | Country Map | `country_map` |
| Maps | World Map | `world_map` |
| Maps | MapBox | `mapbox` |
| Maps | Point cluster | `point_cluster_map` |
| Maps | Cartodiagram | `cartodiagram` |
| Other | Graph | `graph_chart` |
| Other | Tree | `tree_chart` |
| Other | Gantt | `gantt_chart` |
| Other | Horizon | `horizon` |
| Other | Word cloud | `word_cloud` |
| Other | Handlebars | `handlebars` |

### Legacy keys that will FAIL

| Legacy (don't use) | Modern replacement |
|---|---|
| `dist_bar` | `echarts_timeseries_bar` (with `x_axis_force_categorical: true`) |
| `bar` | `echarts_timeseries_bar` |
| `line` | `echarts_timeseries_line` |
| `area` | `echarts_area` |
| `histogram` | `histogram_v2` |
| `heatmap` | `heatmap_v2` |
| `bubble` | `bubble_v2` |
| `pivot_table` | `pivot_table_v2` |
| `sankey` | `sankey_v2` |
| `sunburst` | `sunburst_v2` |
| `treemap` | `treemap_v2` |
