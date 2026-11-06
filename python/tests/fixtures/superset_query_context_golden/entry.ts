import bigNumberTotal from 'superset-frontend/plugins/plugin-chart-echarts/src/BigNumber/BigNumberTotal/buildQuery';
import bigNumber from 'superset-frontend/plugins/plugin-chart-echarts/src/BigNumber/BigNumberWithTrendline/buildQuery';
import pie from 'superset-frontend/plugins/plugin-chart-echarts/src/Pie/buildQuery';
import timeseries from 'superset-frontend/plugins/plugin-chart-echarts/src/Timeseries/buildQuery';
import histogram from 'superset-frontend/plugins/plugin-chart-echarts/src/Histogram/buildQuery';
import table from 'superset-frontend/plugins/plugin-chart-table/src/buildQuery';
import pivot from 'superset-frontend/plugins/plugin-chart-pivot-table/src/plugin/buildQuery';
import heatmap from 'superset-frontend/plugins/plugin-chart-echarts/src/Heatmap/buildQuery';
import bubble from 'superset-frontend/plugins/plugin-chart-echarts/src/Bubble/buildQuery';
import boxPlot from 'superset-frontend/plugins/plugin-chart-echarts/src/BoxPlot/buildQuery';
import funnel from 'superset-frontend/plugins/plugin-chart-echarts/src/Funnel/buildQuery';
import { buildQueryContext } from '@superset-ui/core';

// viz_type -> buildQuery, as registered by the 6.0.0 MainPreset.
const registry: Record<string, any> = {
  big_number_total: bigNumberTotal, big_number: bigNumber, pie,
  echarts_timeseries_bar: timeseries, echarts_timeseries_line: timeseries,
  histogram_v2: histogram, table, pivot_table_v2: pivot, heatmap_v2: heatmap,
  bubble_v2: bubble, box_plot: boxPlot, funnel,
};

// buildV1ChartDataPayload, as the save flow calls it. Adapted from Apache Superset
// superset-frontend/src/explore/exploreUtils/index.js at tag 6.0.0, licensed to the
// Apache Software Foundation (ASF) under the Apache License, Version 2.0.
function buildV1ChartDataPayload(formData: any) {
  const buildQuery = registry[formData.viz_type] ??
    ((fd: any) => buildQueryContext(fd, (base: any) => [{ ...base }]));
  return buildQuery(
    { ...formData, force: false, result_format: 'json', result_type: 'full' },
    { ownState: null, hooks: { setDataMask: null } },
  );
}

const input = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const out: Record<string, any> = {};
for (const [name, fd] of Object.entries(input)) {
  out[name] = JSON.parse(JSON.stringify(buildV1ChartDataPayload(fd)));
}
process.stdout.write(JSON.stringify(out, null, 2));
