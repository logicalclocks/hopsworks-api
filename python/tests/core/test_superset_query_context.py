#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""The query_context builder against the Superset 6.0.0 frontend.

Each golden query_context in superset_query_context_fixtures.json is what the
Superset 6.0.0 frontend code returns for the case's params: the plugins'
buildQuery.ts, superset-ui-core/src/query and the chart-controls operators at
tag 6.0.0, bundled with esbuild and run in node through the explore save path
(buildV1ChartDataPayload), then passed through JSON.stringify.
tests/fixtures/generate_superset_query_context_fixtures.py regenerates it from a
Superset tag. Superset's Python
port (superset/migrations/shared/migrate_viz) lags that frontend: it omits
group_others_when_limit_reached and isColumnReference and has the older rename
operator, so it does not serve as the golden.
"""

import json
from pathlib import Path

import pytest
from hopsworks_common.core.superset_query_context import build_query_context


GOLDEN = json.loads(
    (
        Path(__file__).parent.parent
        / "fixtures"
        / "superset_query_context_fixtures.json"
    ).read_text()
)
DOCUMENTED_VIZ_TYPES = {
    "big_number_total",
    "big_number",
    "pie",
    "echarts_timeseries_bar",
    "echarts_timeseries_line",
    "histogram_v2",
    "table",
    "pivot_table_v2",
    "heatmap_v2",
    "bubble_v2",
    "box_plot",
    "funnel",
}


def test_every_documented_viz_type_has_a_golden():
    assert {case["viz_type"] for case in GOLDEN.values()} >= DOCUMENTED_VIZ_TYPES


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_query_context_matches_the_superset_frontend(name):
    case = GOLDEN[name]
    built = build_query_context(7, case["viz_type"], case["params"])
    # Text, not dicts: as dicts 0 == False and 1 == True.
    assert json.dumps(built, sort_keys=True) == json.dumps(
        case["query_context"], sort_keys=True
    )


def test_the_params_are_not_mutated():
    params = GOLDEN["echarts_timeseries_line__compare_difference"]["params"]
    before = json.dumps(params)
    build_query_context(7, "echarts_timeseries_line", params)
    assert json.dumps(params) == before


def test_a_query_datasource_keeps_its_type():
    built = build_query_context(9, "big_number_total", {"metric": "count"}, "query")
    assert built["datasource"] == {"id": 9, "type": "query"}
    assert built["form_data"]["datasource"] == "9__query"


def test_an_unsupported_whisker_option_is_refused_as_in_the_frontend():
    with pytest.raises(ValueError, match="Unsupported whisker type: Min/max"):
        build_query_context(
            7, "box_plot", {"metrics": ["count"], "whiskerOptions": "Min/max"}
        )
