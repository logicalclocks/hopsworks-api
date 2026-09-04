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

import pytest
from hsfs.core import statistics_api


class TestBuildGetQueryParams:
    """FSTORE-2106: event-time window filters/sorts on StatisticsApi._build_get_query_params.

    Mirrors the pre-existing commit-time bounds so the two families stay symmetric.
    """

    def _api(self):
        return statistics_api.StatisticsApi(
            feature_store_id=99, entity_type="featuregroup"
        )

    def test_commit_time_bounds_unaffected(self):
        api = self._api()
        params = api._build_get_query_params(
            start_commit_time=1_700_000_000_000,
            end_commit_time=1_700_086_400_000,
            filter_eq_times=True,
        )

        assert "window_end_commit_time_eq:1700086400000" in params["filter_by"]
        assert "window_start_commit_time_eq:1700000000000" in params["filter_by"]
        assert "window_end_commit_time:desc" in params["sort_by"]
        assert "window_start_commit_time:asc" in params["sort_by"]
        assert "computation_time:desc" not in params["sort_by"]

    def test_event_time_bounds_emit_eq_filters_and_sorts(self):
        api = self._api()
        params = api._build_get_query_params(
            start_event_time=1_700_000_000_000,
            end_event_time=1_700_086_400_000,
            event_time="datetime",
            filter_eq_times=True,
        )

        assert "window_end_event_time_eq:1700086400000" in params["filter_by"]
        assert "window_start_event_time_eq:1700000000000" in params["filter_by"]
        assert "event_time_eq:datetime" in params["filter_by"]
        assert "window_end_event_time:desc" in params["sort_by"]
        assert "window_start_event_time:asc" in params["sort_by"]
        # computation_time is only appended as a fallback sort when no window bounds
        # (commit or event time) are given.
        assert "computation_time:desc" not in params["sort_by"]

    def test_event_time_bounds_emit_range_filters_when_not_eq(self):
        api = self._api()
        params = api._build_get_query_params(
            start_event_time=1_700_000_000_000,
            end_event_time=1_700_086_400_000,
            filter_eq_times=False,
        )

        assert "window_end_event_time_ltoeq:1700086400000" in params["filter_by"]
        assert "window_start_event_time_gtoeq:1700000000000" in params["filter_by"]

    def test_commit_and_event_time_bounds_not_sent_together(self):
        # Callers pass one family or the other (StatisticsEngine._get_by_time_window),
        # but the query builder itself must not conflate them when only one is given.
        api = self._api()
        params = api._build_get_query_params(
            start_event_time=1_700_000_000_000,
            end_event_time=1_700_086_400_000,
            event_time="datetime",
            filter_eq_times=True,
        )

        assert not any("commit_time" in f for f in params["filter_by"])

    def test_commit_and_event_time_bounds_together_raise(self):
        api = self._api()
        with pytest.raises(ValueError, match="cannot be combined"):
            api._build_get_query_params(
                start_commit_time=1, end_commit_time=2, event_time="datetime"
            )

    def test_no_window_bounds_falls_back_to_computation_time_sort(self):
        api = self._api()
        params = api._build_get_query_params()

        assert params["sort_by"] == ["computation_time:desc"]
        assert "filter_by" not in params
