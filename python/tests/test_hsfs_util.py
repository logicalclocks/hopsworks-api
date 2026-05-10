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

import logging
from unittest.mock import patch

from hsfs import feature, util
from hsfs.constructor import filter as filter_mod


class TestApplyDataIntervalDefaults:
    """hsfs.util.apply_scheduler_time_defaults falls back to HOPS_* env vars."""

    def test_explicit_values_win(self, capsys):
        # Even if the env vars are set, explicit args take precedence — no notice printed.
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }
        with patch.dict("os.environ", env, clear=False):
            start, end = util.apply_scheduler_time_defaults("2024-06-01", "2024-07-01")
        assert start == "2024-06-01"
        assert end == "2024-07-01"
        assert capsys.readouterr().out == ""

    def test_fills_both_from_env_when_unset(self, caplog):
        # The notice is emitted via _logger.info (deliberately not stdout, so
        # SDK / CLI --json callers don't get a surprise line) — capture it
        # through caplog instead of capsys.
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }
        with (
            caplog.at_level(logging.INFO, logger="hsfs.util"),
            patch.dict("os.environ", env, clear=False),
        ):
            start, end = util.apply_scheduler_time_defaults(None, None)
        assert start == "2026-01-01T00:00:00Z"
        assert end == "2026-02-01T00:00:00Z"
        msg = caplog.text
        assert "Using scheduler-injected data interval:" in msg
        assert "start_time=2026-01-01T00:00:00Z" in msg
        assert "end_time=2026-02-01T00:00:00Z" in msg

    def test_fills_only_missing_side(self, caplog):
        # Only end_time is missing — start_time passes through, end_time is filled.
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }
        with (
            caplog.at_level(logging.INFO, logger="hsfs.util"),
            patch.dict("os.environ", env, clear=False),
        ):
            start, end = util.apply_scheduler_time_defaults("2024-06-01", None)
        assert start == "2024-06-01"
        assert end == "2026-02-01T00:00:00Z"
        msg = caplog.text
        assert "end_time=2026-02-01T00:00:00Z" in msg
        assert "start_time" not in msg

    def test_returns_none_when_no_env_and_no_args(self, capsys):
        # Neither env vars nor explicit args: stay None and print nothing.
        import os

        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("HOPS_START_TIME", None)
            os.environ.pop("HOPS_END_TIME", None)
            start, end = util.apply_scheduler_time_defaults(None, None)
        assert start is None
        assert end is None
        assert capsys.readouterr().out == ""

    def test_empty_env_string_is_ignored(self, capsys):
        # Empty env var must not override None, and must not print a notice.
        env = {"HOPS_START_TIME": "", "HOPS_END_TIME": ""}
        with patch.dict("os.environ", env, clear=False):
            start, end = util.apply_scheduler_time_defaults(None, None)
        assert start is None
        assert end is None
        assert capsys.readouterr().out == ""


class TestBuildTimeFilter:
    """hsfs.util.build_time_filter assembles the half-open `[start, end)` window.

    The start-side boundary changed from `>` (exclusive) to `>=` (inclusive) so that
    back-to-back scheduled windows `[t0, t1)` then `[t1, t2)` tile the timeline: an
    event at exactly `t1` is read by the second window only, never by both (duplicate)
    and never by neither (dropped). These tests pin that invariant at the filter level
    — the engine translates `>=` and `<` into SQL predicates, and this is the layer we
    control in the hsfs client.
    """

    @staticmethod
    def _event_time():
        # Use `bigint` so Feature._get_filter_value doesn't timestamp-encode the int
        # value into a formatted string — the operator semantics we're asserting are
        # independent of timestamp coercion, so a numeric column keeps the tests
        # focused on `>=` vs `<` boundary behaviour.
        return feature.Feature(name="event_time", type="bigint")

    def test_start_time_uses_greater_than_or_equal(self):
        # Inclusive lower bound: boundary events at exactly start_time are matched by
        # the filter. Previously this was `GT` (exclusive), which silently dropped any
        # event landing precisely on a scheduled-fire boundary.
        f = util.build_time_filter(self._event_time(), 1000, None)
        assert isinstance(f, filter_mod.Filter)
        assert f.condition == filter_mod.Filter.GE
        assert f.condition != filter_mod.Filter.GT
        assert str(f.value) == "1000"

    def test_end_time_uses_less_than(self):
        # Exclusive upper bound: a boundary event at exactly end_time is NOT matched
        # — it belongs to the next window's [end_time, ...) slice.
        f = util.build_time_filter(self._event_time(), None, 2000)
        assert isinstance(f, filter_mod.Filter)
        assert f.condition == filter_mod.Filter.LT
        assert str(f.value) == "2000"

    def test_combined_half_open_interval(self):
        # Both sides present → AND of `>= start` and `< end`. Verifies the operator
        # composition as well as the two boundary semantics together.
        logic = util.build_time_filter(self._event_time(), 1000, 2000)
        assert isinstance(logic, filter_mod.Logic)
        assert logic.type == filter_mod.Logic.AND
        left = logic.get_left_filter_or_logic()
        right = logic.get_right_filter_or_logic()
        assert left.condition == filter_mod.Filter.GE
        assert str(left.value) == "1000"
        assert right.condition == filter_mod.Filter.LT
        assert str(right.value) == "2000"

    def test_no_bounds_returns_none(self):
        # Neither side provided: no filter. Caller reads all rows.
        assert util.build_time_filter(self._event_time(), None, None) is None

    def test_consecutive_windows_boundary_belongs_to_second(self):
        # The core tiling property: windows [t0, t1) and [t1, t2) share the boundary
        # `t1`. With the new inclusive-start / exclusive-end semantics, an event whose
        # event_time is exactly `t1`:
        #   - is NOT matched by the first window  (condition < t1 is false at t1)
        #   - IS matched by the second window     (condition >= t1 is true at t1)
        # Assert this at the filter-condition level so we don't need a live backend.
        w1 = util.build_time_filter(self._event_time(), 0, 1000)
        w2 = util.build_time_filter(self._event_time(), 1000, 2000)

        # Window 1 ends exclusive at 1000.
        w1_right = w1.get_right_filter_or_logic()
        assert w1_right.condition == filter_mod.Filter.LT
        assert str(w1_right.value) == "1000"

        # Window 2 starts inclusive at 1000 — same boundary value, different operator.
        w2_left = w2.get_left_filter_or_logic()
        assert w2_left.condition == filter_mod.Filter.GE
        assert str(w2_left.value) == "1000"
