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

from unittest.mock import Mock

from hsfs import feature
from hsfs.constructor.filter import Filter, Logic
from hsfs.constructor.partitioned_by_translator import augment_filter


def _fake_fg(
    partitioned_by=None,
    event_time="ts",
    time_travel_format="DELTA",
):
    """Build a Mock that exposes only what augment_filter reads."""
    fg = Mock()
    fg.partitioned_by = partitioned_by
    fg.event_time = event_time
    fg.time_travel_format = time_travel_format
    fg.get_feature = lambda name: feature.Feature(name)
    return fg


def _flat_filters(node):
    """Collect leaf Filter objects from a Filter/Logic AST."""
    if isinstance(node, Filter):
        return [node]
    out = []
    for child in (node._left_f, node._right_f, node._left_l, node._right_l):
        if child is not None:
            out.extend(_flat_filters(child))
    return out


def _has_predicate(node, feature_name, condition, value=None):
    for f in _flat_filters(node):
        if f.feature.name != feature_name or f.condition != condition:
            continue
        if value is None or f.value == value:
            return True
    return False


# region — no-op paths


class TestNoOp:
    def test_returns_input_when_partitioned_by_missing(self):
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        out = augment_filter(f, _fake_fg(partitioned_by=None))
        assert out is f

    def test_returns_input_when_event_time_missing(self):
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        fg = _fake_fg(partitioned_by=["year"], event_time=None)
        out = augment_filter(f, fg)
        assert out is f

    def test_non_hierarchical_falls_back(self):
        # ["month"] without year is non-hierarchical — the translator should
        # leave the filter unchanged rather than producing incorrect bounds.
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        fg = _fake_fg(partitioned_by=["month"], time_travel_format="DELTA")
        out = augment_filter(f, fg)
        assert out is f

    def test_grain_filter_passes_through(self):
        # A filter on a grain column already prunes natively (the grain
        # columns are real partition columns on both formats); no event_time
        # predicate is added.
        f = Filter(feature.Feature("year"), Filter.EQ, 2026)
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="HUDI")
        out = augment_filter(f, fg)
        assert out is f

    def test_or_short_circuits(self):
        # OR'd filters can't be safely tightened without producing incorrect
        # ranges; translator must return the input unchanged.
        f = Logic._Or(
            left_f=Filter(feature.Feature("ts"), Filter.GE, "2026-01-01"),
            right_f=Filter(feature.Feature("ts"), Filter.LT, "2025-01-01"),
        )
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="DELTA")
        out = augment_filter(f, fg)
        assert out is f


# endregion


# region — event_time → derived (all engines, both formats)


class TestEventTimeToDerived:
    def test_year_lower_bound_delta(self):
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="DELTA")
        out = augment_filter(f, fg)
        # Original GE on ts still present
        assert _has_predicate(out, "ts", Filter.GE)
        # And a year >= 2026 added
        assert _has_predicate(out, "year", Filter.GE, 2026)

    def test_year_range_hudi(self):
        f = Logic._And(
            left_f=Filter(feature.Feature("ts"), Filter.GE, "2026-01-01"),
            right_f=Filter(feature.Feature("ts"), Filter.LT, "2027-01-01"),
        )
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="HUDI")
        out = augment_filter(f, fg)
        # Both original predicates preserved
        assert _has_predicate(out, "ts", Filter.GE)
        assert _has_predicate(out, "ts", Filter.LT)
        # year >= 2026 AND year <= 2026 added (since end_excl=2027-01-01,
        # last possibly-matching year is 2026).
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert _has_predicate(out, "year", Filter.LE, 2026)
        # Same year at both ends, so month bounds are emitted too (the full
        # [1, 12] interval for a whole-year range).
        assert _has_predicate(out, "month", Filter.GE, 1)
        assert _has_predicate(out, "month", Filter.LE, 12)

    def test_same_year_range_bounds_month(self):
        f = Logic._And(
            left_f=Filter(feature.Feature("ts"), Filter.GE, "2026-04-03"),
            right_f=Filter(feature.Feature("ts"), Filter.LT, "2026-06-10"),
        )
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="DELTA")
        out = augment_filter(f, fg)
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert _has_predicate(out, "year", Filter.LE, 2026)
        assert _has_predicate(out, "month", Filter.GE, 4)
        assert _has_predicate(out, "month", Filter.LE, 6)

    def test_cross_year_range_stops_at_year_bounds(self):
        # [2026-11, 2027-02) — a month-of-year interval cannot represent a
        # range that crosses a year boundary, so only year bounds appear.
        f = Logic._And(
            left_f=Filter(feature.Feature("ts"), Filter.GE, "2026-11-01"),
            right_f=Filter(feature.Feature("ts"), Filter.LT, "2027-02-01"),
        )
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="DELTA")
        out = augment_filter(f, fg)
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert _has_predicate(out, "year", Filter.LE, 2027)
        assert not _has_predicate(out, "month", Filter.GE)
        assert not _has_predicate(out, "month", Filter.LE)

    def test_one_sided_range_bounds_year_only(self):
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-04-03")
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="DELTA")
        out = augment_filter(f, fg)
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert not _has_predicate(out, "month", Filter.GE)

    def test_event_time_equality(self):
        f = Filter(feature.Feature("ts"), Filter.EQ, "2026-04-12T00:00:00")
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="HUDI")
        out = augment_filter(f, fg)
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert _has_predicate(out, "year", Filter.LE, 2026)

    def test_timezone_aware_and_naive_bounds_mix(self):
        # Aware values are normalized to naive UTC, so mixing an aware string
        # bound with a naive one must not raise on comparison.
        f = Logic._And(
            left_f=Filter(feature.Feature("ts"), Filter.GE, "2026-01-01T00:00:00Z"),
            right_f=Filter(feature.Feature("ts"), Filter.LT, "2027-01-01T00:00:00"),
        )
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="DELTA")
        out = augment_filter(f, fg)
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert _has_predicate(out, "year", Filter.LE, 2026)


# endregion
