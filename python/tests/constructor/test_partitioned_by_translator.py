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

import datetime
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
        out = augment_filter(f, _fake_fg(partitioned_by=None), engine_type="python")
        assert out is f

    def test_returns_input_when_event_time_missing(self):
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        fg = _fake_fg(partitioned_by=["year"], event_time=None)
        out = augment_filter(f, fg, engine_type="python")
        assert out is f

    def test_spark_delta_translates(self):
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="DELTA")
        # Grain columns are real partition columns (no Delta GENERATED
        # expressions), so an event_time range does not prune on its own.
        # The translator adds grain predicates the engine prunes on — for
        # Spark+Delta the same as the Trino/ArrowFlight path.
        out = augment_filter(f, fg, engine_type="spark")
        assert _has_predicate(out, "ts", Filter.GE)
        assert _has_predicate(out, "year", Filter.GE, 2026)

    def test_non_hierarchical_falls_back(self):
        # ["month"] without year is non-hierarchical — the translator should
        # leave the filter unchanged rather than producing incorrect bounds.
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        fg = _fake_fg(partitioned_by=["month"], time_travel_format="DELTA")
        out = augment_filter(f, fg, engine_type="python")
        assert out is f


# endregion


# region — event_time → derived (Trino / python-engine path)


class TestEventTimeToDerived:
    def test_year_lower_bound(self):
        f = Filter(feature.Feature("ts"), Filter.GE, "2026-01-01")
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="DELTA")
        out = augment_filter(f, fg, engine_type="python")
        # Original GE on ts still present
        assert _has_predicate(out, "ts", Filter.GE)
        # And a year >= 2026 added
        assert _has_predicate(out, "year", Filter.GE, 2026)

    def test_year_range(self):
        f = Logic._And(
            left_f=Filter(feature.Feature("ts"), Filter.GE, "2026-01-01"),
            right_f=Filter(feature.Feature("ts"), Filter.LT, "2027-01-01"),
        )
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="HUDI")
        out = augment_filter(f, fg, engine_type="python")
        # Both original predicates preserved
        assert _has_predicate(out, "ts", Filter.GE)
        assert _has_predicate(out, "ts", Filter.LT)
        # year >= 2026 AND year <= 2026 added (since end_excl=2027-01-01,
        # last possibly-matching year is 2026).
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert _has_predicate(out, "year", Filter.LE, 2026)

    def test_event_time_equality(self):
        f = Filter(feature.Feature("ts"), Filter.EQ, "2026-04-12T00:00:00")
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="HUDI")
        out = augment_filter(f, fg, engine_type="python")
        assert _has_predicate(out, "year", Filter.GE, 2026)
        assert _has_predicate(out, "year", Filter.LE, 2026)

    def test_or_short_circuits(self):
        # OR'd filters can't be safely tightened without producing incorrect
        # ranges; translator must return the input unchanged.
        f = Logic._Or(
            left_f=Filter(feature.Feature("ts"), Filter.GE, "2026-01-01"),
            right_f=Filter(feature.Feature("ts"), Filter.LT, "2025-01-01"),
        )
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="DELTA")
        out = augment_filter(f, fg, engine_type="python")
        assert out is f


# endregion


# region — derived → event_time (Spark+Hudi path)


class TestDerivedToEventTime:
    def test_year_only_equality(self):
        f = Filter(feature.Feature("year"), Filter.EQ, 2026)
        fg = _fake_fg(partitioned_by=["year"], time_travel_format="HUDI")
        out = augment_filter(f, fg, engine_type="spark")
        # Original year predicate kept
        assert _has_predicate(out, "year", Filter.EQ, 2026)
        # event_time range added: [2026-01-01, 2027-01-01)
        assert _has_predicate(
            out, "ts", Filter.GE, datetime.datetime(2026, 1, 1).isoformat()
        )
        assert _has_predicate(
            out, "ts", Filter.LT, datetime.datetime(2027, 1, 1).isoformat()
        )

    def test_year_and_month_equality(self):
        f = Logic._And(
            left_f=Filter(feature.Feature("year"), Filter.EQ, 2026),
            right_f=Filter(feature.Feature("month"), Filter.EQ, 4),
        )
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="HUDI")
        out = augment_filter(f, fg, engine_type="spark")
        # event_time range: [2026-04-01, 2026-05-01)
        assert _has_predicate(
            out, "ts", Filter.GE, datetime.datetime(2026, 4, 1).isoformat()
        )
        assert _has_predicate(
            out, "ts", Filter.LT, datetime.datetime(2026, 5, 1).isoformat()
        )

    def test_year_month_december_rolls_over_year(self):
        f = Logic._And(
            left_f=Filter(feature.Feature("year"), Filter.EQ, 2026),
            right_f=Filter(feature.Feature("month"), Filter.EQ, 12),
        )
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="HUDI")
        out = augment_filter(f, fg, engine_type="spark")
        # [2026-12-01, 2027-01-01) — month + 1 with year carry
        assert _has_predicate(
            out, "ts", Filter.LT, datetime.datetime(2027, 1, 1).isoformat()
        )

    def test_month_without_year_no_translation(self):
        # Month alone is ambiguous across years; the translator should leave
        # the filter as-is and let the row-level filter handle it.
        f = Filter(feature.Feature("month"), Filter.EQ, 4)
        fg = _fake_fg(partitioned_by=["year", "month"], time_travel_format="HUDI")
        out = augment_filter(f, fg, engine_type="spark")
        # No event_time predicate added
        assert not _has_predicate(out, "ts", Filter.GE)
        assert not _has_predicate(out, "ts", Filter.LT)


# endregion
