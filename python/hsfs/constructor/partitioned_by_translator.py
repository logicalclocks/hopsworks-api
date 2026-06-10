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
"""Cross-engine partitioned_by predicate translation.

The point of materialising grain columns through the storage engine is that
users keep writing `fg.filter(fg.event_time >= last_week)` (or
`fg.filter(fg.year == 2026)`) like they would on any feature group, and
partition pruning just works.

The grain columns are ordinary materialized partition columns on both Delta
and Hudi (no Delta GENERATED expressions, no Hudi timestamp key generator),
so a filter on a grain column prunes natively on every engine, while an
event_time range prunes on none of them. The translation is therefore the
same for every (engine, format) combination: event_time range filters get
equivalent grain-column predicates added.

The translator runs on `Query.read()` just before SQL generation. It adds
equivalent predicates to the filter but never removes the original — the
row-level filter still produces correct results if the storage engine's
pruning is off.

Non-hierarchical specs (e.g. `["month"]` without year, or `["year","week"]`)
limit what can be translated; the translator falls back to a row-level
filter and emits a debug log line rather than producing incorrect ranges.
"""

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

from hsfs.constructor.filter import Filter, Logic


if TYPE_CHECKING:
    from hsfs import feature_group as fg_mod


_logger = logging.getLogger(__name__)


# Grains in hierarchical order; the translator can compose ranges over any
# strict left-prefix of this list.
_HIERARCHICAL_GRAINS = ("year", "month", "day", "hour")


def augment_filter(
    f: Filter | Logic | None,
    fg: fg_mod.FeatureGroup,
) -> Filter | Logic | None:
    """Return `f` augmented with partition predicates for grain-column pruning.

    Returns the input unchanged when:
      - `fg.partitioned_by` is None or empty, or
      - `fg.event_time` is None (no time anchor for the translation), or
      - the partitioned_by spec is not a strict left-prefix of
        `("year","month","day","hour")` — non-hierarchical specs are
        documented as not pruning on event_time ranges.

    Otherwise, returns the input AND'd with additional grain-column
    predicates equivalent to the event_time range in `f`, which every
    engine prunes on (the grain columns are real partition columns on
    both Delta and Hudi). The original predicate is kept so the
    row-level filter still produces correct results if pruning is off.
    """
    if f is None:
        return None
    if not getattr(fg, "partitioned_by", None):
        return f
    if not getattr(fg, "event_time", None):
        return f
    grains = list(fg.partitioned_by)
    if not _is_hierarchical_prefix(grains):
        _logger.debug(
            "partitioned_by spec %s is not a hierarchical year/month/day/hour "
            "prefix; skipping predicate translation. The filter will run at "
            "row level on this engine.",
            grains,
        )
        return f
    return _augment_event_time_to_derived(f, fg, grains)


def _is_hierarchical_prefix(grains: list[str]) -> bool:
    """True iff `grains` is a strict left-prefix of (year, month, day, hour)."""
    if not grains:
        return False
    return grains == list(_HIERARCHICAL_GRAINS[: len(grains)])


# --- Iteration over filter AST ------------------------------------------


def _collect_filters(node: Filter | Logic) -> list[Filter]:
    """Flatten a Filter/Logic tree into a list of leaf Filters.

    Only AND-connected filters are useful for translation — an OR introduces
    branches the translator can't safely tighten. The caller short-circuits
    on the presence of an OR node anywhere in the path; this helper assumes
    AND-only input.
    """
    if isinstance(node, Filter):
        return [node]
    if isinstance(node, Logic):
        out: list[Filter] = []
        if node._left_f is not None:
            out.extend(_collect_filters(node._left_f))
        if node._right_f is not None:
            out.extend(_collect_filters(node._right_f))
        if node._left_l is not None:
            out.extend(_collect_filters(node._left_l))
        if node._right_l is not None:
            out.extend(_collect_filters(node._right_l))
        return out
    return []


def _has_or(node: Filter | Logic | None) -> bool:
    if isinstance(node, Logic):
        if node._type == Logic.OR:
            return True
        return any(
            _has_or(child)
            for child in (node._left_f, node._right_f, node._left_l, node._right_l)
        )
    return False


# --- event_time → derived (Trino path) ----------------------------------


def _augment_event_time_to_derived(
    f: Filter | Logic, fg: fg_mod.FeatureGroup, grains: list[str]
) -> Filter | Logic:
    """Add grain predicates equivalent to an event_time range.

    Looks for AND-connected filters on `event_time` (>=, >, <, <=, ==) and
    appends equivalent predicates on the grain columns. Falls back to the
    original filter on OR'd expressions or anything else it can't safely
    interpret.
    """
    if _has_or(f):
        return f
    start, end_excl = _event_time_range(_collect_filters(f), fg.event_time)
    if start is None and end_excl is None:
        return f
    grain_preds = _grain_predicates_for_range(start, end_excl, fg, grains)
    return _and_all([f, *grain_preds])


def _event_time_range(
    filters: list[Filter], event_time: str
) -> tuple[datetime.datetime | None, datetime.datetime | None]:
    """Collapse the event_time filters in `filters` into an [start, end) range.

    Returns (None, None) when no event_time filters are present.
    Inequalities become a half-open range; equality becomes a singleton range
    `[ts, ts + 1µs)`.
    """
    start: datetime.datetime | None = None
    end_excl: datetime.datetime | None = None
    for filt in filters:
        if filt.feature.name != event_time:
            continue
        v = _coerce_datetime(filt.value)
        if v is None:
            continue
        if filt.condition == Filter.GE:
            start = v if start is None or v > start else start
        elif filt.condition == Filter.GT:
            v_plus = v + datetime.timedelta(microseconds=1)
            start = v_plus if start is None or v_plus > start else start
        elif filt.condition == Filter.LT:
            end_excl = v if end_excl is None or v < end_excl else end_excl
        elif filt.condition == Filter.LE:
            v_plus = v + datetime.timedelta(microseconds=1)
            end_excl = v_plus if end_excl is None or v_plus < end_excl else end_excl
        elif filt.condition == Filter.EQ:
            start = v if start is None or v > start else start
            v_plus = v + datetime.timedelta(microseconds=1)
            end_excl = v_plus if end_excl is None or v_plus < end_excl else end_excl
    return start, end_excl


def _coerce_datetime(value) -> datetime.datetime | None:
    """Best-effort conversion of a filter value to a naive-UTC datetime.

    The Filter stores `value` as either a string or a datetime depending on
    how it was constructed; both are reachable from the public API.
    Timezone-aware values are normalized to naive UTC so bounds from mixed
    sources stay mutually comparable (naive and aware datetimes raise
    TypeError on comparison).
    """
    dt = None
    if isinstance(value, datetime.datetime):
        dt = value
    elif isinstance(value, datetime.date):
        dt = datetime.datetime(value.year, value.month, value.day)
    elif isinstance(value, str):
        try:
            dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt is not None and dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return dt


def _grain_predicates_for_range(
    start: datetime.datetime | None,
    end_excl: datetime.datetime | None,
    fg: fg_mod.FeatureGroup,
    grains: list[str],
) -> list[Filter]:
    """Compute partition predicates equivalent to [start, end_excl) on event_time.

    The coarsest grain (always "year" for a hierarchical prefix) gets a
    `year >= START_Y AND year <= END_Y` pair, including for one-sided
    ranges. When both ends are known, finer grains get the same treatment
    level by level for as long as every coarser grain value is equal at
    both ends — `[2026-04-03, 2026-06-10)` also bounds `month` to [4, 6],
    but `[2026-11-x, 2027-02-x)` stops at the year bounds because a
    month-of-year interval cannot represent a range that crosses a year
    boundary.
    """
    if not grains:
        return []
    if start is None and end_excl is None:
        return []
    # end_excl is exclusive; end_excl - epsilon is the last possibly-matching
    # instant. For year, the last possibly-matching year is the year of
    # (end_excl - microsecond), which handles both end_excl=2027-01-01
    # (last year = 2026) and end_excl=2027-06-15 (last year = 2027).
    boundary = (
        end_excl - datetime.timedelta(microseconds=1) if end_excl is not None else None
    )
    extractors = [_grain_extractor(g) for g in grains]
    preds: list[Filter] = []
    feat = fg.get_feature(grains[0])
    if start is not None:
        preds.append(Filter(feat, Filter.GE, extractors[0](start)))
    if boundary is not None:
        preds.append(Filter(feat, Filter.LE, extractors[0](boundary)))
    if start is None or boundary is None:
        return preds
    for level in range(1, len(grains)):
        # A finer-grain interval is only valid while every coarser grain
        # value is identical at both ends of the range.
        if any(extractors[i](start) != extractors[i](boundary) for i in range(level)):
            break
        feat = fg.get_feature(grains[level])
        preds.append(Filter(feat, Filter.GE, extractors[level](start)))
        preds.append(Filter(feat, Filter.LE, extractors[level](boundary)))
    return preds


# --- Grain extractors and AND assembly ----------------------------------


def _grain_extractor(grain: str):
    if grain == "year":
        return lambda dt: dt.year
    if grain == "month":
        return lambda dt: dt.month
    if grain == "day":
        return lambda dt: dt.day
    if grain == "hour":
        return lambda dt: dt.hour
    raise ValueError(f"Unsupported grain: {grain}")


def _and_all(parts: list[Filter | Logic | None]) -> Filter | Logic:
    parts = [p for p in parts if p is not None]
    if not parts:
        raise ValueError("Cannot AND an empty filter list")
    result = parts[0]
    for nxt in parts[1:]:
        if isinstance(result, Filter):
            if isinstance(nxt, Filter):
                result = Logic._And(left_f=result, right_f=nxt)
            else:
                result = Logic._And(left_f=result, right_l=nxt)
        else:  # Logic
            if isinstance(nxt, Filter):
                result = Logic._And(left_l=result, right_f=nxt)
            else:
                result = Logic._And(left_l=result, right_l=nxt)
    return result
