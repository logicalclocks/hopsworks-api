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

That promise holds natively for Spark+Delta (the GENERATED expressions let
Delta auto-derive partition predicates from event_time filters and vice
versa), but the three other (engine, format) combinations need explicit
predicate translation:

| Engine | Format | Translation direction                        |
|--------|--------|----------------------------------------------|
| Spark  | Delta  | none (Delta auto-derives)                    |
| Spark  | Hudi   | derived → event_time                         |
| Trino  | Delta  | event_time → derived                         |
| Trino  | Hudi   | event_time → derived                         |

The translator runs on `Query.read()` just before SQL generation. It adds
equivalent predicates to the filter but never removes the original — the
row-level filter still produces correct results if the storage engine's
pruning is off.

Non-hierarchical specs (e.g. `["month"]` without year, or `["year","week"]`)
limit what can be translated; the translator falls back to a row-level
filter and emits a one-line warning rather than producing incorrect ranges.
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
    engine_type: str,
) -> Filter | Logic | None:
    """Return `f` augmented with engine-appropriate partition predicates.

    Returns the input unchanged when:
      - `fg.partitioned_by` is None or empty, or
      - `fg.event_time` is None (no time anchor for the translation), or
      - the combination of `engine_type` and `fg.time_travel_format` does
        not need translation (Spark + Delta auto-derives natively), or
      - the partitioned_by spec is not a strict left-prefix of
        `("year","month","day","hour")` — non-hierarchical specs are
        documented as not partition-pruning-aware on the asymmetric paths.

    Otherwise, returns the input AND'd with an additional predicate that
    the storage engine can use for partition pruning. The original
    predicate is kept so the row-level filter still produces correct
    results if pruning is off.
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
            "prefix; skipping cross-engine predicate translation. The filter "
            "will run at row level on this engine.",
            grains,
        )
        return f

    fmt = (getattr(fg, "time_travel_format", None) or "").upper()
    engine = (engine_type or "").lower()

    if engine == "spark" and fmt == "DELTA":
        return f  # Delta auto-derives both directions; nothing to add.
    if engine == "spark" and fmt == "HUDI":
        return _augment_derived_to_event_time(f, fg, grains)
    if engine == "python":  # python engine → Trino reads
        return _augment_event_time_to_derived(f, fg, grains)
    return f


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
    """Best-effort conversion of a filter value to a datetime.

    The Filter stores `value` as either a string or a datetime depending on
    how it was constructed; both are reachable from the public API.
    """
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _grain_predicates_for_range(
    start: datetime.datetime | None,
    end_excl: datetime.datetime | None,
    fg: fg_mod.FeatureGroup,
    grains: list[str],
) -> list[Filter]:
    """Compute partition predicates equivalent to [start, end_excl) on event_time.

    Strategy: for the coarsest grain in `grains` (always "year" for a
    hierarchical prefix), produce a `year >= START_Y AND year <= END_Y`
    pair. Finer grains are only safe to bound when the range is narrow
    enough that the lower-grain bound is also derivable; the translator
    keeps things simple by emitting just the coarsest-grain bounds, which
    is enough to drop entire partition trees.
    """
    if not grains:
        return []
    if start is None and end_excl is None:
        return []
    grain = grains[0]  # "year" for hierarchical prefixes
    extractor = _grain_extractor(grain)
    feat = fg.get_feature(grain)
    preds: list[Filter] = []
    if start is not None:
        preds.append(Filter(feat, Filter.GE, extractor(start)))
    if end_excl is not None:
        # end_excl is exclusive; end_excl - epsilon is the last possibly-
        # matching value. For year, the last possibly-matching year is the
        # year of (end_excl - microsecond). This handles both
        # end_excl=2027-01-01 (last year = 2026) and end_excl=2027-06-15
        # (last year = 2027).
        boundary = end_excl - datetime.timedelta(microseconds=1)
        preds.append(Filter(feat, Filter.LE, extractor(boundary)))
    return preds


# --- derived → event_time (Spark+Hudi path) -----------------------------


def _augment_derived_to_event_time(
    f: Filter | Logic, fg: fg_mod.FeatureGroup, grains: list[str]
) -> Filter | Logic:
    """Add an event_time range equivalent to grain predicates."""
    if _has_or(f):
        return f
    grain_values = _collect_grain_equalities(_collect_filters(f), grains)
    if not grain_values:
        return f
    # Only the most specific contiguous hierarchical prefix is translated.
    # If year=2026 alone is set, range is [2026-01-01, 2027-01-01).
    # If year=2026 AND month=4, range is [2026-04-01, 2026-05-01).
    prefix_len = 0
    for g in grains:
        if g in grain_values:
            prefix_len += 1
        else:
            break
    if prefix_len == 0:
        return f
    start, end_excl = _hierarchical_range(grain_values, grains[:prefix_len])
    if start is None:
        return f
    et_feat = fg.get_feature(fg.event_time)
    return _and_all(
        [
            f,
            Filter(et_feat, Filter.GE, start.isoformat()),
            Filter(et_feat, Filter.LT, end_excl.isoformat()),
        ]
    )


def _collect_grain_equalities(
    filters: list[Filter], grains: list[str]
) -> dict[str, int]:
    """Collect simple equality predicates on grain columns.

    Inequalities (e.g. `year >= 2026`) and IN-lists are intentionally
    skipped — they're harder to convert into a clean event_time range
    and the user can typically rewrite as an event_time predicate
    directly when they care about pruning.
    """
    out: dict[str, int] = {}
    for filt in filters:
        if filt.feature.name not in grains:
            continue
        if filt.condition != Filter.EQ:
            continue
        try:
            out[filt.feature.name] = int(filt.value)
        except (TypeError, ValueError):
            continue
    return out


def _hierarchical_range(
    values: dict[str, int], grain_prefix: list[str]
) -> tuple[datetime.datetime | None, datetime.datetime | None]:
    """Build [start, end_excl) for a hierarchical-prefix equality combo."""
    year = values.get("year")
    if year is None:
        return None, None
    month = values.get("month", 1)
    day = values.get("day", 1)
    hour = values.get("hour", 0)
    start = datetime.datetime(year, month, day, hour, 0, 0)
    end_excl = _add_one_unit(start, grain_prefix[-1])
    return start, end_excl


def _add_one_unit(ts: datetime.datetime, finest_grain: str) -> datetime.datetime:
    if finest_grain == "year":
        return ts.replace(year=ts.year + 1)
    if finest_grain == "month":
        if ts.month == 12:
            return ts.replace(year=ts.year + 1, month=1)
        return ts.replace(month=ts.month + 1)
    if finest_grain == "day":
        return ts + datetime.timedelta(days=1)
    if finest_grain == "hour":
        return ts + datetime.timedelta(hours=1)
    return ts


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
                result = Logic.And(left_f=result, right_f=nxt)
            else:
                result = Logic.And(left_f=result, right_l=nxt)
        else:  # Logic
            if isinstance(nxt, Filter):
                result = Logic.And(left_l=result, right_f=nxt)
            else:
                result = Logic.And(left_l=result, right_l=nxt)
    return result
