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
"""Parsing and per-format validation of `partitioned_by` transform expressions.

Each element of `partitioned_by` is one partition transform expression:
`identity(col)` (or a bare column name), `bucket(N, col)`, `truncate(W, col)`,
`year(col)`, `month(col)`, `week(col)`, `day(col)`, `hour(col)`, or `void(col)`.
`partitioned_by` describes native partition specifications only: Iceberg
compiles the list into its `PartitionSpec` (hidden partitioning) and Hudi
materializes identity columns and temporal grains as hive-style partitions.
Delta rejects it (Delta has no partition transforms; liquid clustering goes
through `clustered_by`), and the Hudi bucket index goes through
`bucket_index` rather than a transform.

The backend runs the authoritative validation against the final schema; the
checks here fast-fail the common mistakes before the REST call.
"""

from __future__ import annotations

import dataclasses
import re

from hopsworks_common.client.exceptions import FeatureStoreException


IDENTITY = "identity"
BUCKET = "bucket"
TRUNCATE = "truncate"
VOID = "void"
TEMPORAL_TRANSFORMS = ("year", "month", "week", "day", "hour")
ALL_TRANSFORMS = (IDENTITY, BUCKET, TRUNCATE, VOID, *TEMPORAL_TRANSFORMS)

# Transforms taking an integer parameter before the column.
_PARAMETERIZED = (BUCKET, TRUNCATE)

# Per-format transform whitelist: only native partition specifications.
# Delta has no transform partitioning (identity partitions go through
# partition_key, liquid clustering through clustered_by), and the Hudi
# bucket index goes through bucket_index, so neither appears here.
ALLOWED_TRANSFORMS = {
    "ICEBERG": (IDENTITY, BUCKET, TRUNCATE, VOID, "year", "month", "day", "hour"),
    "HUDI": (IDENTITY, *TEMPORAL_TRANSFORMS),
}

# Delta liquid clustering caps clustering columns at 4; zorder_by adopts the
# same cap (z-order effectiveness also degrades beyond a handful of columns).
MAX_CLUSTER_COLUMNS = 4

_EXPR_RE = re.compile(r"^\s*(\w+)\s*\(\s*([^()]*?)\s*\)\s*$")
# Optional partition-field alias, e.g. "bucket(16, id) as shard".
_ALIAS_RE = re.compile(r"^(?P<expr>.*?)\s+as\s+(?P<alias>\w+)\s*$", re.IGNORECASE)


@dataclasses.dataclass(frozen=True)
class PartitionTransform:
    """One parsed `partitioned_by` element: a transform applied to a source column.

    `alias` names the resulting partition field (Iceberg metadata tables and
    evolution refer to fields by name); the format's default name applies
    when unset.
    """

    name: str
    source: str
    param: int | None = None
    alias: str | None = None

    def __str__(self) -> str:
        if self.param is not None:
            base = f"{self.name}({self.param},{self.source})"
        elif self.name == IDENTITY:
            # A bare grain-named element is rejected as legacy syntax, so an
            # identity transform on such a column must keep the explicit form
            # to round-trip through the parser.
            base = (
                f"identity({self.source})"
                if self.source.lower() in TEMPORAL_TRANSFORMS
                else self.source
            )
        else:
            base = f"{self.name}({self.source})"
        return f"{base} as {self.alias}" if self.alias else base

    @property
    def is_temporal(self) -> bool:
        return self.name in TEMPORAL_TRANSFORMS

    @property
    def field_name(self) -> str:
        """The partition-field name: the alias, or Iceberg's generated default."""
        if self.alias:
            return self.alias
        if self.name == IDENTITY:
            return self.source
        if self.name == TRUNCATE:
            return f"{self.source}_trunc"
        if self.name == VOID:
            return f"{self.source}_null"
        return f"{self.source}_{self.name}"


def _parse_expression(expr: str) -> PartitionTransform:
    """Parse one transform expression into a [`PartitionTransform`][hsfs.core.partition_transforms.PartitionTransform].

    A bare column name is an identity transform.

    Parameters:
        expr: The transform expression, e.g. `"bucket(16, user_id)"`.

    Returns:
        The parsed transform.

    Raises:
        hopsworks.client.exceptions.FeatureStoreException: If the expression does not parse.
    """
    if not isinstance(expr, str) or not expr.strip():
        raise FeatureStoreException(
            f"partitioned_by element {expr!r} must be a non-empty string."
        )
    stripped = expr.strip()
    alias = None
    alias_match = _ALIAS_RE.match(stripped)
    if alias_match:
        stripped = alias_match.group("expr").strip()
        alias = alias_match.group("alias")
    match = _EXPR_RE.match(stripped)
    if match is None:
        if re.fullmatch(r"\w+", stripped):
            _reject_bare_grain(stripped)
            # Hopsworks sanitizes feature names to lower case, so the source
            # column is canonicalized to match; otherwise day(Event_TS) would
            # pass validation and fail the case-sensitive Iceberg binding.
            return PartitionTransform(IDENTITY, stripped.lower(), alias=alias)
        raise FeatureStoreException(
            f"Cannot parse partitioned_by element {expr!r}. Expected a column "
            "name or a transform expression such as 'bucket(16, col)', "
            "'truncate(4, col)', 'day(col)', 'identity(col)', optionally "
            "followed by 'as <field_name>'."
        )
    name = match.group(1).lower()
    args = [a.strip() for a in match.group(2).split(",")] if match.group(2) else []
    if name not in ALL_TRANSFORMS:
        raise FeatureStoreException(
            f"Unknown partition transform {name!r} in {expr!r}. "
            f"Supported transforms: {list(ALL_TRANSFORMS)}."
        )
    if name in _PARAMETERIZED:
        if len(args) != 2:
            raise FeatureStoreException(
                f"{name} takes two arguments in {expr!r}: "
                f"'{name}(N, col)' with a positive integer N."
            )
        if not re.fullmatch(r"\d+", args[0]) or int(args[0]) < 1:
            raise FeatureStoreException(
                f"{name} parameter must be a positive integer in {expr!r}."
            )
        param, source = int(args[0]), args[1]
    else:
        if len(args) != 1:
            raise FeatureStoreException(
                f"{name} takes one column argument in {expr!r}: '{name}(col)'."
            )
        param, source = None, args[0]
    if not re.fullmatch(r"\w+", source):
        raise FeatureStoreException(
            f"Invalid column name {source!r} in partitioned_by element {expr!r}."
        )
    # Feature names are sanitized to lower case, so the source column is
    # canonicalized to match the stored (and Iceberg-bound) name.
    return PartitionTransform(name, source.lower(), param, alias=alias)


def _reject_bare_grain(name: str) -> None:
    """Reject the pre-transform grain vocabulary with a migration hint.

    A bare element is an identity transform on a column of that name, so the
    old form `["year", "month"]` would silently become identity partitioning
    on (usually nonexistent) columns named `year` and `month`.
    """
    if name.lower() in TEMPORAL_TRANSFORMS:
        raise FeatureStoreException(
            f"partitioned_by element {name!r} looks like the removed grain "
            f"form. Write it as a transform on your event_time column "
            f"instead, e.g. '{name.lower()}(event_time_col)'."
        )


def _parse(partitioned_by: list[str]) -> list[PartitionTransform]:
    """Parse a `partitioned_by` list, rejecting duplicates.

    Parameters:
        partitioned_by: The transform expression list.

    Returns:
        The parsed transforms, in input order.

    Raises:
        hopsworks.client.exceptions.FeatureStoreException: If an element does not parse or the list contains duplicates.
    """
    transforms = [_parse_expression(e) for e in partitioned_by]
    seen = set()
    field_names = set()
    for t in transforms:
        key = (t.name, t.source, t.param)
        if key in seen:
            raise FeatureStoreException(
                f"partitioned_by contains duplicate transform '{t}'."
            )
        seen.add(key)
        # Partition-field names must be unique: an explicit alias may not
        # repeat, nor collide with another field's generated default name.
        if t.field_name in field_names:
            raise FeatureStoreException(
                f"partitioned_by element '{t}' produces the partition field "
                f"name {t.field_name!r}, which another element already uses. "
                "Rename the alias (e.g. 'as <other_name>')."
            )
        field_names.add(t.field_name)
    return transforms


def _try_parse(partitioned_by: list[str] | None) -> list[PartitionTransform] | None:
    """Parse a stored `partitioned_by` list, returning None when it does not parse.

    Feature groups created before the transform grammar store bare grain names;
    those specs are treated as opaque (reads stay correct through row-level
    filters) rather than failing the whole feature group object.
    """
    if not partitioned_by:
        return None
    try:
        return _parse(partitioned_by)
    except FeatureStoreException:
        return None


def _require_writable(feature_group) -> None:
    """Fail writes to a feature group with an unparseable legacy layout spec.

    Reads of a pre-transform (bare grain) feature group stay correct through
    row-level filters, but a write can no longer derive the grain values, so
    continuing would silently produce a different physical layout (missing or
    null grain columns). The failure must be explicit.
    """
    partitioned_by = getattr(feature_group, "partitioned_by", None)
    if partitioned_by and _try_parse(partitioned_by) is None:
        raise FeatureStoreException(
            f"Feature group {feature_group.name} was created with the removed "
            f"grain form of partitioned_by ({partitioned_by}) and cannot be "
            "written with this client version: the write path no longer "
            "derives the grain values, so continuing would silently change "
            "the physical layout. Recreate the feature group with the "
            "transform grammar (e.g. 'year(<event_time>)'), or write with a "
            "client version that predates the transform grammar. Reads are "
            "unaffected."
        )


def _validate_for_format(
    transforms: list[PartitionTransform],
    time_travel_format: str | None,
    primary_key: list[str] | None,
    event_time: str | None,
) -> None:
    """Apply the per-format validation matrix.

    Parameters:
        transforms: The parsed transforms.
        time_travel_format: The feature group's time travel format.
        primary_key: The feature group's primary key columns.
        event_time: The feature group's event time column.

    Raises:
        hopsworks.client.exceptions.FeatureStoreException: If a transform is not supported for the format.
    """
    fmt = (time_travel_format or "NONE").upper()
    if fmt == "DELTA":
        # Delta has no transform partitioning; accepting transforms and
        # erasing them to their source columns would hide the actual layout.
        raise FeatureStoreException(
            "partitioned_by is not supported on DELTA: Delta has no "
            "partition transforms. Use clustered_by for liquid clustering "
            "or partition_key for identity partitions."
        )
    allowed = ALLOWED_TRANSFORMS.get(fmt)
    if allowed is None:
        raise FeatureStoreException(
            f"partitioned_by requires time_travel_format ICEBERG or HUDI; got {fmt!r}."
        )
    rejected = [str(t) for t in transforms if t.name not in allowed]
    if rejected:
        raise FeatureStoreException(
            f"partitioned_by transforms {rejected} are not supported for "
            f"time_travel_format={fmt}. Supported transforms: {list(allowed)}."
        )
    if fmt == "ICEBERG":
        # Iceberg's PartitionSpec builder rejects more than one temporal
        # transform per source column as redundant.
        temporal_sources = [t.source for t in transforms if t.is_temporal]
        redundant = {s for s in temporal_sources if temporal_sources.count(s) > 1}
        if redundant:
            raise FeatureStoreException(
                "partitioned_by has multiple temporal transforms on "
                f"{sorted(redundant)}; Iceberg allows at most one temporal "
                "transform per source column. Keep the finest grain you "
                "need (day(col) alone already supports year-level pruning)."
            )
    if fmt == "HUDI":
        for t in transforms:
            if t.alias:
                # Hudi has no partition-field naming; accepting an alias and
                # ignoring it would misrepresent the layout.
                raise FeatureStoreException(
                    f"partitioned_by element '{t}' has a partition-field "
                    "alias; aliases are Iceberg-only (Hudi partition paths "
                    "are named after the source column or grain)."
                )
            if t.is_temporal:
                if not event_time:
                    raise FeatureStoreException(
                        f"partitioned_by transform '{t}' on HUDI requires "
                        "event_time to be set on the feature group."
                    )
                if t.source != event_time:
                    raise FeatureStoreException(
                        f"partitioned_by transform '{t}' on HUDI must use the "
                        f"event_time column ({event_time!r}) as its source; "
                        "Hudi grain columns are derived from event_time."
                    )


def _temporal_grains(transforms: list[PartitionTransform]) -> list[str]:
    """Return the grain names of the temporal transforms, in input order.

    These are the synthetic column names (`year`, `month`, ...) used by the
    Hudi materialized-grain mechanism.
    """
    return [t.name for t in transforms if t.is_temporal]


def _identity_columns(transforms: list[PartitionTransform]) -> list[str]:
    """Return the source columns of the identity transforms, in input order."""
    return [t.source for t in transforms if t.name == IDENTITY]


def _serialize(transforms: list[PartitionTransform]) -> list[str]:
    """Return the canonical expression strings for *transforms*."""
    return [str(t) for t in transforms]


_SORT_RE = re.compile(
    r"^\s*(?P<column>\w+)"
    r"(?:\s+(?P<direction>asc|desc))?"
    r"(?:\s+nulls\s+(?P<nulls>first|last))?\s*$",
    re.IGNORECASE,
)


@dataclasses.dataclass(frozen=True)
class SortField:
    """One parsed `sort_order` element: a column with direction and null ordering."""

    column: str
    direction: str = "asc"
    null_order: str = "first"

    def __str__(self) -> str:
        return f"{self.column} {self.direction} nulls {self.null_order}"


def _parse_sort_order(sort_order: list[str]) -> list[SortField]:
    """Parse a `sort_order` list into [`SortField`][hsfs.core.partition_transforms.SortField]s.

    Each element is `"col"`, `"col asc"`, `"col desc"`, optionally followed
    by `"nulls first"` or `"nulls last"`. Defaults follow Iceberg: ascending
    direction, nulls first when ascending, nulls last when descending.

    Parameters:
        sort_order: The sort expression list.

    Returns:
        The parsed sort fields, in input order.

    Raises:
        hopsworks.client.exceptions.FeatureStoreException: If an element does not parse or a column repeats.
    """
    fields: list[SortField] = []
    seen = set()
    for expr in sort_order:
        if not isinstance(expr, str) or not expr.strip():
            raise FeatureStoreException(
                f"sort_order element {expr!r} must be a non-empty string."
            )
        match = _SORT_RE.match(expr)
        if match is None:
            raise FeatureStoreException(
                f"Cannot parse sort_order element {expr!r}. Expected "
                "'col [asc|desc] [nulls first|last]'."
            )
        direction = (match.group("direction") or "asc").lower()
        nulls = match.group("nulls")
        null_order = (
            nulls.lower() if nulls else ("first" if direction == "asc" else "last")
        )
        # Feature names are sanitized to lower case; canonicalize the sort
        # column to match so the duplicate check and the Iceberg binding agree.
        field = SortField(match.group("column").lower(), direction, null_order)
        if field.column in seen:
            raise FeatureStoreException(
                f"sort_order lists column {field.column!r} more than once."
            )
        seen.add(field.column)
        fields.append(field)
    return fields


def _try_parse_sort_order(sort_order: list[str] | None) -> list[SortField] | None:
    """Parse a stored `sort_order` list, returning None when it does not parse."""
    if not sort_order:
        return None
    try:
        return _parse_sort_order(sort_order)
    except FeatureStoreException:
        return None
