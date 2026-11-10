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
"""The validated partition selection behind `FeatureGroup.delete_partitions`.

Values are normalised once, losslessly, from each column's feature type, and every
engine renders the same normalised values, so a value cannot select a different
partition on one engine than on another.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.core import partition_transforms


if TYPE_CHECKING:
    from hsfs import feature_group as fg_mod


_INTEGER_RANGES = {
    "tinyint": (-(2**7), 2**7 - 1),
    "smallint": (-(2**15), 2**15 - 1),
    "int": (-(2**31), 2**31 - 1),
    "integer": (-(2**31), 2**31 - 1),
    "bigint": (-(2**63), 2**63 - 1),
}
_SUPPORTED_TYPES = ("string", "boolean", "date", *_INTEGER_RANGES)

# How Hive, and Hudi writing Hive-style paths, name the partition of a null value.
# Hudi before 0.12 wrote "default".
_NULL_PARTITION_SEGMENTS = ("__HIVE_DEFAULT_PARTITION__", "default")


class PartitionSelection:
    """Whole partitions of one feature group, as `{column: [values]}`.

    Columns are ANDed and values within a column are ORed, so several lists select
    their Cartesian product, and a partition column left out is unrestricted.
    `None` selects the partition holding null values.
    """

    def __init__(self, columns: dict[str, list[Any]], types: dict[str, str]):
        self._columns = columns
        self._types = types

    @property
    def columns(self) -> dict[str, list[Any]]:
        return self._columns

    @classmethod
    def _from_user_input(
        cls, feature_group: fg_mod.FeatureGroup, partitions: Any
    ) -> PartitionSelection:
        """Validate a `delete_partitions` request against the feature group.

        Raises:
            FeatureStoreException: The feature group has no column that can be selected
                on, `partitions` is not a non-empty dict of non-empty lists, a key is not
                a selectable partition column, or a value does not fit its column type.
        """
        allowed = cls._selectable_columns(feature_group)
        if not allowed:
            raise FeatureStoreException(
                f"Feature group {feature_group.name} v{feature_group.version} is not "
                "partitioned, so it has no partitions to delete. Use remove_rows to "
                "delete rows by primary key."
            )
        if not isinstance(partitions, dict) or not partitions:
            raise FeatureStoreException(
                "partitions must be a non-empty dict mapping partition columns to "
                f'lists of values, e.g. {{"{allowed[0]}": [...]}}.'
            )

        feature_types = {
            f.name: str(f.type).lower() for f in feature_group.columns or []
        }
        columns = {}
        types = {}
        for column, values in partitions.items():
            if column not in allowed:
                raise FeatureStoreException(
                    f"{column!r} is not a partition column that can be deleted by. "
                    f"The selectable partition columns are {allowed}. Only partition "
                    "columns select whole files, which is what keeps the delete from "
                    "reading or rewriting data."
                )
            if not isinstance(values, (list, tuple)) or not values:
                raise FeatureStoreException(
                    f"The values for partition column {column!r} must be a non-empty "
                    "list; an empty list would make the whole call delete nothing."
                )
            column_type = feature_types.get(column)
            if column_type not in _SUPPORTED_TYPES:
                raise FeatureStoreException(
                    f"Partition column {column!r} has type {column_type!r}; "
                    "delete_partitions supports partition columns of type "
                    f"{', '.join(_SUPPORTED_TYPES)}."
                )
            columns[column] = [
                cls._normalize(column, column_type, value) for value in values
            ]
            types[column] = column_type
        return cls(columns, types)

    @staticmethod
    def _selectable_columns(feature_group: fg_mod.FeatureGroup) -> list[str]:
        """Partition columns whose values map one-to-one onto storage partitions.

        Delta partitions only by `partition_key`. Iceberg and Hudi also take
        `partitioned_by`, where only identity transforms qualify: a value of the source
        column cannot name the partition of a `day(ts)` or `bucket(16, id)` transform.
        """
        transforms = partition_transforms._try_parse(
            getattr(feature_group, "partitioned_by", None)
        )
        if transforms and feature_group.time_travel_format in ("ICEBERG", "HUDI"):
            return partition_transforms._identity_columns(transforms)
        return list(feature_group.partition_key or [])

    @staticmethod
    def _normalize(column: str, column_type: str, value: Any) -> Any:
        """Return *value* as the Python value of *column_type*, without any coercion.

        Coercing would let `1.9` select partition `1`, so a value of the wrong Python
        type is rejected instead.
        """
        if value is None:
            return None
        if column_type == "string":
            if isinstance(value, str):
                return value
        elif column_type == "boolean":
            if isinstance(value, bool):
                return value
        elif column_type == "date":
            if isinstance(value, datetime.date) and not isinstance(
                value, datetime.datetime
            ):
                return value
            if isinstance(value, str):
                try:
                    return datetime.date.fromisoformat(value)
                except ValueError:
                    pass
        elif isinstance(value, int) and not isinstance(value, bool):
            low, high = _INTEGER_RANGES[column_type]
            if low <= value <= high:
                return value
        raise FeatureStoreException(
            f"Value {value!r} does not fit partition column {column!r} of type "
            f"{column_type}. Expected "
            + {
                "string": "a str",
                "boolean": "a bool",
                "date": "a datetime.date or a 'YYYY-MM-DD' string",
            }.get(column_type, f"an int within the {column_type} range")
            + ", or None for the null partition."
        )

    def _matches(self, stored: dict[str, Any]) -> bool:
        """Whether a partition with the stored values *stored* is selected.

        *stored* maps partition column names to their path segment strings.
        Columns of *stored* that are not selected on are ignored.
        """
        for column, values in self._columns.items():
            if self._from_stored(column, stored.get(column)) not in values:
                return False
        return True

    def _from_stored(self, column: str, raw: str | None) -> Any:
        if raw is None or raw in _NULL_PARTITION_SEGMENTS:
            return None
        column_type = self._types[column]
        try:
            if column_type == "string":
                return raw
            if column_type == "boolean":
                return raw.lower() == "true"
            if column_type == "date":
                return datetime.date.fromisoformat(raw)
            return int(raw)
        except ValueError:
            # A stored partition value that does not parse as the column type cannot
            # be equal to any validated value.
            return raw

    # region Engine renderings

    def _spark_condition(self):
        """The selection as a PySpark column, for Delta's `DeltaTable.delete`."""
        from pyspark.sql import functions as F

        condition = None
        for column, values in self._columns.items():
            # Backticks keep a column name containing a dot from being read as a
            # struct field access.
            spark_column = F.col("`" + column.replace("`", "``") + "`")
            non_null = [v for v in values if v is not None]
            clause = spark_column.isin(non_null) if non_null else None
            if None in values:
                clause = (
                    spark_column.isNull()
                    if clause is None
                    else (clause | spark_column.isNull())
                )
            condition = clause if condition is None else (condition & clause)
        return condition

    def _sql_predicate(self) -> str:
        """The selection as a SQL predicate, for delta-rs `DeltaTable.delete`.

        Identifiers and string literals are quoted with their quote character doubled,
        which is what keeps a value from rewriting the predicate.
        """
        clauses = []
        for column, values in self._columns.items():
            identifier = '"' + column.replace('"', '""') + '"'
            non_null = [v for v in values if v is not None]
            parts = []
            if non_null:
                literals = ", ".join(
                    self._sql_literal(v, self._types[column]) for v in non_null
                )
                parts.append(f"{identifier} IN ({literals})")
            if None in values:
                parts.append(f"{identifier} IS NULL")
            clauses.append(parts[0] if len(parts) == 1 else f"({' OR '.join(parts)})")
        return " AND ".join(clauses)

    @staticmethod
    def _sql_literal(value: Any, column_type: str) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int):
            # A bare integer is an Int64 to DataFusion, and delta-rs refuses to compare
            # it with a narrower partition column.
            return f"CAST({value} AS {column_type.upper()})"
        if isinstance(value, datetime.date):
            return f"CAST('{value.isoformat()}' AS DATE)"
        return "'" + value.replace("'", "''") + "'"

    def _pyiceberg_expression(self):
        """The selection as a pyiceberg row filter."""
        from pyiceberg.expressions import And, In, IsNull, Or

        expression = None
        for column, values in self._columns.items():
            non_null = {v for v in values if v is not None}
            clause = In(column, non_null) if non_null else None
            if None in values:
                clause = (
                    IsNull(column) if clause is None else Or(clause, IsNull(column))
                )
            expression = clause if expression is None else And(expression, clause)
        return expression

    def _iceberg_jvm_expression(self, jvm):
        """The selection as an `org.apache.iceberg.expressions.Expression`."""
        expressions = jvm.org.apache.iceberg.expressions.Expressions
        # `in`, `or` and `and` are Python keywords, so they are reached by name.
        expression = None
        for column, values in self._columns.items():
            non_null = jvm.java.util.ArrayList()
            for value in values:
                if value is not None:
                    non_null.add(
                        value.isoformat() if isinstance(value, datetime.date) else value
                    )
            clause = (
                getattr(expressions, "in")(column, non_null)
                if non_null.size() > 0
                else None
            )
            if None in values:
                is_null = expressions.isNull(column)
                clause = (
                    is_null
                    if clause is None
                    else getattr(expressions, "or")(clause, is_null)
                )
            expression = (
                clause
                if expression is None
                else getattr(expressions, "and")(expression, clause)
            )
        return expression

    # endregion
