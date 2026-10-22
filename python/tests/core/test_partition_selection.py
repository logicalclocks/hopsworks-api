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
from types import SimpleNamespace
from unittest import mock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core.constants import HAS_PYICEBERG
from hsfs.core.partition_selection import PartitionSelection


_TYPES = {
    "day": "date",
    "region": "string",
    "n": "int",
    "tiny": "tinyint",
    "flag": "boolean",
    "ts": "timestamp",
    "id": "bigint",
}


def _make_fg(fmt="DELTA", partition_key=None, partitioned_by=None):
    return SimpleNamespace(
        name="fg",
        version=1,
        time_travel_format=fmt,
        partition_key=partition_key if partition_key is not None else ["day"],
        partitioned_by=partitioned_by,
        columns=[SimpleNamespace(name=n, type=t) for n, t in _TYPES.items()],
    )


class _FakeColumn:
    """Stands in for a PySpark Column, which needs a live SparkContext to build."""

    def __init__(self, text):
        self.text = text

    def isin(self, values):
        return _FakeColumn(f"{self.text} IN {values}")

    def isNull(self):  # noqa: N802
        return _FakeColumn(f"{self.text} IS NULL")

    def __or__(self, other):
        return _FakeColumn(f"({self.text} OR {other.text})")

    def __and__(self, other):
        return _FakeColumn(f"({self.text} AND {other.text})")


class TestPartitionSelection:
    def test_from_user_input_normalizes_values(self):
        # Act
        selection = PartitionSelection._from_user_input(
            _make_fg(partition_key=["day", "region", "n", "flag"]),
            {
                "day": ["2026-09-02", datetime.date(2026, 9, 3), None],
                "region": ["eu"],
                "n": [1],
                "flag": [False],
            },
        )

        # Assert
        assert selection.columns == {
            "day": [datetime.date(2026, 9, 2), datetime.date(2026, 9, 3), None],
            "region": ["eu"],
            "n": [1],
            "flag": [False],
        }

    def test_from_user_input_rejects_unpartitioned_feature_group(self):
        # Act & Assert
        with pytest.raises(FeatureStoreException, match="is not partitioned"):
            PartitionSelection._from_user_input(
                _make_fg(partition_key=[]), {"day": ["2026-09-02"]}
            )

    @pytest.mark.parametrize("partitions", [{}, None, [("day", ["2026-09-02"])]])
    def test_from_user_input_rejects_non_dict_or_empty(self, partitions):
        # Act & Assert
        with pytest.raises(FeatureStoreException, match="non-empty dict"):
            PartitionSelection._from_user_input(_make_fg(), partitions)

    def test_from_user_input_rejects_non_partition_column(self):
        # Act & Assert
        with pytest.raises(FeatureStoreException, match="'region' is not a partition"):
            PartitionSelection._from_user_input(_make_fg(), {"region": ["eu"]})

    def test_from_user_input_rejects_empty_value_list(self):
        # Act & Assert
        with pytest.raises(FeatureStoreException, match="non-empty list"):
            PartitionSelection._from_user_input(_make_fg(), {"day": []})

    def test_from_user_input_rejects_unsupported_column_type(self):
        # Act & Assert
        with pytest.raises(FeatureStoreException, match="type 'timestamp'"):
            PartitionSelection._from_user_input(
                _make_fg(partition_key=["ts"]), {"ts": ["2026-09-02 00:00:00"]}
            )

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("n", 1.9),
            ("n", True),
            ("n", "1"),
            ("n", 2**31),
            ("tiny", 128),
            ("day", datetime.datetime(2026, 9, 2)),
            ("day", "2026-13-01"),
            ("region", 1),
            ("flag", 1),
        ],
    )
    def test_from_user_input_rejects_value_of_wrong_type(self, column, value):
        # Act & Assert
        with pytest.raises(FeatureStoreException, match="does not fit"):
            PartitionSelection._from_user_input(
                _make_fg(partition_key=[column]), {column: [value]}
            )

    def test_from_user_input_iceberg_allows_identity_transform_columns(self):
        # Act
        selection = PartitionSelection._from_user_input(
            _make_fg("ICEBERG", partition_key=[], partitioned_by=["region", "day(ts)"]),
            {"region": ["eu"]},
        )

        # Assert
        assert selection.columns == {"region": ["eu"]}

    def test_from_user_input_iceberg_rejects_transform_source_column(self):
        # Act & Assert
        with pytest.raises(FeatureStoreException, match="'ts' is not a partition"):
            PartitionSelection._from_user_input(
                _make_fg(
                    "ICEBERG", partition_key=[], partitioned_by=["region", "day(ts)"]
                ),
                {"ts": [datetime.date(2026, 9, 2)]},
            )

    def test_sql_predicate_escapes_values_and_identifiers(self):
        # Arrange
        selection = PartitionSelection(
            {'re"gion': ["o'x", None], "day": [datetime.date(2026, 9, 2)]},
            {'re"gion': "string", "day": "date"},
        )

        # Act
        predicate = selection._sql_predicate()

        # Assert
        assert predicate == (
            '("re""gion" IN (\'o\'\'x\') OR "re""gion" IS NULL) AND '
            "\"day\" IN (CAST('2026-09-02' AS DATE))"
        )

    def test_sql_predicate_renders_integers_booleans_and_only_null(self):
        # Arrange
        selection = PartitionSelection(
            {"n": [1, 2], "flag": [True], "region": [None]},
            {"n": "int", "flag": "boolean", "region": "string"},
        )

        # Act
        predicate = selection._sql_predicate()

        # Assert
        assert predicate == (
            '"n" IN (CAST(1 AS INT), CAST(2 AS INT)) AND "flag" IN (true) '
            'AND "region" IS NULL'
        )

    @pytest.mark.parametrize(
        ("column_type", "sql_type"),
        [
            ("tinyint", "TINYINT"),
            ("smallint", "SMALLINT"),
            ("int", "INT"),
            ("integer", "INTEGER"),
            ("bigint", "BIGINT"),
        ],
    )
    def test_sql_predicate_casts_integers_to_the_column_type(
        self, column_type, sql_type
    ):
        # Arrange
        selection = PartitionSelection({"n": [-3]}, {"n": column_type})

        # Act
        predicate = selection._sql_predicate()

        # Assert
        assert predicate == f'"n" IN (CAST(-3 AS {sql_type}))'

    def test_spark_condition_ands_columns_and_ors_null(self, mocker):
        # Arrange
        mocker.patch("pyspark.sql.functions.col", side_effect=_FakeColumn)
        selection = PartitionSelection(
            {"day": [datetime.date(2026, 9, 2), None], "n": [1]},
            {"day": "date", "n": "int"},
        )

        # Act
        condition = selection._spark_condition()

        # Assert
        assert condition.text == (
            "((`day` IN [datetime.date(2026, 9, 2)] OR `day` IS NULL) AND `n` IN [1])"
        )

    @pytest.mark.skipif(not HAS_PYICEBERG, reason="pyiceberg not installed")
    def test_pyiceberg_expression_ands_columns_and_ors_null(self):
        # Arrange
        from pyiceberg.expressions import And, In, IsNull, Or

        selection = PartitionSelection(
            {
                "day": [datetime.date(2026, 9, 2), datetime.date(2026, 9, 3)],
                "r": [None, "eu", "us"],
            },
            {"day": "date", "r": "string"},
        )

        # Act
        expression = selection._pyiceberg_expression()

        # Assert
        assert expression == And(
            In("day", {datetime.date(2026, 9, 2), datetime.date(2026, 9, 3)}),
            Or(In("r", {"eu", "us"}), IsNull("r")),
        )

    def test_iceberg_jvm_expression_calls_expressions_by_name(self):
        # Arrange
        jvm = mock.MagicMock()
        expressions = jvm.org.apache.iceberg.expressions.Expressions
        values = jvm.java.util.ArrayList.return_value
        values.size.return_value = 1
        selection = PartitionSelection(
            {"day": [datetime.date(2026, 9, 2), None]}, {"day": "date"}
        )

        # Act
        expression = selection._iceberg_jvm_expression(jvm)

        # Assert
        values.add.assert_called_once_with("2026-09-02")
        getattr(expressions, "in").assert_called_once_with("day", values)
        expressions.isNull.assert_called_once_with("day")
        getattr(expressions, "or").assert_called_once_with(
            getattr(expressions, "in").return_value, expressions.isNull.return_value
        )
        assert expression is getattr(expressions, "or").return_value

    @pytest.mark.parametrize(
        ("stored", "expected"),
        [
            ({"day": "2026-09-02", "region": "eu"}, True),
            ({"day": datetime.date(2026, 9, 2), "region": "eu"}, True),
            ({"day": "2026-09-02", "region": "us"}, False),
            ({"day": "__HIVE_DEFAULT_PARTITION__", "region": "eu"}, True),
            ({"day": "not-a-date", "region": "eu"}, False),
        ],
    )
    def test_matches_parses_stored_values(self, stored, expected):
        # Arrange
        selection = PartitionSelection(
            {"day": [datetime.date(2026, 9, 2), None], "region": ["eu"]},
            {"day": "date", "region": "string"},
        )

        # Act & Assert
        assert selection._matches(stored) is expected

    def test_matches_ignores_unselected_columns(self):
        # Arrange
        selection = PartitionSelection({"n": [7]}, {"n": "int"})

        # Act & Assert
        assert selection._matches({"n": "7", "day": "anything"})
