#
#   Copyright 2022 Hopsworks AB
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
import decimal
import struct

import numpy as np
import pandas as pd
import pytest
from hsfs import training_dataset_split
from hsfs.training_dataset_feature import TrainingDatasetFeature


def _row_bytes(*values, types=None):
    return training_dataset_split._canonical_row_bytes(
        values, types or [None] * len(values)
    )


class TestTrainingDatasetSplit:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["training_dataset_split"]["get"]["response"]

        # Act
        td_split = training_dataset_split.TrainingDatasetSplit.from_response_json(json)

        # Assert
        assert td_split.name == "test_name"
        assert td_split.percentage == "test_percentage"
        assert td_split.split_type == "test_split_type"
        assert td_split.start_time == "test_start_time"
        assert td_split.end_time == "test_end_time"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["training_dataset_split"]["get_basic_info"]["response"]

        # Act
        td_split = training_dataset_split.TrainingDatasetSplit.from_response_json(json)

        # Assert
        assert td_split.name == "test_name"
        assert td_split.percentage is None
        assert td_split.split_type == "test_split_type"
        assert td_split.start_time is None
        assert td_split.end_time is None


class TestCanonicalRowBytes:
    def test_missing_values_share_one_tag(self):
        other_nan = struct.unpack(">d", b"\x7f\xf8\x00\x00\x00\x00\x00\x01")[0]
        missing = _row_bytes(None)
        for value in (float("nan"), other_nan, np.float32("nan"), pd.NaT, pd.NA):
            assert _row_bytes(value) == missing
            assert _row_bytes(value, types=["double"]) == missing
        assert _row_bytes(np.datetime64("NaT"), types=["timestamp"]) == missing
        assert missing not in (_row_bytes(0), _row_bytes(""), _row_bytes([]))

    def test_declared_type_wins_over_runtime_type(self):
        assert _row_bytes(3.0, types=["bigint"]) == _row_bytes(3, types=["bigint"])
        assert _row_bytes(np.float64(3.0), types=["int"]) == _row_bytes(3)
        assert _row_bytes(pd.Timestamp("2024-01-02"), types=["date"]) == _row_bytes(
            datetime.date(2024, 1, 2), types=["date"]
        )
        assert _row_bytes(np.float32(0.1), types=["float"]) == _row_bytes(
            float(np.float32(0.1)), types=["float"]
        )
        assert _row_bytes(1, types=["bigint"]) != _row_bytes(1, types=["double"])

    @pytest.mark.parametrize(
        ("value", "declared"),
        [
            (3.5, "bigint"),
            (pd.Timestamp("2024-01-02 10:00"), "date"),
            (decimal.Decimal("1.234"), "decimal(10,2)"),
            (0.1, "float"),
        ],
    )
    def test_value_outside_declared_type_raises(self, value, declared):
        with pytest.raises(ValueError, match=declared.split("(")[0]):
            _row_bytes(value, types=[declared])

    def test_timestamps_are_utc_micros(self):
        aware = datetime.datetime(2024, 1, 1, 12, tzinfo=datetime.timezone.utc)
        expected = _row_bytes(aware, types=["timestamp"])
        for value in (
            datetime.datetime(2024, 1, 1, 12),
            pd.Timestamp("2024-01-01 12:00:00.000000999"),
            pd.Timestamp("2024-01-01 13:00", tz="Europe/Stockholm"),
            np.datetime64("2024-01-01T12:00"),
            1704110400000000,
        ):
            assert _row_bytes(value, types=["timestamp"]) == expected

    def test_decimals_are_unscaled_at_declared_scale(self):
        assert _row_bytes(decimal.Decimal("1.5"), types=["decimal(10,2)"]) == (
            _row_bytes(decimal.Decimal("1.50"), types=["decimal(10,2)"])
        )
        assert _row_bytes(decimal.Decimal("1.5"), types=["decimal(10,2)"]) != (
            _row_bytes(decimal.Decimal("1.5"), types=["decimal(10,1)"])
        )

    def test_nested_values_follow_declared_type(self):
        declared = ["array<struct<a:bigint,t:timestamp,d:date>>"]
        python_value = [
            {
                "a": 1,
                "t": datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
                "d": datetime.date(2024, 1, 1),
            }
        ]
        pandas_value = np.array(
            [{"a": 1.0, "t": 1704067200000000, "d": pd.Timestamp("2024-01-01")}],
            dtype=object,
        )
        assert _row_bytes(python_value, types=declared) == _row_bytes(
            pandas_value, types=declared
        )
        assert _row_bytes({"b": 2, "a": 1}, types=["map<string,bigint>"]) == (
            _row_bytes([("a", 1), ("b", 2)], types=["map<string,bigint>"])
        )
        assert _row_bytes(
            [{"key": "a", "value": 1}], types=["map<string,bigint>"]
        ) == _row_bytes({"a": 1}, types=["map<string,bigint>"])
        assert _row_bytes(["a", "b"]) != _row_bytes(["ab"])
        assert _row_bytes("ab", "c") != _row_bytes("a", "bc")


class TestSplitColumns:
    def test_labels_in_helpers_and_extras_out(self):
        view = type(
            "View",
            (),
            {
                "features": [
                    TrainingDatasetFeature(name="f", type="bigint"),
                    TrainingDatasetFeature(name="y", type="double", label=True),
                    TrainingDatasetFeature(
                        name="ih", type="string", inference_helper_column=True
                    ),
                    TrainingDatasetFeature(
                        name="th", type="string", training_helper_column=True
                    ),
                ]
            },
        )()

        columns = training_dataset_split._split_columns(
            view, ["pk", "f", "y", "ih", "th", "event_time"]
        )

        assert columns == [("f", "bigint"), ("y", "double")]
