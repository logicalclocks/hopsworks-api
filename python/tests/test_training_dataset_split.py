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
from hsfs import training_dataset_split


def _row_bytes(*values, naive_tzs=None):
    return training_dataset_split._canonical_row_bytes(values, naive_tzs)


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
    def test_nan_normalized_and_distinct_from_none(self):
        other_nan = struct.unpack(">d", b"\x7f\xf8\x00\x00\x00\x00\x00\x01")[0]
        assert _row_bytes(float("nan")) == _row_bytes(other_nan)
        assert _row_bytes(float("nan")) == _row_bytes(np.float32("nan"))
        assert _row_bytes(None) != _row_bytes(float("nan"))
        assert _row_bytes(None) == _row_bytes(pd.NaT) == _row_bytes(pd.NA)
        assert _row_bytes(None) not in (_row_bytes(0), _row_bytes(""), _row_bytes([]))

    def test_numpy_scalars_match_python_values(self):
        assert _row_bytes(np.int64(3), np.float64(1.5), np.bool_(True)) == _row_bytes(
            3, 1.5, True
        )
        assert _row_bytes(np.array([1, 2])) == _row_bytes([1, 2])
        assert _row_bytes(True) != _row_bytes(1)
        assert _row_bytes(1) != _row_bytes(1.0)

    def test_timestamps_are_utc_micros(self):
        aware = datetime.datetime(2024, 1, 1, 12, tzinfo=datetime.timezone.utc)
        naive = datetime.datetime(2024, 1, 1, 12)
        expected = _row_bytes(aware)
        assert _row_bytes(naive) == expected
        assert _row_bytes(pd.Timestamp("2024-01-01 12:00:00.000000999")) == expected
        assert _row_bytes(pd.Timestamp("2024-01-01 13:00", tz="Europe/Stockholm")) == (
            expected
        )
        assert _row_bytes(np.datetime64("2024-01-01T12:00")) == expected
        assert (
            _row_bytes(
                datetime.datetime(2024, 1, 1, 13), naive_tzs=["Europe/Stockholm"]
            )
            == expected
        )
        assert _row_bytes(datetime.date(2024, 1, 1)) != _row_bytes(
            datetime.datetime(2024, 1, 1)
        )

    def test_decimals_keep_scale(self):
        assert _row_bytes(decimal.Decimal("1.50")) != _row_bytes(decimal.Decimal("1.5"))
        assert _row_bytes(decimal.Decimal("1.50")) == _row_bytes(
            decimal.Decimal("1.50")
        )

    def test_nested_values(self):
        assert _row_bytes({"b": 2, "a": 1}) == _row_bytes({"a": 1, "b": 2})
        assert _row_bytes([{"a": np.int64(1)}]) == _row_bytes(
            np.array([{"a": 1}], dtype=object)
        )
        assert _row_bytes(["a", "b"]) != _row_bytes(["ab"])
        assert _row_bytes("ab", "c") != _row_bytes("a", "bc")
        assert _row_bytes(b"ab") != _row_bytes("ab")
