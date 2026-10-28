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
import hashlib
import json
import os
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


def _decode_golden(value, declared):
    if value is None:
        return None
    kind = declared[0]
    if kind in ("double", "float"):
        return float(value)
    if kind == "timestamp":
        return datetime.datetime.fromisoformat(value)
    if kind == "date":
        return datetime.date.fromisoformat(value)
    if kind == "decimal":
        return decimal.Decimal(value)
    if kind == "binary":
        return bytes.fromhex(value)
    if kind == "struct":
        # Keys as the frame holds them, which may differ in case from the declared type.
        keys = {key.lower(): key for key in value}
        return {
            keys.get(name.lower(), name): _decode_golden(
                value.get(keys.get(name.lower(), name)), t
            )
            for name, t in declared[1]
        }
    if kind == "array":
        return [_decode_golden(item, declared[1]) for item in value]
    if kind == "map":
        return {
            _decode_golden(k, declared[1]): _decode_golden(v, declared[2])
            for k, v in value.items()
        }
    return value


def _golden():
    """Rows with their expected hashes and splits, shared with the Java client tests."""
    with open(
        os.path.join(os.path.dirname(__file__), "fixtures", "hash_split_golden.json")
    ) as f:
        golden = json.load(f)
    names = [column[0] for column in golden["columns"]]
    types = [column[1] for column in golden["columns"]]
    parsed = [training_dataset_split._parse_type(t) for t in types]
    rows = [
        [_decode_golden(v, d) for v, d in zip(row, parsed, strict=True)]
        for row in golden["rows"]
    ]
    return golden, names, types, rows


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
        ],
    )
    def test_value_outside_declared_type_raises(self, value, declared):
        with pytest.raises(ValueError, match=declared.split("(")[0]):
            _row_bytes(value, types=[declared])

    def test_declared_float_hashes_float32_bits(self):
        assert _row_bytes(0.1, types=["float"]) == _row_bytes(
            np.float32(0.1), types=["float"]
        )
        assert _row_bytes(float(np.float32(0.1)), types=["float"]) == _row_bytes(
            0.1, types=["float"]
        )
        assert _row_bytes(0.1, types=["float"]) != _row_bytes(0.1, types=["double"])
        assert _row_bytes(float("nan"), types=["float"]) == _row_bytes(None)

    def test_floats_are_big_endian_float32(self):
        assert _row_bytes(0.1, types=["float"]) == b"f" + bytes.fromhex("3dcccccd")
        assert _row_bytes(-0.0, types=["float"]) == b"f" + bytes.fromhex("80000000")

    def test_float_out_of_float32_range_raises(self):
        with pytest.raises(ValueError, match="float"):
            _row_bytes(1e40, types=["float"])
        assert _row_bytes(float("inf"), types=["float"]) == _row_bytes(
            np.float32("inf"), types=["float"]
        )

    def test_struct_field_names_match_any_case(self):
        assert _row_bytes({"A": 1}, types=["struct<a:bigint>"]) == _row_bytes(
            {"a": 1}, types=["struct<a:bigint>"]
        )

    def test_struct_value_missing_a_field_raises(self):
        with pytest.raises(ValueError, match="struct"):
            _row_bytes({"a": 1}, types=["struct<a:bigint,b:string>"])

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

    @pytest.mark.parametrize(
        "moment",
        [
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            datetime.datetime(1, 1, 1),
        ],
    )
    def test_timestamps_outside_the_nanosecond_range(self, moment):
        expected = _row_bytes(
            moment.replace(tzinfo=datetime.timezone.utc), types=["timestamp"]
        )
        for value in (
            pd.Timestamp(moment).as_unit("us"),
            pd.Timestamp(moment).as_unit("us").tz_localize("UTC"),
            np.datetime64(moment, "us"),
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


class TestGoldenVectors:
    def test_hashes_and_splits(self):
        golden, _, types, rows = _golden()
        salt = golden["seed"].to_bytes(8, "big", signed=True)
        fractions = [tuple(split) for split in golden["splits"]]
        columns = [list(column) for column in zip(*rows, strict=True)]

        assert [
            hashlib.blake2b(
                training_dataset_split._canonical_row_bytes(row, types),
                key=salt,
                digest_size=8,
            ).hexdigest()
            for row in rows
        ] == golden["expected"]["hashes"]
        assert (
            training_dataset_split._split_indexes(columns, types, salt, fractions)
            == golden["expected"]["buckets"]
        )
        legacy = f"{golden['legacy']['id']}:{golden['legacy']['version']}".encode()
        assert (
            training_dataset_split._split_indexes(columns, types, legacy, fractions)
            == golden["expected"]["legacy_buckets"]
        )


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
