#
#   Copyright 2022 Logical Clocks AB
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
from __future__ import annotations

import bisect
import datetime
import decimal
import hashlib
import json
import struct

import humps
from hopsworks_common.core.constants import HAS_NUMPY, HAS_PANDAS
from hsfs import util


if HAS_NUMPY:
    import numpy as np
if HAS_PANDAS:
    import pandas as pd


class TrainingDatasetSplit:
    TIME_SERIES_SPLIT = "TIME_SERIES_SPLIT"
    RANDOM_SPLIT = "RANDOM_SPLIT"
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"

    def __init__(
        self,
        name,
        split_type,
        percentage=None,
        start_time=None,
        end_time=None,
        **kwargs,
    ):
        self._name = name
        self._percentage = percentage
        self._split_type = split_type
        self._start_time = start_time
        self._end_time = end_time

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def percentage(self):
        return self._percentage

    @percentage.setter
    def percentage(self, percentage):
        self._percentage = percentage

    @property
    def split_type(self):
        return self._split_type

    @split_type.setter
    def split_type(self, split_type):
        self._split_type = split_type

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, end_time):
        self._end_time = end_time

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "name": self._name,
            "percentage": self._percentage,
            "splitType": self._split_type,
            "startTime": self._start_time,
            "endTime": self._end_time,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            name=json_decamelized["name"],
            split_type=json_decamelized.get(
                "split_type", TrainingDatasetSplit.RANDOM_SPLIT
            ),
            percentage=json_decamelized.get("percentage", None),
            start_time=json_decamelized.get("start_time", None),
            end_time=json_decamelized.get("end_time", None),
        )


_NAN = struct.pack(">d", float("nan"))
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_ORDINAL = datetime.date(1970, 1, 1).toordinal()
_MICROSECOND = datetime.timedelta(microseconds=1)


def _encode_int(value, out):
    if -(2**63) <= value < 2**63:
        out += b"I" + struct.pack(">q", value)
    else:
        raw = value.to_bytes((value.bit_length() + 8) // 8, "big", signed=True)
        out += b"J" + struct.pack(">I", len(raw)) + raw


def _epoch_micros(value, naive_tz):
    if HAS_PANDAS and isinstance(value, pd.Timestamp):
        if value.tzinfo is None and naive_tz is not None:
            # Ambiguous wall times cannot be resolved, only resolved the same way every time.
            value = value.tz_localize(naive_tz, ambiguous=True)
        return value.value // 1000
    if value.tzinfo is None:
        if naive_tz is not None:
            return _epoch_micros(pd.Timestamp(value), naive_tz)
        value = value.replace(tzinfo=datetime.timezone.utc)
    return (value - _EPOCH) // _MICROSECOND


def _encode(value, out, naive_tz):
    if value is None or (HAS_PANDAS and (value is pd.NaT or value is pd.NA)):
        out += b"N"
    elif isinstance(value, float):
        out += b"F" + (_NAN if value != value else struct.pack(">d", value))
    elif isinstance(value, bool):
        out += b"B\x01" if value else b"B\x00"
    elif isinstance(value, int):
        _encode_int(value, out)
    elif isinstance(value, str):
        raw = value.encode("utf-8")
        out += b"S" + struct.pack(">I", len(raw)) + raw
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        out += b"Y" + struct.pack(">I", len(raw)) + raw
    elif HAS_NUMPY and isinstance(value, np.ndarray):
        _encode(list(value), out, naive_tz)
    elif HAS_NUMPY and isinstance(value, np.datetime64):
        _encode(pd.Timestamp(value), out, naive_tz)
    elif HAS_NUMPY and isinstance(value, np.timedelta64):
        _encode(pd.Timedelta(value), out, naive_tz)
    elif HAS_NUMPY and isinstance(value, np.generic):
        _encode(value.item(), out, naive_tz)
    elif isinstance(value, datetime.datetime):
        out += b"T" + struct.pack(">q", _epoch_micros(value, naive_tz))
    elif isinstance(value, datetime.date):
        out += b"d" + struct.pack(">q", value.toordinal() - _EPOCH_ORDINAL)
    elif isinstance(value, datetime.time):
        micros = (
            (value.hour * 60 + value.minute) * 60 + value.second
        ) * 1_000_000 + value.microsecond
        out += b"t" + struct.pack(">q", micros)
    elif isinstance(value, datetime.timedelta):
        out += b"u" + struct.pack(">q", value // _MICROSECOND)
    elif isinstance(value, decimal.Decimal):
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(map(str, digits)) or "0")
        out += b"D"
        _encode_int(-unscaled if sign else unscaled, out)
        out += struct.pack(">i", -exponent)
    elif isinstance(value, dict):
        # Engines hand both structs and maps over as dicts, so entries are sorted by key.
        entries = []
        for key, item in value.items():
            key_bytes, item_bytes = bytearray(), bytearray()
            _encode(key, key_bytes, naive_tz)
            _encode(item, item_bytes, naive_tz)
            entries.append((bytes(key_bytes), bytes(item_bytes)))
        out += b"M" + struct.pack(">I", len(entries))
        for key_bytes, item_bytes in sorted(entries):
            out += key_bytes + item_bytes
    elif isinstance(value, (list, tuple)):
        out += b"L" + struct.pack(">I", len(value))
        for item in value:
            _encode(item, out, naive_tz)
    else:
        raise TypeError(
            f"Cannot split on a value of type `{type(value).__name__}`: it has no canonical serialization."
        )


def _canonical_row_bytes(values, naive_tzs=None) -> bytes:
    """Serialize one row so every engine produces the same bytes for the same values.

    `naive_tzs` gives, per column, the zone naive timestamps are wall time in; None reads them as UTC.
    """
    out = bytearray()
    for i, value in enumerate(values):
        _encode(value, out, naive_tzs[i] if naive_tzs else None)
    return bytes(out)


def _split_indexes(columns, salt: bytes, fractions, naive_tzs=None) -> list[int]:
    """Assign each row to a split by a keyed hash of its content.

    `columns` holds one list of values per column. The index is that of the split whose
    cumulative fraction range holds the row's hash, read as a fraction of 2^64.
    """
    bounds, total = [], 0.0
    for fraction in fractions[:-1]:
        total += fraction
        bounds.append(int(total * 2**64))
    return [
        bisect.bisect_right(
            bounds,
            int.from_bytes(
                hashlib.blake2b(
                    _canonical_row_bytes(row, naive_tzs), key=salt, digest_size=8
                ).digest(),
                "big",
            ),
        )
        for row in zip(*columns, strict=True)
    ]


def _split_salt(training_dataset) -> bytes:
    if training_dataset.seed is not None:
        return training_dataset.seed.to_bytes(8, "big", signed=True)
    # Versions created before seeds were generated: fixed per version.
    return f"{training_dataset.id}:{training_dataset.version}".encode()


def _split_columns(feature_view_obj, available) -> list[str]:
    """Columns the split hashes: the feature view's features in its order, so read options do not change the split."""
    features = getattr(feature_view_obj, "features", None) or []
    names = [
        feature.name
        for feature in features
        if not feature.inference_helper_column and not feature.training_helper_column
    ]
    return [name for name in names if name in available] or list(available)
