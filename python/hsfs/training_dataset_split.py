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
import functools
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


_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_EPOCH_ORDINAL = datetime.date(1970, 1, 1).toordinal()
_MICROSECOND = datetime.timedelta(microseconds=1)
_KINDS = {
    "tinyint": "int",
    "smallint": "int",
    "int": "int",
    "integer": "int",
    "bigint": "int",
    "boolean": "boolean",
    "float": "float",
    "double": "double",
    "string": "string",
    "varchar": "string",
    "char": "string",
    "binary": "binary",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp",
    "timestamp_ltz": "timestamp",
}
_ANY = ("any",)


def _parse_at(text, i):
    j = i
    while j < len(text) and text[j] not in "<>,:(":
        j += 1
    name = text[i:j].strip().lower()
    args = ""
    if j < len(text) and text[j] == "(":
        end = text.index(")", j)
        args, j = text[j + 1 : end], end + 1
    if name == "array":
        element, j = _parse_at(text, j + 1)
        return ("array", element), j + 1
    if name == "map":
        key, j = _parse_at(text, j + 1)
        value, j = _parse_at(text, j + 1)
        return ("map", key, value), j + 1
    if name == "struct":
        fields, j = [], j + 1
        while text[j] != ">":
            colon = text.index(":", j)
            field_name = text[j:colon].strip()
            field_type, j = _parse_at(text, colon + 1)
            fields.append((field_name, field_type))
            if text[j] == ",":
                j += 1
        return ("struct", tuple(fields)), j + 1
    if name == "decimal":
        return ("decimal", int(args.split(",")[1]) if "," in args else 0), j
    return (_KINDS.get(name, "any"), name), j


@functools.cache
def _parse_type(offline_type):
    """Parse an offline type such as `array<struct<a:bigint,t:timestamp>>` into a tuple tree."""
    if not offline_type:
        return _ANY
    return _parse_at(offline_type, 0)[0]


def _is_missing(value):
    if value is None or isinstance(value, float):
        return value is None or value != value
    if HAS_PANDAS and (value is pd.NA or value is pd.NaT):
        return True
    return HAS_NUMPY and isinstance(value, np.generic) and bool(value != value)


def _misfit(value, kind):
    return ValueError(
        f"Cannot split on {value!r}: it does not fit the declared type `{kind}`."
    )


def _as_int(value, kind="bigint"):
    if HAS_NUMPY and isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        if not value.is_integer():
            raise _misfit(value, kind)
        return int(value)
    if isinstance(value, (int, decimal.Decimal)) and int(value) == value:
        return int(value)
    raise _misfit(value, kind)


def _encode_int(value, out):
    if -(2**63) <= value < 2**63:
        out += b"I" + struct.pack(">q", value)
    else:
        raw = value.to_bytes((value.bit_length() + 8) // 8, "big", signed=True)
        out += b"J" + struct.pack(">I", len(raw)) + raw


def _to_timestamp(value):
    if HAS_NUMPY and isinstance(value, np.datetime64):
        return pd.Timestamp(value)
    return value


def _epoch_micros(value):
    value = _to_timestamp(value)
    if HAS_PANDAS and isinstance(value, pd.Timestamp):
        # Naive values are read as UTC: `.value` is then the wall time as UTC nanoseconds.
        return value.value // 1000
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return (value - _EPOCH) // _MICROSECOND
    if isinstance(value, datetime.date):
        return (value.toordinal() - _EPOCH_ORDINAL) * 86_400_000_000
    # Spark hands timestamps over as UTC epoch microseconds.
    return _as_int(value, "timestamp")


def _epoch_days(value):
    value = _to_timestamp(value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc)
        if value.time() != datetime.time(0):
            raise _misfit(value, "date")
        value = value.date()
    if isinstance(value, datetime.date):
        return value.toordinal() - _EPOCH_ORDINAL
    raise _misfit(value, "date")


def _unscaled(value, scale):
    if HAS_NUMPY and isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        value = decimal.Decimal(repr(value))
    unscaled = decimal.Decimal(value).scaleb(scale)
    if unscaled != unscaled.to_integral_value():
        raise _misfit(value, f"decimal(_,{scale})")
    return int(unscaled)


def _map_entries(value):
    if isinstance(value, dict):
        return value.items()
    # Polars has no map type: entries come as {"key": ..., "value": ...} structs.
    return [
        (entry["key"], entry["value"]) if isinstance(entry, dict) else entry
        for entry in value
    ]


def _encode(value, declared, out):
    if _is_missing(value):
        out += b"N"
        return
    kind = declared[0]
    if kind == "any":
        kind, declared = _runtime_kind(value)
    if kind == "int":
        _encode_int(_as_int(value, declared[-1]), out)
    elif kind == "boolean":
        if HAS_NUMPY and isinstance(value, np.generic):
            value = value.item()
        if value not in (True, False):
            raise _misfit(value, "boolean")
        out += b"B\x01" if value else b"B\x00"
    elif kind in ("double", "float"):
        value = float(value)
        if (
            kind == "float"
            and struct.unpack(">f", struct.pack(">f", value))[0] != value
        ):
            raise _misfit(value, "float")
        out += b"F" + struct.pack(">d", value)
    elif kind == "decimal":
        out += b"D"
        _encode_int(_unscaled(value, declared[1]), out)
        out += struct.pack(">i", declared[1])
    elif kind == "string":
        raw = str(value).encode("utf-8")
        out += b"S" + struct.pack(">I", len(raw)) + raw
    elif kind == "binary":
        raw = bytes(value)
        out += b"Y" + struct.pack(">I", len(raw)) + raw
    elif kind == "date":
        out += b"d" + struct.pack(">q", _epoch_days(value))
    elif kind == "timestamp":
        out += b"T" + struct.pack(">q", _epoch_micros(value))
    elif kind == "array":
        items = list(value)
        out += b"L" + struct.pack(">I", len(items))
        for item in items:
            _encode(item, declared[1], out)
    elif kind == "struct":
        out += b"R" + struct.pack(">I", len(declared[1]))
        for index, (name, field_type) in enumerate(declared[1]):
            _encode(
                value.get(name) if isinstance(value, dict) else value[index],
                field_type,
                out,
            )
    elif kind == "map":
        entries = []
        for key, item in _map_entries(value):
            key_bytes, item_bytes = bytearray(), bytearray()
            _encode(key, declared[1], key_bytes)
            _encode(item, declared[2], item_bytes)
            entries.append((bytes(key_bytes), bytes(item_bytes)))
        out += b"M" + struct.pack(">I", len(entries))
        for key_bytes, item_bytes in sorted(entries):
            out += key_bytes + item_bytes
    else:
        raise TypeError(f"Cannot split on a value of declared type `{kind}`.")


def _runtime_kind(value):
    """The kind of a value whose column has no declared type."""
    if (
        HAS_NUMPY
        and isinstance(value, np.generic)
        and not isinstance(value, (np.datetime64, np.timedelta64))
    ):
        value = value.item()
    if isinstance(value, bool):
        return "boolean", ("boolean",)
    if isinstance(value, int):
        return "int", ("int", "bigint")
    if isinstance(value, float):
        return "double", ("double",)
    if isinstance(value, str):
        return "string", ("string",)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "binary", ("binary",)
    if isinstance(value, decimal.Decimal):
        return "decimal", ("decimal", -value.as_tuple().exponent)
    if isinstance(value, (datetime.datetime, np.datetime64 if HAS_NUMPY else ())):
        return "timestamp", ("timestamp",)
    if isinstance(value, datetime.date):
        return "date", ("date",)
    if isinstance(value, dict):
        return "map", ("map", _ANY, _ANY)
    if isinstance(value, (list, tuple)) or (
        HAS_NUMPY and isinstance(value, np.ndarray)
    ):
        return "array", ("array", _ANY)
    raise TypeError(
        f"Cannot split on a value of type `{type(value).__name__}`: declare the feature type."
    )


def _canonical_row_bytes(values, types) -> bytes:
    """Serialize one row by its columns' declared offline types, so every engine produces the same bytes.

    A column without a declared type (None) is serialized by the runtime type of its values.
    """
    out = bytearray()
    for value, offline_type in zip(values, types, strict=True):
        _encode(value, _parse_type(offline_type), out)
    return bytes(out)


def _split_indexes(columns, types, salt: bytes, fractions) -> list[int]:
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
                    _canonical_row_bytes(row, types), key=salt, digest_size=8
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


def _split_columns(feature_view_obj, available) -> list[tuple[str, str | None]]:
    """The columns the split hashes, with their declared offline types.

    These are the feature view's features and labels in its order, so primary key, event time
    and helper columns a read adds do not change the split. Without a view, every column, untyped.
    """
    features = getattr(feature_view_obj, "features", None) or []
    columns = [
        (feature.name, feature.type)
        for feature in features
        if feature.name in available
        and not feature.inference_helper_column
        and not feature.training_helper_column
    ]
    return columns or [(name, None) for name in available]
