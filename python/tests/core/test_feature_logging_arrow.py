# Copyright 2026 Hopsworks AB. Licensed under the Apache License, Version 2.0.
"""Cross-language fixtures derived from the existing pandas and fastavro path."""

from __future__ import annotations

import base64
import copy
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest
from hopsworks_common.constants import FEATURE_LOGGING as FL
from hopsworks_common.core.feature_logging_arrow import (
    _SERVER_COLUMNS,
    _ArrowBatchBuilder,
    _select_arrow_builder,
)
from hsfs.core.feature_logging import LoggingMetaData
from hsfs.core.kafka_engine import _encode_row, _get_encoder_func
from hsfs.engine.python import Engine
from hsfs.feature_logger import FeatureLogger


pa = pytest.importorskip("pyarrow")

_TIME = datetime(2026, 9, 8, 12, 0, 0, 123456, tzinfo=timezone.utc)
_ID = "00000000-0000-4000-8000-000000000001"


def _fixture(parameters=True, hidden=True, types=False, parameter_data=None):
    definitions = [
        ("id", "bigint", "long"),
        ("amount", "double", "double"),
        ("amount_scaled", "double", "double"),
        ("predicted_label", "int", "int"),
        ("helper", "string", "string"),
        (
            "event_time",
            "timestamp",
            {"type": "long", "logicalType": "timestamp-micros"},
        ),
        ("items", "array<int>", {"type": "array", "items": ["null", "int"]}),
        (
            "details",
            "struct<name:string,score:double>",
            {
                "type": "record",
                "name": "details_record",
                "fields": [
                    {"name": "name", "type": ["null", "string"]},
                    {"name": "score", "type": ["null", "double"]},
                ],
            },
        ),
        ("request_parameters", "string", "string"),
        ("request_id", "string", "string"),
        ("log_time", "timestamp", {"type": "long", "logicalType": "timestamp-micros"}),
        ("log_id", "string", "string"),
        ("td_version", "int", "int"),
        ("model_name", "string", "string"),
        ("model_version", "string", "string"),
        ("deployment_name", "string", "string"),
        ("deployment_version", "int", "int"),
        ("deployment_schema_id", "string", "string"),
        ("request_row", "int", "int"),
    ]
    if types:
        definitions = [
            ("flag", "boolean", "boolean"),
            ("raw", "binary", "bytes"),
            ("single", "float", "float"),
            ("small", "smallint", "int"),
            ("day", "date", {"type": "int", "logicalType": "date"}),
            (
                "price",
                "decimal(12,2)",
                {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 12,
                    "scale": 2,
                },
            ),
            (
                "nested",
                "array<array<string>>",
                {
                    "type": "array",
                    "items": ["null", {"type": "array", "items": ["null", "string"]}],
                },
            ),
        ] + definitions
    features = [SimpleNamespace(name=name, type=kind) for name, kind, _ in definitions]
    extras = features[-4:]
    fv = SimpleNamespace(
        feature_logging=SimpleNamespace(
            untransformed_features=SimpleNamespace(columns=features),
            transformed_features=None,
            extra_logging_columns=extras,
        ),
        _get_transformed_feature_names=lambda td: ["amount_scaled"],
        _get_untransformed_feature_names=lambda td: ["amount"],
        _get_label_column_names=lambda td: ["label"],
        _required_serving_key_names=["id"],
        inference_helper_columns=["helper"],
        request_parameters=["factor", "description"] if parameters else [],
        _root_feature_group_event_time_column_name="event_time",
    )
    frame = pd.DataFrame(
        {
            "amount_scaled": [0.5, 1.0],
            "items": [[1, None, 3], None],
            "details": [{"name": "å", "score": 2.5}, None],
        }
    )
    metadata = LoggingMetaData()
    if types:
        frame["flag"] = [False, None]
        frame["raw"] = [b"\x00\xff", b""]
        frame["single"] = [1.125, None]
        frame["small"] = [-32768, 32767]
        frame["day"] = ["2026-09-08", None]
        frame["price"] = [Decimal("123456.78"), Decimal("-0.01")]
        frame["nested"] = [[["", None, "å"], [], None], []]
        frame["items"] = [[], [None]]
    metadata.transformed_features = [[0.5], [1.0]]
    metadata.untransformed_features = [[10.0], [20.0]]
    metadata.serving_keys = [{"id": 9007199254740993}, {"id": 2}]
    metadata.inference_helper = [["observed"], [None]]
    metadata.event_time = [[_TIME], [_TIME]]
    metadata.request_parameters = (
        [{"factor": 1.0, "description": "å"}, {"factor": None, "description": "two"}]
        if parameters
        else None
    )
    if parameter_data is not None:
        fv.request_parameters = list(parameter_data[0])
        metadata.request_parameters = parameter_data
    if hidden:
        object.__setattr__(frame, "hopsworks_logging_metadata", metadata)
    else:
        frame.attrs["untransformed"] = pd.DataFrame(
            {"amount": [10.0, 20.0], "id": [9007199254740993, 2]}
        )
        metadata = None
    predictions = [1, 0]
    extra = [
        {
            "deployment_name": "dep",
            "deployment_version": 2,
            "deployment_schema_id": "fixture-v1",
            "request_row": i,
        }
        for i in range(2)
    ]
    builder = _ArrowBatchBuilder(fv, 3)
    payload = builder._build(frame, predictions, extra)
    fixture_inputs = (builder, frame, predictions, extra)
    untransformed = frame.attrs.get("untransformed")
    components = dict(  # noqa: C408 - mirrors the engine keyword arguments
        logging_data=frame if hidden else None,
        logging_feature_group_features=features,
        logging_feature_group_feature_names=[f.name for f in features],
        logging_features=[
            f.name for f in features if f.name not in FL.LOGGING_METADATA_COLUMNS
        ],
        transformed_features=(
            getattr(metadata, "transformed_features", None) if hidden else frame,
            ["amount_scaled"],
            "transformed",
        ),
        untransformed_features=(
            getattr(metadata, "untransformed_features", None)
            if hidden
            else untransformed,
            ["amount"],
            "untransformed",
        ),
        predictions=(predictions, ["label"], "predictions"),
        serving_keys=(getattr(metadata, "serving_keys", None), ["id"], "serving"),
        helper_columns=(
            getattr(metadata, "inference_helper", None),
            ["helper"],
            "helper",
        ),
        request_parameters=(
            getattr(metadata, "request_parameters", None),
            fv.request_parameters,
            "parameters",
        ),
        event_time=(getattr(metadata, "event_time", None), ["event_time"], "event"),
        request_id=(["request-1"], ["request_id"], "request"),
        extra_logging_features=(extra, [f.name for f in extras], "extra"),
        td_col_name="td_version",
        time_col_name="log_time",
        model_col_name="model_name",
        training_dataset_version=3,
    )
    with (
        patch("hsfs.engine.python.datetime") as clock,
        patch("hsfs.engine.python.uuid.uuid4", return_value=_ID),
    ):
        clock.now.return_value = _TIME.replace(tzinfo=None)
        baseline, _, _ = Engine.__new__(Engine)._get_feature_logging_df(**components)
    rows = baseline.to_dict("records")
    schema = {
        "type": "record",
        "name": "feature_log",
        "fields": [
            {"name": name, "type": ["null", avro]} for name, _, avro in definitions
        ],
    }
    outer = copy.deepcopy(schema)
    complex_writers = {}
    for field in outer["fields"]:
        if next(f.type for f in features if f.name == field["name"]).startswith(
            ("array<", "struct<")
        ):
            complex_writers[field["name"]] = _get_encoder_func(
                json.dumps(field["type"])
            )
            field["type"] = ["null", "bytes"]
    writer = _get_encoder_func(json.dumps(outer))
    # The Arrow path fixes missing temporal values: the old encoder hands NaT
    # to fastavro. Normalize that known defect in the model-less fixture.
    for row in rows:
        for name, value in row.items():
            if value is pd.NaT:
                row[name] = None
    encoded = [_encode_row(complex_writers, writer, copy.deepcopy(row)) for row in rows]
    attributes = {
        "hopsprojectid": "1",
        "hopsfsid": "2",
        "hopsfeatureview": "fv",
        "hopsfvversion": "1",
        "hopstdversion": "3",
        "hopsschemaid": "fixture-v1",
        "hopsrequestid": "request-1",
        "hopsdeplname": "dep",
        "hopsdeplversion": "2",
    }
    fixture = {
        "baseline": "hopsworks-api ea0bc843b819ce808eec9457782b54d4453db9a9, TZ=UTC; NaT normalized to null",
        "dependencies": {
            name: version(name) for name in ("pandas", "pyarrow", "fastavro")
        },
        "time": _TIME.isoformat(),
        "logId": _ID,
        "attributes": attributes,
        "metadata": {
            "untransformedLogFg": {
                "id": 3,
                "name": "feature_log",
                "version": 1,
                "onlineTopicName": "feature-topic",
                "features": [vars(f) for f in features],
            },
            "extraLoggingColumns": [vars(f) for f in extras],
        },
        "subject": {"id": 4, "schema": json.dumps(schema)},
        "arrow": base64.b64encode(payload).decode(),
        "avro": [base64.b64encode(row).decode() for row in encoded],
    }
    fixture["_inputs"] = fixture_inputs
    return fixture, rows


@pytest.mark.parametrize(
    "parameters",
    [
        [{"factor": 2.0}, {"factor": 3.0}],
        [{"integer": 1, "fraction": 2.5}, {"integer": 2, "fraction": 3.5}],
        [{"large": 9007199254740993}, {"large": 2}],
        [{"factor": 2}, {"factor": None}],
        [{"enabled": True}, {"enabled": False}],
        [{"enabled": True, "factor": 2}, {"enabled": False, "factor": 3}],
        [{"text": "å", "factor": 2.0}, {"text": None, "factor": 3.0}],
        [{"text": "å", "large": 9007199254740993}, {"text": None, "large": 2}],
        [{"text": "x", "factor": float("nan")}, {"text": "y", "factor": 3.0}],
        [{"text": "x", "enabled": True}, {"text": "y", "enabled": False}],
        [{"text": "x", "optional": pd.NA}, {"text": "y", "optional": 2}],
    ],
)
def test_request_parameter_json_matches_baseline(parameters):
    fixture, expected = _fixture(parameter_data=parameters)
    batch = pa.ipc.open_stream(base64.b64decode(fixture["arrow"])).read_all()
    assert batch["request_parameters"].to_pylist() == [
        row["request_parameters"] for row in expected
    ]


@pytest.mark.parametrize(
    "parameters,hidden,types",
    [
        (True, True, False),
        (False, True, False),
        (False, False, False),
        (True, True, True),
    ],
)
def test_columns_match_baseline(parameters, hidden, types):
    fixture, expected = _fixture(parameters, hidden, types)
    batch = (
        pa.ipc.open_stream(base64.b64decode(fixture["arrow"])).read_all().to_pylist()
    )
    for actual, baseline in zip(batch, expected, strict=True):
        assert set(actual) == set(baseline) - _SERVER_COLUMNS
        for name, value in actual.items():
            if isinstance(baseline[name], float) and pd.isna(baseline[name]):
                assert isinstance(value, float) and pd.isna(value)
                continue
            assert value == baseline[name], name
    assert expected[0]["model_version"] == "None"
    if parameters:
        assert (
            batch[0]["request_parameters"]
            == '{"factor": 1.0, "description": "\\u00e5"}'
        )
    else:
        assert expected[0]["request_parameters"] is None


def test_per_request_columns_ride_the_batch_when_supplied():
    from datetime import datetime, timezone

    fixture, expected = _fixture(True, True, False)
    builder, frame, predictions, extra = fixture["_inputs"]
    stamp = datetime(2026, 9, 12, 8, 0, 0, tzinfo=timezone.utc)

    batch = builder._build_batch(frame, predictions, extra, "request-7", stamp)

    assert set(batch.schema.names) == (set(expected[0]) - _SERVER_COLUMNS) | {
        "request_id",
        "log_time",
    }
    rows = batch.to_pylist()
    assert {row["request_id"] for row in rows} == {"request-7"}
    assert {pd.Timestamp(row["log_time"]).tz_localize(None) for row in rows} == {
        pd.Timestamp(stamp).tz_localize(None)
    }
    # Combining two requests keeps each request's own id per row.
    other = builder._build_batch(frame, predictions, extra, "request-8", stamp)
    combined = builder._combine([batch, other])
    assert combined.num_rows == 2 * len(rows)
    assert [row["request_id"] for row in combined.to_pylist()] == ["request-7"] * len(
        rows
    ) + ["request-8"] * len(rows)
    assert (
        pa.ipc.open_stream(builder._serialize(combined)).read_all().num_rows
        == combined.num_rows
    )


def test_inherited_interface_does_not_enable_arrow(monkeypatch):
    class RowsOnly(FeatureLogger):
        def init(self, feature_view):
            pass

        def log(self, **kwargs):
            pass

    monkeypatch.setenv("HOPSWORKS_INFERENCE_LOGGER_CAPABILITIES", "features-arrow-v1")
    assert _select_arrow_builder(None, None, RowsOnly())[0] is None


def test_no_capability_does_not_probe_logger(monkeypatch):
    monkeypatch.delenv("HOPSWORKS_INFERENCE_LOGGER_CAPABILITIES", raising=False)
    assert _select_arrow_builder(None, None, object())[0] is None


if __name__ == "__main__":
    import sys

    os.environ["TZ"] = "UTC"
    time.tzset()
    destination = Path(sys.argv[1])
    destination.mkdir(parents=True, exist_ok=True)
    for name, parameters, hidden, types in [
        ("parameters", True, True, False),
        ("no_parameters", False, True, False),
        ("untransformed_frame", False, False, False),
        ("types", True, True, True),
    ]:
        fixture, _ = _fixture(parameters, hidden, types)
        (destination / (name + ".json")).write_text(
            json.dumps(fixture, indent=2) + "\n"
        )
