# Copyright 2026 Hopsworks AB. Licensed under the Apache License, Version 2.0.
"""Column-oriented transport for the default predictor's pinned logging group."""

from __future__ import annotations

import json
import math
import os

from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core.feature_logging_buffer import _positive_env
from hopsworks_common.decorators import _uses_pyarrow


_SERVER_COLUMNS = {
    "log_time",
    "log_id",
    "td_version",
    "model_name",
    "model_version",
    "request_id",
    "deployment_name",
    "deployment_version",
    "deployment_schema_id",
}

# Columns that differ between the requests coalesced into one post, so the
# batch carries them per row; the sidecar prefers the column over its event
# headers when it is present.
_PER_REQUEST_COLUMNS = ("request_id", "log_time")


class _ArrowBatchBuilder:
    @_uses_pyarrow
    def __init__(self, feature_view, training_dataset_version):
        import pyarrow as pa
        from hopsworks_common.core.type_systems import (
            _convert_offline_type_to_pyarrow_type,
        )

        logging = feature_view.feature_logging
        if (
            logging.transformed_features is not None
            or logging.untransformed_features is None
        ):
            raise ValueError("Arrow logging requires a single logging group")
        self._fv = feature_view
        self._td = training_dataset_version
        self._features = list(logging.untransformed_features.columns)
        extras = {f.name for f in logging.extra_logging_columns or []}
        reserved_types = {
            "deployment_name": "string",
            "deployment_version": "integer",
            "deployment_schema_id": "string",
            "request_row": "integer",
        }
        self._types = {}
        for feature in self._features:
            if "map<" in feature.type.lower():
                raise ValueError("Map logging uses the legacy transport")
            dtype = _convert_offline_type_to_pyarrow_type(feature.type)
            if feature.name in extras and feature.name in reserved_types:
                expected = reserved_types[feature.name]
                if not (
                    pa.types.is_string(dtype)
                    if expected == "string"
                    else pa.types.is_integer(dtype)
                ):
                    raise ValueError("Unsupported reserved logging column type")
            server_owned = (
                feature.name in _SERVER_COLUMNS
                and feature.name not in _PER_REQUEST_COLUMNS
                and (
                    feature.name
                    not in (
                        "deployment_name",
                        "deployment_version",
                        "deployment_schema_id",
                    )
                    or feature.name in extras
                )
            )
            if not server_owned:
                self._types[feature.name] = dtype
        self._transformed = feature_view._get_transformed_feature_names(
            training_dataset_version
        )
        self._untransformed = feature_view._get_untransformed_feature_names(
            training_dataset_version
        )
        self._labels = list(
            feature_view._get_label_column_names(training_dataset_version)
        )
        self._max_bytes = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_MAX_EVENT_BYTES", 8 * 1024 * 1024
        )

    @_uses_pyarrow
    def _build(self, frame, predictions, extra, request_id=None, log_time=None):
        """One request as one Arrow IPC stream, for callers that post per request."""
        return self._serialize(
            self._build_batch(frame, predictions, extra, request_id, log_time)
        )

    @_uses_pyarrow
    def _serialize(self, batch):
        import pyarrow as pa

        if batch.nbytes > self._max_bytes:
            raise ValueError("Feature logging event exceeds the byte limit")
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(
            sink, batch.schema, options=pa.ipc.IpcWriteOptions(compression=None)
        ) as writer:
            writer.write_batch(batch)
        payload = sink.getvalue()
        if payload.size > self._max_bytes:
            raise ValueError("Feature logging event exceeds the byte limit")
        return payload.to_pybytes()

    @_uses_pyarrow
    def _complete(self, batch, values):
        """Add the logging group's server-owned columns to a batch bound for a file.

        The inference logger fills `model_name`, `model_version`, `td_version` and the declared deployment columns from the event headers; a chunk staged for the commit job has no headers, so the same values ride the batch, typed as the group declares them.
        `values` maps column name to the constant for this request group; a name without a value is null.
        """
        import pyarrow as pa
        from hopsworks_common.core.type_systems import (
            _convert_offline_type_to_pyarrow_type,
        )

        for feature in self._features:
            if feature.name in self._types or feature.name == "log_id":
                continue
            if feature.name in batch.schema.names:
                continue
            dtype = _convert_offline_type_to_pyarrow_type(feature.type)
            value = values.get(feature.name)
            if value is not None and pa.types.is_string(dtype):
                value = str(value)
            elif isinstance(value, str) and pa.types.is_integer(dtype):
                # The deployment version arrives from the environment as text.
                value = int(value) if value.strip().lstrip("-").isdigit() else None
            batch = batch.append_column(
                feature.name, pa.array([value] * batch.num_rows, type=dtype)
            )
        return batch

    @staticmethod
    @_uses_pyarrow
    def _combine(batches):
        """Several requests' batches as one, which is what the sidecar accepts per post."""
        import pyarrow as pa

        if len(batches) == 1:
            return batches[0]
        combined = pa.Table.from_batches(batches).combine_chunks().to_batches()
        if len(combined) != 1:
            raise ValueError("Feature logging batches could not be combined")
        return combined[0]

    @_uses_pyarrow
    def _build_batch(self, frame, predictions, extra, request_id=None, log_time=None):
        import pandas as pd
        import pyarrow as pa
        from hopsworks_common.core.type_systems import _cast_column_to_offline_type

        count = len(frame)
        if not count:
            raise ValueError("Empty feature logging batch")
        columns = {}

        def merge(data, names=()):
            if data is None or len(data) == 0:
                return
            if isinstance(data, pd.DataFrame):
                source = {name: data[name] for name in data.columns}
            elif isinstance(data[0], dict):
                source = {
                    name: [row.get(name) for row in data]
                    for name in dict.fromkeys(key for row in data for key in row)
                }
            elif isinstance(data[0], (list, tuple)) or hasattr(data[0], "shape"):
                if len(data[0]) != len(names):
                    raise ValueError("Logging component width mismatch")
                source = dict(zip(names, zip(*data, strict=True), strict=True))
            else:
                if len(names) != 1:
                    raise ValueError("Logging component width mismatch")
                source = {names[0]: data}
            for name, values in source.items():
                size = len(values)
                if size not in (1, count):
                    raise ValueError("Logging component row count mismatch")
                if size == 1 and count > 1:
                    if name not in columns:
                        columns[name] = [
                            values.iloc[0]
                            if isinstance(values, pd.Series)
                            else values[0]
                        ] * count
                elif name not in columns or any(
                    value is not None
                    and value is not pd.NA
                    and value is not pd.NaT
                    and not (isinstance(value, float) and math.isnan(value))
                    for value in values
                ):
                    columns[name] = values

        untransformed = frame.attrs.get("untransformed")
        metadata = (
            getattr(frame, "hopsworks_logging_metadata", None)
            if untransformed is None
            else None
        )
        if untransformed is None:
            merge(frame)
        merge(
            frame
            if untransformed is not None
            else getattr(metadata, "transformed_features", None),
            self._transformed,
        )
        merge(
            untransformed
            if untransformed is not None
            else getattr(metadata, "untransformed_features", None),
            self._untransformed,
        )
        merge(predictions, self._labels)
        merge(
            getattr(metadata, "serving_keys", None),
            self._fv._required_serving_key_names,
        )
        merge(
            getattr(metadata, "inference_helper", None),
            self._fv.inference_helper_columns,
        )
        parameters = getattr(metadata, "request_parameters", None)
        merge(parameters, self._fv.request_parameters)
        event_name = self._fv._root_feature_group_event_time_column_name
        if event_name:
            merge(getattr(metadata, "event_time", None), [event_name])
        merge(extra)
        for name in self._labels:
            if name in columns:
                columns["predicted_" + name] = columns.pop(name)

        # pandas' row coercion and json.dumps formatting are part of the stored
        # request_parameters string contract. Only this small component uses a frame.
        if self._fv.request_parameters and "request_parameters" not in columns:
            if parameters is None:
                params = pd.DataFrame(columns=self._fv.request_parameters)
            elif isinstance(parameters, pd.DataFrame):
                params = parameters.copy(deep=False).reset_index(drop=True)
            elif parameters and isinstance(parameters[0], dict):
                params = pd.DataFrame(parameters)
            else:
                params = pd.DataFrame(parameters, columns=self._fv.request_parameters)
            for name in self._fv.request_parameters:
                if params.empty or name not in params or params[name].isna().all():
                    params[name] = columns.get(name)
            parameter_values = params.to_numpy()
            if params.empty:
                values = ["{}"] * count
            elif parameter_values.dtype.kind in "biuf" or all(
                value is None or type(value) in (str, int, float, bool)
                for value in parameter_values.flat
            ):
                # The common numeric dtype preserves apply(axis=1)'s integer
                # to float coercion. Native mixed values require no pandas boxing.
                names = list(params.columns)
                values = [
                    json.dumps(dict(zip(names, row, strict=True)))
                    for row in parameter_values.tolist()
                ]
                values = values[:count] + [float("nan")] * max(0, count - len(values))
            else:
                values = params.apply(lambda row: json.dumps(row.to_dict()), axis=1)
                values = values.reindex(range(count))
            columns["request_parameters"] = values

        # Per-request columns ride the batch only when the caller supplies them;
        # a caller that posts per request leaves them to the sidecar's headers.
        supplied = set()
        if request_id is not None and "request_id" in self._types:
            columns["request_id"] = [str(request_id)] * count
            supplied.add("request_id")
        if log_time is not None and "log_time" in self._types:
            columns["log_time"] = [log_time] * count
            supplied.add("log_time")

        arrays, names = [], []
        for feature in self._features:
            if feature.name not in self._types:
                continue
            if feature.name in _PER_REQUEST_COLUMNS and feature.name not in supplied:
                continue
            values = columns.get(feature.name, [None] * count)
            dtype = self._types[feature.name]
            if pa.types.is_string(dtype):
                values = [str(value) if value is not None else None for value in values]
            if pa.types.is_integer(dtype) and any(
                isinstance(value, float)
                and not math.isnan(value)
                and (not math.isfinite(value) or not value.is_integer())
                for value in values
            ):
                raise ValueError("Non-integral logging value")
            try:
                if pa.types.is_date(dtype):
                    values = pd.to_datetime(values, utc=True)
                    values = (
                        values.dt.date if isinstance(values, pd.Series) else values.date
                    )
                array = pa.array(values, type=dtype, from_pandas=True)
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
                series = values if isinstance(values, pd.Series) else pd.Series(values)
                series = _cast_column_to_offline_type(series, feature.type)
                array = pa.array(series, type=dtype, from_pandas=True)
            arrays.append(array)
            names.append(feature.name)
        batch = pa.RecordBatch.from_arrays(arrays, names=names)
        if batch.nbytes > self._max_bytes:
            raise ValueError("Feature logging event exceeds the byte limit")
        return batch


def _select_arrow_builder(feature_view, training_dataset_version, logger):
    if "features-arrow-v1" not in os.environ.get(
        "HOPSWORKS_INFERENCE_LOGGER_CAPABILITIES", ""
    ).split(","):
        return None, "sidecar capability absent"
    provider = next(
        (base for base in type(logger).__mro__ if "log_batch" in base.__dict__), None
    )
    if provider is None or (
        provider.__module__ == "hsfs.feature_logger"
        and provider.__name__ == "FeatureLogger"
    ):
        return None, "logger has no batch implementation"
    try:
        return _ArrowBatchBuilder(
            feature_view, training_dataset_version
        ), "features-arrow-v1"
    except (
        ImportError,
        ValueError,
        TypeError,
        AttributeError,
        FeatureStoreException,
    ) as error:
        return None, f"{type(error).__name__}: {error}"
