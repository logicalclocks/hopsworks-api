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
"""What a REST feature-vector read costs the client around the request.

Three pieces of per-call work, each measured on its own with the network left
out, so the numbers are the library's and not the cluster's:

* `url`, rendering the endpoint address a request is sent to,
* `decode`, turning one RDRS wire row into the caller's row, which is where
  date and binary columns are paid for, and
* `assemble`, the whole of a batch response becoming feature vectors.

    python -m benchmarks.rest_feature_vectors --json before.json

Written to run unchanged on a revision that predates any of the three changes, which is what makes a before and after comparison possible.
"""

from __future__ import annotations

import argparse
import functools
import inspect
import json
import statistics
import sys
import time
from pathlib import Path

from furl import furl
from hopsworks_common.client.online_store_rest_client import (
    OnlineStoreRestClientSingleton,
)
from hsfs import feature_group as fg_mod
from hsfs import training_dataset_feature as tdf_mod
from hsfs.core import online_store_rest_client_engine, vector_server


FIELDS = 32
BATCH_ROWS = 512


def _timed(call, iterations: int) -> dict:
    for _ in range(10):
        call()
    samples = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        call()
        samples.append((time.perf_counter_ns() - started) / 1e6)
    samples.sort()
    return {
        "p50_ms": round(statistics.median(samples), 5),
        "p95_ms": round(samples[min(len(samples) - 1, int(len(samples) * 0.95))], 5),
        "iterations": iterations,
    }


# region Fixtures


def _feature(name: str, type_: str) -> tdf_mod.TrainingDatasetFeature:
    feature = tdf_mod.TrainingDatasetFeature(name=name, type=type_, label=False)
    feature.inference_helper_column = False
    feature.training_helper_column = False
    feature._feature_group = fg_mod.FeatureGroup(
        name="fg", version=1, featurestore_id=99, primary_key=[], id=11
    )
    return feature


def _schema(date_columns: int) -> list[tdf_mod.TrainingDatasetFeature]:
    """A view of `FIELDS` columns, the first `date_columns` of them dates."""
    return [
        _feature(f"f{index}", "date" if index < date_columns else "bigint")
        for index in range(FIELDS)
    ]


def _row(date_columns: int) -> list:
    return ["2026-03-04" if index < date_columns else index for index in range(FIELDS)]


def _engine(features) -> online_store_rest_client_engine.OnlineStoreRestClientEngine:
    return online_store_rest_client_engine.OnlineStoreRestClientEngine(
        feature_store_name="fs",
        feature_view_name="fv",
        feature_view_version=1,
        features=features,
    )


@functools.cache
def _accepted(method) -> frozenset | None:
    """Which arguments a revision's method declares, or `None` when it takes any keyword.

    Resolved once per method.
    Reading the signature costs tens of microseconds, which is more than some of the work measured here.
    """
    parameters = inspect.signature(method).parameters.values()
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters):
        # A driver that forwards `**kwargs` declares none of what it passes on, so filtering by name would drop all of it.
        return None
    return frozenset(p.name for p in parameters)


def _call_supported(method, /, **kwargs):
    """Call `method` with the arguments the revision under test declares.

    The assembly signature has grown arguments this benchmark has no opinion about, and an older revision rejects them by name.
    """
    accepted = _accepted(method)
    if accepted is None:
        return method(**kwargs)
    return method(**{k: v for k, v in kwargs.items() if k in accepted})


# endregion


def _url_case(iterations: int) -> dict:
    """Rendering one endpoint address, cached against rebuilt every time."""
    base = furl("https://rdrs.example.internal:4406/0.1.0")
    path_params = ["feature_store"]

    def build() -> str:
        built = base.copy()
        built.path.segments.extend(path_params)
        return built.url

    cached = None
    if hasattr(OnlineStoreRestClientSingleton, "_endpoint_url"):

        class _Stub:
            _base_url = base
            _endpoint_urls: dict = {}

            _endpoint_url = OnlineStoreRestClientSingleton._endpoint_url

        stub = _Stub()
        cached = _timed(lambda: stub._endpoint_url(path_params), iterations)

    return {
        "case": "url",
        "rebuilt_every_call": _timed(build, iterations),
        "as_shipped": cached,
    }


def _decode_case(date_columns: int, iterations: int) -> dict:
    """One wire row becoming the caller's row."""
    engine = _engine(_schema(date_columns))
    row = _row(date_columns)

    def convert():
        return engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            drop_missing=False,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )

    return {
        "case": "decode",
        "fields": FIELDS,
        "date_columns": date_columns,
        "per_row": _timed(convert, iterations),
    }


def _assemble_case(date_columns: int, rows: int, iterations: int) -> dict:
    """A whole batch response becoming feature vectors.

    The conversion and the assembly together, which is every piece of client work between the response body and the rows the caller is handed.
    """
    features = _schema(date_columns)
    engine = _engine(features)
    server = vector_server.VectorServer(
        feature_store_id=1,
        features=features,
        feature_store_name="fs",
        feature_view_name="fv",
        feature_view_version=1,
    )
    server._rest_client_engine = engine
    # Set by serving initialisation, which needs a live feature view.
    server._on_demand_feature_names = []
    response_rows = [_row(date_columns) for _ in range(rows)]

    def assemble():
        vectors = []
        for wire_row in response_rows:
            result = engine._convert_rdrs_response_to_feature_value_row(
                row_feature_values=list(wire_row),
                drop_missing=False,
                return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
            vectors.append(
                _call_supported(
                    server._assemble_feature_vector,
                    result_dict=result,
                    passed_values={},
                    vector_db_result={},
                    allow_missing=True,
                    client="rest",
                    transform=False,
                    on_demand_features=False,
                    request_parameters={},
                    transformation_context=None,
                    logging_meta_data=None,
                    n_processes=None,
                )
            )
        return vectors

    measured = _timed(assemble, iterations)
    return {
        "case": "assemble",
        "fields": FIELDS,
        "date_columns": date_columns,
        "rows": rows,
        "per_batch": measured,
        "per_row_us": round(measured["p50_ms"] * 1000 / rows, 3),
    }


def _serving_case(date_columns: int, rows: int, iterations: int) -> dict:
    """A batch read as a caller makes it, with only the network left out.

    This is the whole of the client's work for one `get_feature_vectors` call:
    the response becoming rows, and the rows becoming the caller's vectors.
    `response_only` is the floor, what the stubbed response costs to produce, since a fresh body has to be built for every call: decoding rewrites the row it is given.
    """
    from hsfs import feature_group as fg_mod
    from hsfs import serving_key as sk_mod

    features = _schema(date_columns)
    group = fg_mod.FeatureGroup(
        name="fg", version=1, featurestore_id=99, primary_key=["f0"], id=11
    )
    server = vector_server.VectorServer(
        feature_store_id=1,
        features=features,
        serving_keys=[
            sk_mod.ServingKey(feature_name="f0", join_index=0, feature_group=group)
        ],
        feature_store_name="fs",
        feature_view_name="fv",
        feature_view_version=1,
    )
    server._rest_client_engine = _engine(features)
    server._on_demand_feature_names = []
    # Set by serving initialisation, which needs a live feature view.
    server._transformed_feature_vector_col_name = [f.name for f in features]
    server._which_client_and_ensure_initialised = lambda **_kwargs: "rest"

    template = _row(date_columns)
    status = [[{"httpStatus": 200, "featureGroupId": 11}]] * rows

    def response(*_args, **_kwargs):
        return {
            "features": [list(template) for _ in range(rows)],
            "detailedStatus": status,
        }

    api = server._rest_client_engine._online_store_rest_client_api
    api._get_batch_raw_feature_vectors = response
    entries = [{"f0": index} for index in range(rows)]

    def read():
        # The public defaults.
        # A view with no transformation functions has nothing to apply whatever they say, and opting out of them would be measuring a call almost nobody makes.
        return _call_supported(
            server._get_feature_vectors,
            entries=entries,
            passed_features=[],
            vector_db_features=[],
            return_type="list",
            allow_missing=False,
            force_rest_client=True,
        )

    measured = _timed(read, iterations)
    vectors = read()
    assert len(vectors) == rows, f"benchmark read {len(vectors)} of {rows} rows"
    return {
        "case": "serving",
        "fields": FIELDS,
        "date_columns": date_columns,
        "rows": rows,
        "per_batch": measured,
        "response_only": _timed(response, iterations),
        "per_row_us": round(measured["p50_ms"] * 1000 / rows, 3),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=BATCH_ROWS)
    parser.add_argument("--json", type=Path, help="Write the results to this file")
    args = parser.parse_args(argv)

    report = {
        "python": sys.version.split()[0],
        "cases": [
            _url_case(20000),
            _decode_case(0, 5000),
            _decode_case(8, 5000),
            _assemble_case(0, args.rows, 50),
            _assemble_case(8, args.rows, 50),
            _serving_case(0, args.rows, 50),
            _serving_case(8, args.rows, 50),
        ],
    }
    rendered = json.dumps(report, indent=2)
    if args.json:
        args.json.write_text(rendered + "\n")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
