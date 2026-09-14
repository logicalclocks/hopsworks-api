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
"""What a deployment request costs the client before it reaches the socket.

Encoding and validating a batch against a deployment schema is pure client CPU,
paid on the request path of every prediction. It is measured here against an
already constructed schema and JSON-native values, so the numbers are the
library's own work rather than the cost of the payload itself: `json_only` is
the floor, what `json.dumps` alone costs for the same rows.

The nested case replaces one scalar field with an eight-element array of
structs, which is where re-parsing a type string per value used to show.

    python -m benchmarks.deployment_schema --json before.json

Written to run unchanged on a revision that predates the compiled checks.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path


try:
    from hsml.deployment_schema import DeploymentSchema, _encode_instances
except ImportError:  # the deployment domain moved into a package
    from hsml.deployment.schema import DeploymentSchema, _encode_instances


FIELDS = 32
NESTED_TYPE = "array<struct<name:string,score:double>>"


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
        "p50_ms": round(statistics.median(samples), 4),
        "p95_ms": round(samples[min(len(samples) - 1, int(len(samples) * 0.95))], 4),
        "iterations": iterations,
    }


def _case(nested: bool, rows: int) -> dict:
    fields = [{"name": f"v{i}", "type": "double"} for i in range(FIELDS - 1)]
    row = {"id": 1, **{f"v{i}": i * 0.5 for i in range(FIELDS - 1)}}
    if nested:
        fields[-1] = {"name": f"v{FIELDS - 2}", "type": NESTED_TYPE}
        row[f"v{FIELDS - 2}"] = [{"name": "n", "score": 1.5}] * 8
    schema = DeploymentSchema(
        serving_keys=[{"name": "id", "type": "bigint"}],
        passed_features=fields,
        max_batch_rows=512,
    )
    batch = [dict(row) for _ in range(rows)]
    iterations = 1000 if rows < 512 else 100

    def encode_validate_serialize():
        encoded = _encode_instances(batch)
        schema._raise_if_invalid(encoded, "benchmark")
        return json.dumps({"instances": encoded})

    def validate_only():
        schema._raise_if_invalid(batch, "benchmark")

    return {
        "nested": nested,
        "rows": rows,
        "fields": FIELDS,
        "encode_validate_serialize": _timed(encode_validate_serialize, iterations),
        "validate_only": _timed(validate_only, iterations),
        "json_only": _timed(lambda: json.dumps({"instances": batch}), iterations),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, nargs="+", default=[1, 10, 512])
    parser.add_argument("--json", type=Path, help="Write the results to this file")
    args = parser.parse_args(argv)

    report = {
        "python": sys.version.split()[0],
        "cases": [
            _case(nested, rows) for nested in (False, True) for rows in args.rows
        ],
    }
    rendered = json.dumps(report, indent=2)
    if args.json:
        args.json.write_text(rendered + "\n")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
