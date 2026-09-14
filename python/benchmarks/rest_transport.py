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
"""What the calling thread spends to send one online store request.

The specification gates replacing Requests on a direct urllib3 client cutting
calling-thread CPU by at least 25 percent against cached-URL Requests. This
measures that: the same request, to the same local server, over a warm
connection, through each transport.

A loopback server answers so both transports do real socket work, and it is the
same server for both, so what differs is the client. CPU is process time, which
is what the gate is about; wall time is reported beside it because a serving
thread waits for both.

    python -m benchmarks.rest_transport --json transports.json
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
import requests.adapters
import urllib3


PAYLOAD = json.dumps(
    {"featureStoreName": "fs", "featureViewName": "fv", "entries": {"id": 1}}
).encode()


def _body(rows: int, columns: int) -> bytes:
    """A response the size a feature vector read returns."""
    return json.dumps(
        {"features": [[i] * columns for i in range(rows)], "status": "COMPLETE"}
    ).encode()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    # Without this the reply leaves in several small writes and loopback pays a
    # delayed acknowledgement for it, which puts 40 ms of waiting into every
    # call and measures the kernel rather than the client.
    disable_nagle_algorithm = True
    body = b"{}"

    def do_POST(self):  # noqa: N802 - the name http.server dispatches on
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        head = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(self.body)}\r\n\r\n"
        ).encode()
        # One write, for the same reason.
        self.wfile.write(head + self.body)

    def log_message(self, *_args):
        pass


def _serve(body: bytes):
    _Handler.body = body
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}/0.1.0/batch_feature_store"


def _measure(call, iterations: int) -> dict:
    for _ in range(50):
        call()
    wall = []
    # Thread time, not process time: the server answers on its own threads in
    # this process, and its work is identical for both transports, so counting
    # it would measure the same thing twice and hide the difference.
    cpu_started = time.thread_time()
    started = time.perf_counter()
    for _ in range(iterations):
        at = time.perf_counter()
        call()
        wall.append((time.perf_counter() - at) * 1000)
    cpu = (time.thread_time() - cpu_started) / iterations * 1000
    elapsed = time.perf_counter() - started
    wall.sort()
    return {
        "cpu_ms": round(cpu, 5),
        "p50_ms": round(statistics.median(wall), 4),
        "p99_ms": round(wall[min(len(wall) - 1, int(len(wall) * 0.99))], 4),
        "requests_per_second": round(iterations / elapsed, 1),
        "iterations": iterations,
    }


def _requests_call(url: str, headers: dict):
    """Requests over a warm session, with the URL already rendered."""
    session = requests.Session()
    session.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=4))

    def call():
        return session.post(url, data=PAYLOAD, headers=headers, timeout=5).content

    return call


def _urllib3_call(url: str, headers: dict):
    """urllib3 directly, which is the pool Requests is a layer over."""
    pool = urllib3.PoolManager(maxsize=4)

    def call():
        return pool.request(
            "POST", url, body=PAYLOAD, headers=headers, timeout=5.0
        ).data

    return call


def _case(rows: int, columns: int, iterations: int) -> dict:
    server, url = _serve(_body(rows, columns))
    headers = {"Content-Type": "application/json", "X-API-KEY": "benchmark"}
    try:
        with_requests = _measure(_requests_call(url, headers), iterations)
        with_urllib3 = _measure(_urllib3_call(url, headers), iterations)
    finally:
        server.shutdown()
        server.server_close()
    saved = (
        (with_requests["cpu_ms"] - with_urllib3["cpu_ms"]) / with_requests["cpu_ms"]
        if with_requests["cpu_ms"]
        else 0.0
    )
    return {
        "rows": rows,
        "columns": columns,
        "response_bytes": len(_body(rows, columns)),
        "requests": with_requests,
        "urllib3": with_urllib3,
        "urllib3_cpu_saving_percent": round(saved * 100, 1),
        "meets_25_percent_gate": saved >= 0.25,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args(argv)

    report = {
        "python": sys.version.split()[0],
        "requests_version": requests.__version__,
        "urllib3_version": urllib3.__version__,
        "cases": [
            _case(1, 32, args.iterations),
            _case(100, 32, max(args.iterations // 2, 1)),
            _case(512, 32, max(args.iterations // 10, 1)),
        ],
    }
    rendered = json.dumps(report, indent=2)
    if args.json:
        args.json.write_text(rendered + "\n")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
