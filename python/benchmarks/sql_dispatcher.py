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
"""What the SQL dispatcher costs a caller, measured against simulated I/O.

The real `AsyncTaskThread` runs, with the database replaced by `asyncio.sleep`
of a fixed duration. That isolates what the client adds, at the price of saying
nothing about MySQL: the numbers here are scheduling behaviour, not cluster
latency. Two things are counted that do not depend on the simulation at all,
and those are the point of the benchmark:

* how many liveness round trips a run of N reads performs, and
* how many reads are ever in flight together.

Run it on two revisions and compare:

    python -m benchmarks.sql_dispatcher --json before.json

Written so it runs unchanged on a revision that predates the bounded-concurrency
arguments, which is what makes a before/after comparison possible.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import inspect
import json
import statistics
import sys
import threading
import time
from pathlib import Path

from hopsworks_common.util import AsyncTask, AsyncTaskThread


PING_SECONDS = 0.010
QUERY_SECONDS = 0.010


class _Counters:
    """Counts what the dispatcher did, independently of the simulated timings."""

    def __init__(self):
        self.pings = 0
        self.in_flight = 0
        self.peak_in_flight = 0
        self._lock = threading.Lock()

    async def ping(self, _pool) -> None:
        with self._lock:
            self.pings += 1
        await asyncio.sleep(PING_SECONDS)

    async def query(self, connection_pool=None) -> int:
        with self._lock:
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(QUERY_SECONDS)
        finally:
            with self._lock:
                self.in_flight -= 1
        return 1


async def _pool(*_args):
    return object()


_RETRY_FLAG_SUPPORTED = (
    "retry_on_connection_error" in inspect.signature(AsyncTask.__init__).parameters
)


def _read(counters: _Counters) -> AsyncTask:
    """A pooled read, marked repeatable where the revision under test supports it.

    The flag is checked against the signature rather than caught as a
    `TypeError`, because `AsyncTask` forwards unknown keyword arguments to the
    task function instead of rejecting them.
    """
    flag = {"retry_on_connection_error": True} if _RETRY_FLAG_SUPPORTED else {}
    return AsyncTask(
        task_function=counters.query, requires_connection_pool=True, **flag
    )


def measure(callers: int, reads_per_caller: int) -> dict:
    """Drive `callers` threads through one shared dispatcher, as a serving process does."""
    counters = _Counters()
    thread = AsyncTaskThread(
        connection_pool_initializer=_pool, connection_test=counters.ping
    )
    thread.start()
    start_together = threading.Barrier(callers)
    latencies: list[float] = []
    latencies_lock = threading.Lock()

    def caller(_index: int) -> None:
        start_together.wait()
        for _ in range(reads_per_caller):
            started = time.perf_counter()
            thread._submit(_read(counters))
            with latencies_lock:
                latencies.append((time.perf_counter() - started) * 1000)

    started_at = time.perf_counter()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=callers) as pool:
            list(pool.map(caller, range(callers)))
        elapsed = time.perf_counter() - started_at
    finally:
        thread._shutdown()

    reads = callers * reads_per_caller
    ordered = sorted(latencies)
    return {
        "callers": callers,
        "reads": reads,
        "wall_seconds": round(elapsed, 4),
        "reads_per_second": round(reads / elapsed, 1),
        "latency_ms": {
            "p50": round(statistics.median(ordered), 2),
            "p95": round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 2),
            "max": round(ordered[-1], 2),
        },
        "liveness_round_trips": counters.pings,
        "peak_reads_in_flight": counters.peak_in_flight,
        "simulated_ping_ms": PING_SECONDS * 1000,
        "simulated_query_ms": QUERY_SECONDS * 1000,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--callers", type=int, nargs="+", default=[1, 4, 16])
    parser.add_argument("--reads-per-caller", type=int, default=10)
    parser.add_argument("--json", type=Path, help="Write the results to this file")
    args = parser.parse_args(argv)

    report = {
        "python": sys.version.split()[0],
        "cases": [measure(callers, args.reads_per_caller) for callers in args.callers],
    }
    rendered = json.dumps(report, indent=2)
    if args.json:
        args.json.write_text(rendered + "\n")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
