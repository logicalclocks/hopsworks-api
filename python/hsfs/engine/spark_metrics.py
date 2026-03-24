#
#   Copyright 2025 Hopsworks AB
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
"""Spark stage metrics via the Spark UI REST API.

Queries ``http://localhost:{port}/api/v1/`` directly (not the Hopsworks
MembraneProxyServlet) to collect stage-level metrics after each operation.
"""

from __future__ import annotations

import json
import logging
import socket
import time
import urllib.request
from contextlib import contextmanager


_logger = logging.getLogger(__name__)


def _format_bytes(n: int) -> str:
    """Return a human-readable byte string."""
    if n < 1024:
        return f"{n} bytes"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    return f"{n / (1024 * 1024 * 1024):.1f} GB"


def _find_spark_ui_port(start: int = 4040, end: int = 4100) -> int | None:
    """Find the port where the Spark UI is listening."""
    for port in range(start, end + 1):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect(("localhost", port))
            s.close()
            return port
        except OSError:
            continue
    return None


class SparkStageMetrics:
    """Collects and reports Spark stage metrics via the REST API.

    Always connects to ``localhost`` to reach the Spark UI directly,
    bypassing any Hopsworks proxy.
    """

    def __init__(self, spark_session) -> None:
        self._spark = spark_session
        self._app_id: str | None = None
        self._base_url: str | None = None
        self._stage_snapshot: int = 0
        self._available = True

        self._discover()

    def _discover(self) -> None:
        """Discover the Spark UI REST API endpoint."""
        try:
            port_str = self._spark.conf.get("spark.ui.port", None)
            port = int(port_str) if port_str else None
        except Exception:
            port = None

        if port is None:
            port = _find_spark_ui_port()

        if port is None:
            _logger.debug("Spark UI not found on ports 4040-4100; metrics disabled.")
            self._available = False
            return

        self._base_url = f"http://localhost:{port}/api/v1"

        try:
            with urllib.request.urlopen(
                f"{self._base_url}/applications/", timeout=2
            ) as resp:
                apps = json.loads(resp.read().decode())
            if apps:
                self._app_id = apps[0]["id"]
            else:
                self._available = False
        except Exception:
            _logger.debug("Could not reach Spark UI REST API; metrics disabled.")
            self._available = False

    def snapshot(self) -> None:
        """Capture the current number of completed stages."""
        if not self._available:
            return
        try:
            stages = self._fetch_stages()
            self._stage_snapshot = len(stages)
        except Exception:
            pass

    def report(self, label: str = "") -> str | None:
        """Fetch stages completed since the last snapshot and print a report.

        Parameters:
            label: Optional label to include in the report header.

        Returns:
            The formatted report string, or `None` if unavailable.
        """
        if not self._available:
            return None

        try:
            stages = self._fetch_stages()
        except Exception:
            return None

        new_stages = stages[self._stage_snapshot :]
        if not new_stages:
            return None

        lines = []
        header = f"=== Spark Stage Metrics{f' ({label})' if label else ''} ==="
        lines.append(header)

        total_duration_ms = 0
        for s in new_stages:
            stage_id = s.get("stageId", "?")
            name = s.get("name", "")
            duration_ms = s.get("executorRunTime", 0)
            total_duration_ms += duration_ms
            duration_s = duration_ms / 1000.0

            input_records = s.get("inputRecords", 0)
            output_records = s.get("outputRecords", 0)
            shuffle_read = s.get("shuffleReadBytes", 0)
            shuffle_write = s.get("shuffleWriteBytes", 0)
            spill_memory = s.get("memoryBytesSpilled", 0)
            spill_disk = s.get("diskBytesSpilled", 0)

            lines.append(f"Stage {stage_id} ({name}): {duration_s:.1f}s")
            lines.append(f"  Records: {input_records:,} in / {output_records:,} out")
            if shuffle_read or shuffle_write:
                lines.append(
                    f"  Shuffle: {_format_bytes(shuffle_read)} read, "
                    f"{_format_bytes(shuffle_write)} write"
                )
            if spill_memory or spill_disk:
                lines.append(
                    f"  Spill: {_format_bytes(spill_memory)} memory, "
                    f"{_format_bytes(spill_disk)} disk"
                )

        total_s = total_duration_ms / 1000.0
        lines.append("=" * len(header))
        lines.append(f"Total executor time: {total_s:.1f}s")

        report_str = "\n".join(lines)
        print(report_str)

        self._stage_snapshot = len(stages)
        return report_str

    @contextmanager
    def measure(self, label: str = ""):
        """Context manager that snapshots before and reports after.

        Parameters:
            label: Optional label to include in the report header.
        """
        self.snapshot()
        wall_start = time.time()
        try:
            yield
        finally:
            wall_elapsed = time.time() - wall_start
            report = self.report(label)
            if report:
                print(f"Wall time: {wall_elapsed:.1f}s")

    def _fetch_stages(self) -> list[dict]:
        """Fetch all completed stages from the REST API."""
        url = f"{self._base_url}/applications/{self._app_id}/stages?status=complete"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode())
