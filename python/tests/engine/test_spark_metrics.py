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
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from hsfs.engine.spark_metrics import SparkStageMetrics, _format_bytes


class TestFormatBytes:
    def test_bytes(self):
        assert _format_bytes(500) == "500 bytes"

    def test_kilobytes(self):
        assert _format_bytes(2048) == "2.0 KB"

    def test_megabytes(self):
        assert _format_bytes(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert _format_bytes(3 * 1024 * 1024 * 1024) == "3.0 GB"


class TestSparkStageMetrics:
    def _make_metrics(self, stages_data=None, port=4040, app_id="app-123"):
        """Create a SparkStageMetrics with mocked HTTP responses."""
        spark = MagicMock()
        spark.conf.get.return_value = str(port)

        apps_response = json.dumps([{"id": app_id}]).encode()
        stages_response = json.dumps(stages_data or []).encode()

        def mock_urlopen(url, timeout=None):
            resp = MagicMock()
            if "/applications/" in url and "/stages" not in url:
                resp.read.return_value = apps_response
            else:
                resp.read.return_value = stages_response
            return resp

        with patch(
            "hsfs.engine.spark_metrics.urllib.request.urlopen", side_effect=mock_urlopen
        ):
            return SparkStageMetrics(spark)

    def test_discover_sets_app_id(self):
        metrics = self._make_metrics()
        assert metrics._app_id == "app-123"
        assert metrics._available is True

    def test_unavailable_when_no_port(self):
        spark = MagicMock()
        spark.conf.get.return_value = None

        with patch("hsfs.engine.spark_metrics._find_spark_ui_port", return_value=None):
            metrics = SparkStageMetrics(spark)
        assert metrics._available is False

    def test_snapshot_and_report(self):
        stage_data = [
            {
                "stageId": 0,
                "name": "scan parquet",
                "executorRunTime": 3200,
                "inputRecords": 1200000,
                "outputRecords": 450000,
                "shuffleReadBytes": 128 * 1024 * 1024,
                "shuffleWriteBytes": 64 * 1024 * 1024,
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
            },
        ]

        metrics = self._make_metrics(stages_data=[])
        metrics.snapshot()
        assert metrics._stage_snapshot == 0

        # Simulate stages appearing after the operation
        new_stages = json.dumps(stage_data).encode()
        with patch("hsfs.engine.spark_metrics.urllib.request.urlopen") as mock_urlopen:
            resp = MagicMock()
            resp.read.return_value = new_stages
            mock_urlopen.return_value = resp

            report = metrics.report("test")

        assert report is not None
        assert "Stage 0" in report
        assert "scan parquet" in report
        assert "3.2s" in report

    def test_report_returns_none_when_unavailable(self):
        metrics = self._make_metrics()
        metrics._available = False
        assert metrics.report() is None

    def test_report_returns_none_when_no_new_stages(self):
        metrics = self._make_metrics(stages_data=[])
        metrics.snapshot()
        with patch("hsfs.engine.spark_metrics.urllib.request.urlopen") as mock_urlopen:
            resp = MagicMock()
            resp.read.return_value = json.dumps([]).encode()
            mock_urlopen.return_value = resp
            assert metrics.report() is None
