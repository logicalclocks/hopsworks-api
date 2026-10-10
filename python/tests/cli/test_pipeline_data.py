"""`hops factory system status`: what a system's feature pipelines read and wrote in the report's window."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from hopsworks.cli import health, pipeline_data


NOW = datetime.now(timezone.utc)


def _commit(log, version, hours_ago, metrics):
    when = int((NOW - timedelta(hours=hours_ago)).timestamp() * 1000)
    info = {"timestamp": when, "operation": "WRITE", "operationMetrics": metrics}
    (log / f"{version:020d}.json").write_text(
        json.dumps({"commitInfo": info})
        + "\n"
        + json.dumps({"add": {"path": "x"}})
        + "\n"
    )


def _table(mount, name, commits):
    """A Delta table of the project `shop` under the mount, with commits (hours ago, metrics)."""
    table = mount / "featurestore" / "shop_featurestore.db" / name
    log = table / "_delta_log"
    log.mkdir(parents=True)
    for version, (hours_ago, metrics) in enumerate(commits):
        _commit(log, version, hours_ago, metrics)
    return table


class _Trino:
    """Answers the column checks: one column always null, one hour of the window empty."""

    def __init__(self, hours):
        self.hours = hours
        self.description = None
        self.result = None

    def cursor(self):
        return self

    def execute(self, sql):
        if "limit 0" in sql:
            self.description = [("order_id",), ("amount",), ("coupon",), ("ts",)]
        elif "date_trunc" in sql:
            start = NOW.replace(minute=0, second=0, microsecond=0)
            self.result = [
                (start - timedelta(hours=h),) for h in range(self.hours + 1) if h != 5
            ]
        else:
            # 100 rows: order_id never null, amount null twice, coupon always null.
            self.result = [(100, 100, 98, 0, 100)]

    def fetchone(self):
        return self.result[0]

    def fetchall(self):
        return self.result

    def close(self):
        pass


def _project(trino):
    return SimpleNamespace(
        name="shop",
        get_trino_api=lambda: SimpleNamespace(connect=lambda **kw: trino),
    )


def test_the_window_counts_only_its_own_commits(tmp_path):
    table = _table(
        tmp_path,
        "orders_1",
        [
            (
                30,
                {"numOutputRows": "500", "numOutputBytes": "5000"},
            ),  # before the window
            (10, {"numOutputRows": "200", "numOutputBytes": "2000"}),
            # A Spark merge: its output rows include the 900 it copied unchanged.
            (
                2,
                {
                    "numTargetRowsInserted": "40",
                    "numTargetRowsUpdated": "10",
                    "numOutputRows": "950",
                },
            ),
            # The Python client's delta-rs writes snake_case metrics, as numbers.
            (
                1,
                {
                    "num_target_rows_inserted": 100,
                    "num_target_rows_updated": 0,
                    "num_output_rows": 100,
                },
            ),
        ],
    )
    written = pipeline_data.delta_written(table, NOW - timedelta(hours=24))
    assert written["commits"] == 3
    assert written["rows"] == 350
    assert written["bytes"] == 2000
    assert written["last_write"].startswith((NOW - timedelta(hours=1)).isoformat()[:13])


def test_files_count_only_what_changed_in_the_window(tmp_path):
    old, new = tmp_path / "old.parquet", tmp_path / "part" / "new.parquet"
    new.parent.mkdir()
    old.write_bytes(b"x" * 10)
    new.write_bytes(b"x" * 30)
    stale = time.time() - 48 * 3600
    os.utime(old, (stale, stale))
    written = pipeline_data.files_written(tmp_path, NOW - timedelta(hours=24))
    assert written["files"] == 1 and written["bytes"] == 30


def test_a_job_schedule_says_how_often_rows_are_expected():
    hourly = {"job": {"schedule": {"cron": "0 0 * * * ?"}}}
    daily = {"job": {"schedule": {"cron": "0 0 2 * * ?"}}}
    unix = {"job": {"cron": "*/15 * * * *"}}
    assert pipeline_data._bucket(hourly) == "hour"
    assert pipeline_data._bucket(daily) == "day"
    assert pipeline_data._bucket(unix) == "hour"
    assert pipeline_data._bucket({"job": {"name": "x"}}) is None


def test_the_report_sets_rows_in_against_rows_out_and_finds_missing_data(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HOPSFS_MOUNT", str(tmp_path))
    monkeypatch.setattr(pipeline_data.silver_status, "_layout", lambda table: None)
    _table(tmp_path, "raw_orders_1", [(3, {"numOutputRows": "1000"})])
    _table(
        tmp_path, "orders_1", [(3, {"numOutputRows": "100", "numOutputBytes": "4096"})]
    )
    (tmp_path / "Resources" / "exports").mkdir(parents=True)
    (tmp_path / "Resources" / "exports" / "orders.csv").write_text("a,b\n")
    doc = {
        "features": {
            "pipelines": [
                {
                    "name": "orders",
                    "engine": "polars",
                    "reads": [
                        "raw_orders",
                        {"data_source": "crm", "table": "customers"},
                    ],
                    "writes": [
                        {
                            "feature_group": "orders",
                            "version": 1,
                            "primary_key": ["order_id"],
                            "event_time": "ts",
                        },
                        {"path": "/Projects/shop/Resources/exports"},
                    ],
                    "job": {"name": "shop-orders", "schedule": {"cron": "0 0 * * * ?"}},
                }
            ]
        }
    }
    [pipeline] = pipeline_data.collect(_project(_Trino(24)), doc, hours=24)
    assert pipeline["rows_in"] == 1000 and pipeline["rows_out"] == 100
    raw, crm = pipeline["inputs"]
    assert raw["name"] == "raw_orders v1" and raw["rows"] == 1000
    assert crm["kind"] == "data source"
    orders, exports = pipeline["outputs"]
    assert orders["nulls"] == {"amount": 2.0, "coupon": 100.0}
    assert orders["checked"] == "window" and orders["checked_rows"] == 100
    assert len(orders["missing"]) == 1
    assert "missing data: coupon is null in every row" in orders["problems"]
    assert any(
        p.startswith("missing data: no rows for 1 hours") for p in orders["problems"]
    )
    # amount's 2% is below no limit, so it is reported but not a problem.
    assert not any("amount" in p for p in orders["problems"])
    assert exports["kind"] == "files" and exports["files"] == 1
    assert exports["problems"] == []


def test_nothing_written_by_a_scheduled_pipeline_is_a_problem(tmp_path, monkeypatch):
    monkeypatch.setenv("HOPSFS_MOUNT", str(tmp_path))
    monkeypatch.setattr(pipeline_data.silver_status, "_layout", lambda table: None)
    _table(tmp_path, "raw_orders_1", [(3, {"numOutputRows": "1000"})])
    _table(tmp_path, "orders_1", [(40, {"numOutputRows": "100"})])
    doc = {
        "features": {
            "pipelines": [
                {
                    "reads": [{"feature_group": "raw_orders", "version": 1}],
                    "writes": {"feature_group": "orders", "version": 1},
                    "job": {"name": "shop-orders", "schedule": {"cron": "0 0 2 * * ?"}},
                }
            ]
        }
    }
    [pipeline] = pipeline_data.collect(
        SimpleNamespace(name="shop", get_trino_api=lambda: None), doc, hours=24
    )
    assert pipeline["flow_problems"] == ["1,000 rows came in but none were written"]
    assert (
        "nothing written in the last 24 h, though the pipeline is scheduled"
        in pipeline["outputs"][0]["problems"]
    )


def test_a_pipeline_problem_degrades_the_system_and_shows_on_the_page(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(health, "_kubectl", lambda *a: "")
    monkeypatch.setattr(
        pipeline_data,
        "collect",
        lambda project, doc, hours: [
            {
                "name": "orders",
                "inputs": [],
                "outputs": [],
                "rows_in": 0,
                "rows_out": 0,
                "bytes_in": 0,
                "bytes_out": 0,
                "flow_problems": [],
                "problems": ["missing data: coupon is null in every row"],
            }
        ],
    )
    project = SimpleNamespace(name="shop", get_model_serving=lambda: None)
    facts = health.collect(project, {"system": {"name": "Orders"}}, "orders")
    assert facts["overall"] == "degraded"
    assert facts["counts"]["pipeline_problems"] == 1
    assert "pipelinesSection" in health.render(facts, None)


def test_an_ingestion_reports_each_source_as_a_pipeline(tmp_path, monkeypatch):
    monkeypatch.setenv("HOPSFS_MOUNT", str(tmp_path))
    monkeypatch.setattr(pipeline_data.silver_status, "_layout", lambda table: None)
    _table(tmp_path, "crm_orders_1", [(3, {"numOutputRows": "100"})])
    doc = {
        "ingestion": {
            "sources": [
                {
                    "name": "crm",
                    "reused": True,
                    "tables": [
                        {
                            "source": "public.orders",
                            "ingested": {"feature_group": "crm_orders", "version": 1},
                            "primary_key": ["order_id"],
                            "event_time": "ts",
                        }
                    ],
                }
            ],
            "job": {"name": "crm-ingest", "schedule": {"cron": "0 0 * * * ?"}},
        }
    }
    [pipeline] = pipeline_data.collect(_project(_Trino(24)), doc, hours=24)
    assert pipeline["name"] == "crm" and pipeline["engine"] == "dlt"
    assert pipeline["inputs"] == [{"name": "crm", "kind": "data source"}]
    [orders] = pipeline["outputs"]
    assert orders["name"] == "crm_orders v1" and orders["rows"] == 100
    assert "missing data: coupon is null in every row" in orders["problems"]
    assert any(
        p.startswith("missing data: no rows for 1 hours") for p in orders["problems"]
    )
