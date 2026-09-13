# Copyright 2026 Hopsworks AB. Licensed under the Apache License, Version 2.0.
from __future__ import annotations

import asyncio
import json
import threading
import time
from types import SimpleNamespace

from hopsworks_common.core.feature_logging_async import _AsyncLogWorker
from hopsworks_common.core.feature_logging_buffer import _Reservation
from hsfs import feature_logger_async as sdk


def test_shared_budget_transfers_rows_and_retains_source_until_assembly_ends():
    worker = _AsyncLogWorker(max_queue_size=3)
    budget = worker._budget
    assert budget._acquire(2, 100)
    assert worker._submit_task("earlier", rows=1, size=20)
    with _Reservation(budget, 2, 100):
        assert worker._submit_task("batch", rows=2, size=40)
        assert budget._snapshot() == {"rows": 3, "bytes": 160}
        assert not worker._submit_task("overflow", rows=1, size=10)
    assert budget._snapshot() == {"rows": 3, "bytes": 60}
    sent = []

    async def send(task):
        sent.append(task)

    worker._initialize_workers(1, send)
    worker.start()
    assert worker._close(2)
    assert sent == ["earlier", "batch"]
    assert budget._snapshot() == {"rows": 0, "bytes": 0}


def test_admission_does_not_traverse_user_objects():
    class Value:
        def __sizeof__(self):
            raise AssertionError("Request data must not be inspected on admission")

    worker = _AsyncLogWorker(max_queue_size=512)
    assert worker._submit_task([Value() for _ in range(512)], rows=512)
    assert worker._budget._snapshot()["rows"] == 512
    worker._event_loop.close()


def test_admission_counts_callbacks_rows_and_bytes():
    dropped = []
    worker = _AsyncLogWorker(
        max_queue_size=3, on_drop=lambda task, reason, rows: dropped.append(rows)
    )
    assert worker._submit_task("first", rows=2, size=10)
    assert not worker._submit_task("second", rows=2, size=10)
    assert dropped == [2]
    assert worker._budget._snapshot() == {"rows": 2, "bytes": 10}
    assert not worker._submit_task("oversized", rows=1, size=worker._max_bytes)
    delivered = []

    async def send(task):
        delivered.append(task)

    worker._initialize_workers(1, send)
    worker.start()
    assert worker._close(2)
    assert delivered == ["first"]
    assert worker._budget._snapshot() == {"rows": 0, "bytes": 0}
    assert not worker._submit_task("after close")


def test_shutdown_cancels_inflight_and_drops_queue_with_finite_deadline():
    started = threading.Event()
    dropped = []
    worker = _AsyncLogWorker(
        max_queue_size=4, on_drop=lambda task, reason, rows: dropped.append(rows)
    )

    async def send(task):
        started.set()
        await asyncio.Event().wait()

    worker._initialize_workers(1, send)
    worker.start()
    assert worker._submit_task("inflight", rows=2)
    assert worker._submit_task("queued", rows=2)
    assert started.wait(2)
    before = time.monotonic()
    assert not worker._close(0.05)
    assert time.monotonic() - before < 0.25
    worker.join(2)
    assert not worker.is_alive()
    assert sum(dropped) == 4
    assert worker._budget._snapshot() == {"rows": 0, "bytes": 0}


def test_sdk_emits_binary_legacy_event_and_counts_delivery(monkeypatch):
    calls = []

    async def post(payload, headers):
        calls.append((payload, headers))

    monkeypatch.setattr(
        sdk, "get_feature_logging_client", lambda: SimpleNamespace(_post=post)
    )
    logger = sdk.AsyncFeatureLogger(1, "http://predictor", "project", "dep")
    group = SimpleNamespace(id=2, _online_topic_name="topic", subject={"id": 3})
    logger._feature_view = SimpleNamespace(
        feature_logging=SimpleNamespace(get_feature_group=lambda _: group)
    )
    logger._feature_encoders = {False: (None, None)}
    monkeypatch.setattr(logger, "_avro_encode_features", lambda *args: b"avro")
    asyncio.run(logger._send_events(({"x": 1}, None)))
    payload, headers = calls[0]
    assert headers["content-type"] == "application/json"
    assert headers["ce-id"] and headers["ce-time"]
    assert json.loads(payload) == [
        {
            "projectId": 1,
            "featureGroupId": 2,
            "topicName": "topic",
            "subjectId": 3,
            "data": "YXZybw==",
        }
    ]
    assert logger._stats["sent"] == 1
    logger._async_worker_thread._event_loop.close()
