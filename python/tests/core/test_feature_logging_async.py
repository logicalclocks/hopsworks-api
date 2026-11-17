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
from __future__ import annotations

import asyncio
import gc
import json
import threading
import time
import weakref
from types import SimpleNamespace

import pytest
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


class TestLoopConnectionPools:
    """The pools the awaited lookups build live exactly as long as their loops.

    Each one opens a connection per feature group in the view. A serving deployment has
    a single loop and never notices, but a script awaiting a lookup through
    `asyncio.run` builds a loop per call, and a pool left behind by each is that many
    sockets the online store holds until the process ends.
    """

    class Pool:
        def __init__(self):
            self.closed = False
            self.waited = False
            self.terminated = False

        def close(self):
            self.closed = True

        async def wait_closed(self):
            await asyncio.sleep(0)
            self.waited = True

        def terminate(self):
            self.terminated = True

    @classmethod
    def _client(cls, created, delay=0.0, fail_first=0):
        from hsfs.core import online_store_sql_engine

        client = online_store_sql_engine.OnlineStoreSqlClient.__new__(
            online_store_sql_engine.OnlineStoreSqlClient
        )
        client._loop_connection_pools = weakref.WeakKeyDictionary()
        client._async_task_thread = None
        client._prepared_statements = {
            online_store_sql_engine.OnlineStoreSqlClient.SINGLE_VECTOR_KEY: [1, 2]
        }
        failures = [fail_first]

        async def make_pool(size):
            assert size == 2
            await asyncio.sleep(delay)
            if failures[0]:
                failures[0] -= 1
                raise ConnectionError("online store unreachable")
            pool = cls.Pool()
            created.append(pool)
            return pool

        client._get_connection_pool = make_pool
        return client

    def test_a_pool_is_reused_within_one_loop(self):
        created = []
        client = self._client(created)

        async def main():
            first = await client._loop_connection_pool()
            second = await client._loop_connection_pool()
            assert first is second

        asyncio.run(main())
        assert len(created) == 1

    def test_each_loop_closes_its_pool_on_the_way_out(self):
        created = []
        client = self._client(created)

        for _ in range(3):
            asyncio.run(client._loop_connection_pool())

        assert [(p.closed, p.waited) for p in created] == [(True, True)] * 3
        assert len(client._loop_connection_pools) == 0

    def test_close_releases_a_live_loop_s_pool_on_that_loop(self):
        created = []
        client = self._client(created)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(client._loop_connection_pool())
            assert len(client._loop_connection_pools) == 1
            client._close()
            assert [(p.closed, p.waited) for p in created] == [(True, True)]
            assert len(client._loop_connection_pools) == 0
        finally:
            loop.close()

    def test_concurrent_first_lookups_share_one_pool(self):
        created = []
        client = self._client(created, delay=0.01)

        async def main():
            pools = await asyncio.gather(
                *(client._loop_connection_pool() for _ in range(16))
            )
            assert len({id(p) for p in pools}) == 1

        asyncio.run(main())
        assert len(created) == 1

    def test_a_cancelled_caller_does_not_cancel_the_opening_for_the_others(self):
        created = []
        client = self._client(created, delay=0.02)

        async def main():
            first = asyncio.ensure_future(client._loop_connection_pool())
            await asyncio.sleep(0)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            return await client._loop_connection_pool()

        assert asyncio.run(main()) is created[0]
        assert len(created) == 1

    def test_a_failed_opening_is_retried_by_the_next_caller(self):
        created = []
        client = self._client(created, fail_first=1)

        async def main():
            with pytest.raises(ConnectionError):
                await client._loop_connection_pool()
            return await client._loop_connection_pool()

        assert asyncio.run(main()) is created[0]

    def test_an_aiomysql_pool_s_idle_connections_close_with_the_loop(self):
        # Against the driver's own pool: terminate() never touched idle connections,
        # and the finalizer that called it kept the loop it was meant to clean up for.
        import aiomysql.pool
        from hsfs.core import online_store_sql_engine

        client = online_store_sql_engine.OnlineStoreSqlClient.__new__(
            online_store_sql_engine.OnlineStoreSqlClient
        )
        client._loop_connection_pools = weakref.WeakKeyDictionary()
        client._async_task_thread = None
        client._prepared_statements = {client.SINGLE_VECTOR_KEY: [1]}
        idle = SimpleNamespace(close_calls=0)

        def close():
            idle.close_calls += 1

        idle.close = close
        idle.closed = False
        loops = []

        async def make_pool(size):
            loop = asyncio.get_running_loop()
            loops.append(weakref.ref(loop))
            pool = aiomysql.pool.Pool(0, 10, False, -1, loop)
            pool._free.append(idle)
            return pool

        client._get_connection_pool = make_pool
        asyncio.run(client._loop_connection_pool())
        gc.collect()

        assert idle.close_calls == 1
        assert len(client._loop_connection_pools) == 0
        assert loops[0]() is None, "the closed loop is still referenced"
