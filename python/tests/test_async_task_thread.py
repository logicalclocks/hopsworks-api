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

import threading

from hopsworks_common.util import AsyncTask, AsyncTaskThread


class _FakePool:
    """Stands in for the aiomysql engine the real thread holds."""

    def __init__(self):
        self.closed = False
        self.waited = False

    def close(self):
        self.closed = True

    async def wait_closed(self):
        self.waited = True


class TestAsyncTaskThread:
    def test_stop_does_not_shadow_the_thread_internal(self):
        """Overriding `threading.Thread._stop` breaks every thread's `is_alive()`.

        CPython calls `_stop` from `_wait_for_tstate_lock` to mark a thread
        finished, so an override also breaks `join()` for every instance.
        """
        assert AsyncTaskThread._stop is threading.Thread._stop

    def test_shutdown_ends_the_thread(self):
        thread = AsyncTaskThread()
        thread.start()

        assert thread._shutdown() is True
        assert thread._event_loop.is_closed()
        thread.join(timeout=15)
        assert not thread.is_alive()

    def test_shutdown_is_idempotent(self):
        thread = AsyncTaskThread()
        thread.start()

        assert thread._shutdown() is True
        assert thread._shutdown() is True

    def test_shutdown_closes_the_connection_pool(self):
        """The pool has to be closed from inside the loop, before it stops.

        `aiomysql.Connection.close()` only calls `transport.close()`, which
        schedules the socket close as a loop callback. Stop the loop first and
        the connections stay open on the server.
        """
        pool = _FakePool()
        thread = AsyncTaskThread(connection_pool_initializer=_pool_initializer(pool))
        thread.start()

        assert thread._shutdown() is True
        assert pool.closed, "pool.close() was not called"
        assert pool.waited, "pool.wait_closed() was not awaited"

    def test_a_submitted_task_still_runs(self):
        thread = AsyncTaskThread()
        thread.start()
        try:
            assert thread._submit(AsyncTask(task_function=_answer)) == 42
        finally:
            thread._shutdown()

    def test_shutdown_unblocks_a_thread_waiting_on_an_empty_queue(self):
        """The stop flag alone is not enough.

        `_execute_task` blocks in `queue.Queue.get()`, so the flag is only read
        once a task arrives; without the sentinel the thread waits forever.
        """
        thread = AsyncTaskThread()
        thread.start()
        thread.stop_event.set()

        thread.join(timeout=2)
        assert thread.is_alive(), "expected the bare flag to leave it blocked"

        assert thread._shutdown() is True
        thread.join(timeout=15)
        assert not thread.is_alive()


async def _answer():
    return 42


def _pool_initializer(pool):
    async def initialize(*_args):
        return pool

    return initialize
