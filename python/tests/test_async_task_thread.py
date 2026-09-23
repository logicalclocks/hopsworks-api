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
import concurrent.futures
import threading
import time

import pytest
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
        """No class of ours may define `_stop`.

        Up to Python 3.12 `threading.Thread._stop` is what CPython calls from
        `_wait_for_tstate_lock` to mark a thread finished, so a subclass that
        defines its own `_stop` breaks `is_alive()` and `join()` for every
        thread. Python 3.13 removed the attribute, so the classes we own are
        checked directly rather than compared against it.
        """
        ours = AsyncTaskThread.__mro__[
            : AsyncTaskThread.__mro__.index(threading.Thread)
        ]

        assert [klass.__name__ for klass in ours if "_stop" in vars(klass)] == []

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

    def test_a_failed_pool_close_keeps_the_pool_handle(self):
        """A close that does not finish must not drop the handle or claim success.

        The thread is never started, so nothing services the queue and the close
        task cannot complete: the same state as a thread too wedged to drain it.
        """
        pool = _FakePool()
        thread = AsyncTaskThread(connection_pool_initializer=_pool_initializer(pool))
        thread._connection_pool = pool

        assert thread._shutdown(timeout=0.05) is False
        assert thread._connection_pool is pool, "pool handle was dropped"
        assert not pool.closed

    def test_a_submitted_task_still_runs(self):
        thread = AsyncTaskThread()
        thread.start()
        try:
            assert thread._submit(AsyncTask(task_function=_answer)) == 42
        finally:
            thread._shutdown()

    def test_shutdown_ends_a_thread_the_stop_flag_alone_does_not(self):
        """The stop flag does not reach the loop; only stopping the loop does.

        `run()` hands the thread to `run_forever()`, so setting the flag leaves
        it running until `_shutdown` stops the loop from another thread.
        """
        thread = AsyncTaskThread()
        thread.start()
        thread.stop_event.set()

        thread.join(timeout=2)
        assert thread.is_alive(), "expected the bare flag to leave it running"

        assert thread._shutdown() is True
        thread.join(timeout=15)
        assert not thread.is_alive()


class TestPoolTaskScheduling:
    """A pooled read runs without a liveness round trip, and alongside other reads."""

    def test_no_connection_test_runs_per_task(self):
        """Creating the pool is the check; a read costs no liveness round trip."""
        pool = _FakePool()
        checks = []
        thread = AsyncTaskThread(
            connection_pool_initializer=_pool_initializer(pool),
            connection_test=_counting_check(checks),
        )
        thread.start()
        try:
            for _ in range(5):
                assert thread._submit(_read_task()) == "read"
        finally:
            thread._shutdown()

        assert checks == [], "a read must not cost a liveness round trip"

    def test_independent_reads_overlap(self):
        """Four callers of one thread must not be serialized behind each other.

        Each read waits on an event that only the fourth arrival sets, so the
        submissions can only all return if they were in flight together.
        """
        pool = _FakePool()
        thread = AsyncTaskThread(
            connection_pool_initializer=_pool_initializer(pool), max_concurrent_tasks=4
        )
        thread.start()
        arrived = _Rendezvous(4)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as callers:
                results = list(
                    callers.map(
                        lambda _: thread._submit(
                            AsyncTask(
                                task_function=arrived.wait,
                                requires_connection_pool=True,
                            ),
                            timeout=10,
                        ),
                        range(4),
                    )
                )
        finally:
            thread._shutdown()

        assert results == [4, 4, 4, 4]
        assert arrived.peak == 4, f"only {arrived.peak} read(s) were ever in flight"

    def test_concurrency_is_bounded(self):
        """The bound is the pool's, so reads queue in the loop instead of oversubscribing it."""
        pool = _FakePool()
        thread = AsyncTaskThread(
            connection_pool_initializer=_pool_initializer(pool), max_concurrent_tasks=2
        )
        thread.start()
        counter = _Rendezvous(2)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as callers:
                list(
                    callers.map(
                        lambda _: thread._submit(
                            AsyncTask(
                                task_function=counter.wait,
                                requires_connection_pool=True,
                            ),
                            timeout=10,
                        ),
                        range(4),
                    )
                )
        finally:
            thread._shutdown()

        assert counter.peak == 2, f"{counter.peak} reads were in flight, the bound is 2"

    def test_a_dead_connection_is_recovered_on_the_read(self):
        """No ping in front of every read; the read itself surfaces a dead connection."""
        pool = _FakePool()
        checks = []
        attempts = []
        thread = AsyncTaskThread(
            connection_pool_initializer=_pool_initializer(pool),
            connection_test=_counting_check(checks),
            is_connection_error=lambda error: isinstance(error, _Gone),
        )
        thread.start()
        try:
            result = thread._submit(
                AsyncTask(
                    task_function=_failing_once(attempts),
                    requires_connection_pool=True,
                    retry_on_connection_error=True,
                ),
                timeout=10,
            )
        finally:
            thread._shutdown()

        assert result == "read"
        assert len(attempts) == 2, "the read should have been tried again"
        assert checks == [pool], "the pool is checked once, after the failure"

    def test_an_unrelated_failure_is_not_retried(self):
        attempts = []
        thread = AsyncTaskThread(
            connection_pool_initializer=_pool_initializer(_FakePool()),
            is_connection_error=lambda error: isinstance(error, _Gone),
        )
        thread.start()
        try:
            with pytest.raises(ValueError, match="query"):
                thread._submit(
                    AsyncTask(
                        task_function=_always_fails(attempts, ValueError("query")),
                        requires_connection_pool=True,
                        retry_on_connection_error=True,
                    ),
                    timeout=10,
                )
        finally:
            thread._shutdown()

        assert len(attempts) == 1

    def test_a_task_that_does_not_say_it_is_safe_is_not_repeated(self):
        """Only an operation that declares itself repeatable is run twice."""
        attempts = []
        thread = AsyncTaskThread(
            connection_pool_initializer=_pool_initializer(_FakePool()),
            is_connection_error=lambda error: isinstance(error, _Gone),
        )
        thread.start()
        try:
            with pytest.raises(_Gone):
                thread._submit(
                    AsyncTask(
                        task_function=_always_fails(attempts, _Gone()),
                        requires_connection_pool=True,
                    ),
                    timeout=10,
                )
        finally:
            thread._shutdown()

        assert len(attempts) == 1


class TestSubmitBounds:
    def test_a_submission_can_time_out(self):
        thread = AsyncTaskThread()
        thread.start()
        try:
            with pytest.raises(TimeoutError):
                thread._submit(AsyncTask(task_function=_forever), timeout=0.05)
        finally:
            thread._shutdown()

    def test_the_default_timeout_applies_when_no_timeout_is_given(self):
        thread = AsyncTaskThread(default_timeout=0.05)
        thread.start()
        try:
            with pytest.raises(TimeoutError):
                thread._submit(AsyncTask(task_function=_forever))
        finally:
            thread._shutdown()

    def test_shutdown_after_a_timeout_leaves_nothing_running(self):
        """A loop closed with pending tasks abandons them and their connections."""
        thread = AsyncTaskThread()
        thread.start()
        with pytest.raises(TimeoutError):
            thread._submit(AsyncTask(task_function=_forever), timeout=0.05)

        assert thread._shutdown() is True
        thread.join(timeout=15)
        assert not thread.is_alive()
        assert thread._event_loop.is_closed()

    def test_a_caller_with_no_timeout_does_not_wait_on_a_dead_thread(self):
        """A task on a loop that closes never completes.

        The caller that named no timeout would wait for an answer nobody is
        going to give, which on a cluster turned one transient connect failure
        into a hung feature lookup.
        """
        thread = AsyncTaskThread()
        thread.start()
        submitted = []

        def submit():
            try:
                thread._submit(AsyncTask(task_function=_forever))
            except BaseException as error:  # noqa: BLE001 - recorded, then asserted
                submitted.append(error)

        caller = threading.Thread(target=submit, daemon=True)
        caller.start()
        time.sleep(0.1)
        # The loop dies without going through _shutdown, which is what an
        # exception out of run() does: nothing cancels the pending task, so its
        # future never completes.
        thread._event_loop.call_soon_threadsafe(thread._event_loop.stop)

        caller.join(timeout=15)
        assert not caller.is_alive(), "the caller is still waiting on a dead thread"
        assert isinstance(submitted[0], (RuntimeError, TimeoutError)), submitted

    def test_a_failed_startup_raises_instead_of_waiting(self):
        """A caller must not wait on a thread whose pool never came up."""
        thread = AsyncTaskThread(connection_pool_initializer=_failing_initializer)
        thread.start()
        thread.join(timeout=15)

        with pytest.raises(RuntimeError, match="no pool"):
            thread._submit(AsyncTask(task_function=_answer))


class _Rendezvous:
    """Blocks each arrival until `expected` of them are in flight together."""

    def __init__(self, expected: int):
        self._expected = expected
        self._arrived = 0
        self.peak = 0
        self._all_here = None

    async def wait(self, connection_pool=None):
        if self._all_here is None:
            self._all_here = asyncio.Event()
        self._arrived += 1
        self.peak = max(self.peak, self._arrived)
        if self._arrived >= self._expected:
            self._all_here.set()
        try:
            await asyncio.wait_for(self._all_here.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        finally:
            self._arrived -= 1
        return self.peak


class TestShutdownCancelsInFlightReads:
    """A read still running must not hold the pool close open."""

    def test_a_read_with_no_timeout_does_not_stall_the_close(self):
        in_flight = []

        class _WaitingPool(_FakePool):
            """Like aiomysql: wait_closed waits for checked-out connections."""

            async def wait_closed(self):
                while in_flight:
                    await asyncio.sleep(0.01)
                self.waited = True

        async def hold(connection_pool=None):
            in_flight.append(True)
            try:
                await asyncio.sleep(3600)
            finally:
                in_flight.clear()

        pool = _WaitingPool()
        thread = AsyncTaskThread(connection_pool_initializer=_pool_initializer(pool))
        thread.start()
        future = thread._schedule(
            AsyncTask(task_function=hold, requires_connection_pool=True)
        )
        while not in_flight:
            time.sleep(0.01)

        started = time.monotonic()
        assert thread._shutdown(timeout=5) is True
        assert time.monotonic() - started < 2
        assert pool.waited
        assert future.done()

    def test_a_read_submitted_during_shutdown_is_refused(self):
        thread = AsyncTaskThread(
            connection_pool_initializer=_pool_initializer(_FakePool())
        )
        thread.start()
        thread._ready.wait(5)
        thread.stop_event.set()
        try:
            with pytest.raises(RuntimeError, match="shutting down"):
                thread._submit(_read_task(), timeout=5)
        finally:
            thread._shutdown()


class _Gone(Exception):
    """Stands in for the driver error a closed pooled connection raises."""


async def _answer():
    return 42


async def _forever():
    await asyncio.sleep(3600)


def _read_task():
    return AsyncTask(task_function=_read, requires_connection_pool=True)


async def _read(connection_pool=None):
    return "read"


def _counting_check(checks):
    async def check(pool):
        checks.append(pool)

    return check


def _failing_once(attempts):
    async def read(connection_pool=None):
        attempts.append(connection_pool)
        if len(attempts) == 1:
            raise _Gone
        return "read"

    return read


def _always_fails(attempts, error):
    async def read(connection_pool=None):
        attempts.append(connection_pool)
        raise error

    return read


async def _failing_initializer(*_args):
    raise RuntimeError("no pool")


def _pool_initializer(pool):
    async def initialize(*_args):
        return pool

    return initialize


class TestShutdownDuringStartup:
    """A shutdown that arrives while the pool is being built leaves it alone.

    Stopping the loop under `run_until_complete`, or cancelling the task it is
    waiting on, raises out of `run()`. The thread then dies with a traceback
    and the loop is never closed, which is worse than the shutdown simply not
    finishing. A feature view whose features are all on demand opens a client
    and closes it again straight away, which is where this showed.
    """

    def _slow_thread(self, seconds=2.0):
        async def pool(*_args):
            await asyncio.sleep(seconds)
            return _FakePool()

        return AsyncTaskThread(connection_pool_initializer=pool)

    def test_it_gives_up_rather_than_interrupt_the_startup(self, capfd):
        thread = self._slow_thread()
        thread.start()
        try:
            assert thread._shutdown(timeout=0.2) is False, (
                "a shutdown cannot have finished while the pool is still being built"
            )
            assert thread.is_alive(), "the startup was killed instead of left alone"
            assert thread._startup_error is None
            assert (
                "error occurred in the async task thread" not in capfd.readouterr().out
            )
        finally:
            thread._shutdown()

    def test_a_shutdown_that_waits_long_enough_still_closes_it(self):
        thread = self._slow_thread(seconds=0.2)
        thread.start()

        assert thread._shutdown(timeout=10) is True
        thread.join(timeout=10)
        assert not thread.is_alive()


class TestStartupSpendsTheCallersDeadline:
    """A caller's timeout covers waiting for the pool, not just the query.

    The pool is built before any task can run, so a slow initializer is time the
    caller spends waiting for its answer. Waiting for it without a bound meant a
    20 ms deadline returned after 150 ms, and an initializer that never returned
    held every submission open for as long as it took.
    """

    def _thread(self, seconds):
        async def pool(*_args):
            await asyncio.sleep(seconds)
            return _FakePool()

        return AsyncTaskThread(connection_pool_initializer=pool)

    def test_a_slow_startup_times_the_caller_out(self):
        thread = self._thread(seconds=5)
        thread.start()
        try:
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                thread._submit(AsyncTask(task_function=_noop), timeout=0.2)
            assert time.monotonic() - started < 2, "the caller waited for startup"
        finally:
            thread._shutdown()

    def test_startup_time_comes_out_of_the_deadline(self):
        """What is left after startup is what the task gets, not a fresh budget."""
        thread = self._thread(seconds=0.2)
        thread.start()
        try:
            started = time.monotonic()
            with pytest.raises(TimeoutError):
                thread._submit(AsyncTask(task_function=_sleep_forever), timeout=0.4)
            assert time.monotonic() - started < 0.9, (
                "the task was given a full deadline of its own after startup"
            )
        finally:
            thread._shutdown()

    def test_a_caller_with_no_deadline_still_waits_for_startup(self):
        thread = self._thread(seconds=0.1)
        thread.start()
        try:
            assert thread._submit(AsyncTask(task_function=_noop), timeout=None) == 1
        finally:
            thread._shutdown()


async def _noop(*_args, **_kwargs):
    return 1


async def _sleep_forever(*_args, **_kwargs):
    await asyncio.sleep(30)


class TestAFailedCloseKeepsThePool:
    """The event says the task finished, not that it worked.

    A task that raises publishes the exception as its result and sets its event
    in a finally, so waiting on the event alone reported a clean shutdown and
    dropped the pool handle while its connections were still open on the server,
    leaving nothing to retry the close with.
    """

    def test_a_pool_that_would_not_close_is_kept(self):
        pool = _FakePool()

        async def make_pool(*_args):
            return pool

        thread = AsyncTaskThread(connection_pool_initializer=make_pool)

        async def refuse(_pool):
            raise OSError("the server hung up")

        thread._close_connection_pool = refuse
        thread.start()
        try:
            assert thread._submit(AsyncTask(task_function=_noop)) == 1

            assert thread._shutdown() is False, "a failed close reported success"
            assert thread._connection_pool is pool, (
                "the handle needed to retry the close was dropped"
            )
        finally:
            thread._close_connection_pool = (
                AsyncTaskThread._close_connection_pool.__get__(thread)
            )
            thread._shutdown()
