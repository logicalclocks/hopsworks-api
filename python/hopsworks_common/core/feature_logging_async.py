# Copyright 2026 Hopsworks AB. Licensed under the Apache License, Version 2.0.
"""Bounded delivery workers for the SDK and serving-image feature loggers."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time

from hopsworks_common.core.feature_logging_buffer import (
    _active_reservation,
    _BufferBudget,
    _positive_env,
)


class _AsyncLogWorker(threading.Thread):
    def __init__(self, max_queue_size=1000, on_drop=None, close_client=None):
        super().__init__(name="hopsworks-feature-logger", daemon=True)
        self._max_bytes = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_MAX_BUFFER_BYTES", 64 * 1024 * 1024
        )
        self._budget = _BufferBudget(max_queue_size, self._max_bytes)
        self._on_drop = on_drop
        self._close_client = close_client
        self._event_loop = asyncio.new_event_loop()
        self._tasks_queue = asyncio.Queue()
        self._workers = []
        self._admission = threading.Lock()
        self._closing = False
        self._finalizer = None

    def _submit_task(self, task, rows=1, size=None):
        size = rows * 1024 if size is None else size
        with self._admission:
            reservation = getattr(_active_reservation, "current", None)
            acquire = (
                reservation._transfer
                if reservation is not None and reservation._budget is self._budget
                else self._budget._acquire
            )
            if rows < 1 or not acquire(rows, size):
                self._drop(task, "buffer limit or shutdown", rows)
                return False
            try:
                # Reserve before creating a callback, so a blocked loop cannot
                # accumulate an unbounded second queue of pending callbacks.
                self._event_loop.call_soon_threadsafe(
                    self._tasks_queue.put_nowait, (task, rows, size)
                )
            except RuntimeError:
                self._budget._release(rows, size)
                self._drop(task, "event loop closed", rows)
                return False
        return True

    def _drop(self, task, reason, rows):
        if self._on_drop:
            self._on_drop(task, reason, rows)

    def _initialize_workers(self, num_workers, worker_function):
        if num_workers < 1:
            raise ValueError("Feature logger requires at least one worker")
        for _ in range(num_workers):
            self._workers.append(
                self._event_loop.create_task(self._worker(worker_function))
            )

    def run(self):
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_forever()
        self._event_loop.close()

    async def _worker(self, send):
        while True:
            task, rows, size = await self._tasks_queue.get()
            try:
                await send(task)
            except asyncio.CancelledError:
                self._drop(task, "shutdown deadline", rows)
                raise
            except Exception:  # noqa: BLE001 - a bad log must not terminate a worker
                self._drop(task, "worker failed", rows)
            finally:
                self._budget._release(rows, size)
                self._tasks_queue.task_done()

    def _close(self, timeout=None):
        timeout = (
            _positive_env("HOPSWORKS_FEATURE_LOGGER_SHUTDOWN_SECONDS", 5)
            if timeout is None
            else max(0.0, timeout)
        )
        deadline = time.monotonic() + timeout
        with self._admission:
            if not self._closing:
                self._closing = True
                self._budget._close()
                if not self.is_alive():
                    return self._budget._snapshot()["rows"] == 0
                self._finalizer = asyncio.run_coroutine_threadsafe(
                    self._finalize(deadline), self._event_loop
                )
        if self._finalizer is None:
            return True
        try:
            result = self._finalizer.result(
                timeout=max(0.0, deadline - time.monotonic())
            )
            self.join(max(0.0, deadline - time.monotonic()))
            return result
        except (concurrent.futures.TimeoutError, concurrent.futures.CancelledError):
            return False

    async def _finalize(self, deadline):
        drained = True
        try:
            await asyncio.wait_for(
                self._tasks_queue.join(), max(0.0, deadline - time.monotonic())
            )
        except asyncio.TimeoutError:
            drained = False
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        while not self._tasks_queue.empty():
            task, rows, size = self._tasks_queue.get_nowait()
            self._drop(task, "shutdown deadline", rows)
            self._budget._release(rows, size)
            self._tasks_queue.task_done()
        if self._close_client:
            try:
                await asyncio.wait_for(
                    self._close_client(), max(0.001, deadline - time.monotonic())
                )
            except (asyncio.TimeoutError, RuntimeError):
                drained = False
        self._event_loop.call_soon(self._event_loop.stop)
        return drained
