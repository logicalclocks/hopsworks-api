# Copyright 2026 Hopsworks AB. Licensed under the Apache License, Version 2.0.
"""Admission accounting shared by predictor and asynchronous logging workers."""

from __future__ import annotations

import os
import threading


_active_reservation = threading.local()


def _positive_env(name, default):
    try:
        return max(1, int(os.environ.get(name, default)))
    except ValueError:
        return default


class _BufferBudget:
    def __init__(self, max_rows, max_bytes):
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._rows = 0
        self._bytes = 0
        self._closed = False
        self._lock = threading.Lock()

    def _acquire(self, rows, size):
        with self._lock:
            if (
                self._closed
                or rows < 0
                or size < 0
                or rows > self._max_rows - self._rows
                or size > self._max_bytes - self._bytes
            ):
                return False
            self._rows += rows
            self._bytes += size
            return True

    def _release(self, rows, size):
        with self._lock:
            self._rows -= rows
            self._bytes -= size

    def _close(self):
        with self._lock:
            self._closed = True

    def _snapshot(self):
        with self._lock:
            return {"rows": self._rows, "bytes": self._bytes}


class _Reservation:
    """Transfer existing row slots while retaining source bytes until assembly ends."""

    def __init__(self, budget, rows, size):
        self._budget = budget
        self._rows = rows
        self._size = size

    def _transfer(self, rows, size):
        if rows > self._rows or not self._budget._acquire(0, size):
            return False
        self._rows -= rows
        return True

    def __enter__(self):
        _active_reservation.current = self
        return self

    def __exit__(self, *args):
        _active_reservation.current = None
        self._budget._release(self._rows, self._size)
