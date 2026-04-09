#
#   Copyright 2024 Logical Clocks AB
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

import time


class RateLimiter:
    """Enforces a maximum number of calls per second.

    Tracks call timestamps and blocks until the rate limit allows the next call.
    """

    def __init__(self, max_calls: int, period: float = 1.0):
        """Initialize the rate limiter.

        Args:
            max_calls: Maximum number of calls allowed per period.
            period: Time window in seconds.
        """
        if max_calls <= 0:
            raise ValueError("max_calls must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
        self._max_calls = max_calls
        self._period = period
        self._calls: list[float] = []

    def acquire(self) -> None:
        """Block until a call is allowed under the rate limit."""
        now = time.monotonic()
        self._calls = [t for t in self._calls if now - t < self._period]
        if len(self._calls) >= self._max_calls:
            sleep_time = self._period - (now - self._calls[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self._calls.append(time.monotonic())

    @property
    def max_calls(self) -> int:
        """Maximum number of calls allowed per period."""
        return self._max_calls

    @property
    def period(self) -> float:
        """Length of the rate limit window in seconds."""
        return self._period

    def reset(self) -> None:
        """Clear all recorded call timestamps."""
        self._calls = []
