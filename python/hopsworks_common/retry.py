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

import functools
import logging
import time
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Callable


_logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior on transient errors.

    Controls the number of attempts and the exponential back-off delay
    applied between them.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        exceptions: tuple[type[Exception], ...] = (Exception,),
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if delay < 0:
            raise ValueError("delay must be non-negative")
        if backoff < 1:
            raise ValueError("backoff must be at least 1")
        self._max_attempts = max_attempts
        self._delay = delay
        self._backoff = backoff
        self._exceptions = exceptions

    @property
    def max_attempts(self) -> int:
        """Maximum number of attempts, including the first call."""
        return self._max_attempts

    @property
    def delay(self) -> float:
        """Initial wait time in seconds before the first retry."""
        return self._delay

    @property
    def backoff(self) -> float:
        """Multiplier applied to the delay between successive retries."""
        return self._backoff

    @property
    def exceptions(self) -> tuple[type[Exception], ...]:
        """Exception types that trigger a retry."""
        return self._exceptions

    def __repr__(self) -> str:
        return (
            f"RetryConfig(max_attempts={self._max_attempts}, "
            f"delay={self._delay}, backoff={self._backoff})"
        )


def with_retry(config: RetryConfig | None = None) -> Callable:
    """Decorate a function to automatically retry it on transient exceptions.

    Parameters:
        config: Retry configuration; a default `RetryConfig` is used when None.

    Returns:
        A decorator that wraps the target function with retry logic.
    """
    resolved = config or RetryConfig()

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            delay = resolved.delay
            last_exc: Exception | None = None
            for attempt in range(resolved.max_attempts):
                try:
                    return fn(*args, **kwargs)
                except resolved.exceptions as exc:
                    last_exc = exc
                    if attempt < resolved.max_attempts - 1:
                        _logger.debug(
                            "Attempt %d/%d failed (%s), retrying in %.1fs",
                            attempt + 1,
                            resolved.max_attempts,
                            type(exc).__name__,
                            delay,
                        )
                        time.sleep(delay)
                        delay *= resolved.backoff
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator
