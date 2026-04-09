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
from typing import Callable, TypeVar


T = TypeVar("T")


def retry(
    fn: Callable[[], T],
    attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """Call fn repeatedly until it succeeds or attempts are exhausted.

    Args:
        fn: The callable to retry.
        attempts: Maximum number of attempts.
        delay: Initial delay in seconds between attempts.
        backoff: Multiplier applied to delay after each failure.
        exceptions: Exception types that trigger a retry.

    Returns:
        The return value of fn on success.

    Raises:
        Exception: The last exception raised by fn if all attempts fail.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_exc: Exception | None = None
    current_delay = delay
    for attempt in range(attempts):
        try:
            return fn()
        except exceptions as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(current_delay)
                current_delay *= backoff
    raise last_exc  # type: ignore[misc]


def is_retryable(status_code: int) -> bool:
    """Return True if an HTTP status code should trigger a retry.

    Args:
        status_code: The HTTP response status code.

    Returns:
        True for 429, 502, 503, and 504; False otherwise.
    """
    return status_code in {429, 502, 503, 504}
