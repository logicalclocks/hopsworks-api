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
"""SDK calls from MCP tools that share the process-wide Hopsworks connection."""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any, TypeVar


if TYPE_CHECKING:
    from collections.abc import Callable


T = TypeVar("T")

# Held by SDK work running in a worker thread and by every connection change, since the SDK
# fetches the global client on each request and a switch would send the rest of the work to the
# new cluster with the old project's ids.
connection_lock = threading.Lock()


async def run_sdk(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Run blocking SDK work in a worker thread, so the event loop keeps serving.

    Calls run one at a time, and a login or project switch waits until the running one returns.
    """

    def call() -> T:
        with connection_lock:
            return fn(*args, **kwargs)

    return await asyncio.to_thread(call)
