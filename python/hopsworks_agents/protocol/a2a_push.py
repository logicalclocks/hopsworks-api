"""Push notifications: a task's events posted to the URL its caller registered.

A caller that will not hold a connection open for a long task registers a
webhook with it, and the agent posts the task's events there.
The ``a2a-sdk`` keeps the registered configs and calls the sender for every
event; what is ours is how the sender delivers:

- **Queued, never inline.** The SDK awaits the sender before it hands an event
  to the task's subscribers, so a receiver that is slow or down would stall the
  task's own stream by as long as the delivery takes. ``send_notification``
  only puts the event on the task's queue and returns; one worker per task
  delivers in order.
- **To the address that was checked.** A webhook URL is a caller's string. The
  host is resolved once, every address checked against the policy, and the
  request made to that address with the name kept for TLS and ``Host``, with
  no redirects: a DNS answer that changes between the check and the
  connection reaches nothing the check refused.
- **The terminal event is the one that must arrive.** Other events are best
  effort and may be dropped under backpressure, since the task store stays the
  source of truth. The terminal one is kept as an outbox row beside the task's
  lease until it was delivered, and the platform re-delivers it when this
  replica could not.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING, Any

from a2a.server.tasks.base_push_notification_sender import (
    BasePushNotificationSender,
)
from a2a.types import Task, TaskStatusUpdateEvent
from a2a.utils.constants import TERMINAL_TASK_STATES
from a2a.utils.proto_utils import to_stream_response
from google.protobuf.json_format import MessageToDict

from .models import AgentError


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from a2a.server.tasks.push_notification_config_store import (
        PushNotificationConfigStore,
    )
    from a2a.types import TaskPushNotificationConfig


_logger = logging.getLogger(__name__)

#: When set, the only hosts a webhook may be on: comma-separated, set by the platform's administrator.
ALLOWED_HOSTS_ENV = "HOPSWORKS_AGENT_PUSH_ALLOWED_HOSTS"
#: How long one delivery attempt may take.
ATTEMPT_TIMEOUT_S = 10.0
#: The pause before each attempt after the first; their number plus one is the attempts made.
BACKOFF_S = (1.0, 4.0, 16.0)
#: How many events one task may have waiting for delivery.
QUEUE_LIMIT = 256
#: How long a task's worker waits for another event before it ends.
IDLE_S = 60.0
#: How many finished tasks' delivery records are kept, for the lease and for tests.
RECORD_LIMIT = 2000


def _is_terminal(event: Any) -> bool:
    """Whether an event says the task ended."""
    if isinstance(event, (Task, TaskStatusUpdateEvent)):
        return event.status.state in TERMINAL_TASK_STATES
    return False


def _allowed_hosts() -> set[str]:
    return {
        host.strip().lower()
        for host in (os.environ.get(ALLOWED_HOSTS_ENV) or "").split(",")
        if host.strip()
    }


def _pin_webhook(url: str) -> Any:
    """Check a webhook URL against the policy and pin it to the address checked.

    Raises:
        AgentError: The URL is not one the agent may post to.
    """
    from urllib.parse import urlparse  # noqa: PLC0415

    from .a2a import _pin  # noqa: PLC0415

    allowed = _allowed_hosts()
    host = (urlparse(url).hostname or "").lower()
    if allowed and host not in allowed:
        raise AgentError(
            f"The push notification URL's host is not one the platform allows: {host}",
            "invalid_push_url",
            422,
        )
    return _pin(url, what="The push notification URL")


async def _valid_webhook(url: str) -> bool:
    """Whether a caller may register this webhook: the policy, applied when the config is stored."""
    try:
        await asyncio.to_thread(_pin_webhook, url)
    except AgentError as err:
        _logger.info("refused a push notification URL: %s", err.message)
        return False
    except Exception:  # noqa: BLE001 -- fail closed
        _logger.debug("could not check a push notification URL", exc_info=True)
        return False
    return True


class _Delivery:
    """What happened to one task's notifications, for the lease and for whoever asks."""

    def __init__(self) -> None:
        self.attempts = 0
        self.delivered = 0
        self.dropped = 0
        self.last_status: int | None = None
        self.last_error: str | None = None
        #: The terminal event reached every receiver registered for the task.
        self.terminal_delivered = False

    def _facts(self) -> dict[str, Any]:
        return {
            "attempts": self.attempts,
            "delivered": self.delivered,
            "dropped": self.dropped,
            "last_status": self.last_status,
            "last_error": self.last_error,
        }


class _PushSender(BasePushNotificationSender):
    """Delivers a task's events to its webhooks from a queue, in order, to the address it checked."""

    def __init__(
        self,
        config_store: PushNotificationConfigStore,
        on_terminal_delivered: Callable[[str], Awaitable[None]] | None = None,
    ):
        # the base's client is never used: each delivery opens its own, pinned
        super().__init__(httpx_client=None, config_store=config_store)  # type: ignore[arg-type]
        self._on_terminal_delivered = on_terminal_delivered
        self._queues: dict[str, asyncio.Queue] = {}
        self._workers: dict[str, asyncio.Task] = {}
        #: Per task, what delivery came to. Bounded, oldest forgotten first.
        self.records: dict[str, _Delivery] = {}

    # region the SDK's entry point

    async def send_notification(self, task_id: str, event: Any) -> None:
        """Queue the event for delivery and return at once."""
        record = self._record(task_id)
        queue = self._queues.get(task_id)
        if queue is None:
            queue = self._queues[task_id] = asyncio.Queue()
        if queue.qsize() >= QUEUE_LIMIT:
            self._drop_oldest(queue, record)
        queue.put_nowait(event)
        worker = self._workers.get(task_id)
        if worker is None or worker.done():
            self._workers[task_id] = asyncio.create_task(self._work(task_id, queue))

    # endregion

    def _record(self, task_id: str) -> _Delivery:
        record = self.records.get(task_id)
        if record is None:
            record = self.records[task_id] = _Delivery()
            while len(self.records) > RECORD_LIMIT:
                self.records.pop(next(iter(self.records)))
        return record

    @staticmethod
    def _drop_oldest(queue: asyncio.Queue, record: _Delivery) -> None:
        """The queue is full: the oldest event that is not the terminal one gives way."""
        kept: list[Any] = []
        dropped = False
        while not queue.empty():
            event = queue.get_nowait()
            if not dropped and not _is_terminal(event):
                dropped = True
                continue
            kept.append(event)
        for event in kept:
            queue.put_nowait(event)
        if dropped:
            record.dropped += 1

    async def _work(self, task_id: str, queue: asyncio.Queue) -> None:
        """Deliver the task's events in order until it ended or went quiet."""
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=IDLE_S)
                except asyncio.TimeoutError:
                    return
                terminal = _is_terminal(event)
                try:
                    delivered = await self._deliver(task_id, event)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 -- one event's failure does not end the queue
                    _logger.debug("push delivery failed", exc_info=True)
                    delivered = False
                if terminal:
                    if delivered:
                        await self._terminal_delivered(task_id)
                    return
        finally:
            if self._queues.get(task_id) is queue and queue.empty():
                self._queues.pop(task_id, None)
            if self._workers.get(task_id) is asyncio.current_task():
                self._workers.pop(task_id, None)

    async def _terminal_delivered(self, task_id: str) -> None:
        self._record(task_id).terminal_delivered = True
        if self._on_terminal_delivered is None:
            return
        try:
            await self._on_terminal_delivered(task_id)
        except Exception:  # noqa: BLE001 -- the outbox row is then delivered again by the platform
            _logger.debug(
                "could not clear the push outbox of %s", task_id, exc_info=True
            )

    async def _deliver(self, task_id: str, event: Any) -> bool:
        """Post one event to every webhook of the task; whether all of them took it.

        A task with no webhook is delivered, trivially.
        """
        try:
            configs = await self._config_store.get_info_for_dispatch(task_id)
        except Exception:  # noqa: BLE001 -- best effort, said in the log
            _logger.warning(
                "could not read the push configs of task %s", task_id, exc_info=True
            )
            return False
        if not configs:
            return True
        body = MessageToDict(to_stream_response(event))
        results = await asyncio.gather(
            *(self._post(task_id, config, body) for config in configs)
        )
        return all(results)

    async def _post(
        self, task_id: str, config: TaskPushNotificationConfig, body: dict[str, Any]
    ) -> bool:
        """One webhook, with its attempts: again on a connection error or a 5xx, never on a 3xx or 4xx."""
        import httpx  # noqa: PLC0415

        record = self._record(task_id)
        headers = self._build_headers(config, task_id)
        for attempt in range(len(BACKOFF_S) + 1):
            if attempt:
                await asyncio.sleep(BACKOFF_S[attempt - 1])
            record.attempts += 1
            try:
                # checked again at each attempt: the address is the one the
                # request is made to, so a changed answer cannot slip past
                target = await asyncio.to_thread(_pin_webhook, config.url)
            except AgentError as err:
                record.last_status, record.last_error = None, err.message
                _logger.warning(
                    "push notification for task %s not sent: %s", task_id, err.message
                )
                return False
            try:
                async with httpx.AsyncClient(
                    follow_redirects=False, timeout=ATTEMPT_TIMEOUT_S
                ) as client:
                    response = await client.post(
                        target.url,
                        json=body,
                        headers={**headers, "Host": target.host_header},
                        extensions={"sni_hostname": target.host},
                    )
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 -- a connection error: tried again
                record.last_status = None
                record.last_error = f"{type(err).__name__}: {err}"
                continue
            record.last_status = response.status_code
            if response.status_code < 300:
                record.last_error = None
                record.delivered += 1
                return True
            record.last_error = f"the receiver answered {response.status_code}"
            if response.status_code < 500:
                # a redirect is a second, unchecked destination; a 4xx is the
                # receiver's refusal: neither is tried again
                break
        _logger.warning(
            "push notification for task %s to %s failed: %s",
            task_id,
            config.url,
            record.last_error,
        )
        return False

    async def _drain(self, timeout: float) -> None:
        """At shutdown: give the queued notifications a moment, then stop."""
        workers = [w for w in self._workers.values() if not w.done()]
        if not workers:
            return
        _done, pending = await asyncio.wait(workers, timeout=timeout)
        for worker in pending:
            worker.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
