"""Waits that print each status change and fail naming the state they were stuck in.

Hopsworks reports why a pod does not start in a ``KubeMonitorDTO``: its pods, their last
condition and their Kubernetes events, where scheduling and image pull problems show up as
Warning events (``FailedScheduling: 0/5 nodes are available: 3 Insufficient memory``).
"""

from __future__ import annotations

import re
import time
from typing import Any

import click
from hopsworks.cli import output


_AGE_PART = re.compile(r"(\d+)([dhms])")
_AGE_SECONDS = {"d": 86400, "h": 3600, "m": 60, "s": 1}
_TOKEN = re.compile(r"([?&]token=)[^&\s\"']+")


def kube_reason(monitor: dict | None) -> str | None:
    """The most useful reason a pod gives in a KubeMonitorDTO, or None.

    The newest Warning event first, then the last condition of a pod that is not ready.
    """
    events: list[dict] = []
    conditions: list[dict] = []
    _collect(monitor, events, conditions)
    warnings = [e for e in events if e.get("type") == "Warning" and e.get("message")]
    if warnings:
        newest = min(
            enumerate(warnings), key=lambda pair: (_age(pair[1].get("age")), -pair[0])
        )[1]
        message, reason = _untokened(newest["message"]), newest.get("reason")
        return f"{reason}: {message}" if reason and reason not in message else message
    for condition in conditions:
        if condition.get("message"):
            return _untokened(condition["message"])
    return None


def _untokened(message: str) -> str:
    # A readiness probe event quotes the probe URL, and Jupyter's carries the server's token.
    return _TOKEN.sub(r"\1<hidden>", message)


def _items(value: Any) -> list[dict]:
    """The entries of a KubeEventDTO or KubePodDTO list: ``{"items": [...]}``."""
    items = value.get("items") if isinstance(value, dict) else None
    return [i for i in items or [] if isinstance(i, dict)]


def _collect(node: Any, events: list[dict], conditions: list[dict]) -> None:
    # Pods sit under `deployment` or `job` depending on what runs them, so read them wherever they are.
    if isinstance(node, list):
        for item in node:
            _collect(item, events, conditions)
    elif isinstance(node, dict):
        events.extend(_items(node.get("events")))
        for pod in _items(node.get("pods")):
            # The backend sends a pod condition without its status: a pod not ready names what holds it.
            if pod.get("ready") is not True and isinstance(
                pod.get("lastCondition"), dict
            ):
                conditions.append(pod["lastCondition"])
        for key, value in node.items():
            if key != "events":
                _collect(value, events, conditions)


def _age(age: Any) -> float:
    """Seconds in a Kubernetes age such as ``2m30s``; unknown ages sort last."""
    parts = _AGE_PART.findall(str(age or ""))
    if not parts:
        return float("inf")
    return sum(int(n) * _AGE_SECONDS[unit] for n, unit in parts)


class Watch:
    """A bounded wait that prints a line each time the state or its reason changes; silent in JSON mode."""

    def __init__(self, label: str, timeout_s: float, interval_s: float):
        self.label = label
        self.timeout_s = timeout_s
        self.interval_s = interval_s
        self.started = time.monotonic()
        self.state: str | None = None
        self.reason: str | None = None
        self._printed: tuple[str | None, str | None] | None = None

    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def left(self) -> float:
        return max(0.0, self.timeout_s - self.elapsed())

    def see(self, state: str, reason: str | None = None) -> None:
        self.state, self.reason = state, reason
        if self._printed == (state, reason):
            return
        self._printed = (state, reason)
        if output.JSON_MODE:
            return
        line = f"{self.label}: {state}" + (f": {reason}" if reason else "")
        click.echo(f"{line} ({self.elapsed():.0f} s)")

    def sleep(self) -> None:
        time.sleep(self.interval_s)

    def expired(self) -> bool:
        return self.elapsed() >= self.timeout_s

    def fail(self, hint: str) -> None:
        reason = f": {self.reason.rstrip('.')}" if self.reason else ""
        raise click.ClickException(
            f"{self.label} still {self.state} after {self.elapsed():.0f} s{reason}. {hint}"
        )
