"""The project's egress policy, read from ``GET /project/<id>/kube/egress``.

The answer is ``{mode, enforced, allowed: [{port, endPort, protocol, source}], ask}``.
``ask`` is filled for the port and protocol the request names and is null without them.
"""

from __future__ import annotations

from typing import Any


def read(port: int | None = None, protocol: str = "TCP") -> dict[str, Any]:
    """Read the policy; with a port, ``ask`` carries how to request it."""
    from hopsworks_common.core import rest  # noqa: PLC0415

    query = {"query_params": {"port": port, "protocol": protocol}} if port else {}
    answer = rest._send_request("GET", rest._project_path("kube", "egress"), **query)
    if not isinstance(answer, dict):
        raise ValueError("kube/egress answered something other than an object")
    return answer


def _range(entry: dict[str, Any]) -> str:
    port, end = entry["port"], entry.get("endPort")
    span = f"{port}-{end}" if end and end != port else f"{port}"
    return f"{span}/{entry.get('protocol') or 'TCP'}"


def is_open(policy: dict[str, Any], port: int, protocol: str = "TCP") -> bool:
    """True when the policy does not enforce, or lists the port under the protocol."""
    if not policy.get("enforced"):
        return True
    for entry in policy.get("allowed") or []:
        if (entry.get("protocol") or "TCP").upper() != protocol.upper():
            continue
        if entry["port"] <= port <= (entry.get("endPort") or entry["port"]):
            return True
    return False


def summary(policy: dict[str, Any]) -> str:
    """One line: ``off``, ``not enforced in this project``, or the ports by source."""
    if str(policy.get("mode") or "").lower() == "off":
        return "off"
    if not policy.get("enforced"):
        return "not enforced in this project"
    by_source: dict[str, list[str]] = {}
    for entry in policy.get("allowed") or []:
        by_source.setdefault(entry.get("source") or "?", []).append(_range(entry))
    groups = "; ".join(f"{' '.join(ports)} ({src})" for src, ports in by_source.items())
    return f"enforced: {groups}" if groups else "enforced: no port open"


def closed_line(project: str, port: int, ask: str | None, protocol: str = "TCP") -> str:
    """The refused-port text the dlt job prints after ``hopsworks egress: ``."""
    text = f"Egress from project {project} to port {port}/{protocol} is closed."
    return f"{text} {ask}" if ask else text


def row() -> list[str]:
    """The ``Egress`` row of ``hops project info``; ``unavailable`` when the read fails."""
    try:
        return ["Egress", summary(read())]
    except Exception:  # noqa: BLE001 - the rest of the info still prints
        return ["Egress", "unavailable"]


def warn_if_closed(project: str | None, port: Any) -> None:
    """Warn once when an enforced policy does not list the source's port.

    The connection test runs in the install namespace and passes there, so the
    job's refusal is the first time the user would learn of it. A read that fails
    prints nothing.
    """
    from hopsworks.cli import output  # noqa: PLC0415

    try:
        port = int(port)
        policy = read(port)
        if is_open(policy, port):
            return
        output.warn("%s", closed_line(project or "?", port, policy.get("ask")))
    except Exception:  # noqa: BLE001 - a failed read leaves the command as it was
        return
