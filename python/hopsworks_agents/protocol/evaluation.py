"""Per-request evaluation mode.

An evaluation trial arrives at the agent as an ordinary chat request, with the
run, suite, task and trial ids in W3C ``baggage`` — the runner already sends
them so the trace can be found afterwards. This module lets the *agent* read
the same baggage and behave accordingly: skip the writes that would reach
production, while calling the same tools with the same arguments so the run
measures the agent that serves customers and not a different one.

Two ways to be in evaluation, and the environment wins:

- ``EVAL_MODE=true`` on the deployment. Everything the deployment does is an
  evaluation; nothing needs checking per request. This is the whole-deployment
  switch, for a deployment that exists only to be evaluated.
- The request carries ``hopsworks.eval.run_id`` **and** the app declared
  ``eval_per_request``. Then one deployment serves customers and evaluations
  at once, and each turn decides for itself.

The second form trusts a header, so the header is verified: the run has to
exist in Hopsworks and target this deployment before the turn is treated as an
evaluation. Without that, anyone who can reach the agent could send
``baggage: hopsworks.eval.run_id=x`` and have it tell a customer their order is
recorded while recording nothing — the exact failure the sandboxed suites exist
to catch. A trial that cannot be verified is refused rather than run as
production traffic: the runner then sees an errored trial, and no real write
happens either way.

Dependency-free beyond the standard library on purpose: it sits on the request
path of every agent and must not pull in the eval package (see
``tests/test_import_isolation.py``).
"""

from __future__ import annotations

import contextvars
import dataclasses
import logging
import os
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from . import conventions


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping


log = logging.getLogger(__name__)

__all__ = [
    "EvalTrial",
    "RunVerifier",
    "active",
    "current_trial",
    "env_eval_mode",
    "in_evaluation",
    "parse_baggage",
]


@dataclasses.dataclass(frozen=True)
class EvalTrial:
    """What the runner said about the trial this turn belongs to."""

    run_id: str
    suite_id: str = ""
    suite_version: str = ""
    task_id: str = ""
    task_version: str = ""
    trial_id: str = ""
    trial_index: int | None = None


_current: contextvars.ContextVar[EvalTrial | None] = contextvars.ContextVar(
    "hopsworks_agent_eval_trial", default=None
)


def env_eval_mode() -> bool:
    """The whole-deployment switch, ``EVAL_MODE``."""
    return os.environ.get(conventions.EVAL_MODE_ENV, "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def current_trial() -> EvalTrial | None:
    """The verified trial this turn belongs to, or None.

    None also when the deployment runs with ``EVAL_MODE`` and the request
    carried no baggage — a turn can be an evaluation without being a trial.
    """
    return _current.get()


def in_evaluation() -> bool:
    """Whether the code asking should skip its production side effects.

    Read this in tools, which get no context object. True under ``EVAL_MODE``,
    and true inside a turn whose trial was verified. Everything the run can
    observe should stay the same either way: the same tools, called with the
    same arguments, returning the same text.
    """
    return env_eval_mode() or _current.get() is not None


@contextmanager
def active(trial: EvalTrial | None) -> Iterator[None]:
    """Make ``trial`` the current one for the duration of a turn.

    A no-op for None, so a call site can be written once for both cases.
    ContextVar rather than a global because two turns can be in flight at
    once, and a task or thread started inside the block inherits it.
    """
    if trial is None:
        yield
        return
    token = _current.set(trial)
    try:
        yield
    finally:
        _current.reset(token)


# ── the header ───────────────────────────────────────────────────────────────

_FIELDS = {
    conventions.EVAL_RUN_ID: "run_id",
    conventions.EVAL_SUITE_ID: "suite_id",
    conventions.EVAL_SUITE_VERSION: "suite_version",
    conventions.EVAL_TASK_ID: "task_id",
    conventions.EVAL_TASK_VERSION: "task_version",
    conventions.EVAL_TRIAL_ID: "trial_id",
    conventions.EVAL_TRIAL_INDEX: "trial_index",
}


def parse_baggage(headers: Mapping[str, str] | None) -> EvalTrial | None:
    """The eval ids in a request's W3C ``baggage``, or None without a run id.

    Parsed by hand rather than through OpenTelemetry so it works on a
    deployment with tracing off — the runner refuses those anyway, but the
    SDK must not fail a customer's request over a header it did not ask for.
    A malformed header is treated as absent for the same reason.
    """
    if not headers:
        return None
    raw = next(
        (value for key, value in headers.items() if str(key).lower() == "baggage"),
        None,
    )
    if not raw:
        return None
    found: dict[str, Any] = {}
    for member in str(raw).split(","):
        entry = member.split(";", 1)[0].strip()  # drop properties
        if "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        field = _FIELDS.get(key.strip())
        if field is None:
            continue
        found[field] = unquote(value.strip())
    if not found.get("run_id"):
        return None
    if "trial_index" in found:
        try:
            found["trial_index"] = int(found["trial_index"])
        except ValueError:
            found.pop("trial_index")
    return EvalTrial(**found)


# ── verification ─────────────────────────────────────────────────────────────


class RunVerifier:
    """Checks a claimed run with Hopsworks before a turn is treated as one.

    A run is accepted when Hopsworks knows it and, if both sides know which
    deployment they are, it targets this one. Accepted runs are remembered, so
    a suite of a hundred tasks costs one lookup rather than a hundred.
    Rejections are not remembered: a transient error on the first trial must
    not fail the rest of the run, and a forged id costing one request per
    attempt is the acceptable side of that trade.

    ``lookup`` takes a run id and returns the run as a dict, or None when it
    does not exist. The default asks Hopsworks through whatever credentials the
    container has; tests pass their own.
    """

    def __init__(self, lookup: Callable[[str], dict[str, Any] | None] | None = None):
        self._lookup = lookup or _lookup_run
        self._accepted: set[str] = set()
        self._lock = threading.Lock()

    def verify(self, trial: EvalTrial) -> bool:
        with self._lock:
            if trial.run_id in self._accepted:
                return True
        try:
            run = self._lookup(trial.run_id)
        except Exception:  # noqa: BLE001 — any failure means "not verified"
            log.warning(
                "could not verify evaluation run %s", trial.run_id, exc_info=True
            )
            return False
        if not run:
            log.warning("evaluation run %s is unknown to Hopsworks", trial.run_id)
            return False
        deployment = os.environ.get("DEPLOYMENT_ID")
        target = run.get("deploymentId")
        if deployment and target is not None and str(target) != str(deployment):
            log.warning(
                "evaluation run %s targets deployment %s, not this one (%s)",
                trial.run_id,
                target,
                deployment,
            )
            return False
        with self._lock:
            self._accepted.add(trial.run_id)
        return True


def _lookup_run(run_id: str) -> dict[str, Any] | None:
    """GET the run from the project's evaluation API.

    Through the connected hopsworks client when there is one — it has already
    solved the cluster's own CA and the container's credentials — and
    otherwise from the same environment a Hopsworks container is given.
    """
    client = _hopsworks_client()
    if client is not None:
        from hopsworks_common.client.exceptions import RestAPIError

        try:
            return client._send_request(
                "GET",
                ["project", client._project_id, "agent-evals", "runs", run_id],
            )
        except RestAPIError as err:
            if getattr(getattr(err, "response", None), "status_code", None) == 404:
                return None
            raise

    import json
    import urllib.error
    import urllib.request

    host = os.environ.get("REST_ENDPOINT") or os.environ.get("HOPSWORKS_HOST")
    project_id = os.environ.get("HOPSWORKS_PROJECT_ID")
    if not host or not project_id:
        raise RuntimeError(
            "no hopsworks client and no REST_ENDPOINT/HOPSWORKS_PROJECT_ID: "
            "cannot reach the evaluation API"
        )
    request = urllib.request.Request(
        f"{host.rstrip('/')}/hopsworks-api/api/project/{project_id}"
        f"/agent-evals/runs/{run_id}",
        headers={"Authorization": _authorization()},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read() or b"null")
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return None
        raise


def _hopsworks_client() -> Any | None:
    try:
        from hopsworks_common import client
    except ImportError:
        return None
    accessor = getattr(client, "_get_instance", None) or getattr(
        client, "get_instance", None
    )
    if accessor is None:
        return None
    try:
        instance = accessor()
    except Exception:  # noqa: BLE001 — not connected
        return None
    if instance is None or not hasattr(instance, "_send_request"):
        return None
    if not getattr(instance, "_project_id", None):
        return None
    return instance


def _authorization() -> str:
    api_key = os.environ.get("HOPSWORKS_API_KEY")
    if api_key:
        return "ApiKey " + api_key
    token = os.path.join(os.environ.get("SECRETS_DIR", ""), "token.jwt")
    if os.path.exists(token):
        with open(token, encoding="utf-8") as handle:
            return "Bearer " + handle.read().strip()
    raise RuntimeError("no HOPSWORKS_API_KEY and no token.jwt in SECRETS_DIR")
