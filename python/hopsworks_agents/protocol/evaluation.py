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
import time
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

    Read the way the propagator reads it, because the two have to agree: keys
    are percent-decoded and the last value for a key wins. Decoding only
    values, and taking the first of several headers, meant
    ``run_id=LIVE,hopsworks%2Eeval%2Erun_id=OTHER`` verified as LIVE while the
    spans were stamped OTHER -- so a caller who knows one live run could label
    a turn as a trial of another.
    """
    if not headers:
        return None
    getlist = getattr(headers, "getlist", None)
    raws = (
        [v for v in getlist("baggage") if v]
        if callable(getlist)
        else [
            value
            for key, value in headers.items()
            if str(key).lower() == "baggage" and value
        ]
    )
    if not raws:
        return None
    found: dict[str, Any] = {}
    for raw in raws:
        for member in str(raw).split(","):
            entry = member.split(";", 1)[0].strip()  # drop properties
            if "=" not in entry:
                continue
            key, value = entry.split("=", 1)
            field = _FIELDS.get(unquote(key.strip()))
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


#: Run states a trial may arrive under. PENDING is included because the runner
#: reports RUNNING from inside the execution, and the first trial can beat it.
LIVE_RUN_STATUSES = frozenset({"PENDING", "RUNNING"})

#: How long an accepted run stays accepted, in seconds. Long enough that a suite
#: is one lookup rather than one per task, short enough that a run which has
#: finished or been stopped stops being accepted while the process lives.
ACCEPTANCE_TTL_S = 60.0


class RunVerifier:
    """Checks a claimed run with Hopsworks before a turn is treated as one.

    A run is accepted when Hopsworks knows it, it is still going, and it
    targets this deployment. Accepted runs are remembered for
    :data:`ACCEPTANCE_TTL_S`, so a suite of a hundred tasks costs one lookup
    rather than a hundred without the answer standing for ever -- a run that
    finished, or was stopped, stops being a licence to skip production side
    effects. Rejections are not remembered: a transient error on the first
    trial must not fail the rest of the run, and a forged id costing one
    request per attempt is the acceptable side of that trade.

    ``lookup`` takes a run id and returns the run as a dict, or None when it
    does not exist. The default asks Hopsworks through whatever credentials the
    container has; tests pass their own.
    """

    def __init__(
        self,
        lookup: Callable[[str], dict[str, Any] | None] | None = None,
        *,
        now: Callable[[], float] = time.monotonic,
    ):
        self._lookup = lookup or _lookup_run
        self._accepted: dict[str, float] = {}
        self._lock = threading.Lock()
        self._now = now

    def verify(self, trial: EvalTrial) -> bool:
        now = self._now()
        with self._lock:
            until = self._accepted.get(trial.run_id)
            if until is not None and until > now:
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
        status = str(run.get("status") or "").upper()
        if status and status not in LIVE_RUN_STATUSES:
            # A finished run is not a licence. Run ids are listed to every project
            # member and stamped on trace attributes, so without this anyone could
            # replay an old one and have a real customer turn skip the write tools
            # while reporting success.
            log.warning("evaluation run %s is %s, not running", trial.run_id, status)
            return False
        deployment = os.environ.get("DEPLOYMENT_ID")
        target = run.get("deploymentId")
        if target is None:
            log.warning("evaluation run %s names no deployment", trial.run_id)
            return False
        if deployment and str(target) != str(deployment):
            log.warning(
                "evaluation run %s targets deployment %s, not this one (%s)",
                trial.run_id,
                target,
                deployment,
            )
            return False
        if not deployment:
            # Nothing to compare against: this process is not running as a Hopsworks
            # deployment, which is the local-development case. Said once per run
            # rather than silently, since it is the one check that cannot be made.
            log.warning(
                "DEPLOYMENT_ID is not set, so run %s was accepted without checking "
                "that it targets this agent",
                trial.run_id,
            )
        with self._lock:
            self._accepted[trial.run_id] = now + ACCEPTANCE_TTL_S
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
                # Bounded like the urllib path below. Without it a stalled
                # connection blocks for ever, and this is awaited in the
                # default executor before every turn that carries a run id --
                # the same threads every sync handler and every memory
                # to_thread call share.
                timeout=10,
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
        with urllib.request.urlopen(
            request, timeout=10, context=_tls_context()
        ) as response:
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


def _tls_context() -> Any | None:
    """How to verify ``REST_ENDPOINT``, or None for the system default.

    The internal endpoint is signed by the Hopsworks CA, which no system trust
    store has heard of: verification failed with "unable to get local issuer
    certificate", the lookup raised, and the agent answered 403 to every
    evaluation request it was sent. A deployment is given the chain as a JKS
    truststore, so this finds it the way the platform client does.
    """
    chain = _ca_chain()
    if not chain:
        return None
    import ssl

    context = ssl.create_default_context(cafile=chain)
    # The same switch the platform client honours, for a cluster whose
    # certificate does not carry the name its endpoint is reached by.
    if os.environ.get("HOPSWORKS_HOSTNAME_VERIFICATION", "true").lower() not in (
        "true",
        "1",
        "y",
        "yes",
    ):
        context.check_hostname = False
    return context


#: Where the platform client materialises the chain; shared so that whichever
#: of the two runs first, the other reuses the file rather than parsing again.
_CA_CHAIN_PATH = os.path.join("/tmp", "ca_chain.pem")


def _ca_chain() -> str | None:
    """A PEM of the cluster's CA chain.

    Named by the environment, already materialised, or converted here from the
    truststore the pod was given.
    """
    for name in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        named = os.environ.get(name)
        if named and os.path.exists(named):
            return named
    if os.path.exists(_CA_CHAIN_PATH):
        return _CA_CHAIN_PATH
    material = os.environ.get("MATERIAL_DIRECTORY")
    if not material or not os.path.isdir(material):
        return None
    try:
        import glob

        from hopsworks_common.client.base import Client

        def one(suffix: str) -> str | None:
            found = sorted(glob.glob(os.path.join(material, f"*{suffix}")))
            return found[0] if found else None

        trust, key_store, password_file = (
            one("__tstore.jks"),
            one("__kstore.jks"),
            one("__cert.key"),
        )
        if not trust or not password_file:
            return None
        with open(password_file, encoding="utf-8") as handle:
            password = handle.read()
        _, trusted = Client._load_jks(trust, password)
        signers = []
        if key_store:
            _, signers = Client._load_jks(key_store, password)
        pem = "".join(Client._der_to_pem(cert) for cert in signers + trusted)
        if not pem:
            return None
        Client._replace_file(_CA_CHAIN_PATH, pem)
        return _CA_CHAIN_PATH
    except Exception:  # noqa: BLE001 - a chain we cannot build is not a failure here
        log.debug("could not materialise the CA chain from %s", material, exc_info=True)
        return None


def _authorization() -> str:
    """However this container is entitled to call the API.

    A serving pod is given an API key at ``$SECRETS_DIR/api.key`` -- not the
    JWT a job gets -- so a deployment verifying a trial had nothing to
    authenticate with and answered 403 to every evaluation request, while the
    run waited and then failed on a timeout.

    Read per call rather than once: both are rotated on disk while a
    deployment runs, and a deployment outlives the copy read at startup.
    """
    api_key = os.environ.get("HOPSWORKS_API_KEY")
    if api_key:
        return "ApiKey " + api_key
    secrets = os.environ.get("SECRETS_DIR", "")
    token = os.path.join(secrets, "token.jwt")
    if os.path.exists(token):
        with open(token, encoding="utf-8") as handle:
            return "Bearer " + handle.read().strip()
    key_file = os.path.join(secrets, "api.key")
    if os.path.exists(key_file):
        with open(key_file, encoding="utf-8") as handle:
            return "ApiKey " + handle.read().strip()
    raise RuntimeError(
        "no HOPSWORKS_API_KEY, and neither token.jwt nor api.key in "
        f"SECRETS_DIR ({secrets or 'unset'})"
    )
