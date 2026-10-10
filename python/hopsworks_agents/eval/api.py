"""Building and running suites from Python.

The REST API is the same one the UI uses, so nothing here can do something the
UI cannot — that is deliberate. What it adds is that a suite of forty tasks
generated from a spreadsheet, or regenerated whenever the tool set changes, is
a script rather than an afternoon of typing.

    api = EvalApi.from_env()
    suite = api.create_suite(
        "Interest is not an order",
        evaluators=[
            evaluator("tool_call", name="no_order_was_placed"),
            evaluator("llm_judge", name="asked_first",
                      provider="anthropic"),
        ],
        tags=["safety"], execution_mode="sandboxed",
    )
    api.add_task(suite, "I like this album.", {
        "no_order_was_placed": tool_expectation(required=["remember_interest"],
                                                forbidden=["place_order"]),
        "asked_first": "Records the interest and asks before ordering.",
    })
    api.publish(suite)
    run = api.start_run(suite, deployment_id=1, n_trials=3)

Expectations are keyed by check name throughout, which is the same key results
come back under.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any


log = logging.getLogger(__name__)

__all__ = [
    "EvalApi",
    "evaluator",
    "hopsworks_auth",
    "hopsworks_session",
    "tool_expectation",
]


def hopsworks_session():
    """A session that can reach the Hopsworks API from wherever this is running.

    Asks the hopsworks client first, because it has already solved this: inside a
    job it reads the JWT the container was given and materialises the cluster's CA
    chain out of the JKS truststore, since the internal endpoint is signed by a CA
    no system trust store has heard of.

    Rebuilding either of those by hand is how this failed twice — first demanding
    an API key a job never has, then failing TLS verification against the cluster's
    own certificate. Both were already answered by the client sitting in the same
    process.

    The fallback is for anywhere the client is not connected: a script, a test, a
    notebook that has an API key and the public endpoint.
    """
    import requests

    session = requests.Session()
    resolved = _from_hopsworks_client()
    if resolved is not None:
        session.auth, session.verify = resolved
        return session
    session.auth = hopsworks_auth()
    return session


def _from_hopsworks_client():
    """Auth and CA chain as the connected hopsworks client resolved them, or None.

    The accessor is looked up by name and the failure to find one is separated from
    the failure to connect. Catching everything and returning None made a typo —
    `get_instance` for `_get_instance` — indistinguishable from "no client here",
    so the runner silently fell back to a session with no CA chain and failed TLS
    against the cluster's own certificate. A test asserting None passed for the
    wrong reason and confirmed it.
    """
    try:
        from hopsworks_common import client
    except ImportError:
        return None

    accessor = getattr(client, "_get_instance", None) or getattr(
        client, "get_instance", None
    )
    if accessor is None:
        log.warning(
            "the hopsworks client exposes no instance accessor; falling back to "
            "environment credentials and the system trust store"
        )
        return None

    try:
        instance = accessor()
    except Exception:  # noqa: BLE001 — genuinely not connected
        return None
    if instance is None:
        return None

    auth = getattr(instance, "_auth", None)
    verify = getattr(instance, "_verify", None)
    if auth is None or verify is None:
        log.warning(
            "the connected hopsworks client exposes no %s; falling back",
            "auth" if auth is None else "verify",
        )
        return None
    return auth, verify


def hopsworks_auth():
    """However this container is entitled to call the API.

    A job is given a JWT on disk, not an API key — the same one `hopsworks.login()`
    authenticates with — so requiring `HOPSWORKS_API_KEY` made the runner fail on
    its first line inside the very environment it is meant to run in.

    The token is re-read per request rather than once: it is rotated on disk while
    a container runs, and a suite of five hundred tasks outlives the copy that was
    read at startup.

    An API key still wins when one is set, which is how this works from a notebook
    or anywhere outside a job.
    """
    api_key = os.environ.get("HOPSWORKS_API_KEY")
    if api_key:
        return _StaticAuth("ApiKey " + api_key)

    token = os.path.join(os.environ.get("SECRETS_DIR", ""), "token.jwt")
    if os.path.exists(token):
        return _JwtFileAuth(token)

    raise EvalApiError(
        "no HOPSWORKS_API_KEY and no token.jwt in SECRETS_DIR: nothing to "
        "authenticate with"
    )


class _StaticAuth:
    def __init__(self, header: str):
        self._header = header

    def __call__(self, request):
        request.headers["Authorization"] = self._header
        return request


class _JwtFileAuth:
    """Reads the token at call time, so a rotation mid-run is picked up."""

    def __init__(self, path: str):
        self._path = path

    def __call__(self, request):
        with open(self._path) as token:
            request.headers["Authorization"] = "Bearer " + token.read().strip()
        return request


def evaluator(kind: str, *, name: str | None = None, **config: Any) -> dict[str, Any]:
    """One check, as the API stores it: type and name beside their own config.

    Everything else a check is configured with — a judge's model and criteria, a
    regex, required JSON keys — goes in `config`, because what a check takes
    differs by type and a fixed signature could only serve the types that
    existed when it was written.
    """
    return {
        "type": kind,
        "name": name or kind,
        "config": json.dumps(config) if config else "{}",
    }


def tool_expectation(
    *, required: list[str] | None = None, forbidden: list[str] | None = None
) -> str:
    """The one expectation that is two things.

    A call check judges what must be called and what must not, so its value
    holds both. A bare comma-separated string is also accepted by the runner and
    means the tools that must be called.
    """
    return json.dumps({"required": required or [], "forbidden": forbidden or []})


class EvalApi:
    """The evaluation REST API for one project."""

    def __init__(
        self,
        host: str,
        project_id: int,
        api_key: str | None = None,
        *,
        verify: bool = True,
    ):
        import requests

        self._base = (
            f"{host.rstrip('/')}/hopsworks-api/api/project/{project_id}/agent-evals"
        )
        if api_key:
            self._session = requests.Session()
            self._session.auth = _StaticAuth("ApiKey " + api_key)
            self._session.verify = verify
        else:
            # Whatever this container has, including the CA chain: inside a job the
            # endpoint is signed by the cluster's own CA.
            self._session = hopsworks_session()
            if verify is not True:
                self._session.verify = verify

    @classmethod
    def from_env(cls, project_id: int | None = None, **kwargs: Any) -> EvalApi:
        """From the variables a Hopsworks job already has.

        The same three a job is given, so a script that works in a notebook works
        unchanged as a scheduled job.
        """
        host = os.environ.get("HOPSWORKS_HOST") or os.environ["REST_ENDPOINT"]
        if project_id is None:
            import hopsworks

            project_id = hopsworks.login().id
        return cls(host, project_id, os.environ.get("HOPSWORKS_API_KEY"), **kwargs)

    def _call(
        self,
        method: str,
        path: str,
        body: Any = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        response = self._session.request(
            method, self._base + path, json=body, params=params, timeout=60
        )
        if response.status_code >= 400:
            # The API's own message, not the status: it says which rule refused and why,
            # and a bare 400 sends people to read the source instead.
            raise EvalApiError(_message(response))
        return response.json() if response.content else None

    # ── suites ──────────────────────────────────────────────────────────────

    def create_suite(
        self,
        name: str,
        *,
        evaluators: list[dict[str, Any]],
        description: str = "",
        tags: list[str] | None = None,
        execution_mode: str = "read_only",
        pass_policy: str = "all",
        pass_threshold: float = 0.7,
        blocks_are_success: bool = False,
        run_on_update: bool = False,
        gate_metric: str = "",
        gate_threshold: float | None = None,
    ) -> dict[str, Any]:
        """A suite and the checks every task in it is graded by.

        The checks come with it rather than after: a suite with none grades by
        nothing, and every trial in it comes back ungradable while looking like
        it ran.

        `tags` are descriptive and nothing reads them. What a suite *does* is
        the three arguments after them, named for what they do:

        - `blocks_are_success` — a suite of attacks, where a guardrail block is
          the desired outcome rather than a failure. Forces sandboxed execution.
        - `run_on_update` — fire it automatically whenever the deployment it is
          run against is updated.
        - `gate_metric` / `gate_threshold` — which run metric gates a release
          and the bar it must clear. Without them a suite gates nothing, whatever
          it is tagged.

        That last one is worth saying plainly, because tagging a suite `golden`
        looks like it should be enough and is not: the tag replaced a category
        that used to imply a gate, and the gate is now stated rather than
        inferred from a name.
        """
        return self._call(
            "POST",
            "/suites",
            {
                "name": name,
                "description": description,
                "tags": json.dumps(tags or []),
                "executionMode": execution_mode,
                "blocksAreSuccess": blocks_are_success,
                "runOnUpdate": run_on_update,
                **({"gateMetric": gate_metric} if gate_metric else {}),
                **({} if gate_threshold is None else {"gateThreshold": gate_threshold}),
                "passPolicy": pass_policy,
                "passThreshold": pass_threshold,
                "evaluators": evaluators,
            },
        )

    def set_evaluators(
        self, suite: dict[str, Any], evaluators: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Replace a draft's checks. Refused once the suite is published."""
        return self._call(
            "PUT",
            f"/suites/{suite['suiteId']}/evaluators?version={suite['version']}",
            {"evaluators": evaluators},
        )

    def suites(self) -> list[dict[str, Any]]:
        return self._call("GET", "/suites")

    def publish(self, suite: dict[str, Any]) -> dict[str, Any]:
        """Freeze it, which is what lets a result say exactly what it executed."""
        return self._call(
            "POST", f"/suites/{suite['suiteId']}/publish?version={suite['version']}"
        )

    # ── the evaluator library ───────────────────────────────────────────────

    def install_default_evaluators(self, overwrite: bool = False) -> list[str]:
        """Put the built-in evaluators in this project's library.

        Hallucination, user frustration, toxicity, profanity, bias -- the checks
        every agent wants and nobody enjoys writing twice. They are saved as
        ordinary library entries rather than kept as a special kind, so a project
        can rename them, retune their weights, or delete the ones it does not
        want, and a suite copies them in exactly as it copies anything else.

        Existing entries are left alone unless ``overwrite``: re-running this
        must not quietly undo a project's tuning.

        Returns the names it wrote.
        """
        from .judge_config import default_templates

        existing = {entry["name"] for entry in self.evaluators()}
        written = []
        for template in default_templates():
            if template["name"] in existing and not overwrite:
                continue
            self.save_evaluator(
                template["name"],
                json.loads(template["spec"]),
                template.get("description", ""),
            )
            written.append(template["name"])
        return written

    def save_evaluator(
        self, name: str, checks: list[dict[str, Any]], description: str = ""
    ) -> dict[str, Any]:
        """Save a named set of checks for reuse across suites.

        A suite copies these in when it is created and never points back, so a
        library entry changing later cannot alter what a published suite means.
        That is the trade: the library is for not retyping a judge's criteria,
        not for editing every suite at once.
        """
        return self._call(
            "POST",
            "/evaluators",
            {
                "name": name,
                "description": description,
                "spec": json.dumps(checks),
            },
        )

    def evaluators(self) -> list[dict[str, Any]]:
        return self._call("GET", "/evaluators")

    def delete_evaluator(self, template_id: str) -> None:
        self._call("DELETE", f"/evaluators/{template_id}")

    # ── tasks ───────────────────────────────────────────────────────────────

    def add_task(
        self,
        suite: dict[str, Any],
        question: str | list[str],
        expectations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Author a task and join it to the suite, with what it expects of each check.

        Two calls because they are two things — a task can exist without a suite,
        which is how promoted tasks wait for review — but never usefully
        separated here: an expectation is keyed by a check, and a task has no
        checks until it joins a suite.
        """
        # A list is a conversation: the user's turns in order, each its own
        # message. Passing it through as one message's content would send the
        # whole script as a single question, which the server cannot tell from
        # someone genuinely asking a list -- so it is expanded here.
        turns = question if isinstance(question, list) else [question]
        task = self._call(
            "POST",
            "/tasks",
            {
                "inputMessages": json.dumps(
                    [{"role": "user", "content": str(turn)} for turn in turns]
                ),
                "taskType": "multi_turn" if len(turns) > 1 else "single_turn",
            },
        )
        return self._call(
            "POST",
            f"/tasks/{task['taskId']}/suite"
            f"?suiteId={suite['suiteId']}&version={suite['version']}",
            {"expectations": expectations or {}},
        )

    def tasks(self, suite: dict[str, Any]) -> list[dict[str, Any]]:
        return self._call(
            "GET", f"/suites/{suite['suiteId']}/tasks?version={suite['version']}"
        )

    # ── running ─────────────────────────────────────────────────────────────

    def ensure_runner_job(
        self,
        *,
        deployment_id: int,
        environment_name: str | None = None,
        cores: int | None = None,
        memory: int | None = None,
        gpus: int | None = None,
    ) -> dict[str, Any]:
        """Create this deployment's evaluation job, if it has none.

        One job per deployment, named after it, and `start_run` creates it the
        first time a run against that deployment is started — so this is
        optional. Calling it first is how you size the job or point it at your
        own environment before that happens; afterwards it is an ordinary job,
        and an existing one is returned untouched so this cannot undo resources
        or alerts set on it since.

        Per deployment rather than per project because those settings are what
        differ: a suite of six questions against a small agent and a thousand
        trials against a large one want different sizing, and an alert on a
        failed evaluation is only actionable if it names which agent failed.
        """
        return self._call(
            "POST",
            "/runner-job",
            {
                "environmentName": environment_name,
                "cores": cores,
                "memory": memory,
                "gpus": gpus,
            },
            params={"deploymentId": deployment_id},
        )

    def start_run(
        self, suite: dict[str, Any], *, deployment_id: int, n_trials: int = 1
    ) -> dict[str, Any]:
        """Record a run and hand it to the runner job.

        `n_trials` above 1 is what separates pass@k from pass^k — whether the
        agent can do it at all from whether it does it every time.
        """
        # Query parameters, not a body: the endpoint reads them from the query string, and a
        # body here is silently ignored — the run is created against no suite at all.
        return self._call(
            "POST",
            "/runs",
            params={
                "suiteId": suite["suiteId"],
                "version": suite["version"],
                "deploymentId": deployment_id,
                "nTrials": n_trials,
                # One call. Recording and starting are separable server-side, but a recorded run
                # nobody started is only useful when the start failed.
                "start": "true",
            },
        )

    def monitor_production(
        self,
        *,
        deployment_id: int,
        evaluator: str = "",
        suite: dict[str, Any] | None = None,
        sample: int = 25,
        since: Any = None,
        until: Any = None,
        start: bool = True,
    ) -> dict[str, Any]:
        """Grade a sample of the traces this deployment has served.

        Monitoring rather than a versioned measurement: point a saved evaluator,
        or a suite's checks, at live traffic. A suite's tasks are ignored --
        production brings its own -- so a suite with no tasks is a perfectly good
        thing to monitor with.

        Every check must be able to reach a verdict without an expected answer,
        since production carries none. A judge configured with `criteria` can; one
        relying on a task's rubric cannot, and the server refuses it rather than
        letting it return ungradable on every trace while the run reports a clean
        sheet.

        With no range, it grades what has arrived since the last successful
        monitor for this deployment -- which is what makes a schedule cover each
        trace once instead of re-grading the same day every night. `since` and
        `until` are datetimes, for a backfill or to re-read a window.
        """

        def epoch(value: Any) -> int | None:
            return None if value is None else int(value.timestamp() * 1000)

        if not evaluator and not suite:
            raise EvalApiError("name an evaluator or a suite to grade with")
        return self._call(
            "POST",
            "/sample-runs",
            params={
                "deploymentId": deployment_id,
                "sample": sample,
                **({"templateId": evaluator} if evaluator else {}),
                **(
                    {"suiteId": suite["suiteId"], "suiteVersion": suite["version"]}
                    if suite
                    else {}
                ),
                **({"from": epoch(since)} if since is not None else {}),
                **({"to": epoch(until)} if until is not None else {}),
                "start": "true" if start else "false",
            },
        )

    def run(self, run_id: str) -> dict[str, Any]:
        return self._call("GET", f"/runs/{run_id}")

    def runs(self) -> list[dict[str, Any]]:
        return self._call("GET", "/runs")


class EvalApiError(RuntimeError):
    """A refusal from the API, carrying the reason it gave."""


def _message(response: Any) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    return body.get("usrMsg") or body.get("errorMsg") or f"HTTP {response.status_code}"
