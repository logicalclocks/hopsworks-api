"""The evaluation side of the client: suites, tasks, the library, runs and the jobs.

Every handle here is a thin object over the same transport. Methods take and
return the models in :mod:`models`; where a model can act on itself (publish a
suite, join a task to a suite, wait for a run) the method lives on the model and
calls back through the client it came from.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ._transport import AgentServingError, Transport
from .models import (
    Check,
    EvalJob,
    EvaluatorResult,
    EvaluatorTemplate,
    RegressionSuite,
    ReviewJob,
    Run,
    RunMetric,
    Suite,
    Task,
    Trial,
)


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from .client import AgentServing

EVALS = "/agent-evals"


def epoch_ms(value: Any) -> int | None:
    """A datetime, or epoch milliseconds already, as epoch milliseconds."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return int(value)


def check(type: str, name: str | None = None, **config: Any) -> dict[str, Any]:  # noqa: A002
    """One check as a suite stores it: its type, the name results report under, its settings.

    ``check("llm_judge", "quality", provider="anthropic", criteria=[...])``.
    """
    return {"type": type, "name": name or type, "config": json.dumps(config)}


def _suite_ref(
    suite: Suite | str, version: int | None = None
) -> tuple[str, int | None]:
    if isinstance(suite, Suite):
        return suite.suite_id, suite.version
    return suite, version


def _task_id(task: Task | str) -> str:
    return task.task_id if isinstance(task, Task) else task


def _messages(
    question: str | Sequence[str] | Sequence[dict[str, Any]],
) -> tuple[str, str]:
    """A question, a list of user turns, or full messages, as the JSON a task stores."""
    if isinstance(question, str):
        messages: list[dict[str, Any]] = [{"role": "user", "content": question}]
    else:
        messages = [
            m if isinstance(m, dict) else {"role": "user", "content": str(m)}
            for m in question
        ]
    user_turns = sum(1 for m in messages if m.get("role") == "user")
    return json.dumps(messages), "multi_turn" if user_turns > 1 else "single_turn"


class Suites:
    def __init__(self, client: AgentServing):
        self._client = client
        self._http: Transport = client.http

    def list(self) -> list[Suite]:
        return self._client._bind_all(
            Suite.list_from_api(self._http.get(f"{EVALS}/suites"))
        )

    def get(self, suite_id: str, version: int | None = None) -> Suite:
        return self._client._bind(
            Suite.from_api(
                self._http.get(f"{EVALS}/suites/{suite_id}", version=version)
            )
        )

    def find(self, name: str) -> Suite | None:
        """The newest version of the suite with this name, or none."""
        matches = [s for s in self.list() if s.name == name]
        return max(matches, key=lambda s: s.version) if matches else None

    def create(
        self,
        name: str,
        *,
        checks: Sequence[dict[str, Any]] = (),
        description: str = "",
        tags: Sequence[str] = (),
        blocks_are_success: bool = False,
        run_on_update: bool = False,
        gate_metric: str = "",
        gate_threshold: float | None = None,
        pass_policy: str = "all",
        pass_threshold: float | None = None,
        execution_mode: str | None = None,
    ) -> Suite:
        """A new draft suite with the checks every task in it is graded by.

        A suite of attacks (``blocks_are_success``) is sandboxed whatever else is asked, because
        the backend refuses anything else; ``execution_mode`` is otherwise ``read_only``.
        """
        mode = execution_mode or ("sandboxed" if blocks_are_success else "read_only")
        body: dict[str, Any] = {
            "name": name,
            "description": description,
            "tags": json.dumps(list(tags)),
            "executionMode": mode,
            "blocksAreSuccess": blocks_are_success,
            "runOnUpdate": run_on_update,
            "passPolicy": pass_policy,
            "evaluators": list(checks),
        }
        if gate_metric:
            body["gateMetric"] = gate_metric
            body["gateThreshold"] = 1.0 if gate_threshold is None else gate_threshold
        if pass_threshold is not None:
            body["passThreshold"] = pass_threshold
        return self._client._bind(
            Suite.from_api(self._http.post(f"{EVALS}/suites", body))
        )

    def new_version(self, suite: Suite | str, version: int | None = None) -> Suite:
        """A new draft version of a suite, carrying its settings and checks; tasks are joined anew."""
        current = suite if isinstance(suite, Suite) else self.get(suite, version)
        body = {
            "suiteId": current.suite_id,
            "name": current.name,
            "description": current.description,
            "tags": current.tags,
            "executionMode": current.execution_mode,
            "blocksAreSuccess": current.blocks_are_success,
            "runOnUpdate": current.run_on_update,
            "gateMetric": current.gate_metric,
            "gateThreshold": current.gate_threshold,
            "passPolicy": current.pass_policy,
            "passThreshold": current.pass_threshold,
            "evaluators": current.evaluators,
        }
        return self._client._bind(
            Suite.from_api(self._http.post(f"{EVALS}/suites", body))
        )

    def update(
        self, suite: Suite | str, version: int | None = None, **changes: Any
    ) -> Suite:
        """Change name, tags or description at any time; how it runs while a draft.

        Keyword names are the model's: ``name``, ``description``, ``tags`` (a list),
        ``blocks_are_success``, ``run_on_update``, ``gate_metric``, ``gate_threshold``,
        ``pass_policy``, ``pass_threshold``, ``execution_mode``.
        """
        suite_id, version = _suite_ref(suite, version)
        body: dict[str, Any] = {}
        for key, value in changes.items():
            if value is None:
                continue
            if key == "tags" and not isinstance(value, str):
                value = json.dumps(list(value))
            body[
                "".join(
                    p.capitalize() if i else p for i, p in enumerate(key.split("_"))
                )
            ] = value
        return self._client._bind(
            Suite.from_api(
                self._http.put(f"{EVALS}/suites/{suite_id}", body, version=version)
            )
        )

    def set_checks(
        self,
        suite: Suite | str,
        checks: Sequence[dict[str, Any]],
        version: int | None = None,
    ) -> list[Check]:
        """Replace a draft's checks. Refused once published."""
        suite_id, version = _suite_ref(suite, version)
        rows = self._http.put(
            f"{EVALS}/suites/{suite_id}/evaluators",
            {"evaluators": list(checks)},
            version=version,
        )
        return Check.list_from_api(rows)

    def publish(self, suite: Suite | str, version: int | None = None) -> Suite:
        """Freeze it, which is what lets a run say exactly what it executed."""
        suite_id, version = _suite_ref(suite, version)
        return self._client._bind(
            Suite.from_api(
                self._http.post(f"{EVALS}/suites/{suite_id}/publish", version=version)
            )
        )

    def delete(
        self, suite: Suite | str, version: int | None = None, force: bool = False
    ) -> None:
        suite_id, version = _suite_ref(suite, version)
        self._http.delete(
            f"{EVALS}/suites/{suite_id}",
            version=version,
            force="true" if force else None,
        )

    def tasks(self, suite: Suite | str, version: int | None = None) -> list[Task]:
        suite_id, version = _suite_ref(suite, version)
        return self._client._bind_all(
            Task.list_from_api(
                self._http.get(f"{EVALS}/suites/{suite_id}/tasks", version=version)
            )
        )

    def import_tasks(
        self,
        suite: Suite | str,
        tasks: Iterable[dict[str, Any]],
        version: int | None = None,
    ) -> dict[str, int]:
        """Load many tasks at once; each is ``{"inputMessages"|"question", "expectations", ...}``.

        Returns ``{"imported": n, "skipped": m}``.
        """
        suite_id, version = _suite_ref(suite, version)
        rows = []
        for task in tasks:
            row = dict(task)
            if "question" in row and "inputMessages" not in row:
                row["inputMessages"], row["taskType"] = _messages(row.pop("question"))
            rows.append(row)
        return (
            self._http.post(
                f"{EVALS}/suites/{suite_id}/tasks/import", rows, version=version
            )
            or {}
        )


class Tasks:
    def __init__(self, client: AgentServing):
        self._client = client
        self._http = client.http

    def list(
        self, *, unassigned: bool = False, pending_redaction: bool = False
    ) -> list[Task]:
        """Every task, or the promotion queue: unassigned to a suite, or waiting for a redaction review."""
        return self._client._bind_all(
            Task.list_from_api(
                self._http.get(
                    f"{EVALS}/tasks",
                    unassigned="true" if unassigned else None,
                    pendingRedaction="true" if pending_redaction else None,
                )
            )
        )

    def get(self, task_id: str) -> Task:
        return self._client._bind(
            Task.from_api(self._http.get(f"{EVALS}/tasks/{task_id}"))
        )

    def create(
        self,
        question: str | Sequence[str] | Sequence[dict[str, Any]],
        *,
        expectations: dict[str, str] | None = None,
        category: str | None = None,
    ) -> Task:
        """Author a task on its own; it joins a suite with :meth:`add_to_suite`."""
        input_messages, task_type = _messages(question)
        body: dict[str, Any] = {"inputMessages": input_messages, "taskType": task_type}
        if expectations:
            body["expectations"] = expectations
        if category:
            body["category"] = category
        return self._client._bind(
            Task.from_api(self._http.post(f"{EVALS}/tasks", body))
        )

    def add_to_suite(
        self,
        task: Task | str,
        suite: Suite | str,
        version: int | None = None,
        expectations: dict[str, str] | None = None,
    ) -> Task:
        """Join a task to a suite with what it expects of each of the suite's checks, by check name."""
        suite_id, version = _suite_ref(suite, version)
        return self._client._bind(
            Task.from_api(
                self._http.post(
                    f"{EVALS}/tasks/{_task_id(task)}/suite",
                    {"expectations": expectations or {}},
                    suiteId=suite_id,
                    version=version,
                )
            )
        )

    def remove_from_suite(self, task: Task | str) -> Task:
        return self._client._bind(
            Task.from_api(self._http.delete(f"{EVALS}/tasks/{_task_id(task)}/suite"))
        )

    def delete(self, task: Task | str) -> None:
        self._http.delete(f"{EVALS}/tasks/{_task_id(task)}")

    def promote(
        self,
        deployment_id: int,
        trace_id: str,
        *,
        expectations: dict[str, str] | None = None,
        question: str | Sequence[str] | Sequence[dict[str, Any]] | None = None,
        category: str | None = None,
    ) -> Task:
        """A task from a production trace. Lands PENDING_REDACTION: a person confirms before it joins."""
        body: dict[str, Any] = {"taskType": "single_turn"}
        if question is not None:
            body["inputMessages"], body["taskType"] = _messages(question)
        if expectations:
            body["expectations"] = expectations
        if category:
            body["category"] = category
        return self._client._bind(
            Task.from_api(
                self._http.post(
                    f"{EVALS}/tasks/from-trace/{trace_id}",
                    body,
                    deploymentId=deployment_id,
                )
            )
        )

    def confirm_redaction(
        self,
        task: Task | str,
        *,
        input_messages: str | Sequence[dict[str, Any]] | None = None,
        expectations: dict[str, str] | None = None,
    ) -> Task:
        """Record that a named person checked a promoted task for personal data, with any edits."""
        body: dict[str, Any] = {}
        if input_messages is not None:
            body["inputMessages"] = (
                input_messages
                if isinstance(input_messages, str)
                else json.dumps(list(input_messages))
            )
        if expectations is not None:
            body["expectations"] = expectations
        return self._client._bind(
            Task.from_api(
                self._http.post(f"{EVALS}/tasks/{_task_id(task)}/redaction", body)
            )
        )

    def forget_trace(self, deployment_id: int, trace_id: str) -> None:
        """Remove every task promoted from a trace, for a deletion request."""
        self._http.delete(
            f"{EVALS}/tasks/from-trace/{trace_id}", deploymentId=deployment_id
        )

    def add_to_regressions(
        self, task: Task | str, *, input_messages: str | None = None
    ) -> Task:
        """Confirm the redaction if pending and join the deployment's standing regression suite, in one call."""
        body = {"inputMessages": input_messages} if input_messages is not None else {}
        return self._client._bind(
            Task.from_api(
                self._http.post(f"{EVALS}/tasks/{_task_id(task)}/regressions", body)
            )
        )


class Evaluators:
    """The project's library of saved checks."""

    def __init__(self, client: AgentServing):
        self._client = client
        self._http = client.http

    def list(self) -> list[EvaluatorTemplate]:
        return EvaluatorTemplate.list_from_api(self._http.get(f"{EVALS}/evaluators"))

    def find(self, name: str) -> EvaluatorTemplate | None:
        return next((e for e in self.list() if e.name == name), None)

    def save(
        self, name: str, checks: Sequence[dict[str, Any]], description: str = ""
    ) -> EvaluatorTemplate:
        """Save a named set of checks for reuse. A suite copies them in and never points back."""
        return EvaluatorTemplate.from_api(
            self._http.post(
                f"{EVALS}/evaluators",
                {
                    "name": name,
                    "description": description,
                    "spec": json.dumps(list(checks)),
                },
            )
        )

    def delete(self, template: EvaluatorTemplate | str) -> None:
        template_id = (
            template.template_id
            if isinstance(template, EvaluatorTemplate)
            else template
        )
        self._http.delete(f"{EVALS}/evaluators/{template_id}")

    def install_defaults(self, overwrite: bool = False) -> list[str]:
        """The built-in judges (hallucination, frustration, toxicity, ...) into this library."""
        from ..judge_config import default_templates

        existing = {e.name for e in self.list()}
        written = []
        for template in default_templates():
            if template["name"] in existing and not overwrite:
                continue
            self.save(
                template["name"],
                json.loads(template["spec"]),
                template.get("description", ""),
            )
            written.append(template["name"])
        return written

    def judge_models(self, provider: str, secret: str | None = None) -> list[str]:
        """The models a provider currently offers, asked of the provider with the project's key."""
        return list(
            self._http.get(f"{EVALS}/judge-models", provider=provider, secret=secret)
            or []
        )


class Runs:
    def __init__(self, client: AgentServing):
        self._client = client
        self._http = client.http

    def list(self, deployment_id: int | None = None) -> list[Run]:
        return self._client._bind_all(
            Run.list_from_api(
                self._http.get(f"{EVALS}/runs", deploymentId=deployment_id)
            )
        )

    def get(self, run_id: str) -> Run:
        return self._client._bind(
            Run.from_api(self._http.get(f"{EVALS}/runs/{run_id}"))
        )

    def start(
        self,
        suite: Suite | str,
        deployment_id: int,
        *,
        version: int | None = None,
        n_trials: int = 1,
        start: bool = True,
    ) -> Run:
        """Run a published suite against a deployment. ``n_trials`` above 1 is pass^k, not pass@k."""
        suite_id, version = _suite_ref(suite, version)
        return self._client._bind(
            Run.from_api(
                self._http.post(
                    f"{EVALS}/runs",
                    suiteId=suite_id,
                    version=version,
                    deploymentId=deployment_id,
                    nTrials=n_trials,
                    start="true" if start else "false",
                )
            )
        )

    def sample(
        self,
        deployment_id: int,
        *,
        evaluator: EvaluatorTemplate | str | None = None,
        suite: Suite | None = None,
        since: Any = None,
        until: Any = None,
        sample: int | None = None,
        start: bool = True,
    ) -> Run:
        """Grade a sample of real traffic with a saved evaluator or a suite's checks.

        Without a window, what arrived since the last successful sample; ``since`` and ``until``
        are datetimes or epoch milliseconds. ``sample`` caps how many conversations are graded,
        since each costs a judge call.
        """
        if evaluator is None and suite is None:
            raise AgentServingError("name an evaluator or a suite to grade with")
        template_id = (
            evaluator.template_id
            if isinstance(evaluator, EvaluatorTemplate)
            else evaluator
        )
        return self._client._bind(
            Run.from_api(
                self._http.post(
                    f"{EVALS}/sample-runs",
                    deploymentId=deployment_id,
                    templateId=template_id,
                    suiteId=suite.suite_id if suite else None,
                    suiteVersion=suite.version if suite else None,
                    **{"from": epoch_ms(since), "to": epoch_ms(until)},
                    sample=sample,
                    start="true" if start else "false",
                )
            )
        )

    def sample_watermark(self, deployment_id: int) -> int | None:
        """Where the next sample would start, as epoch milliseconds."""
        body = (
            self._http.get(f"{EVALS}/sample-runs/watermark", deploymentId=deployment_id)
            or {}
        )
        return body.get("from")

    def start_recorded(self, run: Run | str) -> Run:
        run_id = run.run_id if isinstance(run, Run) else run
        return self._client._bind(
            Run.from_api(self._http.post(f"{EVALS}/runs/{run_id}/start"))
        )

    def trials(self, run: Run | str) -> list[Trial]:
        run_id = run.run_id if isinstance(run, Run) else run
        return Trial.list_from_api(self._http.get(f"{EVALS}/runs/{run_id}/trials"))

    def results(
        self, run: Run | str, trial: Trial | str | None = None
    ) -> list[EvaluatorResult]:
        run_id = run.run_id if isinstance(run, Run) else run
        trial_id = trial.trial_id if isinstance(trial, Trial) else trial
        return EvaluatorResult.list_from_api(
            self._http.get(f"{EVALS}/runs/{run_id}/evaluator-results", trialId=trial_id)
        )

    def metrics(self, run: Run | str) -> list[RunMetric]:
        run_id = run.run_id if isinstance(run, Run) else run
        return RunMetric.list_from_api(self._http.get(f"{EVALS}/runs/{run_id}/metrics"))

    def trend(self, deployment_id: int, limit: int = 20) -> list[RunMetric]:
        """Run-scope metrics across a deployment's recent runs."""
        return RunMetric.list_from_api(
            self._http.get(
                f"{EVALS}/runs/metrics", deploymentId=deployment_id, limit=limit
            )
        )

    def review_trial(
        self,
        run: Run | str,
        trial: Trial | str,
        *,
        passed: bool,
        score: float | None = None,
        reason: str = "",
        task_id: str | None = None,
        evaluator: str | None = None,
    ) -> None:
        """A person's verdict on a trial: about one judge (``evaluator``), or the trial as a whole.

        Per judge is how a suite with several judges is calibrated: saying the hallucination
        judge was wrong about a trial says nothing about the helpfulness judge.
        """
        run_id = run.run_id if isinstance(run, Run) else run
        trial_id = trial.trial_id if isinstance(trial, Trial) else trial
        if task_id is None and isinstance(trial, Trial):
            task_id = trial.task_id
        body: dict[str, Any] = {
            "taskId": task_id,
            "passed": passed,
            "score": score,
            "reason": reason,
        }
        if evaluator:
            body["evaluatorName"] = evaluator
        self._http.post(f"{EVALS}/runs/{run_id}/trials/{trial_id}/review", body)

    def wait(
        self, run: Run | str, *, timeout_s: float = 1800, poll_s: float = 5
    ) -> Run:
        """Poll until the run finishes. Raises AgentServingError on timeout; returns the final row."""
        run_id = run.run_id if isinstance(run, Run) else run
        deadline = time.monotonic() + timeout_s
        while True:
            current = self.get(run_id)
            if current.finished:
                return current
            if time.monotonic() >= deadline:
                raise AgentServingError(
                    f"run {run_id} still {current.status} after {timeout_s:.0f}s"
                )
            time.sleep(poll_s)


class Jobs:
    """The per-deployment jobs: the evaluation job and the failure-analysis job."""

    def __init__(self, client: AgentServing):
        self._client = client
        self._http = client.http

    # ── evaluation job ─────────────────────────────────────────────────────

    def eval_job(self, deployment_id: int) -> EvalJob:
        """The deployment's evaluation job, or what creating one would look like (``exists`` says which)."""
        return EvalJob.from_api(
            self._http.get(f"{EVALS}/runner-job", deploymentId=deployment_id)
        )

    def eval_jobs(self, deployment_id: int) -> list[EvalJob]:
        return EvalJob.list_from_api(
            self._http.get(f"{EVALS}/eval-jobs", deploymentId=deployment_id)
        )

    def ensure_eval_job(
        self,
        deployment_id: int,
        *,
        name: str | None = None,
        suites: Sequence[Suite | str] = (),
        evaluators: Sequence[EvaluatorTemplate | str] = (),
        monitor: bool | None = None,
        environment_name: str | None = None,
        cores: int | None = None,
        memory: int | None = None,
        gpus: int | None = None,
    ) -> EvalJob:
        """Create the deployment's evaluation job unless it exists; an existing one is returned untouched."""
        body = {
            "name": name,
            "suites": [
                f"{s.suite_id}:{s.version}" if isinstance(s, Suite) else s
                for s in suites
            ]
            or None,
            "evaluators": [
                e.template_id if isinstance(e, EvaluatorTemplate) else e
                for e in evaluators
            ]
            or None,
            "monitor": monitor,
            "environmentName": environment_name,
            "cores": cores,
            "memory": memory,
            "gpus": gpus,
        }
        return EvalJob.from_api(
            self._http.post(
                f"{EVALS}/runner-job",
                {k: v for k, v in body.items() if v is not None},
                deploymentId=deployment_id,
            )
        )

    def update_eval_job(
        self,
        name: str,
        *,
        suites: Sequence[Suite | str] | None = None,
        evaluators: Sequence[EvaluatorTemplate | str] | None = None,
        monitor: bool | None = None,
        environment_name: str | None = None,
        cores: int | None = None,
        memory: int | None = None,
    ) -> EvalJob:
        body: dict[str, Any] = {}
        if suites is not None:
            body["suites"] = [
                f"{s.suite_id}:{s.version}" if isinstance(s, Suite) else s
                for s in suites
            ]
        if evaluators is not None:
            body["evaluators"] = [
                e.template_id if isinstance(e, EvaluatorTemplate) else e
                for e in evaluators
            ]
        for key, value in (
            ("monitor", monitor),
            ("environmentName", environment_name),
            ("cores", cores),
            ("memory", memory),
        ):
            if value is not None:
                body[key] = value
        return EvalJob.from_api(self._http.put(f"{EVALS}/eval-jobs/{name}", body))

    def delete_eval_job(self, name: str) -> None:
        self._http.delete(f"{EVALS}/eval-jobs/{name}")

    def run_eval_job(self, name: str) -> list[Run]:
        """Run everything the job is configured with, now. One run per suite, plus the monitor."""
        return self._client._bind_all(
            Run.list_from_api(self._http.post(f"{EVALS}/eval-jobs/{name}/run"))
        )

    # ── failure analysis job ───────────────────────────────────────────────

    def review_jobs(self, deployment_id: int) -> list[ReviewJob]:
        return self._client._bind_all(
            ReviewJob.list_from_api(
                self._http.get(f"{EVALS}/review-jobs", deploymentId=deployment_id)
            )
        )

    def review_job(self, deployment_id: int) -> ReviewJob | None:
        jobs = self.review_jobs(deployment_id)
        return jobs[0] if jobs else None

    def review_job_defaults(self, deployment_id: int) -> ReviewJob:
        return ReviewJob.from_api(
            self._http.get(f"{EVALS}/review-jobs/defaults", deploymentId=deployment_id)
        )

    def ensure_review_job(self, deployment_id: int, **settings: Any) -> ReviewJob:
        """Create the deployment's failure-analysis job unless it exists.

        Settings use the model's field names: ``provider``, ``model``, ``reasoning_effort``,
        ``api_key_env``, ``base_url``, ``headers`` (a dict), ``budget_calls``, ``context_turns``,
        ``sources`` (a list or comma-separated text), ``read_source_code``, ``auto_promote`` and its
        thresholds, plus ``name``, ``environment_name``, ``cores``, ``memory``.
        """
        return self._client._bind(
            ReviewJob.from_api(
                self._http.post(
                    f"{EVALS}/review-jobs",
                    _review_body(settings),
                    deploymentId=deployment_id,
                )
            )
        )

    def update_review_job(self, job: ReviewJob | str, **settings: Any) -> ReviewJob:
        name = job.name if isinstance(job, ReviewJob) else job
        return self._client._bind(
            ReviewJob.from_api(
                self._http.put(f"{EVALS}/review-jobs/{name}", _review_body(settings))
            )
        )

    def delete_review_job(self, job: ReviewJob | str) -> None:
        name = job.name if isinstance(job, ReviewJob) else job
        self._http.delete(f"{EVALS}/review-jobs/{name}")

    def analyse(
        self,
        job: ReviewJob | str,
        *,
        since: Any = None,
        until: Any = None,
        trace_id: str | None = None,
    ) -> Run:
        """Run the analysis now: since the last run by default, or over a window, or on one trace."""
        name = job.name if isinstance(job, ReviewJob) else job
        return self._client._bind(
            Run.from_api(
                self._http.post(
                    f"{EVALS}/review-jobs/{name}/run",
                    **{"from": epoch_ms(since), "to": epoch_ms(until)},
                    traceId=trace_id,
                )
            )
        )

    # ── regressions ────────────────────────────────────────────────────────

    def regressions(self, deployment_id: int) -> RegressionSuite:
        """The deployment's standing regression suite, whether or not it exists yet."""
        return RegressionSuite.from_api(
            self._http.get(f"{EVALS}/regressions", deploymentId=deployment_id)
        )

    def run_regressions(self, deployment_id: int) -> Run:
        """Publish the draft regressions, if any, and run them against the deployment."""
        return self._client._bind(
            Run.from_api(
                self._http.post(f"{EVALS}/regressions/run", deploymentId=deployment_id)
            )
        )


def _review_body(settings: dict[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for key, value in settings.items():
        if value is None:
            continue
        if key == "sources" and not isinstance(value, str):
            value = ",".join(value)
        if key == "headers" and not isinstance(value, str):
            value = json.dumps(dict(value))
        body[
            "".join(p.capitalize() if i else p for i, p in enumerate(key.split("_")))
        ] = value
    return body
