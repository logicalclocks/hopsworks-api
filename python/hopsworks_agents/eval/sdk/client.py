"""The agent-serving client: one object bound to a project.

    project = hopsworks.login()
    agents = project.get_agent_serving()

    agent = agents.get_agent("support")          # by name, or by id
    reply = agent.chat("Where is order 42?")
    for trace in agent.traces(): ...

    suite = agents.suites.create("Refunds", checks=[check("llm_judge", "quality", criteria=[...])])
    agents.tasks.create("Refund order 42", expectations={"quality": "..."}).add_to(suite)
    suite.publish()
    run = agent.run(suite).wait()

Everything the UI does for agents -- talking to them, their traces and
feedback, evaluation suites and runs, the failure analysis -- is reachable from
here. The agents are the deployments model serving knows as agents; deploying
one is `deploy_agent`, the same call as on model serving.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from hopsworks_apigen import public

from ._transport import AgentServingError, Transport
from .agent import Agent, is_agent
from .evals import Evaluators, Jobs, Runs, Suites, Tasks
from .models import ApiModel, ReviewJob, Run, Suite, Task


if TYPE_CHECKING:
    from collections.abc import Iterable


M = TypeVar("M", bound=ApiModel)


@public
class AgentServing:
    """The agents of a project: talk to them, read their traces, evaluate them.

    Fetched with `project.get_agent_serving()`; never constructed directly.

    Attributes:
        suites: The project's evaluation suites.
        tasks: The tasks suites are made of.
        evaluators: The evaluator library (saved judge and check templates).
        runs: Evaluation runs, across agents.
        jobs: The per-agent evaluation and failure-analysis jobs.
    """

    def __init__(
        self,
        host: str,
        project_id: int,
        *,
        api_key: str | None = None,
        verify: bool | str = True,
        session: Any = None,
        gateway_url: str | None = None,
        transport: Transport | None = None,
    ):
        self.http = transport or Transport(
            host,
            project_id,
            api_key=api_key,
            verify=verify,
            session=session,
            gateway_url=gateway_url,
        )
        self.project_id = int(project_id)
        self.suites = Suites(self)
        self.tasks = Tasks(self)
        self.evaluators = Evaluators(self)
        self.runs = Runs(self)
        self.jobs = Jobs(self)

    def __repr__(self) -> str:
        return f"AgentServing(project={self.project_id}, host={self.http.host!r})"

    # ── the agents ─────────────────────────────────────────────────────────

    @public
    def get_agent(self, agent: str | int) -> Agent | None:
        """Get an agent by name or by deployment id.

        Example:
            ```python
            agents = project.get_agent_serving()
            agent = agents.get_agent("support")
            print(agent.chat("hello").text)
            ```

        Parameters:
            agent: The deployment's name, or its id.

        Returns:
            `Agent`: the agent, or `None` when no deployment has that name or id.

        Raises:
            `AgentServingError`: If the deployment exists but is a model deployment rather than an agent.
        """
        serving = self._serving_row(agent)
        if serving is None:
            return None
        if not is_agent(serving):
            raise AgentServingError(
                f"deployment {serving.get('name')!r} serves a model, not an agent; "
                "use project.get_model_serving() for it"
            )
        return Agent(self, int(serving["id"]), serving=serving)

    @public
    def get_agents(self) -> list[Agent]:
        """Every agent deployment in the project.

        Returns:
            `list[Agent]`: the agents, as the serving API lists them.
        """
        rows = self.http.get("/serving") or []
        return [
            Agent(self, int(row["id"]), serving=row) for row in rows if is_agent(row)
        ]

    @public
    def deploy_agent(self, entry: str, name: str | None = None, **kwargs: Any) -> Agent:
        """Deploy a Python script or package as an agent; the same call as `ModelServing.deploy_agent`.

        The agent is created on first call and updated on the next ones. Its
        running state is left alone: `agent.start()` after the first deploy,
        `agent.restart()` to roll a running agent onto new code.

        Parameters:
            entry: Path to the agent's entry script or package directory.
            name: Name of the deployment; the entry's file name when omitted.
            **kwargs: Everything `ModelServing.deploy_agent` takes (requirements, environment, resources, git_url, ...).

        Returns:
            `Agent`: the deployed agent.
        """
        from hopsworks_common import client as hopsworks_client  # noqa: PLC0415

        serving = hopsworks_client._get_connection()._get_model_serving()
        deployment = serving.deploy_agent(entry, name=name, **kwargs)
        agent = self.get_agent(int(deployment.id))
        if agent is None:  # pragma: no cover - the deployment was just created
            raise AgentServingError(f"deployment {deployment.id} vanished after deploy")
        return agent

    def _serving_row(self, agent: str | int) -> dict[str, Any] | None:
        by_id = isinstance(agent, int) or (isinstance(agent, str) and agent.isdigit())
        try:
            if by_id:
                return self.http.get(f"/serving/{int(agent)}")
            return self.http.get("/serving", name=str(agent))
        except AgentServingError as err:
            if err.status == 404:
                return None
            raise

    # models that act on themselves need the client they came from
    def _bind(self, model: M) -> M:
        model._client = self  # type: ignore[attr-defined]
        return model

    def _bind_all(self, models: Iterable[M]) -> list[M]:
        return [self._bind(m) for m in models]


# ── methods on the models, calling back through the client they came from ──


def _client_of(model: ApiModel) -> AgentServing:
    client = getattr(model, "_client", None)
    if client is None:
        raise AgentServingError(
            "this object was not fetched through a client; use the client's methods instead"
        )
    return client


def _suite_publish(self: Suite) -> Suite:
    return _client_of(self).suites.publish(self)


def _suite_tasks(self: Suite) -> list[Task]:
    return _client_of(self).suites.tasks(self)


def _suite_add_task(
    self: Suite,
    question: Any,
    expectations: dict[str, str] | None = None,
    **kwargs: Any,
) -> Task:
    """Author a task and join it to this suite with what it expects of each check."""
    client = _client_of(self)
    task = client.tasks.create(question, **kwargs)
    return client.tasks.add_to_suite(task, self, expectations=expectations)


def _suite_update(self: Suite, **changes: Any) -> Suite:
    return _client_of(self).suites.update(self, **changes)


def _suite_set_checks(self: Suite, checks: Any) -> Any:
    return _client_of(self).suites.set_checks(self, checks)


def _suite_delete(self: Suite, force: bool = False) -> None:
    _client_of(self).suites.delete(self, force=force)


def _suite_new_version(self: Suite) -> Suite:
    return _client_of(self).suites.new_version(self)


def _suite_import(self: Suite, tasks: Any) -> dict[str, int]:
    return _client_of(self).suites.import_tasks(self, tasks)


def _suite_run(self: Suite, deployment_id: int, n_trials: int = 1) -> Run:
    return _client_of(self).runs.start(self, deployment_id, n_trials=n_trials)


Suite.publish = _suite_publish  # type: ignore[attr-defined]
Suite.tasks = _suite_tasks  # type: ignore[attr-defined]
Suite.add_task = _suite_add_task  # type: ignore[attr-defined]
Suite.update = _suite_update  # type: ignore[attr-defined]
Suite.set_checks = _suite_set_checks  # type: ignore[attr-defined]
Suite.delete = _suite_delete  # type: ignore[attr-defined]
Suite.new_version = _suite_new_version  # type: ignore[attr-defined]
Suite.import_tasks = _suite_import  # type: ignore[attr-defined]
Suite.run = _suite_run  # type: ignore[attr-defined]


def _task_add_to(
    self: Task,
    suite: Suite | str,
    expectations: dict[str, str] | None = None,
    version: int | None = None,
) -> Task:
    return _client_of(self).tasks.add_to_suite(
        self, suite, version=version, expectations=expectations
    )


def _task_leave(self: Task) -> Task:
    return _client_of(self).tasks.remove_from_suite(self)


def _task_confirm(self: Task, **kwargs: Any) -> Task:
    return _client_of(self).tasks.confirm_redaction(self, **kwargs)


def _task_regressions(self: Task, **kwargs: Any) -> Task:
    return _client_of(self).tasks.add_to_regressions(self, **kwargs)


def _task_delete(self: Task) -> None:
    _client_of(self).tasks.delete(self)


Task.add_to = _task_add_to  # type: ignore[attr-defined]
Task.leave_suite = _task_leave  # type: ignore[attr-defined]
Task.confirm_redaction = _task_confirm  # type: ignore[attr-defined]
Task.add_to_regressions = _task_regressions  # type: ignore[attr-defined]
Task.delete = _task_delete  # type: ignore[attr-defined]


def _run_refresh(self: Run) -> Run:
    return _client_of(self).runs.get(self.run_id)


def _run_wait(self: Run, **kwargs: Any) -> Run:
    return _client_of(self).runs.wait(self, **kwargs)


def _run_trials(self: Run) -> Any:
    return _client_of(self).runs.trials(self)


def _run_results(self: Run, trial: Any = None) -> Any:
    return _client_of(self).runs.results(self, trial)


def _run_metrics(self: Run) -> Any:
    return _client_of(self).runs.metrics(self)


def _run_review(self: Run, trial: Any, **kwargs: Any) -> None:
    _client_of(self).runs.review_trial(self, trial, **kwargs)


Run.refresh = _run_refresh  # type: ignore[attr-defined]
Run.wait = _run_wait  # type: ignore[attr-defined]
Run.trials = _run_trials  # type: ignore[attr-defined]
Run.results = _run_results  # type: ignore[attr-defined]
Run.metrics = _run_metrics  # type: ignore[attr-defined]
Run.review_trial = _run_review  # type: ignore[attr-defined]


def _review_analyse(self: ReviewJob, **kwargs: Any) -> Run:
    return _client_of(self).jobs.analyse(self, **kwargs)


def _review_update(self: ReviewJob, **settings: Any) -> ReviewJob:
    return _client_of(self).jobs.update_review_job(self, **settings)


def _review_delete(self: ReviewJob) -> None:
    _client_of(self).jobs.delete_review_job(self)


ReviewJob.analyse = _review_analyse  # type: ignore[attr-defined]
ReviewJob.update = _review_update  # type: ignore[attr-defined]
ReviewJob.delete = _review_delete  # type: ignore[attr-defined]
