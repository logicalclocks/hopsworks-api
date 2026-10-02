"""The job the backend starts when a run is created.

    python -m hopsworks_agents.eval.run_job --run-id <runId>

The run id is the only argument, deliberately: which suite version, which
deployment and how many trials all live on the row it names, so the job and the
record cannot disagree about what was executed.

What it does, in order: read the run, load the suite's tasks, execute them
against the deployment, write trials, evaluator results and metrics to the feature
store, and report the outcome back. Reporting back matters as much as the work
— a run stuck in RUNNING because the job died is indistinguishable from one
still going, and the UI has no way to tell you which.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from .api import hopsworks_session
from .evaluator_spec import SpecError, evaluators_for_suite
from .judges import DEFAULT_MODEL, LlmJudgeEvaluator, anthropic_completer
from .metrics import run_metrics
from .models import ExecutionMode, PassPolicy, Suite, Task
from .run_fields import run_trials
from .runner import RunnerConfig, SuiteRefused, run_suite
from .sample_job import evaluators_for, run_sample


log = logging.getLogger(__name__)

TRIALS_FG = "agent_eval_trials"
EVALUATOR_RESULTS_FG = "agent_eval_evaluator_results"
RUN_METRICS_FG = "agent_eval_run_metrics"


def _api(host: str, project_id: int) -> str:
    return f"{host.rstrip('/')}/hopsworks-api/api/project/{project_id}/agent-evals"


def _query_for(project: Any) -> Any:
    """A read-only query function for state evaluators, or None.

    Bound to the feature store the job already authenticated to, so a state
    assertion sees exactly what the project can see and nothing wider. Returned
    lazily: a suite with no sql_state evaluator should not pay for a session.
    """

    def query(sql: str) -> Any:
        return project.get_feature_store().sql(sql)

    return query


def _judge_for() -> LlmJudgeEvaluator | None:
    """The judge used by a spec that names no variable of its own.

    EVAL_JUDGE_API_KEY when set, so evaluation can be pointed at a separate key,
    otherwise the provider's own variable. Both are environment variables and
    nothing else: a key set on a Hopsworks account is injected into every job
    container, so it is already here by the time this runs.

    Asking a secrets API for it was a second mechanism that had to be configured
    separately — and, being asked wrongly, silently skipped every judge while the
    run reported success.
    """
    from .judge_config import PROVIDERS, JudgeConfig

    env_var = PROVIDERS["anthropic"]["env_var"]
    api_key = os.environ.get("EVAL_JUDGE_API_KEY") or os.environ.get(env_var)
    if not api_key:
        log.info(
            "no default judge key in EVAL_JUDGE_API_KEY or %s. Set one in your "
            "account's environment variables and it reaches every job. A judge "
            "naming its own variable is unaffected; tasks graded only by the "
            "default judge report nothing.",
            env_var,
        )
        return None

    model = os.environ.get("EVAL_JUDGE_MODEL", DEFAULT_MODEL)
    log.info("LLM judge enabled (model=%s)", model)
    return LlmJudgeEvaluator(
        anthropic_completer(api_key, model), JudgeConfig(model=model)
    )


def _tags(raw: Any) -> list[str]:
    """Tags are stored as a JSON array of strings; anything else is no tags."""
    if isinstance(raw, list):
        return [str(tag) for tag in raw]
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        return [str(tag) for tag in parsed] if isinstance(parsed, list) else []
    return []


def _to_suite(run: dict[str, Any], tasks: list[dict[str, Any]]) -> Suite:
    """The API's shape as the runner's models.

    Expectations arrive keyed by check name, which is the same key results are
    reported under, so this is a copy rather than a translation. It used to map
    four fixed fields — expectedOutput, requiredTools, forbiddenTools, rubric —
    which no longer exist on either side.
    """
    return Suite(
        suite_id=run["suiteId"],
        suite_version=run.get("suiteVersion", 1),
        tags=_tags(run.get("tags")),
        blocks_are_success=bool(run.get("blocksAreSuccess")),
        execution_mode=ExecutionMode(run.get("executionMode", "read_only")),
        evaluators=run.get("evaluators") or "",
        pass_policy=PassPolicy(run.get("passPolicy") or "all"),
        pass_threshold=float(run.get("passThreshold") or 0.7),
        tasks=[
            Task(
                task_id=t["taskId"],
                task_version=t.get("version", 1),
                input_messages=t.get("inputMessages") or "[]",
                task_type=t.get("taskType", "single_turn"),
                expectations={
                    name: value
                    for name, value in (t.get("expectations") or {}).items()
                    if value
                },
                category=t.get("category") or "",
            )
            for t in tasks
        ],
    )


def _write_results(
    feature_store: Any,
    result: Any,
    run: dict[str, Any],
    tasks: list[dict[str, Any]] | None = None,
) -> None:
    import pandas as pd

    now = datetime.now(tz=timezone.utc)
    trials = [
        {
            "run_id": t.run_id,
            "trial_id": t.trial_id,
            "task_id": t.task_id,
            "task_version": t.task_version,
            "trial_index": t.trial_index,
            "deployment_id": t.deployment_id,
            "trace_id": t.trace_id,
            "trace_status": t.trace_status.value,
            "session_id": t.session_id,
            "status": t.status.value,
            "started_at": t.started_at,
            "completed_at": t.completed_at or now,
            "latency_ms": t.latency_ms,
            "input_tokens": t.input_tokens or 0,
            "output_tokens": t.output_tokens or 0,
            "estimated_cost": t.estimated_cost,
            "final_output": t.final_output,
            "error_type": t.error_type,
            "error_message": t.error_message,
            "created_at": now,
        }
        for t in result.trials
    ]
    evaluator_rows = [
        {
            "run_id": t.run_id,
            "result_id": f"{t.trial_id}/{g.evaluator_name}",
            "trial_id": t.trial_id,
            "task_id": t.task_id,
            "evaluator_name": g.evaluator_name,
            "evaluator_type": g.evaluator_type,
            "score": g.score,
            "passed": g.passed,
            "ungradable": g.ungradable,
            "reason": g.reason,
            "assertions_json": json.dumps(g.assertions),
            "judge_model": str(g.assertions.get("judge_model", "")),
            "evaluator_version": "1",
            "created_at": now,
        }
        for t in result.trials
        for g in t.evaluator_results
    ]
    metric_rows = [
        # A sample has no suite, so its metric rows carry an empty suite id and
        # version 0. Empty rather than absent: the row records that this run
        # executed no suite, which is what tells a query reading both kinds
        # apart when it has only the metrics in hand.
        {**m, "suite_version": run.get("suiteVersion") or 0, "created_at": now}
        for m in run_metrics(
            result.run_id,
            run.get("suiteId") or "",
            run["deploymentId"],
            result.trials,
            blocks_are_success=bool(run.get("blocksAreSuccess")),
            # so score-by-category has categories to group on; the tasks are
            # already in hand from building the suite
            categories={t["taskId"]: t.get("category") or "" for t in (tasks or [])},
        )
    ]

    for name, rows in (
        (TRIALS_FG, trials),
        (EVALUATOR_RESULTS_FG, evaluator_rows),
        (RUN_METRICS_FG, metric_rows),
    ):
        if not rows:
            continue
        group = feature_store.get_feature_group(name, 1)
        group.insert(
            _match_schema(group, pd.DataFrame(rows)),
            write_options={"mode": "append"},
        )
        log.info("wrote %d rows to %s", len(rows), name)


# What pandas infers, against what a feature group declares. Two dtypes per
# type: the plain one, and the nullable one for a column with a hole in it.
_FEATURE_TYPES = {
    "int": ("int32", "Int32"),
    "bigint": ("int64", "Int64"),
    "smallint": ("int16", "Int16"),
    "tinyint": ("int8", "Int8"),
    "float": ("float32", "Float32"),
    "double": ("float64", "Float64"),
    "boolean": ("bool", "boolean"),
    "string": ("string", "string"),
}


def _match_schema(group: Any, frame: Any) -> Any:
    """Types as the feature group declares them, not as pandas guessed.

    Two guesses go wrong. Python has one integer type and pandas reads it as
    int64, so a column the feature group declares `int` arrives as `bigint` and
    the insert is refused. And a column that is None in every row — trace_id
    when no trial got a trace, error_message when none failed — has no type at
    all: pandas keeps it as object, Arrow infers `null`, and Delta refuses a
    null-typed column outright. Both refusals come after the whole suite has
    run, which is the most expensive moment to discover them; the second also
    arrives exactly when a run has already failed, and takes the record of
    that failure with it.

    Read off the group rather than a list of column names here: the schema is
    defined in the backend, and a list kept in this file would be a copy that
    drifts the first time a column is added there.

    A column with nulls gets the nullable form of its type — pandas' `Int32`
    rather than `int32` — which Arrow carries as the declared type with nulls
    instead of as `null`.
    """
    for feature in getattr(group, "features", None) or []:
        declared = (getattr(feature, "type", "") or "").lower()
        name = getattr(feature, "name", None)
        if name not in frame.columns:
            continue
        if declared == "timestamp":
            # A timestamp column that is None in every row -- decided_at before anyone has
            # decided -- is the null-typed case above in another type. Parsed as UTC datetimes so
            # Arrow carries a timestamp with nulls, at microseconds because that is what Delta
            # stores and what pandas would otherwise warn about casting to.
            frame[name] = _as_timestamps(frame[name])
            continue
        dtypes = _FEATURE_TYPES.get(declared)
        if not dtypes:
            continue
        plain, nullable = dtypes
        target = nullable if frame[name].isna().any() else plain
        try:
            frame[name] = frame[name].astype(target)
        except (TypeError, ValueError):
            # Not what the schema says at all; the feature store's own error
            # will be clearer than one invented here.
            log.debug("left %s as %s", name, frame[name].dtype)
    return frame


def _as_timestamps(column: Any) -> Any:
    import pandas as pd  # noqa: PLC0415 -- only reached with a frame in hand

    try:
        parsed = pd.to_datetime(column, utc=True)
    except (TypeError, ValueError):
        log.debug("left a timestamp column as %s", column.dtype)
        return column
    try:
        return parsed.astype("datetime64[us, UTC]")
    except (TypeError, ValueError):
        return parsed


def _execute(run_id: str, session, base: str, project, host: str, args) -> bool:
    """One recorded run. True when it finished, false when it failed.

    Returns rather than exits, because a job runs several: a suite that refuses
    must not take the three beside it down, and each has its own row to say what
    became of it.
    """

    def report(status: str, error: str | None = None) -> None:
        # Reported even on the failure paths: a run left RUNNING because the job
        # died looks exactly like one still going, and nothing can tell you which.
        try:
            session.put(
                f"{base}/runs/{run_id}/status",
                params={"status": status, **({"errorMessage": error} if error else {})},
                timeout=30,
            )
        except Exception:  # noqa: BLE001 — never mask the real failure
            log.exception("could not report status %s", status)

    try:
        run = session.get(f"{base}/runs/{run_id}", timeout=60).json()

        from .client import HopsworksAgentClient

        judge = _judge_for()
        # The judge is a evaluator; the spec needs the bare completer behind it, so
        # a task can ask for a rubric judge and a pairwise judge independently.
        completer = judge._complete if judge is not None else None  # noqa: SLF001
        query = _query_for(project)

        client = HopsworksAgentClient(
            session=session,
            api_base=host,
            project_id=project.id,
            project_name=project.name,
            deployment_id=run["deploymentId"],
        )

        # Which question this run answers. Read off the row rather than taken as
        # an argument, so a scheduled execution and a hand-started one cannot
        # differ.
        if run.get("runType") == "ONLINE_SAMPLE":
            result = run_sample(
                client,
                session,
                host,
                project.id,
                run,
                evaluators_for(run, judge_completer=completer),
            )
            _write_results(project.get_feature_store(), result, run)
            report(result.status)
            log.info(
                "online sample %s finished: %d traces graded",
                run_id,
                len(result.trials),
            )
            return True

        tasks = session.get(
            f"{base}/suites/{run['suiteId']}/tasks",
            params={"version": run.get("suiteVersion")},
            timeout=60,
        ).json()
        suite = _to_suite(run, tasks)

        result = run_suite(
            client,
            suite,
            run_id=run_id,
            deployment_id=run["deploymentId"],
            # One list for the whole suite: every task is measured the same
            # way, which is what makes the run's pass rate comparable to the
            # next one's.
            evaluators=evaluators_for_suite(
                suite, judge_completer=completer, query=query
            ),
            config=RunnerConfig(
                n_trials=run_trials(run, 1),
                readiness_timeout_s=args.readiness_timeout_s,
                max_concurrency=args.max_concurrency,
                input_token_price_per_million=run.get("inputTokenPricePerMillion"),
                output_token_price_per_million=run.get("outputTokenPricePerMillion"),
            ),
        )
        _write_results(project.get_feature_store(), result, run, tasks)
        report(result.status)
        log.info("run %s finished: %s", run_id, result.status)
        return result.status == "SUCCEEDED"
    except SpecError as err:
        # Authoring validates the spec, so reaching here means a task was written
        # before that check existed or around it. Named as a run failure rather
        # than an agent one.
        log.exception("a task has an unusable evaluator spec")
        report("FAILED", f"evaluator spec: {err}")
        return False
    except SuiteRefused as err:
        # A refusal is a result, not a crash: the run would have produced
        # numbers that looked valid, and saying so is the point.
        log.error("run refused: %s", err)
        report("FAILED", str(err))
        return False
    except Exception as err:  # noqa: BLE001
        log.exception("run %s failed", run_id)
        report("FAILED", str(err))
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    # Repeatable: one evaluation job runs everything it is configured to run --
    # three suites and a monitor, say -- in a single execution, so its resources
    # are paid for once rather than per target.
    parser.add_argument("--run-id", required=True, action="append", dest="run_ids")
    parser.add_argument(
        "--readiness-timeout-s",
        type=float,
        default=120.0,
        help="derive from the Stage 1 probe's trajectory-stable "
        "p95 rather than accepting this default",
    )
    parser.add_argument("--max-concurrency", type=int, default=4)
    # The scheduler appends "-start_time <fire time>" to every scheduled execution's arguments.
    # This job reads its window off its run rows, not that flag, so it is tolerated and ignored
    # rather than refused -- a refusal here is a scheduled job that never runs.
    args, ignored = parser.parse_known_args()
    if ignored:
        logging.getLogger(__name__).info(
            "ignoring arguments this job does not read: %s", " ".join(ignored)
        )
    logging.basicConfig(level=logging.INFO)

    import hopsworks

    project = hopsworks.login()
    host = os.environ.get("HOPSWORKS_HOST") or os.environ["REST_ENDPOINT"]
    # Auth and the cluster's CA chain, both as the hopsworks client already
    # resolved them for the login above. Building either by hand failed twice:
    # a job has no API key, and the internal endpoint is signed by a CA no
    # system trust store carries.
    session = hopsworks_session()
    base = _api(host, project.id)

    outcomes = [
        _execute(run_id, session, base, project, host, args) for run_id in args.run_ids
    ]
    failed = outcomes.count(False)
    if failed:
        # Non-zero so the job reads as failed and any alert on it fires, while
        # every run that did finish keeps the status it reported for itself.
        log.error("%d of %d runs failed", failed, len(outcomes))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
