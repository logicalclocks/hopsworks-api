"""The feedback review job: triage the verdicts people gave, with a model's help.

One job per agent, run by hand from the Feedback tab or on a schedule. Each
execution is a recorded run of type FEEDBACK_REVIEW whose window is on when the
feedback was given; the backend holds the watermark, so this reads the window off
the run row and only reports back.

The shape is the evaluation runner's: ``--run-id`` is repeatable, one failed run
does not take its siblings down, and rows are written through ``_match_schema`` so
a column that happens to be all-null does not trip the Delta writer.

What it does per verdict is in :mod:`triage`; what it decides is nothing. Every
row it writes is a proposal a person will look at.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .agent_source import (
    DEFAULT_SOURCE_CHARS,
    CodeLocation,
    SourceBundle,
    load_agent_source,
    relevant_files,
)
from .api import hopsworks_session
from .client import HopsworksAgentClient
from .clusters import Cluster, assign_clusters, from_api, known_signatures
from .judge_config import (
    JudgeConfig,
    api_key_for,
    api_key_source,
    completer_for,
    tool_calls_text,
)
from .run_fields import run_trials
from .run_job import _api, _match_schema
from .sample_job import conversation_before, question_and_answer, started_ns
from .triage import TriageInput, triage, triage_row


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


log = logging.getLogger(__name__)

TRIAGE_FG = "agent_feedback_triage"
CLUSTERS_FG = "agent_feedback_clusters"
#: How many feedback rows one listing request asks for.
PAGE = 100
#: A ceiling on one execution whatever the job says, so a mistyped budget cannot run up a bill.
MAX_BUDGET = 5000


def _otel(host: str, project_id: int, deployment_id: int) -> str:
    return (
        f"{host.rstrip('/')}/hopsworks-api/api/project/{project_id}"
        f"/otel/servings/{deployment_id}"
    )


def _jobs(host: str, project_id: int) -> str:
    return f"{host.rstrip('/')}/hopsworks-api/api/project/{project_id}/jobs"


def _serving(host: str, project_id: int, deployment_id: int) -> str:
    return f"{host.rstrip('/')}/hopsworks-api/api/project/{project_id}/serving/{deployment_id}"


#: What a review reads by default: people's verdicts, the errors the platform found in traces,
#: and the traces an online judge failed. Anomalies (latency, tool loops) are opt-in: they are
#: thresholds, and a threshold is an opinion. The backend applies these when it starts a run;
#: the job carries them so its log says what the run was fed.
DEFAULT_SOURCES = ("feedback", "errors", "judge")


def review_settings(job: dict[str, Any] | None) -> dict[str, Any]:
    """The model settings off the job's configuration, with the defaults the backend uses."""
    config = (job or {}).get("config") or {}
    raw_sources = str(config.get("sources") or "")
    sources = (
        tuple(s.strip() for s in raw_sources.split(",") if s.strip()) or DEFAULT_SOURCES
    )
    read_code = config.get("readSourceCode")
    return {
        "provider": str(config.get("provider") or "anthropic"),
        "model": str(config.get("model") or ""),
        "reasoning_effort": str(config.get("reasoningEffort") or ""),
        "api_key_env": str(config.get("apiKeyEnv") or ""),
        # where the model is when not the provider's own, and what a gateway in front of it wants
        "base_url": str(config.get("baseUrl") or ""),
        "headers": _headers_setting(config.get("headers")),
        "context_turns": int(config.get("contextTurns") or 20),
        "sources": sources,
        # on unless the job says otherwise: a review that cannot see the code cannot tell a bug
        # from a bad answer, and telling them apart is the point
        "read_source_code": True if read_code is None else bool(read_code),
        "source_location": str(config.get("sourceLocation") or ""),
        "source_chars": int(config.get("sourceChars") or DEFAULT_SOURCE_CHARS),
    }


def _headers_setting(raw: Any) -> dict[str, str]:
    """The job's extra headers: a JSON object as text, or already an object; anything else is none."""
    if isinstance(raw, str):
        if not raw.strip():
            return {}
        try:
            raw = json.loads(raw)
        except ValueError:
            log.warning("the job's headers are not a JSON object; sending none")
            return {}
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items() if str(k).strip()}


def code_location(
    session: Any,
    host: str,
    project_id: int,
    deployment_id: int,
    settings: dict[str, Any],
) -> CodeLocation:
    """Where the agent's code is: the job's override when set, else what the deployment says."""
    override = CodeLocation.from_override(settings.get("source_location") or "")
    view: dict[str, Any] = {}
    try:
        response = session.get(_serving(host, project_id, deployment_id), timeout=60)
        response.raise_for_status()
        view = response.json() or {}
    except Exception:  # noqa: BLE001 -- the code is a help; a review without it still reviews
        log.exception(
            "could not read deployment %s; its code location is unknown", deployment_id
        )
    described = CodeLocation.from_serving(view)
    if override.known():
        # the entry script is still the deployment's, unless the override names a file itself
        override.script_file = override.script_file or described.script_file
        return override
    return described


def completer_from(settings: dict[str, Any]) -> tuple[Callable[[str], str] | None, str]:
    """The model call, or why there is none. A missing key is a reason, not an exception."""
    config = JudgeConfig(
        provider=settings["provider"],
        model=settings["model"],
        reasoning_effort=settings["reasoning_effort"],
        api_key_env=settings["api_key_env"],
        base_url=settings.get("base_url", ""),
        headers=dict(settings.get("headers") or {}),
    )
    if config.provider == "custom" and not config.base_url:
        return (
            None,
            "an OpenAI-compatible provider needs a base URL; set one on the job",
        )
    key = api_key_for(config)
    if not key:
        return (
            None,
            f"no API key: set {api_key_source(config)} on the job's environment",
        )
    return completer_for(config, key), ""


def _ms(value: Any) -> float:
    """An epoch-millisecond reading of whatever the API sent for a timestamp."""
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp() * 1000


TRACE_SOURCE_PREFIX = "feedback:trace:"


def trace_of(run: dict[str, Any]) -> str:
    """The one trace a run was asked to review, or empty for a window."""
    source = str(run.get("sampleSource") or "")
    return (
        source[len(TRACE_SOURCE_PREFIX) :]
        if source.startswith(TRACE_SOURCE_PREFIX)
        else ""
    )


def feedback_in_window(
    session: Any, otel_base: str, from_ms: float, to_ms: float, trace_id: str = ""
) -> list[dict[str, Any]]:
    """Every verdict that needs attention in (from, to], oldest first -- or every verdict.

    on one trace, whenever given, when a reviewer asked for that trace again.

    "negative" to the server means everything that is not an endorsement, so a
    false alarm is included: those are often a mislabelled negative, and the
    model's reading of them is worth having.
    """
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        params: dict[str, Any] = {
            "verdict": "negative",
            "limit": PAGE,
            "offset": offset,
        }
        if trace_id:
            params["traceId"] = trace_id
        else:
            params["from"] = str(int(from_ms))
            params["to"] = str(int(to_ms))
        response = session.get(f"{otel_base}/feedback", params=params, timeout=60)
        response.raise_for_status()
        page = response.json() or {}
        items = page.get("items") or []
        rows.extend(items)
        offset += len(items)
        if not items or offset >= int(page.get("count") or 0):
            break
    rows.sort(key=lambda r: (_ms(r.get("createdAt")), str(r.get("feedbackId") or "")))
    return rows


def existing_clusters(session: Any, otel_base: str) -> list[Cluster]:
    """The deployment's clusters as the backend has them; none when they cannot be read,.

    because a review that cannot see its clusters still reviews, it just files under new ones.
    """
    try:
        response = session.get(f"{otel_base}/feedback/clusters", timeout=60)
        response.raise_for_status()
        body = response.json() or []
        items = body if isinstance(body, list) else body.get("items") or []
        return [from_api(item) for item in items]
    except Exception:  # noqa: BLE001 -- clustering is a help, not a requirement
        log.exception(
            "could not read existing clusters; new ones will be made where needed"
        )
        return []


def review_feedback(
    session: Any,
    client: Any,
    otel_base: str,
    run: dict[str, Any],
    settings: dict[str, Any],
    complete: Callable[[str], str] | None,
    no_model_reason: str = "",
    clusters: Sequence[Cluster] = (),
    source: SourceBundle | None = None,
) -> tuple[list[dict[str, Any]], float | None]:
    """Triage the run's window. Returns the rows to write and, when the budget cut the.

    window short, the timestamp the next run should start from.
    """
    from_ms = _ms(run.get("sampleFrom"))
    to_ms = _ms(run.get("sampleTo")) or datetime.now(tz=timezone.utc).timestamp() * 1000
    budget = min(run_trials(run, 0) or MAX_BUDGET, MAX_BUDGET)

    trace_id = trace_of(run)
    pending = feedback_in_window(session, otel_base, from_ms, to_ms, trace_id)
    if not pending:
        if trace_id:
            log.info("no feedback needing attention on trace %s", trace_id)
        else:
            log.info("no feedback to review between %s and %s", from_ms, to_ms)
        return [], None
    chosen = pending[:budget]
    if len(chosen) < len(pending):
        log.warning(
            "%d verdicts in the window, reviewing the oldest %d; the rest wait for the next run",
            len(pending),
            len(chosen),
        )
    else:
        log.info("reviewing %d verdicts", len(chosen))

    rows: list[dict[str, Any]] = []
    sessions: dict[str, list[dict[str, Any]]] = {}
    signatures = known_signatures(clusters)
    for feedback in chosen:
        rows.append(
            _review_one(
                session,
                client,
                otel_base,
                run,
                settings,
                complete,
                no_model_reason,
                feedback,
                sessions,
                signatures,
                source,
            )
        )

    processed_through = (
        _ms(chosen[-1].get("createdAt")) if len(chosen) < len(pending) else None
    )
    return rows, processed_through


def _review_one(
    session: Any,
    client: Any,
    otel_base: str,
    run: dict[str, Any],
    settings: dict[str, Any],
    complete: Callable[[str], str] | None,
    no_model_reason: str,
    feedback: dict[str, Any],
    sessions: dict[str, list[dict[str, Any]]],
    signatures: Sequence[str] = (),
    source: SourceBundle | None = None,
) -> dict[str, Any]:
    provenance = {
        "run_id": str(run["runId"]),
        "provider": settings["provider"],
        "model": settings["model"],
    }
    if complete is None:
        return triage_row(feedback, None, error=no_model_reason, **provenance)
    trace_id = str(feedback.get("traceId") or "")
    try:
        detail = session.get(f"{otel_base}/traces/{trace_id}", timeout=60).json()
        trace = client.fetch_trace(trace_id)
    except Exception as err:  # noqa: BLE001 -- one unreadable trace is one row that says so
        log.exception("could not read trace %s", trace_id)
        return triage_row(
            feedback, None, error=f"could not read trace: {err}", **provenance
        )
    question, answer = question_and_answer(detail)
    earlier = conversation_before(
        session,
        otel_base,
        str(feedback.get("sessionId") or ""),
        started_ns({}, detail),
        sessions,
    )
    tool_calls, tool_results = tool_calls_text(trace)
    source_files: list[tuple[str, str]] = []
    if source:
        # the files this trace has a reason to be read against: what it called, and the words
        # of the failure (a detector's error message, a reviewer's note)
        source_files = relevant_files(
            source,
            tool_names=list((trace or {}).get("tool_names") or []),
            clues=[
                str(feedback.get("note") or ""),
                str(feedback.get("expectedToolBehavior") or ""),
            ],
            budget_chars=int(settings.get("source_chars") or DEFAULT_SOURCE_CHARS),
        )
    result, why = triage(
        complete,
        TriageInput(
            feedback=feedback,
            question=question,
            answer=answer,
            earlier_turns=earlier,
            tool_calls=tool_calls,
            tool_results=tool_results,
            known_signatures=signatures,
            source_files=source_files,
            source_origin=source.origin if source else "",
        ),
        context_turns=settings["context_turns"],
    )
    return triage_row(feedback, result, error=why, **provenance)


def write_clusters(feature_store: Any, clusters: Sequence[Cluster]) -> None:
    """Upsert the clusters that changed. The online table keys on cluster_id, so a.

    cluster written again replaces its row; offline history keeps every version.
    """
    if not clusters:
        return
    import pandas as pd  # noqa: PLC0415

    now = datetime.now(tz=timezone.utc)
    group = feature_store.get_feature_group(CLUSTERS_FG, 1)
    if group is None:
        raise RuntimeError(
            f"feature group {CLUSTERS_FG} v1 does not exist in this project's feature store; "
            "Hopsworks provisions it when a review job is started -- check the server log for why "
            "that failed, then run the review again"
        )
    frame = pd.DataFrame([c.to_row(now) for c in clusters])
    group.insert(_match_schema(group, frame), write_options={"mode": "append"})
    log.info("wrote %d clusters to %s", len(clusters), CLUSTERS_FG)


def write_triage(feature_store: Any, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        return
    import pandas as pd  # noqa: PLC0415 -- only a job that has rows pays for pandas

    group = feature_store.get_feature_group(TRIAGE_FG, 1)
    if group is None:
        # The backend creates this when a review is started; a missing group means that step
        # failed, and its reason is in the server log, not here. Said plainly rather than left
        # as an attribute error on None.
        raise RuntimeError(
            f"feature group {TRIAGE_FG} v1 does not exist in this project's feature store; "
            "Hopsworks provisions it when a review job is started -- check the server log for "
            "why that failed, then run the review again"
        )
    group.insert(
        _match_schema(group, pd.DataFrame(list(rows))), write_options={"mode": "append"}
    )
    log.info("wrote %d rows to %s", len(rows), TRIAGE_FG)


def _execute(run_id: str, session: Any, base: str, project: Any, host: str) -> bool:
    def report(
        status: str, error: str | None = None, processed_through: float | None = None
    ) -> None:
        params: dict[str, Any] = {"status": status}
        if error:
            params["errorMessage"] = error
        if processed_through is not None:
            # the window closes where the budget ran out, so the next run picks up the rest
            params["processedThrough"] = str(int(processed_through))
        try:
            session.put(f"{base}/runs/{run_id}/status", params=params, timeout=30)
        except Exception:  # noqa: BLE001 -- never mask the real failure
            log.exception("could not report status %s", status)

    try:
        run = session.get(f"{base}/runs/{run_id}", timeout=60).json()
        if run.get("runType") != "FEEDBACK_REVIEW":
            report(
                "FAILED",
                f"run {run_id} is a {run.get('runType')} run, not a feedback review",
            )
            return False
        job = {}
        if run.get("jobName"):
            try:
                job = session.get(
                    f"{_jobs(host, project.id)}/{run['jobName']}", timeout=60
                ).json()
            except Exception:  # noqa: BLE001 -- defaults are a worse answer than the job's, never a wrong one
                log.exception(
                    "could not read job %s; reviewing with default settings",
                    run["jobName"],
                )
        settings = review_settings(job)
        complete, reason = completer_from(settings)
        if complete is None:
            log.error(
                "%s -- every row of this run will be recorded as ungradable", reason
            )
        deployment_id = int(run["deploymentId"])
        client = HopsworksAgentClient(
            session=session,
            api_base=host,
            project_id=project.id,
            project_name=project.name,
            deployment_id=deployment_id,
        )
        otel_base = _otel(host, project.id, deployment_id)
        clusters = existing_clusters(session, otel_base)
        log.info("sources for this run: %s", ", ".join(settings["sources"]))
        source: SourceBundle | None = None
        if settings["read_source_code"]:
            location = code_location(session, host, project.id, deployment_id, settings)
            source = load_agent_source(location, dataset_api=_dataset_api(project))
            if not source:
                log.warning(
                    "reviewing without the agent's source code (%s)",
                    source.origin or "location unknown",
                )
        rows, processed_through = review_feedback(
            session,
            client,
            otel_base,
            run,
            settings,
            complete,
            reason,
            clusters,
            source,
        )
        changed = assign_clusters(rows, clusters)
        feature_store = project.get_feature_store()
        write_triage(feature_store, rows)
        write_clusters(feature_store, changed)
        report("SUCCEEDED", processed_through=processed_through)
        return True
    except Exception as err:  # noqa: BLE001 -- the row must say what happened
        log.exception("review run %s failed", run_id)
        report("FAILED", str(err))
        return False


def _dataset_api(project: Any) -> Any:
    try:
        return project.get_dataset_api()
    except Exception:  # noqa: BLE001 -- only needed for a HopsFS code path
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, action="append", dest="run_ids")
    # The scheduler appends "-start_time <fire time>" to every scheduled execution's arguments.
    # This job reads its window off its run rows, not that flag, so it is tolerated and ignored
    # rather than refused -- a refusal here is a scheduled job that never runs.
    args, ignored = parser.parse_known_args()
    if ignored:
        logging.getLogger(__name__).info(
            "ignoring arguments this job does not read: %s", " ".join(ignored)
        )
    logging.basicConfig(level=logging.INFO)

    import hopsworks  # noqa: PLC0415 -- only inside a job

    project = hopsworks.login()
    host = os.environ.get("HOPSWORKS_HOST") or os.environ["REST_ENDPOINT"]
    session = hopsworks_session()
    base = _api(host, project.id)
    outcomes = [
        _execute(run_id, session, base, project, host) for run_id in args.run_ids
    ]
    failed = outcomes.count(False)
    if failed:
        log.error("%d of %d review runs failed", failed, len(outcomes))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
