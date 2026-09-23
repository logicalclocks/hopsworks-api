# ruff: noqa: INP001
"""Check this system's system.yaml against the rules in hops-reqs/references/system-yaml.md.

Runs with the unit tests. Every phase also runs it on the file it is about to write,
before renaming that file into place, so an invalid file never replaces a valid one:

    python tests/unit/test_system_yaml.py /tmp/system.yaml.new && mv /tmp/system.yaml.new system.yaml
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml


SCHEMA_VERSION = 1
SYSTEM_YAML = Path(__file__).resolve().parents[2] / "system.yaml"

PHASE_STATUS = {
    "requirements": {"pending", "met", "stale"},
    "data": {"pending", "running", "met", "unmet", "stale"},
    "features": {"pending", "running", "met", "unmet", "stale"},
    "training": {"pending", "running", "met", "unmet", "accepted", "stale", "skipped"},
    "inference": {"pending", "running", "met", "unmet", "accepted", "stale"},
    "app": {"pending", "running", "met", "skipped", "stale"},
    "verify": {"pending", "pass", "fail"},
}
SYSTEM_STATUS = {"draft", "building", "verified", "deployed"}
TASKS_BUILT = {"classification", "regression", "forecasting"}
TASKS_CAPTURED = {"ranking", "anomaly", "rag", "agentic"}
SYSTEM_TYPES = {"batch", "realtime", "agent"}
SOURCE_KINDS = {"feature_group", "datasource", "file", "url", "synthetic"}
SOURCE_STATUS = {
    "present",
    "connected",
    "needs_connection",
    "needs_download",
    "needs_generation",
}
ROW_STATES = {"submitted", "running", "finished", "failed"}
RUN_VERDICTS = {"keep", "discard", "crash"}
RUN_MODES = {"scheduled", "continuous"}
DECIDERS = {"user", "claude"}

IDENT = re.compile(r"^[a-z][a-z0-9_]*$")
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")
ALNUM = re.compile(r"^[a-z0-9]+$")
SECRET_KEYS = {"password", "api_key", "apikey", "token", "hf_token", "secret"}
SECRET_VALUES = re.compile(
    r"(hf_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{20,}|ghp_[A-Za-z0-9]{20,})"
)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_status(problems: list[str], where: str, block: dict, allowed: set) -> None:
    status = block.get("status")
    if status not in allowed:
        problems.append(
            f"{where}.status is {status!r}; expected one of {sorted(allowed)}"
        )


def _check_evidence(problems: list[str], where: str, row: object) -> None:
    """A row of evidence names the run and the commit it came from."""
    if not isinstance(row, dict):
        problems.append(f"{where} must be a mapping")
        return
    for key in ("run_id", "commit"):
        if not row.get(key):
            problems.append(f"{where} has no {key}")


def _check_secrets(problems: list[str], node: object, where: str = "") -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{where}.{key}" if where else str(key)
            if str(key).lower() in SECRET_KEYS and isinstance(value, str) and value:
                problems.append(
                    f"{path} holds a literal value; reference a project secret by name instead"
                )
            _check_secrets(problems, value, path)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            _check_secrets(problems, item, f"{where}[{i}]")
    elif isinstance(node, str) and SECRET_VALUES.search(node):
        problems.append(f"{where} contains what looks like a credential")


def _check_requirements(problems: list[str], req: dict) -> None:
    problem = req.get("problem") or {}
    task = problem.get("task")
    if task not in TASKS_BUILT | TASKS_CAPTURED:
        problems.append(f"requirements.problem.task {task!r} is not a known task")
    if problem.get("generalises_to") not in (None, "new_periods", "new_entities"):
        problems.append(
            "requirements.problem.generalises_to must be new_periods or new_entities"
        )

    system_type = req.get("system_type")
    if system_type not in SYSTEM_TYPES:
        problems.append(
            f"requirements.system_type {system_type!r} is not one of {sorted(SYSTEM_TYPES)}"
        )
    sla = req.get("sla") or {}
    if len(sla) != 1:
        problems.append(
            f"requirements.sla must hold exactly one block, found {sorted(sla)}"
        )
    elif system_type in SYSTEM_TYPES and system_type not in sla:
        problems.append(
            f"requirements.sla holds {sorted(sla)} but system_type is {system_type}"
        )

    for i, source in enumerate(req.get("data_sources") or []):
        where = f"requirements.data_sources[{i}]"
        if not source.get("name"):
            problems.append(f"{where} has no name")
        if source.get("kind") not in SOURCE_KINDS:
            problems.append(
                f"{where}.kind {source.get('kind')!r} is not one route of {sorted(SOURCE_KINDS)}"
            )
        if source.get("status") not in SOURCE_STATUS:
            problems.append(
                f"{where}.status {source.get('status')!r} is not one of {sorted(SOURCE_STATUS)}"
            )
        if source.get("kind") == "datasource" and not source.get("type"):
            problems.append(f"{where} is a datasource without a connector type")
        if source.get("kind") == "synthetic" and source.get("shape") not in (
            "batch",
            "events",
        ):
            problems.append(f"{where} is synthetic and needs shape batch or events")

    targets = req.get("targets") or {}
    if targets:
        if not targets.get("metric"):
            problems.append("requirements.targets.metric is missing")
        if not _is_number(targets.get("target")):
            problems.append("requirements.targets.target must be a number")
        if targets.get("direction") not in ("max", "min"):
            problems.append("requirements.targets.direction must be max or min")

    operations = req.get("operations") or {}
    for name in ("features", "training", "inference"):
        run = (operations.get(name) or {}).get("run")
        if run is not None and run not in RUN_MODES:
            problems.append(
                f"requirements.operations.{name}.run must be scheduled or continuous"
            )

    for i, feature in enumerate(req.get("features") or []):
        if feature.get("computed_in") not in (
            None,
            "feature_pipeline",
            "streaming",
            "on_demand",
        ):
            problems.append(
                f"requirements.features[{i}].computed_in is not a known location"
            )


def _check_training(problems: list[str], training: dict) -> None:
    for i, row in enumerate(training.get("runs") or []):
        where = f"training.runs[{i}]"
        _check_evidence(problems, where, row)
        if not isinstance(row, dict):
            continue
        if not isinstance(row.get("n"), int) or not isinstance(row.get("attempt"), int):
            problems.append(f"{where} needs integer n and attempt")
        state = row.get("state")
        if state not in ROW_STATES:
            problems.append(
                f"{where}.state {state!r} is not one of {sorted(ROW_STATES)}"
            )
        if state == "finished" and row.get("status") not in RUN_VERDICTS:
            problems.append(
                f"{where} finished without a keep, discard or crash verdict"
            )
        if state == "finished" and row.get("status") in ("keep", "discard"):
            model = row.get("model") or {}
            name = str(model.get("name", ""))
            if not name.startswith("hf:") and not name.endswith("_research"):
                problems.append(
                    f"{where}.model.name {name!r} must be a <ident>_research version"
                )
            if not name.startswith("hf:") and not isinstance(model.get("version"), int):
                problems.append(
                    f"{where}.model.version must be the version registration returned"
                )
    for i, row in enumerate(training.get("acceptance") or []):
        where = f"training.acceptance[{i}]"
        _check_evidence(problems, where, row)
        if isinstance(row, dict) and row.get("state") == "finished" and row.get("met"):
            name = str((row.get("model") or {}).get("name", ""))
            if not name.endswith("_model"):
                problems.append(
                    f"{where}.model.name {name!r} must be the <ident>_model name"
                )
    model = training.get("model") or {}
    if model and not str(model.get("name", "")).endswith("_model"):
        problems.append(
            "training.model.name must be the <ident>_model name inference reads"
        )


def _check_names(problems: list[str], doc: dict) -> None:
    """The three identifier flavours of rule 6."""
    slug = (doc.get("system") or {}).get("slug", "")
    if not SLUG.match(str(slug)):
        problems.append(f"system.slug {slug!r} must be lowercase with hyphens")
    for pipeline in (doc.get("features") or {}).get("pipelines") or []:
        fg = (pipeline.get("writes") or {}).get("feature_group")
        if fg and not IDENT.match(fg):
            problems.append(f"feature group {fg!r} must be snake_case")
        job = (pipeline.get("job") or {}).get("name")
        if job and not SLUG.match(job):
            problems.append(f"job {job!r} must be lowercase with hyphens")
    training = doc.get("training") or {}
    fv = (training.get("feature_view") or {}).get("name")
    if fv and not IDENT.match(fv):
        problems.append(f"feature view {fv!r} must be snake_case")
    inference = doc.get("inference") or {}
    for mode in ("realtime", "agent"):
        deployment = (inference.get(mode) or {}).get("deployment")
        if deployment and not ALNUM.match(deployment):
            problems.append(
                f"deployment {deployment!r} must be lowercase letters and digits"
            )
    app = (doc.get("app") or {}).get("name")
    if app and not SLUG.match(app):
        problems.append(f"app {app!r} must be lowercase with hyphens")


def validate(doc: object) -> list[str]:
    """Return every rule this document breaks; an empty list means it is valid."""
    if not isinstance(doc, dict):
        return ["system.yaml must be a mapping"]
    problems: list[str] = []
    if doc.get("schema_version") != SCHEMA_VERSION:
        problems.append(f"schema_version must be {SCHEMA_VERSION}")

    system = doc.get("system") or {}
    for key in ("name", "slug", "target"):
        if not system.get(key):
            problems.append(f"system.{key} is missing")
    if (system.get("target") or {}).get("stage", "development") != "development":
        problems.append(
            "system.target.stage must be development; v1 builds nothing else"
        )
    if system.get("status") not in SYSTEM_STATUS:
        problems.append(
            f"system.status {system.get('status')!r} is not one of {sorted(SYSTEM_STATUS)}"
        )

    for key, allowed in PHASE_STATUS.items():
        if key in doc:
            block = doc[key]
            if not isinstance(block, dict):
                problems.append(f"{key} must be a mapping")
                continue
            _check_status(problems, key, block, allowed)
    if "requirements" not in doc:
        problems.append("requirements is missing")
    else:
        _check_requirements(problems, doc["requirements"])

    for i, pipeline in enumerate((doc.get("features") or {}).get("pipelines") or []):
        where = f"features.pipelines[{i}]"
        _check_status(problems, where, pipeline, PHASE_STATUS["features"])
        if pipeline.get("run") not in RUN_MODES:
            problems.append(f"{where}.run must be scheduled or continuous")
        last = (pipeline.get("tests") or {}).get("last_run")
        if last:
            _check_evidence(problems, f"{where}.tests.last_run", last)

    if isinstance(doc.get("training"), dict):
        _check_training(problems, doc["training"])
    for i, row in enumerate((doc.get("inference") or {}).get("measured") or []):
        where = f"inference.measured[{i}]"
        _check_evidence(problems, where, row)
        if isinstance(row, dict) and not row.get("execution"):
            problems.append(f"{where} has no execution")

    decisions = doc.get("decisions")
    if decisions is not None and not isinstance(decisions, list):
        problems.append("decisions must be a list")
    for i, decision in enumerate(decisions or []):
        where = f"decisions[{i}]"
        if not isinstance(decision, dict):
            problems.append(f"{where} must be a mapping")
            continue
        missing = [k for k in ("phase", "by", "what", "why") if not decision.get(k)]
        if missing:
            problems.append(f"{where} is missing {', '.join(missing)}")
        if decision.get("by") not in DECIDERS:
            problems.append(f"{where}.by must be user or claude")

    _check_names(problems, doc)
    _check_secrets(problems, doc)
    return problems


def test_system_yaml():
    doc = yaml.safe_load(SYSTEM_YAML.read_text(encoding="utf-8"))
    problems = validate(doc)
    assert not problems, "\n".join(problems)


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else SYSTEM_YAML
    found = validate(yaml.safe_load(target.read_text(encoding="utf-8")))
    for line in found:
        print(line, file=sys.stderr)
    sys.exit(1 if found else 0)
