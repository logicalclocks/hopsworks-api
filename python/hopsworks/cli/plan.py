"""The plan of a system, for ``hops factory system plan``: each phase's state, read from the project.

A phase is done when every asset its block records exists in the project, and
not done when one is missing, whatever its ``status`` says: the status goes
stale when an asset is deleted, and a user may bring an asset (a model trained
elsewhere, an existing feature view) that no phase of this build made. The
first phase, in the factory's order, whose block names an asset makes it; a
later phase that names it needs it. So a build starts wherever the project
already is, and only what is missing is built.

A phase whose block records no asset (requirements, a design, verify) is a
decision or a check, and keeps the status it says. A phase whose assets all
exist keeps a judgement it recorded (``unmet``: the model exists but misses its
target). An analytics layer records its tables and jobs under ``outputs`` (a gold
layer's per data mart, under ``marts``), not per phase: its phases keep their
status, and the plan lists the tables and jobs that are gone.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from hopsworks.cli import system_doc, teardown


# The phases of a system recorded before factories listed their own, and the
# names system.progress gives them.
ML_PHASES = (
    "requirements",
    "data",
    "features",
    "training",
    "inference",
    "app",
    "verify",
)
PROGRESS_NAME = {"requirements": "reqs", "training": "train", "inference": "infer"}
DECISIONS = {"requirements", "verify"}
SATISFIED = {"met", "skipped", "accepted", "pass", "done"}
JUDGED = {"unmet", "fail", "failed"}

# Keys whose value names an asset of that kind: a name, or a mapping with `name` (and `version`).
_KEYS = {
    "feature_group": "feature group",
    "log_feature_group": "feature group",
    "feature_view": "feature view",
    "model": "model",
    "embedding_model": "model",
    "deployment": "deployment",
    "job": "job",
}
# Keys whose value is a list of them, as an analytics layer's `outputs` and a factory's records hold.
_LISTS = {
    "tables": "feature group",
    "feature_groups": "feature group",
    "feature_views": "feature view",
    "models": "model",
    "deployments": "deployment",
    "jobs": "job",
    "apps": "app",
}
# Records of what was tried or decided: their models and runs are the past, not what the phase makes.
_HISTORY = {"runs", "measured", "history", "decisions", "releases", "edits", "rounds"}
# A `{name, kind}` entry, as requirements and data list their sources.
_KINDS = {
    "feature_group": "feature group",
    "feature_view": "feature view",
    "model": "model",
}


@dataclass(frozen=True)
class Ref:
    """An asset a block names; `version` None is any version."""

    kind: str
    name: str
    version: int | None = None

    def __str__(self) -> str:
        return f"{self.kind} {self.name}" + (
            f" v{self.version}" if self.version is not None else ""
        )


@dataclass
class Phase:
    """One phase of the plan."""

    key: str
    status: str | None
    makes: list[Ref] = field(default_factory=list)
    needs: list[Ref] = field(default_factory=list)
    state: str = "todo"
    missing: list[Ref] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "phase": self.key,
            "state": self.state,
            "status": self.status,
            "makes": [str(r) for r in self.makes],
            "needs": [str(r) for r in self.needs],
            "missing": [str(r) for r in self.missing],
            "unknown": self.unknown,
        }


def _version(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return int(value) if isinstance(value, str) and value.isdigit() else None


def _ref(kind: str, value: Any) -> Ref | None:
    if isinstance(value, str) and value.strip():
        named = teardown._NAMED_VERSION.match(value.strip())
        if named:
            return Ref(kind, named.group(1), int(named.group(2)))
        return Ref(kind, value.strip())
    if isinstance(value, dict) and isinstance(value.get("name"), str):
        return Ref(kind, value["name"], _version(value.get("version")))
    return None


def refs(key: str, block: Any) -> list[Ref]:
    """The assets the block of phase `key` records, in the order it names them.

    Args:
        key: The phase.
        block: Its block in system.yaml.

    Returns:
        The assets.
    """
    if key in DECISIONS:
        return []
    found: list[Ref] = []

    def add(ref: Ref | None) -> None:
        if ref is not None and ref not in found:
            found.append(ref)

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        kind = _KINDS.get(str(node.get("kind")))
        if kind:
            add(_ref(kind, node))
        for k, value in node.items():
            if k in _HISTORY:
                continue
            if k in _KEYS:
                ref = _ref(_KEYS[k], value)
                # {feature_group: name, version: 2}: the version sits beside the name.
                if ref and ref.version is None and isinstance(value, str):
                    ref = Ref(ref.kind, ref.name, _version(node.get("version")))
                add(ref)
            if k in _LISTS and isinstance(value, list):
                for item in value:
                    add(_ref(_LISTS[k], item))
            walk(value)

    if key == "app" and isinstance(block, dict):
        # A dashboard is a Superset dashboard, which the project does not list by name.
        if block.get("wanted") is not False and block.get("kind") != "dashboard":
            add(_ref("app", block.get("name")))
        walk({k: v for k, v in block.items() if k != "name"})
    else:
        walk(block)
    return found


def phase_keys(doc: dict) -> list[str]:
    """The phases of `doc` in order: the factory's, an analytics layer's, else the ML phases."""
    listed = (doc.get("factory") or {}).get("phases")
    if isinstance(listed, list) and listed:
        return [str(p["key"]) for p in listed if isinstance(p, dict) and p.get("key")]
    if isinstance(doc.get("layer"), dict):
        # A gold layer's phases are per data mart, under `marts`.
        phases = doc.get("phases")
        return [str(k) for k in phases] if isinstance(phases, dict) else []
    return list(ML_PHASES)


def _blocks(doc: dict) -> dict:
    if isinstance(doc.get("layer"), dict):
        return doc["phases"] if isinstance(doc.get("phases"), dict) else {}
    return doc


class Project:
    """Whether assets exist in one project, each looked up once.

    `exists` answers True, False, or None when Hopsworks could not say (an
    error other than not found), which leaves a phase as its status says.
    """

    def __init__(self, project: Any) -> None:
        self.project = project
        self._fs = None
        self._seen: dict[Ref, bool | None] = {}
        self.errors: dict[Ref, str] = {}

    @property
    def fs(self) -> Any:
        if self._fs is None:
            self._fs = self.project.get_feature_store()
        return self._fs

    def exists(self, ref: Ref) -> bool | None:
        if ref not in self._seen:
            try:
                self._seen[ref] = bool(self._lookup(ref))
            except Exception as exc:  # noqa: BLE001 - a lookup that fails decides nothing
                if teardown._missing(exc):
                    self._seen[ref] = False
                else:
                    self._seen[ref] = None
                    self.errors[ref] = str(exc)
        return self._seen[ref]

    def _lookup(self, ref: Ref) -> Any:
        if ref.kind == "feature group":
            if ref.version is None:
                return self.fs.get_feature_groups(ref.name)
            return self.fs.get_feature_group(ref.name, ref.version)
        if ref.kind == "feature view":
            if ref.version is None:
                return self.fs.get_feature_views(ref.name)
            return self.fs.get_feature_view(ref.name, ref.version)
        if ref.kind == "model":
            registry = self.project.get_model_registry()
            if ref.version is None:
                return registry.get_models(ref.name)
            return registry.get_model(ref.name, ref.version)
        if ref.kind == "deployment":
            return self.project.get_model_serving().get_deployment(ref.name)
        if ref.kind == "job":
            return self.project.get_job_api().get_job(ref.name)
        if ref.kind == "app":
            return self.project.get_app_api().get_app(ref.name)
        raise ValueError(f"no lookup for {ref.kind}")


def compute(doc: dict, exists: Any, goal: str | None = None) -> dict:
    """The plan of `doc`: each phase's state and the next phase to work on.

    States: `done`; `build` (something it makes is missing and what it needs
    exists); `blocked` (something it needs is missing, made by `by`);
    `todo` (a decision or check not satisfied yet); `failed` (its assets exist
    and its block records a judgement against them); `skipped`; `unknown`
    (Hopsworks could not say).

    Args:
        doc: The parsed system.yaml.
        exists: Answers whether a `Ref` exists: True, False or None.
        goal: Plan only the phases this phase needs, and itself.

    Returns:
        `{"phases": [...], "next": key or None, "outputs_missing": [...]}`.
    """
    keys = phase_keys(doc)
    if goal is not None and goal not in keys:
        raise ValueError(f"no phase {goal!r}; the phases are {', '.join(keys)}")
    blocks = _blocks(doc)
    phases: list[Phase] = []
    producer: dict[tuple[str, str], str] = {}
    for key in keys:
        block = blocks.get(key) if isinstance(blocks.get(key), dict) else {}
        status = str(block["status"]) if block.get("status") is not None else None
        phase = Phase(key, status)
        for ref in refs(key, block):
            owner = producer.setdefault((ref.kind, ref.name), key)
            (phase.makes if owner == key else phase.needs).append(ref)
        phases.append(phase)

    by_key = {p.key: p for p in phases}
    for phase in phases:
        phase.missing = [r for r in phase.makes if exists(r) is False]
        lacking = [r for r in phase.needs if exists(r) is False]
        phase.unknown = [str(r) for r in phase.makes + phase.needs if exists(r) is None]
        if not phase.makes:
            phase.state = (
                "skipped"
                if phase.status == "skipped"
                else "done"
                if phase.status in SATISFIED
                else "failed"
                if phase.status in JUDGED
                else "todo"
            )
            if phase.state == "todo" and lacking:
                phase.state = "blocked"
                phase.missing = lacking
        elif phase.unknown:
            phase.state = "unknown"
        elif not phase.missing:
            phase.state = "failed" if phase.status in JUDGED else "done"
        elif phase.status == "skipped":
            phase.state = "skipped"
        elif lacking:
            phase.state = "blocked"
            phase.missing = lacking
        else:
            phase.state = "build"

    wanted = set(keys)
    if goal is not None:
        wanted = {goal}
        frontier = [goal]
        while frontier:
            for ref in by_key[frontier.pop()].needs:
                owner = producer[(ref.kind, ref.name)]
                if owner not in wanted:
                    wanted.add(owner)
                    frontier.append(owner)
        # Decisions come first whatever the goal: the requirements say what to build.
        wanted |= {k for k in keys if k == "requirements"}
    planned = [p for p in phases if p.key in wanted]
    nxt = next(
        (
            p.key
            for p in planned
            if p.state not in ("done", "skipped") and p.key != "verify"
        ),
        None,
    )
    if nxt is None and any(p.key == "verify" and p.state != "done" for p in planned):
        nxt = "verify"

    gone: list[str] = []
    if isinstance(doc.get("layer"), dict):
        gone = [
            str(r)
            for r in refs(
                "outputs", {"outputs": doc.get("outputs"), "marts": doc.get("marts")}
            )
            if exists(r) is False
        ]
    return {
        "phases": [
            {
                **p.as_dict(),
                **({"by": _by(p, producer)} if p.state == "blocked" else {}),
            }
            for p in planned
        ],
        "next": nxt,
        "outputs_missing": gone,
    }


def _by(phase: Phase, producer: dict) -> list[str]:
    return sorted(
        {
            producer[(r.kind, r.name)]
            for r in phase.missing
            if (r.kind, r.name) in producer
        }
        - {phase.key}
    )


def sync(directory: Any, plan: dict, token: str | None = None) -> dict:
    """Write the computed state back into system.yaml: `met` on a phase whose assets all exist, `stale` on one marked satisfied whose assets are gone, and `system.progress`'s `done` and `phase`.

    Only phases that record assets change; decisions keep the status they say.
    Needs the build lease when one is held.

    Args:
        directory: The system's directory.
        plan: What `compute` returned for its system.yaml.
        token: The lease's token; default `HOPS_LEASE_TOKEN`.

    Returns:
        The document written.

    Raises:
        LeaseHeld: another invocation holds the build lease.
    """
    token = token or os.environ.get(system_doc.TOKEN_ENV)
    held = system_doc.lease(directory)
    if held is not None and not system_doc.expired(held) and held.get("token") != token:
        raise system_doc.LeaseHeld(system_doc._held_message(directory, held))
    computed = {p["phase"]: p for p in plan["phases"]}

    def change(doc: dict) -> None:
        blocks = _blocks(doc)
        for key, p in computed.items():
            block = blocks.get(key)
            if not isinstance(block, dict) or not p["makes"]:
                continue
            if p["state"] == "done" and block.get("status") not in SATISFIED:
                block["status"] = "met"
            elif p["state"] in ("build", "blocked") and block.get(
                "status"
            ) in SATISFIED - {"skipped"}:
                block["status"] = "stale"
        if isinstance(doc.get("layer"), dict):
            return
        system = doc.setdefault("system", {})
        progress = system.setdefault("progress", {})
        progress["done"] = [
            PROGRESS_NAME.get(k, k)
            for k in phase_keys(doc)
            if k in computed and computed[k]["state"] in ("done", "skipped")
        ]
        if plan["next"]:
            progress["phase"] = PROGRESS_NAME.get(plan["next"], plan["next"])

    return system_doc.update(directory, change)
