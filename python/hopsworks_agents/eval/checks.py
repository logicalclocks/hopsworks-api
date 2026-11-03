#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""User-defined checks: a function the project wrote, graded like a built-in one.

A suite names a check of type ``python`` and where its code is; the code is
never in the spec. The file declares what it offers with the decorator::

    from hopsworks_agents.eval.checks import check, CheckContext, CheckResult

    @check(needs_trace=True, description="Quoted price equals the catalogue price")
    def price_matches_catalogue(ctx: CheckContext, tolerance: float = 0.01):
        quoted = ...
        return CheckResult(passed=ok, score=1.0 if ok else 0.0, reason=...)

and a suite's check row says ``{"type": "python", "name": "price_matches_catalogue",
"file": "Resources/agent_evals/checks/pricing.py", "params": {"tolerance": 0.005}}``.

Three rules the rest of the runner already lives by hold here too. The function
never raises into the run: an exception, a timeout or a file that will not load
is the harness failing, reported ungradable and kept out of every pass rate,
never a pass and never the agent's fault. No trace is not a failing score. And
what a check receives is the :class:`CheckContext` below and nothing else -- the
runner's own ``Task``, ``Trial`` and trace dict stay private and may change
shape, the context is the one thing promised to stay.

Where the code comes from is the job's business, not the check's. A
:class:`_CheckSources` built by the job resolves ``file`` (the project's HopsFS),
``git`` (a repository, or the evaluated deployment's own) and ``module`` (the
job's environment) into an imported module; without one, only ``module`` loads,
which is what keeps authoring-time validation free of any I/O.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import os
import posixpath
import re
import shutil
import sys
import tempfile
import threading
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public

from .evaluators import SKIPPED, _ungradable
from .models import EvaluatorResult, Task, Trial


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from types import ModuleType

    from .evaluators import Trace


log = logging.getLogger(__name__)

#: The check type a suite names.
TYPE = "python"
#: Seconds a check may take on one trial before its verdict is ungradable.
DEFAULT_TIMEOUT_S = 30.0
#: The spec may not ask for more than this; a check is not a job.
MAX_TIMEOUT_S = 600.0
#: A package directory of checks is copied whole into a published suite's
#: snapshot. Bounded, because a check is a few files and a repository is not.
MAX_SNAPSHOT_FILES = 200
MAX_SNAPSHOT_BYTES = 5 * 1024 * 1024
#: Directories never copied from a package of checks.
_SKIPPED_DIRS = {".git", "__pycache__", ".venv", "node_modules", ".mypy_cache"}

_IDENTIFIER = re.compile(r"^[A-Za-z_]\w*$")
_MODULE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)*$")
_COMMIT = re.compile(r"^[0-9a-fA-F]{7,40}$")
_REF = re.compile(r"^[\w./-]+$")
#: What a published suite's snapshot records about the code it froze.
MANIFEST = "manifest.json"
#: The attribute the decorator leaves on a function.
_MARK = "__hopsworks_check__"


# ── what a check sees ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class ToolCall:
    """One tool the agent called during the trial, as its span recorded it."""

    name: str = ""
    #: The arguments as the instrumentation wrote them, usually JSON text.
    arguments: str = ""
    #: What the tool returned, as text.
    result: str = ""
    #: The span's status code; ``failed`` reads it.
    status: str = ""
    duration_ms: float | None = None
    call_id: str = ""
    span_id: str = ""

    @property
    def failed(self) -> bool:
        return self.status.upper().endswith("ERROR")

    @property
    def arguments_json(self) -> Any:
        """The arguments parsed as JSON, or ``None`` when they are not JSON."""
        return _parse_json(self.arguments)

    @property
    def result_json(self) -> Any:
        """The result parsed as JSON, or ``None`` when it is not JSON."""
        return _parse_json(self.result)


@dataclass(frozen=True)
class TrialInfo:
    """What the runner knows about the trial being graded."""

    trial_id: str = ""
    run_id: str = ""
    trial_index: int = 0
    deployment_id: int | None = None
    status: str = ""
    latency_ms: float = 0.0
    trace_id: str = ""
    trace_status: str = ""
    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass(frozen=True)
class TaskInfo:
    """The task the trial ran, or the trace that stands in for one on live traffic."""

    task_id: str = ""
    version: int = 1
    task_type: str = "single_turn"
    category: str = ""
    tags: tuple[str, ...] = ()
    subject: str = ""
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CheckContext:
    """Everything a check may read, and the one shape promised to stay stable.

    Attributes:
        question: What the user asked, every turn of a conversation task numbered.
        messages: The task's input messages, parsed, as role/content dicts.
        transcript: The whole exchange as text for a multi-turn trial, else empty.
        answer: The agent's final reply.
        expectation: What the task expects of this check, by the check's name.
            Empty on live traffic, which carries no task.
        expectations: Every expectation on the task, by check name.
        tools: The tool calls of the trace, in order; empty without a trace.
        trace: The raw trace as the runner assembled it, or ``None``.
        trial: What the runner knows about the trial.
        task: What it knows about the task.
        params: The parameters the check was configured with, merged over its defaults.
        check_name: The name results are reported under.
    """

    question: str = ""
    messages: tuple[Mapping[str, Any], ...] = ()
    transcript: str = ""
    answer: str = ""
    expectation: str = ""
    expectations: Mapping[str, str] = field(default_factory=dict)
    tools: tuple[ToolCall, ...] = ()
    trace: Mapping[str, Any] | None = None
    trial: TrialInfo = field(default_factory=TrialInfo)
    task: TaskInfo = field(default_factory=TaskInfo)
    params: Mapping[str, Any] = field(default_factory=dict)
    check_name: str = ""

    @property
    def tool_names(self) -> list[str]:
        """The tools called, in order, repeats included."""
        return [tool.name for tool in self.tools]

    @property
    def has_trace(self) -> bool:
        return self.trace is not None

    @public
    def calls(self, tool: str) -> list[ToolCall]:
        """The calls of one tool, in order."""
        return [call for call in self.tools if call.name == tool]

    @public
    def answer_json(self) -> Any:
        """The answer parsed as JSON, or ``None`` when it is not JSON."""
        return _parse_json(self.answer)


@dataclass
class CheckResult:
    """What a check returns when a bare ``True`` or a score is not enough.

    ``passed`` decides the verdict; ``score`` is the measure (0 to 1) the
    threshold pass policy averages; ``reason`` is what a reader sees on the row
    and what failure analysis is told; ``assertions`` is anything else worth
    keeping beside the verdict -- the values compared, say.
    """

    passed: bool | None = None
    score: float | None = None
    reason: str = ""
    assertions: dict[str, Any] = field(default_factory=dict)
    ungradable: bool = False


@public
def ungradable(reason: str) -> CheckResult:
    """No verdict, with the reason: the trial says nothing about the agent either way.

    For a check whose precondition did not hold -- the tool it inspects was never
    called, the answer carries nothing to compare. Not a failure and not a pass.
    """
    return CheckResult(reason=reason, ungradable=True)


# ── declaring a check ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Declared:
    """What the decorator recorded about one function."""

    fn: Callable[..., Any]
    name: str
    needs_trace: bool = True
    reads_expectation: bool = False
    timeout_s: float = DEFAULT_TIMEOUT_S
    description: str = ""
    #: The keyword parameters and their defaults, read off the signature, so a
    #: spec with no ``params`` is valid and a picker can prefill a form.
    defaults: Mapping[str, Any] = field(default_factory=dict)

    def _as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "needs_trace": self.needs_trace,
            "reads_expectation": self.reads_expectation,
            "timeout_s": self.timeout_s,
            "description": self.description,
            "defaults": {k: _json_safe(v) for k, v in self.defaults.items()},
        }


@public
def check(
    name: str | Callable[..., Any] | None = None,
    *,
    needs_trace: bool = True,
    reads_expectation: bool = False,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    description: str = "",
) -> Any:
    """Declare a function as a check a suite can name.

    Used bare, with a name, or with settings::

        @check
        def answers_in_english(ctx): ...

        @check("refund_amounts_agree", needs_trace=True)
        def _refund(ctx, tolerance: float = 0.01): ...

    The function takes a :class:`CheckContext` and the keyword parameters it
    declares, and returns ``True``/``False``, a score from 0 to 1, a
    :class:`CheckResult`, or :func:`ungradable`. A suite names it by ``name``,
    which defaults to the function's own, and the settings here are the defaults
    the suite's row may override.

    Parameters:
        name: The name a suite uses; the function's name when omitted.
        needs_trace: Whether the check reads the trajectory. True reports it
            ungradable when no trace arrived, rather than grading half a trial.
        reads_expectation: Whether it reads the task's expectation. True keeps it
            out of monitors, which grade live traffic that has none.
        timeout_s: How long one trial may take before the verdict is ungradable.
        description: Shown in the library and the picker.
    """

    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        declared = _Declared(
            fn=fn,
            name=str(explicit or fn.__name__),
            needs_trace=bool(needs_trace),
            reads_expectation=bool(reads_expectation),
            timeout_s=_bounded_timeout(timeout_s),
            description=str(description or inspect.getdoc(fn) or "").strip(),
            defaults=_defaults_of(fn),
        )
        setattr(fn, _MARK, declared)
        return fn

    if callable(name):
        explicit = None
        return decorate(name)
    explicit = name
    return decorate


def _defaults_of(fn: Callable[..., Any]) -> dict[str, Any]:
    """The parameters after the context that carry a default."""
    try:
        parameters = list(inspect.signature(fn).parameters.values())[1:]
    except (TypeError, ValueError):
        return {}
    return {
        p.name: p.default
        for p in parameters
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        and p.default is not inspect.Parameter.empty
    }


def _bounded_timeout(value: Any) -> float:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT_S
    if seconds <= 0:
        return DEFAULT_TIMEOUT_S
    return min(seconds, MAX_TIMEOUT_S)


def _declared_in(module: ModuleType) -> dict[str, _Declared]:
    """Every check the module declares, by name."""
    found: dict[str, _Declared] = {}
    for value in vars(module).values():
        declared = getattr(value, _MARK, None)
        if isinstance(declared, _Declared) and declared.fn is value:
            found[declared.name] = declared
    return found


def _declared_of(fn: Callable[..., Any]) -> _Declared:
    """The declaration on a function, or one made up for a plain function."""
    declared = getattr(fn, _MARK, None)
    if isinstance(declared, _Declared):
        return declared
    return _Declared(
        fn=fn, name=getattr(fn, "__name__", "check"), defaults=_defaults_of(fn)
    )


def _resolve_in(
    module: ModuleType, name: str, *, check_name: str = "", function: str = ""
) -> _Declared:
    """The check a row asks for: the declared one by name, else a plain function.

    Raises ``LookupError`` with the names the module does declare, so the
    reason on the row says what was there to pick from.
    """
    declared = _declared_in(module)
    wanted = check_name or name
    if function:
        candidate = getattr(module, function, None)
        if not callable(candidate):
            raise LookupError(
                f"{module.__name__} has no function {function!r}"
                + _declared_hint(declared)
            )
        return _declared_of(candidate)
    if wanted in declared:
        return declared[wanted]
    candidate = getattr(module, wanted, None)
    if callable(candidate) and not inspect.isclass(candidate):
        return _declared_of(candidate)
    raise LookupError(f"no check named {wanted!r}" + _declared_hint(declared))


def _declared_hint(declared: Mapping[str, _Declared]) -> str:
    if not declared:
        return "; the module declares no @check"
    return "; it declares " + ", ".join(sorted(declared))


# ── the spec ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Source:
    """Where a check's code is, as the spec says it."""

    file: str = ""
    module: str = ""
    git_url: str = ""
    git_ref: str = ""
    git_commit: str = ""
    #: The evaluated deployment's own repository.
    deployment: bool = False
    #: Within a repository: the file or package, relative to its root.
    path: str = ""

    @property
    def kind(self) -> str:
        if self.file:
            return "file"
        if self.module:
            return "module"
        if self.deployment:
            return "deployment"
        return "git"

    def _describe(self) -> str:
        if self.file:
            return self.file
        if self.module:
            return f"module {self.module}"
        if self.deployment:
            return f"the deployment's repository, {self.path}"
        return (
            f"{_scrubbed(self.git_url)}@{self.git_commit or self.git_ref or 'HEAD'}"
            f":{self.path}"
        )


def _parse_source(entry: Mapping[str, Any]) -> _Source:
    """The source a row names, refusing one the job could not resolve.

    Raises ``ValueError`` with the reason; the spec builder turns it into a
    ``SpecError`` so authoring refuses it with the entry number.
    """
    file = str(entry.get("file") or "").strip()
    module = str(entry.get("module") or "").strip()
    git = entry.get("git")
    given = [k for k, v in (("file", file), ("module", module), ("git", git)) if v]
    if len(given) != 1:
        raise ValueError(
            "a python check names exactly one of file, module and git"
            + (f"; got {', '.join(given)}" if given else "")
        )
    if file:
        if ".." in Path(file).parts:
            raise ValueError("file must not contain '..'")
        return _Source(file=file)
    if module:
        if not _MODULE.match(module):
            raise ValueError(f"module {module!r} is not a dotted module name")
        return _Source(module=module)

    path = str(entry.get("path") or "").strip().strip("/")
    if not path:
        raise ValueError("a git source needs a path within the repository")
    if ".." in Path(path).parts:
        raise ValueError("path must not contain '..'")
    if isinstance(git, str):
        if git.strip().lower() != "deployment":
            raise ValueError(
                "git is an object {url, ref, commit} or the string 'deployment'"
            )
        return _Source(deployment=True, path=path)
    if not isinstance(git, Mapping):
        raise ValueError(
            "git is an object {url, ref, commit} or the string 'deployment'"
        )
    url = str(git.get("url") or "").strip()
    ref = str(git.get("ref") or git.get("branch") or "").strip()
    commit = str(git.get("commit") or "").strip()
    if not url.startswith("https://"):
        raise ValueError("a git url must start with https://")
    if ref and not _REF.match(ref):
        raise ValueError(f"git ref {ref!r} is not a branch or tag name")
    if commit and not _COMMIT.match(commit):
        raise ValueError(f"git commit {commit!r} is not a commit hash")
    return _Source(git_url=url, git_ref=ref, git_commit=commit, path=path)


def _validate_entry(entry: Mapping[str, Any]) -> None:
    """Refuse a python row the job could not run, without touching any source."""
    _parse_source(entry)
    for key in ("check", "function"):
        value = entry.get(key)
        if value and not _IDENTIFIER.match(str(value)):
            raise ValueError(f"{key} {value!r} is not an identifier")
    if entry.get("check") and entry.get("function"):
        raise ValueError("a python check names check or function, not both")
    params = entry.get("params")
    if params is not None and not isinstance(params, Mapping):
        raise ValueError("params is an object")
    timeout = entry.get("timeout_s")
    if timeout is not None:
        try:
            seconds = float(timeout)
        except (TypeError, ValueError) as err:
            raise ValueError("timeout_s is a number of seconds") from err
        if seconds <= 0 or seconds > MAX_TIMEOUT_S:
            raise ValueError(f"timeout_s is between 0 and {int(MAX_TIMEOUT_S)}")


# ── resolving the code ──────────────────────────────────────────────────────


@dataclass
class _Loaded:
    """A module imported from a source, and where it really came from."""

    module: ModuleType
    origin: str = ""
    sha256: str = ""
    commit: str = ""
    #: True when read from a published suite's snapshot.
    pinned: bool = False
    #: The local file or directory the module was imported from.
    local: str = ""


class _CheckSources:
    """Turns a :class:`_Source` into an imported module, for one job execution.

    Built by the job, which is the only place the pieces exist: a dataset API
    for the project's files, the serving view of the deployment under test,
    and the published suite's snapshot directory when there is one. Checkouts
    and downloads are shared across the suites of one execution, so a file two
    suites name is fetched once.

    Parameters:
        dataset_api: The project's dataset API, for ``file`` sources and snapshots.
        project_name: Makes a relative ``file`` absolute: ``/Projects/<name>/...``.
        serving_view: Returns the deployment's serving row, for ``"deployment"``.
        snapshot_dir: The published suite's ``checks/`` directory in HopsFS, or
            ``None`` for a draft, a monitor, or a job that must not pin.
        workdir: Where checkouts land; a temporary directory, removed by ``close``.
    """

    def __init__(
        self,
        *,
        dataset_api: Any = None,
        project_name: str = "",
        serving_view: Callable[[], Mapping[str, Any]] | None = None,
        snapshot_dir: str | None = None,
        workdir: str | None = None,
        clone: Callable[..., str] | None = None,
    ):
        self._dataset_api = dataset_api
        self._project_name = project_name
        self._serving_view = serving_view
        self._snapshot_dir = snapshot_dir.rstrip("/") if snapshot_dir else None
        self._workdir = workdir or tempfile.mkdtemp(prefix="agent-checks-")
        self._own_workdir = workdir is None
        self._clone = clone
        self._checkouts: dict[tuple[str, str, str], str] = {}
        self._loaded: dict[str, _Loaded] = {}

    def _close(self) -> None:
        if self._own_workdir:
            shutil.rmtree(self._workdir, ignore_errors=True)

    def _load(self, source: _Source) -> _Loaded:
        """The module for a source, imported once per execution."""
        key = source._describe()
        cached = self._loaded.get(key)
        if cached is not None:
            return cached
        loaded = self._fetch(source)
        self._loaded[key] = loaded
        return loaded

    def _fetch(self, source: _Source) -> _Loaded:
        if source.module:
            module = importlib.import_module(source.module)
            path = getattr(module, "__file__", "") or ""
            return _Loaded(
                module=module,
                origin=f"module {source.module} ({path or 'built in'})",
                sha256=_digest_of(path) if path and os.path.isfile(path) else "",
                local=path,
            )

        snapshot = self._snapshot_of(source)
        if snapshot is not None:
            local, manifest = snapshot
            return _Loaded(
                module=_import_path(local),
                origin=f"snapshot {self._snapshot_dir}",
                sha256=_digest_of(local),
                commit=str(manifest.get("commit") or ""),
                pinned=True,
                local=local,
            )

        if source.file:
            local = self._download(self._absolute(source.file))
            return _Loaded(
                module=_import_path(local),
                origin=f"hopsfs {self._absolute(source.file)}",
                sha256=_digest_of(local),
                local=local,
            )

        url, ref, commit = self._git_target(source)
        root = self._checkout(url, ref, commit)
        local = os.path.join(root, source.path)
        if not os.path.exists(local):
            raise FileNotFoundError(
                f"{source.path} is not in {_scrubbed(url)}@{commit or ref or 'HEAD'}"
            )
        resolved = commit or _head_of(root)
        loaded = _Loaded(
            module=_import_path(local, package_root=root),
            origin=f"git {_scrubbed(url)}@{resolved or ref or 'HEAD'}:{source.path}",
            sha256=_digest_of(local),
            commit=resolved,
            local=local,
        )
        if not source.deployment:
            # The deployment's own repository is deliberately never pinned: its
            # checks move with the agent, which is the point of naming it.
            self._snapshot(source, loaded, url=url, ref=ref)
        return loaded

    # -- file ---------------------------------------------------------------

    def _absolute(self, file: str) -> str:
        if file.startswith("/"):
            return file
        if not self._project_name:
            raise RuntimeError("no project to resolve a relative file path against")
        return f"/Projects/{self._project_name}/{file.strip('/')}"

    def _download(self, path: str) -> str:
        """A HopsFS file or directory, copied under the work directory."""
        if self._dataset_api is None:
            raise RuntimeError("no dataset API to read project files with")
        if os.path.isdir("/hopsfs") and os.path.exists(f"/hopsfs{path}"):
            # Mounted: read in place. Cheaper than a download and the same bytes.
            return f"/hopsfs{path}"
        target = os.path.join(self._workdir, "files", uuid.uuid4().hex)
        os.makedirs(target, exist_ok=True)
        is_dir, _ = self._stat(path)
        if not is_dir:
            local = os.path.join(target, posixpath.basename(path))
            self._dataset_api.download(path, local, overwrite=True)
            return local
        local = os.path.join(target, posixpath.basename(path.rstrip("/")))
        self._download_tree(path, local)
        return local

    def _stat(self, path: str) -> tuple[bool, int]:
        """Whether a HopsFS path is a directory, and its size."""
        try:
            row = self._dataset_api._get(path.lstrip("/"))  # noqa: SLF001
        except Exception as err:  # noqa: BLE001 -- said as a reason, not a traceback
            raise FileNotFoundError(f"{path} is not readable: {_short(err)}") from err
        attributes = (row or {}).get("attributes") or row or {}
        return bool(attributes.get("dir")), int(attributes.get("size") or 0)

    def _download_tree(self, path: str, local: str) -> None:
        root = path.rstrip("/")
        os.makedirs(local, exist_ok=True)
        pending = [root]
        copied = 0
        total = 0
        while pending:
            current = pending.pop()
            for child in self._dataset_api.list(current.lstrip("/"), limit=1000) or []:
                child_path = child if str(child).startswith("/") else f"/{child}"
                if not child_path.startswith("/Projects/"):
                    child_path = f"/Projects/{self._project_name}{child_path}"
                name = posixpath.basename(child_path)
                is_dir, size = self._stat(child_path)
                if is_dir:
                    if name not in _SKIPPED_DIRS:
                        pending.append(child_path)
                    continue
                copied += 1
                total += size
                if copied > MAX_SNAPSHOT_FILES or total > MAX_SNAPSHOT_BYTES:
                    raise RuntimeError(
                        f"{root} holds more than {MAX_SNAPSHOT_FILES} files or "
                        f"{MAX_SNAPSHOT_BYTES // (1024 * 1024)} MB; a package of "
                        "checks is smaller than that"
                    )
                relative = child_path[len(root) :].lstrip("/")
                destination = os.path.join(local, relative)
                os.makedirs(os.path.dirname(destination), exist_ok=True)
                self._dataset_api.download(child_path, destination, overwrite=True)

    # -- git ----------------------------------------------------------------

    def _git_target(self, source: _Source) -> tuple[str, str, str]:
        if not source.deployment:
            return source.git_url, source.git_ref, source.git_commit
        if self._serving_view is None:
            raise RuntimeError("no deployment to read a repository from")
        view = dict(self._serving_view() or {})
        url = str(view.get("gitUrl") or "").strip()
        if not url:
            raise RuntimeError(
                f"deployment {view.get('name') or '?'} is not git-backed, so it "
                "has no repository to read checks from"
            )
        return (
            url,
            str(view.get("gitResolvedBranch") or view.get("gitBranch") or "").strip(),
            str(view.get("gitCurrentCommit") or "").strip(),
        )

    def _checkout(self, url: str, ref: str, commit: str) -> str:
        key = (url, ref, commit)
        root = self._checkouts.get(key)
        if root is not None:
            return root
        into = os.path.join(self._workdir, "git", uuid.uuid4().hex)
        os.makedirs(into, exist_ok=True)
        clone = self._clone
        if clone is None:
            from .agent_source import _clone_repository

            clone = _clone_repository
        try:
            root = clone(url, ref, commit, into)
        except Exception as err:  # noqa: BLE001 -- the reason goes on the row
            from .agent_source import _reason

            raise RuntimeError(
                f"could not clone {_scrubbed(url)}: {_reason(err)}"
            ) from None
        self._checkouts[key] = root
        return root

    # -- snapshots ----------------------------------------------------------

    def _snapshot_of(self, source: _Source) -> tuple[str, dict[str, Any]] | None:
        """The published copy of this source, when the suite has one."""
        if self._snapshot_dir is None or self._dataset_api is None:
            return None
        name = _snapshot_name(source)
        try:
            is_dir, _ = self._stat(f"{self._snapshot_dir}/{name}")
        except FileNotFoundError:
            return None
        local = self._download(f"{self._snapshot_dir}/{name}")
        manifest: dict[str, Any] = {}
        try:
            manifest_local = self._download(f"{self._snapshot_dir}/{MANIFEST}")
            with open(manifest_local, encoding="utf-8") as handle:
                manifests = json.load(handle)
            manifest = dict((manifests or {}).get(name) or {})
        except Exception:  # noqa: BLE001 -- the code is what matters
            log.debug("no readable manifest beside %s", name)
        return local, manifest

    def _snapshot(
        self, source: _Source, loaded: _Loaded, *, url: str, ref: str
    ) -> None:
        """Freeze a git source beside the published suite's tasks, once."""
        if self._snapshot_dir is None or self._dataset_api is None:
            return
        name = _snapshot_name(source)
        try:
            self._dataset_api.mkdir(self._snapshot_dir)
        except Exception:  # noqa: BLE001 -- exists already, most often
            log.debug("snapshot directory %s not created", self._snapshot_dir)
        try:
            self._dataset_api.upload(loaded.local, self._snapshot_dir, overwrite=True)
            manifests: dict[str, Any] = {}
            try:
                existing = self._download(f"{self._snapshot_dir}/{MANIFEST}")
                with open(existing, encoding="utf-8") as handle:
                    manifests = dict(json.load(handle) or {})
            except Exception:  # noqa: BLE001 -- first snapshot of the suite
                manifests = {}
            manifests[name] = {
                "url": _scrubbed(url),
                "ref": ref,
                "commit": loaded.commit,
                "sha256": loaded.sha256,
                "path": source.path,
                "pinned_at": datetime.now(tz=timezone.utc).isoformat(),
            }
            manifest_local = os.path.join(self._workdir, MANIFEST)
            with open(manifest_local, "w", encoding="utf-8") as handle:
                json.dump(manifests, handle, indent=2, sort_keys=True)
            self._dataset_api.upload(manifest_local, self._snapshot_dir, overwrite=True)
            loaded.pinned = True
            loaded.origin = f"{loaded.origin} (pinned)"
            log.info("pinned %s at %s into %s", name, loaded.commit, self._snapshot_dir)
        except Exception:  # noqa: BLE001 -- a run that cannot pin still grades
            log.warning(
                "could not snapshot %s into %s; this run grades with the live "
                "code and the next one pins",
                name,
                self._snapshot_dir,
                exc_info=True,
            )


def _snapshot_name(source: _Source) -> str:
    """The name a source is frozen under: its own basename."""
    return posixpath.basename((source.path or source.file).rstrip("/"))


def _import_path(local: str, *, package_root: str | None = None) -> ModuleType:
    """Import a file or a package directory under a name nothing else uses.

    Two suites in one execution may name files called ``checks.py`` in different
    places; a fixed module name would hand the second the first's code. The
    parent directory (or the repository root) goes on ``sys.path`` so the file's
    own imports resolve, which is what lets a check share helpers with the agent.
    """
    local = os.path.abspath(local)
    parent = package_root or os.path.dirname(local)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    unique = f"hopsworks_check_{hashlib.sha1(local.encode()).hexdigest()[:12]}"  # noqa: S324
    if os.path.isdir(local):
        init = os.path.join(local, "__init__.py")
        if not os.path.isfile(init):
            raise ImportError(f"{local} is a directory without __init__.py")
        spec = importlib.util.spec_from_file_location(
            unique, init, submodule_search_locations=[local]
        )
    else:
        if not local.endswith(".py"):
            raise ImportError(f"{local} is not a Python file")
        spec = importlib.util.spec_from_file_location(unique, local)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {local}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[unique] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(unique, None)
        raise
    return module


def _digest_of(local: str) -> str:
    """sha256 of a file, or of a directory's Python files in path order."""
    digest = hashlib.sha256()
    path = Path(local)
    if path.is_dir():
        for file in sorted(path.rglob("*.py")):
            if any(part in _SKIPPED_DIRS for part in file.relative_to(path).parts):
                continue
            digest.update(str(file.relative_to(path)).encode())
            digest.update(file.read_bytes())
    elif path.is_file():
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _head_of(root: str) -> str:
    """The commit a checkout is at, without asking git to be installed."""
    try:
        head = Path(root, ".git", "HEAD").read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    if head.startswith("ref: "):
        try:
            return Path(root, ".git", head[5:]).read_text(encoding="utf-8").strip()
        except OSError:
            return ""
    return head


def _scrubbed(url: str) -> str:
    from .agent_source import _without_credentials

    return _without_credentials(url)


# ── the evaluator ───────────────────────────────────────────────────────────


class PythonCheckEvaluator:
    """A check the project wrote, run like a built-in one.

    Loads lazily on the first trial, so building a suite's evaluators (which
    authoring does to validate a spec) never imports user code. A load that
    fails -- unreadable file, bad import, no such check -- is reported on every
    trial as skipped, which the runner turns into EVALUATOR_ERROR: the harness
    failing, visibly, rather than a suite that quietly graded without one of
    its checks.
    """

    type = TYPE

    def __init__(
        self,
        name: str,
        source: _Source,
        *,
        check_name: str = "",
        function: str = "",
        params: Mapping[str, Any] | None = None,
        needs_trace: bool | None = None,
        reads_expectation: bool | None = None,
        timeout_s: float | None = None,
        sources: _CheckSources | None = None,
    ):
        self.name = name
        self.source = source
        self.check_name = check_name
        self.function = function
        self.params = dict(params or {})
        self._needs_trace = needs_trace
        self._reads_expectation = reads_expectation
        self._timeout_s = _bounded_timeout(timeout_s) if timeout_s else None
        self._sources = sources
        self._declared: _Declared | None = None
        self._loaded: _Loaded | None = None
        self._load_error: str | None = None
        # The Protocol's attribute. Refined once the declaration is read.
        self.needs_trace = True if needs_trace is None else bool(needs_trace)

    @property
    def reads_expectation(self) -> bool:
        if self._reads_expectation is not None:
            return self._reads_expectation
        return bool(self._declared and self._declared.reads_expectation)

    @property
    def timeout_s(self) -> float:
        if self._timeout_s:
            return self._timeout_s
        return self._declared.timeout_s if self._declared else DEFAULT_TIMEOUT_S

    def _ensure_loaded(self) -> str | None:
        """Load once; the reason it could not, or ``None``."""
        if self._declared is not None or self._load_error is not None:
            return self._load_error
        try:
            if self._sources is None:
                if not self.source.module:
                    raise RuntimeError(
                        "this run has no way to read project files or repositories"
                    )
                sources = _CheckSources()
            else:
                sources = self._sources
            self._loaded = sources._load(self.source)
            self._declared = _resolve_in(
                self._loaded.module,
                self.name,
                check_name=self.check_name,
                function=self.function,
            )
            if self._needs_trace is None:
                self.needs_trace = self._declared.needs_trace
            log.info("check %r loaded from %s", self.name, self._loaded.origin)
        except Exception as err:  # noqa: BLE001 -- the reason goes on every row
            self._load_error = (
                f"check {self.name!r} could not be loaded from "
                f"{self.source._describe()}: {_short(err)}"
            )
            log.warning("%s", self._load_error)
        return self._load_error

    def _grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        failure = self._ensure_loaded()
        if failure or self._declared is None:
            result = _ungradable(self.name, self.type, failure or "not loaded")
            result.assertions = {SKIPPED: True, "reason": failure}
            return result
        if self.needs_trace and trace is None:
            return _ungradable(self.name, self.type, "no trace arrived for this trial")
        params = {**dict(self._declared.defaults), **self.params}
        context = _context_for(self.name, task, trial, trace, params)
        try:
            value = _call_with_timeout(
                self._declared.fn, context, params, self.timeout_s
            )
        except _Timeout:
            return _ungradable(
                self.name,
                self.type,
                f"timed out after {self.timeout_s:g}s",
            )
        except Exception as err:  # noqa: BLE001 -- a bad check is not a bad agent
            return _ungradable(self.name, self.type, f"check raised: {_short(err)}")
        result = _as_result(self.name, value, pass_score=params.get("pass_score"))
        result.assertions.setdefault("source", self._provenance())
        return result

    def _provenance(self) -> dict[str, Any]:
        loaded = self._loaded
        if loaded is None:
            return {}
        return {
            "origin": loaded.origin,
            "sha256": loaded.sha256,
            **({"commit": loaded.commit} if loaded.commit else {}),
            "pinned": loaded.pinned,
        }


class _Timeout(Exception):
    pass


def _call_with_timeout(
    fn: Callable[..., Any],
    context: CheckContext,
    params: Mapping[str, Any],
    timeout_s: float,
) -> Any:
    """The function's return value, or ``_Timeout``.

    A thread per call: the function is the user's and may block on anything. A
    thread cannot be killed, so a check that hangs costs one leaked daemon
    thread and an ungradable row, and the run moves on.
    """
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["value"] = fn(context, **params)
        except BaseException as err:  # noqa: BLE001 -- carried back to the caller
            box["error"] = err

    worker = threading.Thread(target=run, name="agent-check", daemon=True)
    worker.start()
    worker.join(timeout_s)
    if worker.is_alive():
        raise _Timeout
    if "error" in box:
        raise box["error"]
    return box.get("value")


def _as_result(name: str, value: Any, *, pass_score: Any = None) -> EvaluatorResult:
    """Whatever the function returned, as the row the runner records."""
    if isinstance(value, CheckResult):
        if value.ungradable:
            result = _ungradable(name, TYPE, value.reason or "ungradable")
            result.assertions = dict(value.assertions)
            return result
        score = value.score
        passed = value.passed
        if passed is None and score is None:
            return _ungradable(name, TYPE, value.reason or "no verdict returned")
        if passed is None:
            passed = float(score) >= _pass_score(pass_score)
        if score is None:
            score = 1.0 if passed else 0.0
        return EvaluatorResult(
            name,
            TYPE,
            _clamp(score),
            bool(passed),
            value.reason,
            dict(value.assertions),
        )
    if isinstance(value, bool):
        return EvaluatorResult(name, TYPE, 1.0 if value else 0.0, value, "")
    if isinstance(value, (int, float)):
        score = _clamp(float(value))
        return EvaluatorResult(
            name, TYPE, score, score >= _pass_score(pass_score), f"scored {score:g}"
        )
    if isinstance(value, Mapping):
        return _as_result(
            name, CheckResult(**_result_fields(value)), pass_score=pass_score
        )
    if value is None:
        return _ungradable(name, TYPE, "the check returned nothing")
    return _ungradable(
        name, TYPE, f"the check returned {type(value).__name__}, not a verdict"
    )


def _result_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {"passed", "score", "reason", "assertions", "ungradable"}
    fields_ = {k: v for k, v in value.items() if k in allowed}
    if "assertions" in fields_ and not isinstance(fields_["assertions"], Mapping):
        fields_["assertions"] = {"value": fields_["assertions"]}
    else:
        fields_["assertions"] = dict(fields_.get("assertions") or {})
    return fields_


def _pass_score(value: Any) -> float:
    try:
        return float(value) if value is not None else 1.0
    except (TypeError, ValueError):
        return 1.0


def _clamp(score: float) -> float:
    return max(0.0, min(1.0, float(score)))


# ── the context ─────────────────────────────────────────────────────────────


def _context_for(
    name: str,
    task: Task,
    trial: Trial,
    trace: Trace | None,
    params: Mapping[str, Any],
) -> CheckContext:
    """The runner's objects, as the one shape a check is shown."""
    return CheckContext(
        question=task.asked,
        messages=tuple(_messages_of(task.input_messages)),
        transcript=trial.transcript or "",
        answer=trial.final_output or "",
        expectation=task._expects_text(name),
        expectations=dict(task.expectations),
        tools=tuple(_tool_calls_of(trace)),
        trace=dict(trace) if trace is not None else None,
        trial=TrialInfo(
            trial_id=trial.trial_id,
            run_id=trial.run_id,
            trial_index=trial.trial_index,
            deployment_id=trial.deployment_id,
            status=getattr(trial.status, "value", str(trial.status)),
            latency_ms=float(trial.latency_ms or 0.0),
            trace_id=trial.trace_id or "",
            trace_status=getattr(trial.trace_status, "value", str(trial.trace_status)),
            input_tokens=trial.input_tokens,
            output_tokens=trial.output_tokens,
        ),
        task=TaskInfo(
            task_id=task.task_id,
            version=task.task_version,
            task_type=task.task_type,
            category=task.category or "",
            tags=tuple(task.tags or ()),
            subject=task.subject or "",
            context=dict(task.context or {}),
        ),
        params=dict(params),
        check_name=name,
    )


def _messages_of(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [m for m in raw if isinstance(m, dict)]
    try:
        parsed = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        return [{"role": "user", "content": str(raw)}] if raw else []
    if isinstance(parsed, list):
        return [m for m in parsed if isinstance(m, dict)]
    return []


def _tool_calls_of(trace: Mapping[str, Any] | None) -> list[ToolCall]:
    if not trace:
        return []
    calls = trace.get("tool_calls")
    if not isinstance(calls, list):
        return []
    return [
        ToolCall(
            name=str(call.get("name") or ""),
            arguments=str(call.get("arguments") or ""),
            result=str(call.get("result") or ""),
            status=str(call.get("status") or ""),
            duration_ms=call.get("duration_ms"),
            call_id=str(call.get("call_id") or ""),
            span_id=str(call.get("span_id") or ""),
        )
        for call in calls
        if isinstance(call, Mapping)
    ]


# ── trying a check before a run does ────────────────────────────────────────


@public
def run_check(
    fn: Callable[..., Any],
    *,
    answer: str = "",
    question: str = "",
    messages: Sequence[Mapping[str, Any]] | None = None,
    transcript: str = "",
    expectation: str = "",
    expectations: Mapping[str, str] | None = None,
    tools: Sequence[Mapping[str, Any] | ToolCall] | None = None,
    trace: Any = None,
    subject: str = "",
    name: str | None = None,
    timeout_s: float | None = None,
    **params: Any,
) -> EvaluatorResult:
    """Run a check the way the evaluation job would, on inputs you choose.

    No cluster is needed: give it an answer and the tools you want it to see,
    or a whole trace read with ``agent.trace(trace_id)``, and it returns the
    exact row the job would record -- verdict, score, reason, assertions --
    including an ungradable row when the check raised or timed out.

    Example:
        ```python
        from hopsworks_agents.eval.checks import run_check

        run_check(price_matches_catalogue, answer="It costs 19.99",
                  tools=[{"name": "lookup_product", "result": '{"price": 19.99}'}],
                  tolerance=0.01)

        trace = agent.trace("4bf1...")
        run_check(price_matches_catalogue, trace=trace)
        ```

    Parameters:
        fn: The check, decorated or plain.
        answer: The agent's final reply; read off the trace when omitted.
        question: What the user asked; read off the trace when omitted.
        messages: The task's input messages; one user turn is made from ``question`` when omitted.
        transcript: The whole exchange, for a conversation.
        expectation: What the task expects of this check.
        expectations: Every expectation on the task, by check name.
        tools: Tool calls as dicts (``name``, ``arguments``, ``result``, ``status``) or ``ToolCall``.
        trace: A trace from ``agent.trace()``, the job's trace dict, or ``None``.
        subject: Who the task is asked as.
        name: The name to report under; the check's own when omitted.
        timeout_s: Overrides the check's own timeout.
        **params: The check's parameters, merged over its defaults.

    Returns:
        `EvaluatorResult`: the row the job would record.
    """
    declared = _declared_of(fn)
    check_name = name or declared.name
    trace_dict = _as_runner_trace(trace)
    if trace_dict is not None and (not answer or not question):
        asked, replied = _question_and_answer_of(trace)
        question = question or asked
        answer = answer or replied
    if tools is not None:
        trace_dict = dict(trace_dict or {})
        trace_dict["tool_calls"] = [
            _tool_dict(tool) for tool in tools if tool is not None
        ]
        trace_dict["tool_names"] = [c["name"] for c in trace_dict["tool_calls"]]
        trace_dict["tool_error_count"] = sum(
            1 for c in trace_dict["tool_calls"] if str(c["status"]).endswith("ERROR")
        )
    all_expectations = dict(expectations or {})
    if expectation:
        all_expectations[check_name] = expectation
    task = Task(
        task_id="local",
        input_messages=json.dumps(
            list(messages) if messages else [{"role": "user", "content": question}]
        ),
        task_type="multi_turn" if transcript else "single_turn",
        expectations=all_expectations,
        subject=subject,
    )
    trial = Trial(
        trial_id="local",
        run_id="local",
        task_id="local",
        task_version=1,
        trial_index=0,
        deployment_id=0,
        trace_id=str((trace_dict or {}).get("trace_id") or ""),
        transcript=transcript,
        final_output=answer,
    )
    merged = {**dict(declared.defaults), **params}
    if declared.needs_trace and trace_dict is None and tools is None:
        return _ungradable(check_name, TYPE, "no trace arrived for this trial")
    context = _context_for(check_name, task, trial, trace_dict, merged)
    try:
        value = _call_with_timeout(
            declared.fn,
            context,
            merged,
            _bounded_timeout(timeout_s or declared.timeout_s),
        )
    except _Timeout:
        return _ungradable(check_name, TYPE, "timed out")
    except Exception as err:  # noqa: BLE001 -- the same rule as in the job
        return _ungradable(check_name, TYPE, f"check raised: {_short(err)}")
    return _as_result(check_name, value, pass_score=merged.get("pass_score"))


def _tool_dict(tool: Mapping[str, Any] | ToolCall) -> dict[str, Any]:
    if isinstance(tool, ToolCall):
        return {
            "name": tool.name,
            "arguments": tool.arguments,
            "result": tool.result,
            "status": tool.status,
            "duration_ms": tool.duration_ms,
            "call_id": tool.call_id,
            "span_id": tool.span_id,
        }
    return {
        "name": str(tool.get("name") or ""),
        "arguments": _text(tool.get("arguments")),
        "result": _text(tool.get("result")),
        "status": str(tool.get("status") or "OK"),
        "duration_ms": tool.get("duration_ms"),
        "call_id": str(tool.get("call_id") or ""),
        "span_id": str(tool.get("span_id") or ""),
    }


def _as_runner_trace(trace: Any) -> dict[str, Any] | None:
    """A trace in any of the shapes a caller has, as the job's dict."""
    if trace is None:
        return None
    if isinstance(trace, Mapping) and "tool_calls" in trace:
        return dict(trace)
    raw = getattr(trace, "raw", None)
    if isinstance(raw, Mapping) and raw.get("spans"):
        from .client import _assemble_trace

        return _assemble_trace(str(getattr(trace, "trace_id", "") or ""), dict(raw))
    if isinstance(trace, Mapping) and trace.get("spans"):
        from .client import _assemble_trace

        first = (trace.get("spans") or [{}])[0]
        return _assemble_trace(str(first.get("traceId") or ""), dict(trace))
    raise TypeError(
        "trace is what agent.trace() returns, the job's trace dict, or None"
    )


def _question_and_answer_of(trace: Any) -> tuple[str, str]:
    from .sample_job import _question_and_answer

    raw = getattr(trace, "raw", None)
    detail = raw if isinstance(raw, Mapping) else trace
    if not isinstance(detail, Mapping):
        return "", ""
    try:
        return _question_and_answer(dict(detail))
    except Exception:  # noqa: BLE001 -- a trace with no messages
        return "", ""


# ── small helpers ───────────────────────────────────────────────────────────


def _parse_json(text: str) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return str(value)


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)
    return value


def _short(err: BaseException) -> str:
    text = f"{type(err).__name__}: {err}" if str(err) else type(err).__name__
    return text if len(text) <= 500 else text[:497] + "..."
