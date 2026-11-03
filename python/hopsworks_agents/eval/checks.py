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

and a suite's row says ``{"type": "python", "name": "pricing",
"file": "Resources/agent_evals/checks/pricing.py"}``: every check the file
declares runs, each reported under its own name, the way a test file is
pointed at and every test in it runs. ``only`` / ``exclude`` narrow that, and
``params`` is keyed by check: ``{"price_matches_catalogue": {"tolerance": 0.005}}``.

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

import contextlib
import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import math
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
from .models import EvaluatorResult, Task, TraceStatus, Trial, TrialStatus


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
#: A git source is frozen with the repository tree around it, so the helpers a
#: check imports from elsewhere in the repository are frozen with it. Bounded
#: too: past this a repository is not pinned and the run says so.
MAX_TREE_FILES = 2000
MAX_TREE_BYTES = 20 * 1024 * 1024
MAX_TREE_FILE_BYTES = 2 * 1024 * 1024
#: The directory inside a snapshot that holds the frozen tree.
TREE = "tree"
#: How long fetching a source (a clone, a download) may take. Separate from the
#: check's own timeout, which bounds the check's code -- its import included.
FETCH_BUDGET_S = 300.0
#: Directories never copied from a package of checks.
_SKIPPED_DIRS = {".git", "__pycache__", ".venv", "node_modules", ".mypy_cache"}

_IDENTIFIER = re.compile(r"^[A-Za-z_]\w*$")
#: A Windows drive path, absolute wherever the job's own path rules say.
_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")
_MODULE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)*$")
_COMMIT = re.compile(r"^[0-9a-fA-F]{7,40}$")
_REF = re.compile(r"^[\w./-]+$")
#: What a published suite's snapshot records about the code it froze.
MANIFEST = "manifest.json"
#: The attribute the decorator leaves on a function.
_MARK = "__hopsworks_check__"
#: Importing a source edits sys.path and sys.modules, which every thread shares;
#: two trials importing two sources at once handed one the other's helper.
_IMPORT_LOCK = threading.RLock()


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


def _declared_hint(declared: Mapping[str, _Declared]) -> str:
    if not declared:
        return "; it declares no @check"
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
    for key in ("only", "exclude"):
        names = entry.get(key)
        if names is None:
            continue
        if not isinstance(names, (list, tuple)) or not all(
            isinstance(n, str) and _IDENTIFIER.match(n) for n in names
        ):
            raise ValueError(f"{key} is a list of check names")
    params = entry.get("params")
    if params is not None:
        if not isinstance(params, Mapping):
            raise ValueError("params is an object keyed by check name")
        for check_name, value in params.items():
            if not _IDENTIFIER.match(str(check_name)) or not isinstance(value, Mapping):
                raise ValueError(
                    "params is an object keyed by check name, each value an object "
                    "of that check's parameters"
                )


def _selected(
    declared: Mapping[str, _Declared],
    only: Sequence[str] | None,
    exclude: Sequence[str] | None,
) -> tuple[list[_Declared], list[str]]:
    """The declared checks a row runs, and the names in ``only`` it does not declare."""
    # None is "no filter"; an empty list is "nothing", which is what the UI
    # writes when every box is unticked and must not read as everything.
    wanted = list(declared) if only is None else list(only)
    skipped = [name for name in (exclude or []) if name]
    chosen = [declared[n] for n in wanted if n in declared and n not in skipped]
    missing = [n for n in wanted if n not in declared]
    return chosen, missing


# ── resolving the code ──────────────────────────────────────────────────────


@dataclass
class _Fetched:
    """A source's code on local disk, before anything of it has run."""

    local: str
    package_root: str | None = None
    origin: str = ""
    commit: str = ""
    #: True when read from, or just frozen into, a published suite's snapshot.
    pinned: bool = False
    manifest: dict[str, Any] = field(default_factory=dict)


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
    #: What the import pulled in from beside the source, taken back out of
    #: ``sys.modules`` so another source's helper of the same name imports
    #: fresh. Held here so the functions that bound them keep working.
    held: dict[str, ModuleType] = field(default_factory=dict)


class _TooLarge(Exception):
    """A repository bigger than a snapshot is allowed to be."""


class _CheckSources:
    """Turns a :class:`_Source` into an imported module, for one job execution.

    Built by the job, which is the only place the pieces exist: a dataset API
    for the project's files, the serving view of the deployment under test,
    and the published suite's snapshot directory when there is one. Checkouts
    and imports are shared across the suites of one execution, so a file two
    suites name is fetched once.

    Fetching and importing are two steps on purpose. Fetching is I/O with a
    budget of its own; importing runs the check's code, so it runs under the
    check's own timeout. :meth:`_load` does both for callers that do not care.

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
        self._lock = threading.RLock()
        #: Which row reports under each declared check name. Two sources may both
        #: declare ``verdict``; the first row keeps the bare name and the second
        #: reports as ``<row>/verdict``, so two results never share one id.
        self._claims: dict[str, str] = {}

    def _close(self) -> None:
        if self._own_workdir:
            shutil.rmtree(self._workdir, ignore_errors=True)

    def _load(self, source: _Source) -> _Loaded:
        """Fetch and import, once per execution."""
        cached = self._loaded.get(source._describe())
        if cached is not None:
            return cached
        return self._import(source, self._fetch_local(source))

    def _claim(self, check_name: str, row: str) -> str:
        """The name a row reports a declared check under, unique across the execution.

        Bookkeeping only: this lock is never held while a source imports, so a
        source that blocks at import (and keeps the import lock on its daemon
        thread for ever) cannot stop the rows after it from being named.
        """
        with self._lock:
            owner = self._claims.setdefault(check_name, row)
            return check_name if owner == row else f"{row}/{check_name}"

    def _reserve(self, names: Sequence[str]) -> None:
        """Reserve the names of the suite's other evaluators, so no python check reuses one.

        A built-in ``exact_match`` named ``verdict`` and a declared check
        ``verdict`` wrote the same stored key; the built-in is in the spec
        first and keeps the bare name, the check reports as ``<row>/verdict``.
        """
        with self._lock:
            for name in names:
                self._claims.setdefault(name, f"\0{name}")

    def _import(
        self, source: _Source, fetched: _Fetched, *, lock_timeout_s: float | None = None
    ) -> _Loaded:
        """Import what was fetched, and remember it for the rest of the execution.

        The cache is read and written under the bookkeeping lock; the import
        itself runs under the process-wide import lock, acquired with a bound,
        so an import still blocked from an earlier row fails this one quickly
        and clearly instead of stalling it.
        """
        key = source._describe()
        with self._lock:
            cached = self._loaded.get(key)
        if cached is not None:
            return cached
        if source.module:
            module = importlib.import_module(source.module)
            held: dict[str, ModuleType] = {}
        else:
            module, held = _import_path(
                fetched.local,
                package_root=fetched.package_root,
                lock_timeout_s=lock_timeout_s,
            )
        loaded = _Loaded(
            module=module,
            origin=fetched.origin,
            sha256=(
                _digest_of(fetched.local)
                if fetched.local and os.path.exists(fetched.local)
                else ""
            ),
            commit=fetched.commit,
            pinned=fetched.pinned,
            local=fetched.local,
            held=held,
        )
        with self._lock:
            return self._loaded.setdefault(key, loaded)

    def _fetch_local(self, source: _Source) -> _Fetched:
        """The source's code on local disk. I/O only: nothing of it runs here."""
        if source.module:
            spec = importlib.util.find_spec(source.module)
            if spec is None:
                raise ModuleNotFoundError(f"no module named {source.module!r}")
            path = (
                spec.origin if spec.origin not in (None, "built-in", "frozen") else ""
            )
            return _Fetched(
                local=path or "",
                origin=f"module {source.module} ({path or 'built in'})",
            )

        if source.file:
            absolute = self._absolute(source.file)
            local = self._download(absolute)
            # A published suite's row points at the copy the backend froze
            # beside its tasks; nothing has to be looked up to know that.
            pinned = self._snapshot_dir is not None and absolute.startswith(
                self._snapshot_dir + "/"
            )
            return _Fetched(
                local=local,
                origin=f"{'snapshot' if pinned else 'hopsfs'} {absolute}",
                pinned=pinned,
            )

        snapshot = self._snapshot_of(source)
        if snapshot is not None:
            return snapshot

        url, ref, commit = self._git_target(source)
        root = self._checkout(url, ref, commit)
        local = os.path.join(root, source.path)
        if not os.path.exists(local):
            raise FileNotFoundError(
                f"{source.path} is not in {_scrubbed(url)}@{commit or ref or 'HEAD'}"
            )
        resolved = commit or _head_of(root)
        fetched = _Fetched(
            local=local,
            package_root=root,
            origin=f"git {_scrubbed(url)}@{resolved or ref or 'HEAD'}:{source.path}",
            commit=resolved,
        )
        if source.deployment:
            # The deployment's own repository is deliberately never pinned: its
            # checks move with the agent, which is the point of naming it.
            return fetched
        return self._pin(source, fetched, root=root, url=url, ref=ref) or fetched

    # -- file ---------------------------------------------------------------

    def _absolute(self, file: str) -> str:
        if file.startswith("/") or os.path.isabs(file) or _DRIVE.match(file):
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
                if copied > MAX_TREE_FILES or total > MAX_TREE_BYTES:
                    raise RuntimeError(
                        f"{root} holds more than {MAX_TREE_FILES} files or "
                        f"{MAX_TREE_BYTES // (1024 * 1024)} MB; a snapshot of "
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

    def _snapshot_key(self, source: _Source) -> str:
        """What a source is frozen under: its whole identity, hashed, plus its basename.

        The basename alone collided: two ``checks.py`` from two places froze
        into one copy, and whichever was copied last graded for both.
        """
        if source.file:
            identity = f"hopsfs {self._absolute(source.file)}"
        else:
            # The commit, when the row pins one: two rows pinned to two commits
            # are two sources, and one snapshot for both graded with the first's.
            at = source.git_commit or source.git_ref
            identity = f"git {_scrubbed(source.git_url)}@{at}:{source.path}"
        return _snapshot_name(identity, source.path or source.file)

    def _snapshot_of(self, source: _Source) -> _Fetched | None:
        """The published copy of a git source, when the suite has one."""
        if self._snapshot_dir is None or self._dataset_api is None:
            return None
        name = self._snapshot_key(source)
        target = f"{self._snapshot_dir}/{name}"
        try:
            self._stat(f"{target}/{MANIFEST}")
        except FileNotFoundError:
            return None
        return self._from_snapshot(self._download(target), source, name)

    def _from_snapshot(self, local_dir: str, source: _Source, name: str) -> _Fetched:
        manifest: dict[str, Any] = {}
        try:
            with open(os.path.join(local_dir, MANIFEST), encoding="utf-8") as handle:
                manifest = dict(json.load(handle) or {})
        except Exception:  # noqa: BLE001 -- the code is what matters
            log.debug("no readable manifest in snapshot %s", name)
        tree = os.path.join(local_dir, TREE)
        return _Fetched(
            local=os.path.join(tree, source.path),
            package_root=tree,
            origin=f"snapshot {self._snapshot_dir}/{name}",
            commit=str(manifest.get("commit") or ""),
            pinned=True,
            manifest=manifest,
        )

    def _pin(
        self, source: _Source, fetched: _Fetched, *, root: str, url: str, ref: str
    ) -> _Fetched | None:
        """Freeze a git source beside the published suite's tasks, once, whoever is first.

        Two runs of one version can both find no snapshot, clone a branch that
        moved in between, and each believe its copy is the frozen one. So each
        stages its copy under a name of its own and renames it into place:
        HopsFS renames atomically and refuses an existing destination, so
        exactly one wins, and the loser throws its copy away and grades with
        the winner's -- the same code every later run reads.

        The whole repository tree goes in, not the one file: a check may
        import a helper from anywhere in the repository, and a snapshot that
        held only the entry file passed on its first run and failed with
        ``ModuleNotFoundError`` in the next job.
        """
        if self._snapshot_dir is None or self._dataset_api is None:
            return None
        name = self._snapshot_key(source)
        staging_local = os.path.join(self._workdir, "pin", uuid.uuid4().hex, name)
        try:
            copied = _copy_tree(root, os.path.join(staging_local, TREE))
        except _TooLarge as err:
            log.warning(
                "not pinning %s: %s; this run grades with the live checkout", name, err
            )
            return None
        manifest = {
            "url": _scrubbed(url),
            "ref": ref,
            "path": source.path,
            "basename": posixpath.basename(source.path),
            "commit": fetched.commit,
            "sha256": _digest_of(fetched.local),
            "files": copied,
            "pinned_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        with open(
            os.path.join(staging_local, MANIFEST), "w", encoding="utf-8"
        ) as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)

        staging_remote = f"{self._snapshot_dir}/.staging-{uuid.uuid4().hex}"
        target = f"{self._snapshot_dir}/{name}"
        won = False
        try:
            try:
                self._dataset_api.mkdir(self._snapshot_dir)
            except Exception:  # noqa: BLE001 -- exists already, most often
                log.debug("snapshot directory %s not created", self._snapshot_dir)
            self._dataset_api.mkdir(staging_remote)
            self._dataset_api.upload(staging_local, staging_remote, overwrite=True)
            try:
                self._dataset_api.move(
                    f"{staging_remote}/{name}", target, overwrite=False
                )
                won = True
            except Exception:  # noqa: BLE001 -- the other run's copy is the snapshot
                won = False
        except Exception:  # noqa: BLE001 -- a run that cannot pin still grades
            log.warning(
                "could not snapshot %s into %s; this run grades with the live "
                "checkout and the next one pins",
                name,
                self._snapshot_dir,
                exc_info=True,
            )
            return None
        finally:
            try:
                self._dataset_api.remove(staging_remote)
            except Exception:  # noqa: BLE001 -- best effort
                log.debug("staging %s not removed", staging_remote)
        if won:
            log.info(
                "pinned %s at %s into %s", name, fetched.commit, self._snapshot_dir
            )
            tree = os.path.join(staging_local, TREE)
            return _Fetched(
                local=os.path.join(tree, source.path),
                package_root=tree,
                origin=f"{fetched.origin} (pinned)",
                commit=fetched.commit,
                pinned=True,
                manifest=manifest,
            )
        log.info("another run pinned %s first; grading with its copy", name)
        return self._from_snapshot(self._download(target), source, name)


def _snapshot_name(identity: str, path: str) -> str:
    """``<12 hex of the identity>-<basename>``: unique per source, readable by a person."""
    digest = hashlib.sha256(identity.encode()).hexdigest()[:12]
    return f"{digest}-{posixpath.basename(path.rstrip('/'))}"


def _copy_tree(root: str, into: str) -> int:
    """A repository's files, layout kept, under the snapshot caps.

    Raises :class:`_TooLarge` past them: a repository that big is not pinned,
    and the run says so rather than freezing half of it.
    """
    copied = 0
    total = 0
    for current, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in _SKIPPED_DIRS]
        for file in files:
            path = os.path.join(current, file)
            if os.path.islink(path):
                continue
            size = os.path.getsize(path)
            if size > MAX_TREE_FILE_BYTES:
                continue
            copied += 1
            total += size
            if copied > MAX_TREE_FILES or total > MAX_TREE_BYTES:
                raise _TooLarge(
                    f"more than {MAX_TREE_FILES} files or "
                    f"{MAX_TREE_BYTES // (1024 * 1024)} MB"
                )
            relative = os.path.relpath(path, root)
            destination = os.path.join(into, relative)
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            shutil.copyfile(path, destination)
    return copied


def _import_path(
    local: str, *, package_root: str | None = None, lock_timeout_s: float | None = None
) -> tuple[ModuleType, dict[str, ModuleType]]:
    """Import a file or a package directory, isolated from every other source.

    The entry gets a name nothing else uses, so two ``checks.py`` in one
    execution do not hand each other their code. Its own imports need the same
    care: ``from helpers import yes`` lands ``helpers`` in ``sys.modules`` by
    that name, and a second source's ``helpers`` would silently reuse the
    first's. So the parent directory (or the repository root) goes on
    ``sys.path`` only for the import, and whatever the import pulled in from
    under it is taken back out of ``sys.modules`` afterwards and returned,
    held, so the functions that bound it keep working. A check that imports a
    helper lazily inside the function will not find it: import at the top.
    """
    budget = DEFAULT_TIMEOUT_S if lock_timeout_s is None else lock_timeout_s
    if not _IMPORT_LOCK.acquire(timeout=budget):
        raise RuntimeError(
            "another source's import has not finished and still holds the import "
            f"lock after {budget:g}s; this one could not start"
        )
    try:
        return _import_path_locked(local, package_root=package_root)
    finally:
        _IMPORT_LOCK.release()


def _import_path_locked(
    local: str, *, package_root: str | None = None
) -> tuple[ModuleType, dict[str, ModuleType]]:
    local = os.path.abspath(local)
    parent = package_root or os.path.dirname(local)
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
    before = set(sys.modules)
    added_path = parent not in sys.path
    if added_path:
        sys.path.insert(0, parent)
    importlib.invalidate_caches()
    sys.modules[unique] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(unique, None)
        raise
    finally:
        if added_path:
            with contextlib.suppress(ValueError):
                sys.path.remove(parent)
        held: dict[str, ModuleType] = {}
        prefix = parent.rstrip(os.sep) + os.sep
        for name in set(sys.modules) - before:
            if name == unique or name.startswith(unique + "."):
                continue
            pulled = sys.modules.get(name)
            file = getattr(pulled, "__file__", None) or ""
            if file.startswith(prefix):
                held[name] = sys.modules.pop(name)
    return module, held


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


class PythonSourceEvaluator:
    """A source the project wrote: every check it declares, run like built-in ones.

    One row, as many results as the source declares checks (``only`` and
    ``exclude`` narrow that), each reported under the check's own name with
    the row's name beside it in the assertions. Loads lazily on the first
    trial, so building a suite's evaluators (which authoring does to validate
    a spec) never imports user code. A load that fails -- unreadable file, bad
    import -- is reported on every trial as one skipped row, which the runner
    turns into EVALUATOR_ERROR: the harness failing, visibly, rather than a
    suite that quietly graded without its checks. So is a name in ``only``
    the source does not declare.
    """

    type = TYPE
    # The Protocol's attribute: whether a trace is needed is per declared
    # check, and each one says so on its own result.
    needs_trace = False

    def __init__(
        self,
        name: str,
        source: _Source,
        *,
        only: Sequence[str] | None = None,
        exclude: Sequence[str] | None = None,
        params: Mapping[str, Mapping[str, Any]] | None = None,
        sources: _CheckSources | None = None,
    ):
        self.name = name
        self.source = source
        self.only = None if only is None else [str(n) for n in only]
        self.exclude = [str(n) for n in (exclude or [])]
        self.params = {str(k): dict(v or {}) for k, v in (params or {}).items()}
        self._sources = sources
        self._checks: list[_Declared] | None = None
        self._declared_names: list[str] = []
        self._missing: list[str] = []
        self._report: dict[str, str] = {}
        self._loaded: _Loaded | None = None
        self._load_error: str | None = None
        self._lock = threading.Lock()

    def _ensure_loaded(self) -> str | None:
        """Load once; the reason it could not, or ``None``.

        Two budgets. Fetching (a clone, a download) is I/O and gets
        ``FETCH_BUDGET_S``. Importing runs the source's module, which is the
        checks' code as much as the functions are, so it gets the largest
        timeout the source's checks declare -- a module that blocks at import
        used to stall the run before any timeout had something to measure.
        """
        if self._checks is not None or self._load_error is not None:
            return self._load_error
        with self._lock:
            if self._checks is not None or self._load_error is not None:
                return self._load_error
            return self._load()

    def _load(self) -> str | None:
        try:
            if self._sources is None:
                if not self.source.module:
                    raise RuntimeError(
                        "this run has no way to read project files or repositories"
                    )
                sources = _CheckSources()
            else:
                sources = self._sources
            loaded = sources._loaded.get(self.source._describe())
            if loaded is None:
                try:
                    fetched = _run_with_timeout(
                        lambda: sources._fetch_local(self.source), FETCH_BUDGET_S
                    )
                except _Timeout:
                    raise RuntimeError(
                        f"fetching the source took longer than {FETCH_BUDGET_S:g}s"
                    ) from None
                try:
                    loaded = _run_with_timeout(
                        lambda: sources._import(
                            self.source, fetched, lock_timeout_s=DEFAULT_TIMEOUT_S
                        ),
                        DEFAULT_TIMEOUT_S,
                    )
                except _Timeout:
                    raise RuntimeError(
                        f"importing the source took longer than {DEFAULT_TIMEOUT_S:g}s"
                    ) from None
            self._loaded = loaded
            declared = _declared_in(loaded.module)
            if not declared:
                raise LookupError("it declares no @check")
            self._declared_names = sorted(declared)
            self._checks, self._missing = _selected(declared, self.only, self.exclude)
            self._report = {
                c.name: sources._claim(c.name, self.name) for c in self._checks
            }
            log.info(
                "source %r loaded from %s: %s",
                self.name,
                loaded.origin,
                ", ".join(c.name for c in self._checks) or "nothing selected",
            )
        except KeyboardInterrupt:
            raise
        except BaseException as err:  # noqa: BLE001 -- the reason goes on every row
            self._load_error = (
                f"checks {self.name!r} could not be loaded from "
                f"{self.source._describe()}: {_short(err)}"
            )
            log.warning("%s", self._load_error)
        return self._load_error

    def _grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        """The Protocol's one result; the runner asks for ``_grade_many`` first."""
        results = self._grade_many(task, trial, trace)
        return (
            results[0]
            if len(results) == 1
            else _ungradable(
                self.name,
                self.type,
                f"{len(results)} results; read them with _grade_many",
            )
        )

    def _grade_many(
        self, task: Task, trial: Trial, trace: Trace | None
    ) -> list[EvaluatorResult]:
        failure = self._ensure_loaded()
        if failure or self._checks is None:
            result = _ungradable(self.name, self.type, failure or "not loaded")
            result.assertions = {SKIPPED: True, "reason": failure, "row": self.name}
            return [result]
        results = []
        if not self._checks and not self._missing:
            result = _ungradable(
                self.name, self.type, "no check selected: this row grades nothing"
            )
            result.assertions = {"row": self.name}
            results.append(result)
        for name in self._missing:
            result = _ungradable(
                name,
                self.type,
                f"{self.source._describe()} declares no check named {name!r}; "
                f"it declares {', '.join(self._declared_names)}",
            )
            result.assertions = {
                SKIPPED: True,
                "reason": result.reason,
                "row": self.name,
            }
            results.append(result)
        for declared in self._checks:
            results.append(self._grade_one(declared, task, trial, trace))
        return results

    def _grade_one(
        self, declared: _Declared, task: Task, trial: Trial, trace: Trace | None
    ) -> EvaluatorResult:
        name = declared.name
        report = self._report.get(name, name)
        if declared.needs_trace and trace is None:
            result = _ungradable(report, self.type, "no trace arrived for this trial")
        elif declared.reads_expectation and not task._expects_text(name):
            # Declared to read what only a task carries. Graded anyway, an empty
            # expectation matched an answer on live traffic and read as a pass.
            # The backend refuses such a file for a monitor when it can scan it;
            # a git or module source cannot be scanned, so the rule is kept here.
            result = _ungradable(
                report,
                self.type,
                "reads the task's expectation, and this trial carries none for it",
            )
        else:
            params = {**dict(declared.defaults), **self.params.get(name, {})}
            context = _context_for(name, task, trial, trace, params)
            try:
                value = _call_with_timeout(
                    declared.fn, context, params, declared.timeout_s
                )
            except _Timeout:
                result = _ungradable(
                    report, self.type, f"timed out after {declared.timeout_s:g}s"
                )
            except KeyboardInterrupt:
                raise
            except BaseException as err:  # noqa: BLE001 -- a bad check is not a bad agent
                # BaseException, not Exception: a dependency calling sys.exit()
                # is the check failing, not the job ending.
                result = _ungradable(report, self.type, f"check raised: {_short(err)}")
            else:
                result = _as_result(report, value, pass_score=params.get("pass_score"))
        result.assertions.setdefault("source", self._provenance())
        result.assertions.setdefault("row", self.name)
        if report != name:
            result.assertions.setdefault("check", name)
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


def _preload(evaluators: Sequence[Any]) -> None:
    """Load every python source before the first trial, in spec order.

    Loading is lazy so authoring never imports user code, but a run should not
    discover a broken source on its first trial, and the name a row reports a
    shared check under depends on which row claimed it first: done here, in
    spec order, that is the same in every run rather than whichever trial
    thread got there first.
    """
    for evaluator in evaluators:
        if isinstance(evaluator, PythonSourceEvaluator):
            evaluator._ensure_loaded()


class _Timeout(Exception):
    pass


def _run_with_timeout(fn: Callable[[], Any], timeout_s: float) -> Any:
    """``fn()`` on a thread, or ``_Timeout``.

    A thread per call: the code is the user's and may block on anything. A
    thread cannot be killed, so something that hangs costs one leaked daemon
    thread and an ungradable row, and the run moves on.
    """
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["value"] = fn()
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


def _call_with_timeout(
    fn: Callable[..., Any],
    context: CheckContext,
    params: Mapping[str, Any],
    timeout_s: float,
) -> Any:
    """The function's return value on one trial, or ``_Timeout``."""
    return _run_with_timeout(lambda: fn(context, **params), timeout_s)


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
        if score is not None and not _finite(score):
            return _ungradable(name, TYPE, f"score {score!r} is not a finite number")
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
        if not _finite(value):
            # NaN compared as greater-or-equal to anything is False, and then
            # clamped read as a confident 1.0: a perfect pass from no number.
            return _ungradable(name, TYPE, f"score {value!r} is not a finite number")
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


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


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
    context: Mapping[str, Any] | None = None,
    task: Mapping[str, Any] | None = None,
    trial: Mapping[str, Any] | None = None,
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
        context: The request context the task replays, as ``ctx.task.context``.
        task: The task's details (``task_id``, ``version``, ``task_type``, ``category``, ``tags``), for ``ctx.task``.
        trial: The trial's details (``trial_id``, ``run_id``, ``trial_index``, ``deployment_id``, ``status``, ``latency_ms``, ``trace_status``, ``input_tokens``, ``output_tokens``), for ``ctx.trial``.
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
    about = dict(task or {})
    details = dict(trial or {})
    task_row = Task(
        task_id=str(about.get("task_id") or "local"),
        task_version=int(about.get("version") or 1),
        input_messages=json.dumps(
            list(messages) if messages else [{"role": "user", "content": question}]
        ),
        task_type=str(
            about.get("task_type") or ("multi_turn" if transcript else "single_turn")
        ),
        expectations=all_expectations,
        category=str(about.get("category") or ""),
        tags=[str(t) for t in (about.get("tags") or [])],
        subject=subject or str(about.get("subject") or ""),
        context=dict(context if context is not None else about.get("context") or {}),
    )
    trial_row = Trial(
        trial_id=str(details.get("trial_id") or "local"),
        run_id=str(details.get("run_id") or "local"),
        task_id=task_row.task_id,
        task_version=task_row.task_version,
        trial_index=int(details.get("trial_index") or 0),
        deployment_id=int(details.get("deployment_id") or 0),
        trace_id=str((trace_dict or {}).get("trace_id") or ""),
        transcript=transcript,
        final_output=answer,
        latency_ms=float(details.get("latency_ms") or 0.0),
        session_id=str(details.get("session_id") or ""),
        input_tokens=_int_or_none(details.get("input_tokens")),
        output_tokens=_int_or_none(details.get("output_tokens")),
    )
    for key, enum in (("status", TrialStatus), ("trace_status", TraceStatus)):
        given = details.get(key)
        if given:
            try:
                setattr(trial_row, key, enum(str(given)))
            except ValueError:
                log.debug("ignoring unknown %s %r", key, given)
    merged = {**dict(declared.defaults), **params}
    if declared.needs_trace and trace_dict is None and tools is None:
        return _ungradable(check_name, TYPE, "no trace arrived for this trial")
    shown = _context_for(check_name, task_row, trial_row, trace_dict, merged)
    try:
        value = _call_with_timeout(
            declared.fn,
            shown,
            merged,
            _bounded_timeout(timeout_s or declared.timeout_s),
        )
    except _Timeout:
        return _ungradable(check_name, TYPE, "timed out")
    except KeyboardInterrupt:
        raise
    except BaseException as err:  # noqa: BLE001 -- the same rule as in the job
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


def _int_or_none(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


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
