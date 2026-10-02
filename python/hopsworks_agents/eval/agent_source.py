"""The agent's own source code, for a reviewer that has to tell a bug from a bad answer.

A trace shows what the agent did; the code shows why. A tool that failed because the
agent passed the wrong argument, a lookup that ignores the key the user gave, a
retry loop with no exit -- these are bugs in the agent, not weaknesses of the model,
and a triage that cannot see the code files them all under "wrong answer". So the
review job fetches the code the deployment runs and shows the model the files that
matter for the trace in hand.

Two places code comes from, because deployments are made two ways: a git repository
(url, branch, the commit that is running) or a path in HopsFS (the model artifact and
its script). Both end up as the same thing here: a map of relative path to text.

Which files matter is decided without a model call. The entry script first, then
whatever it imports within the repository, then any file that mentions a tool the
trace called or a word from the failure. A budget in characters keeps the prompt
bounded; a file that does not fit is clipped and says so.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence


log = logging.getLogger(__name__)

#: File kinds worth showing a model. Anything else in a repository is bytes.
SOURCE_SUFFIXES = (
    ".py",
    ".md",
    ".txt",
    ".toml",
    ".yaml",
    ".yml",
    ".json",
    ".cfg",
    ".ini",
    ".sql",
)
#: Directories nobody's agent lives in.
SKIPPED_DIRS = {
    ".git",
    "__pycache__",
    "node_modules",
    ".venv",
    "venv",
    "dist",
    "build",
    ".mypy_cache",
    ".pytest_cache",
    ".idea",
    ".vscode",
}
#: A file bigger than this is generated or data, not agent code.
MAX_FILE_BYTES = 200 * 1024
#: Total text kept from one repository. Enough for an agent; not a monorepo.
MAX_BUNDLE_BYTES = 5 * 1024 * 1024
#: Characters of source shown per triage call by default. Roughly 6k tokens.
DEFAULT_SOURCE_CHARS = 24_000


@dataclass
class SourceBundle:
    """The agent's code as a map of relative path to text, and where it came from."""

    files: dict[str, str]
    #: The script the deployment runs, relative to the bundle root; empty when unknown.
    entry: str = ""
    #: A human-readable origin: "git <url>@<ref>" or "hopsfs <path>".
    origin: str = ""
    #: Files that were present but not kept, with why; for the log and the prompt's honesty.
    skipped: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.files)


@dataclass
class CodeLocation:
    """Where a deployment's code is, as the serving API describes it."""

    git_url: str = ""
    git_branch: str = ""
    git_commit: str = ""
    model_path: str = ""
    script_file: str = ""

    @classmethod
    def from_serving(cls, view: dict[str, Any] | None) -> CodeLocation:
        view = view or {}
        return cls(
            git_url=str(view.get("gitUrl") or "").strip(),
            git_branch=str(
                view.get("gitResolvedBranch") or view.get("gitBranch") or ""
            ).strip(),
            git_commit=str(view.get("gitCurrentCommit") or "").strip(),
            model_path=str(view.get("modelPath") or "").strip(),
            script_file=str(view.get("predictor") or "").strip(),
        )

    @classmethod
    def from_override(cls, location: str, script_file: str = "") -> CodeLocation:
        """A location typed into the job's settings: an https git url or a HopsFS path."""
        location = (location or "").strip()
        if not location:
            return cls()
        if location.startswith(("http://", "https://")):
            url, _, ref = location.partition("#")
            return cls(git_url=url, git_branch=ref, script_file=script_file)
        return cls(model_path=location, script_file=script_file)

    def known(self) -> bool:
        return bool(self.git_url or self.model_path)


# --- Reading files -----------------------------------------------------------------


def _is_source(path: str) -> bool:
    parts = Path(path).parts
    if any(part in SKIPPED_DIRS for part in parts[:-1]):
        return False
    return Path(path).suffix.lower() in SOURCE_SUFFIXES


def bundle_from_directory(
    root: str | os.PathLike[str], *, entry: str = "", origin: str = ""
) -> SourceBundle:
    """Every source file under ``root``, as relative paths with forward slashes."""
    root_path = Path(root)
    files: dict[str, str] = {}
    skipped: list[str] = []
    total = 0
    for path in sorted(p for p in root_path.rglob("*") if p.is_file()):
        relative = path.relative_to(root_path).as_posix()
        if not _is_source(relative):
            continue
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            skipped.append(f"{relative} ({size} bytes, too large)")
            continue
        if total + size > MAX_BUNDLE_BYTES:
            skipped.append(f"{relative} (bundle budget spent)")
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as err:
            skipped.append(f"{relative} ({err})")
            continue
        files[relative] = text
        total += size
    return SourceBundle(
        files=files, entry=_resolve_entry(files, entry), origin=origin, skipped=skipped
    )


def _resolve_entry(files: dict[str, str], entry: str) -> str:
    """The entry script as a key of ``files``, tolerating a leading directory or slash."""
    if not entry:
        return ""
    candidate = entry.strip().lstrip("/")
    if candidate in files:
        return candidate
    # a HopsFS script path carries the model directory; a git one is relative to the checkout
    for path in files:
        if (
            candidate.endswith("/" + path)
            or path.endswith("/" + candidate)
            or Path(path).name == Path(candidate).name
        ):
            return path
    return ""


#: How Hopsworks hands a pod the git providers a user configured, as "https://user:token@host"
#: entries separated by spaces. The container writes them to git's credential store at start
#: (setup_git_credentials.sh); this module reads them only for the tarball fallback, where
#: there is no git to read the store.
GIT_CREDENTIALS_ENV = "GIT_CREDENTIALS"


def git_credentials(environ: dict[str, str] | None = None) -> list[str]:
    """The "https://user:token@host" entries Hopsworks put in the environment, if any."""
    raw = (environ if environ is not None else os.environ).get(GIT_CREDENTIALS_ENV, "")
    return [entry for entry in raw.split() if entry.startswith("http")]


def _credential_for(url: str, entries: Sequence[str]) -> tuple[str, str]:
    """The (user, token) configured for the repository's host, or empty strings."""
    host = url.split("//", 1)[-1].split("/", 1)[0].lower()
    for entry in entries:
        rest = entry.split("//", 1)[-1]
        if "@" not in rest:
            continue
        auth, entry_host = rest.rsplit("@", 1)
        if entry_host.split("/", 1)[0].lower() == host:
            user, _, token = auth.partition(":")
            return user, token
    return "", ""


def clone_repository(
    url: str,
    ref: str,
    commit: str,
    into: str,
    *,
    run: Callable[..., Any] = subprocess.run,
) -> str:
    """A shallow checkout of the repository at the commit that is running, or the branch's tip.

    ``git`` when there is one, authenticating through the credential store the container
    wrote at start from the user's git providers (``setup_git_credentials.sh``, the same as
    Jupyter and git-backed deployments run); otherwise, for GitHub, the archive tarball.
    Returns the directory holding the checkout. Raises on failure: no code is a reason in the
    log, not a triage row that silently lacked it.
    """
    if shutil.which("git"):
        target = os.path.join(into, "repo")
        base = ["git", "-c", "advice.detachedHead=false"]
        if commit:
            run([*base, "init", "-q", target], check=True, capture_output=True)
            run(
                [*base, "-C", target, "remote", "add", "origin", url],
                check=True,
                capture_output=True,
            )
            run(
                [*base, "-C", target, "fetch", "-q", "--depth", "1", "origin", commit],
                check=True,
                capture_output=True,
            )
            run(
                [*base, "-C", target, "checkout", "-q", "FETCH_HEAD"],
                check=True,
                capture_output=True,
            )
        else:
            command = [*base, "clone", "-q", "--depth", "1"]
            if ref:
                command += ["--branch", ref]
            command += [url, target]
            run(command, check=True, capture_output=True)
        return target
    if "github.com" not in url:
        raise RuntimeError(
            "git is not installed and only GitHub repositories can be fetched without it"
        )
    _, token = _credential_for(url, git_credentials())
    return _github_tarball(url, commit or ref or "HEAD", token, into)


def _github_tarball(url: str, ref: str, token: str, into: str) -> str:
    import requests  # noqa: PLC0415 -- only on the path that needs it

    owner_repo = url.rstrip("/").removesuffix(".git").split("github.com/", 1)[-1]
    archive = f"https://api.github.com/repos/{owner_repo}/tarball/{ref}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    response = requests.get(archive, headers=headers, timeout=120, stream=True)
    response.raise_for_status()
    tar_path = os.path.join(into, "repo.tar.gz")
    with open(tar_path, "wb") as handle:
        for chunk in response.iter_content(chunk_size=1 << 16):
            handle.write(chunk)
    target = os.path.join(into, "repo")
    os.makedirs(target, exist_ok=True)
    with tarfile.open(tar_path) as tar:
        tar.extractall(target, filter="data")  # noqa: S202 -- 'data' filter refuses paths outside target
    # GitHub wraps the tree in one directory named after the commit
    children = [os.path.join(target, name) for name in os.listdir(target)]
    return children[0] if len(children) == 1 and os.path.isdir(children[0]) else target


def download_hopsfs_directory(dataset_api: Any, path: str, into: str) -> str:
    """Every source file under a HopsFS path, downloaded flat into ``into`` keeping structure."""
    root = path.rstrip("/")
    target = os.path.join(into, "model")
    os.makedirs(target, exist_ok=True)
    pending = [root]
    while pending:
        current = pending.pop()
        listing = dataset_api.list(current.lstrip("/"), limit=1000) or {}
        for item in listing.get("items") or []:
            attributes = item.get("attributes") or item
            item_path = str(attributes.get("path") or "")
            if not item_path:
                continue
            if attributes.get("dir"):
                name = Path(item_path).name
                if name not in SKIPPED_DIRS:
                    pending.append(item_path)
                continue
            relative = (
                item_path[len(root) :].lstrip("/")
                if item_path.startswith(root)
                else Path(item_path).name
            )
            if (
                not _is_source(relative)
                or int(attributes.get("size") or 0) > MAX_FILE_BYTES
            ):
                continue
            local = os.path.join(target, relative)
            os.makedirs(os.path.dirname(local), exist_ok=True)
            dataset_api.download(item_path, local, overwrite=True)
    return target


def load_agent_source(
    location: CodeLocation, *, dataset_api: Any = None, workdir: str | None = None
) -> SourceBundle:
    """The code at ``location``, or an empty bundle with the reason logged.

    Nothing here raises: a review without the code is a poorer review, not a failed one,
    and the rows it writes still say what the model saw.
    """
    if not location.known():
        log.info(
            "the deployment's code location is not known; reviewing without source"
        )
        return SourceBundle(files={})
    into = workdir or tempfile.mkdtemp(prefix="agent-source-")
    try:
        if location.git_url:
            ref = location.git_commit or location.git_branch or "default branch"
            root = clone_repository(
                location.git_url, location.git_branch, location.git_commit, into
            )
            bundle = bundle_from_directory(
                root, entry=location.script_file, origin=f"git {location.git_url}@{ref}"
            )
        else:
            if dataset_api is None:
                raise RuntimeError("no dataset API to read the model path with")
            root = download_hopsfs_directory(dataset_api, location.model_path, into)
            bundle = bundle_from_directory(
                root, entry=location.script_file, origin=f"hopsfs {location.model_path}"
            )
        log.info(
            "read %d source files from %s (entry %s)",
            len(bundle.files),
            bundle.origin,
            bundle.entry or "unknown",
        )
        return bundle
    except Exception as err:  # noqa: BLE001 -- said in the log, not thrown at the run
        log.warning(
            "could not read the agent's source from %s: %s",
            location.git_url or location.model_path,
            err,
        )
        return SourceBundle(files={}, origin=f"unreadable: {err}")
    finally:
        if workdir is None:
            shutil.rmtree(into, ignore_errors=True)


# --- Choosing what to show ------------------------------------------------------------

_IMPORT = re.compile(
    r"^\s*(?:from\s+([.\w]+)\s+import|import\s+([\w.]+(?:\s*,\s*[\w.]+)*))",
    re.MULTILINE,
)


def _module_candidates(files: dict[str, str], importer: str, module: str) -> list[str]:
    """Repository files a Python import could refer to, relative imports resolved from the importer."""
    if module.startswith("."):
        dots = len(module) - len(module.lstrip("."))
        base = Path(importer).parent
        for _ in range(dots - 1):
            base = base.parent
        rest = module.lstrip(".")
        prefix = (base / rest.replace(".", "/")).as_posix() if rest else base.as_posix()
    else:
        prefix = module.replace(".", "/")
    candidates = []
    for tail in (f"{prefix}.py", f"{prefix}/__init__.py"):
        tail = tail.lstrip("./")
        for path in files:
            if path == tail or path.endswith("/" + tail):
                candidates.append(path)
    # a top-level package imported by its first segment: keep its __init__ and direct modules small
    return candidates


def imported_files(files: dict[str, str], entry: str, limit: int = 40) -> list[str]:
    """Files the entry script imports, transitively, within the repository. Entry first."""
    if not entry or entry not in files:
        return []
    ordered = [entry]
    seen = {entry}
    queue = [entry]
    while queue and len(ordered) < limit:
        current = queue.pop(0)
        for match in _IMPORT.finditer(files[current]):
            modules = (
                [match.group(1)]
                if match.group(1)
                else [m.strip() for m in match.group(2).split(",")]
            )
            for module in modules:
                for path in _module_candidates(files, current, module):
                    if path not in seen:
                        seen.add(path)
                        ordered.append(path)
                        queue.append(path)
    return ordered[:limit]


_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]{3,}")


def _mentions(text: str, terms: Iterable[str]) -> int:
    lowered = text.lower()
    return sum(1 for term in terms if term and term.lower() in lowered)


def relevant_files(
    bundle: SourceBundle,
    *,
    tool_names: Sequence[str] = (),
    clues: Sequence[str] = (),
    budget_chars: int = DEFAULT_SOURCE_CHARS,
) -> list[tuple[str, str]]:
    """The files to show for one trace, most relevant first, clipped to the budget.

    Order: the entry script and what it imports, then files that mention the tools the
    trace called or words from the failure (an error message, a note), then nothing --
    a file nobody has a reason to read is a file the model would invent a bug in.
    """
    if not bundle.files or budget_chars <= 0:
        return []
    terms = {t for t in tool_names if t}
    for clue in clues:
        terms.update(word for word in _WORD.findall(clue or "") if len(word) > 3)
    ordered = imported_files(bundle.files, bundle.entry)
    rest = [path for path in bundle.files if path not in ordered]
    mentioned = sorted(
        (path for path in rest if _mentions(bundle.files[path], terms) > 0),
        key=lambda path: (-_mentions(bundle.files[path], terms), path),
    )
    if not ordered and not mentioned:
        # no entry and nothing matched: the python files, smallest first, so the model sees the
        # shape of the agent rather than nothing at all
        mentioned = sorted(
            (p for p in rest if p.endswith(".py")),
            key=lambda p: (len(bundle.files[p]), p),
        )
    chosen: list[tuple[str, str]] = []
    remaining = budget_chars
    for path in [*ordered, *mentioned]:
        if remaining <= 200:
            break
        text = bundle.files[path]
        if len(text) > remaining:
            text = text[:remaining] + "\n… [clipped: file continues]"
        chosen.append((path, text))
        remaining -= len(text)
    return chosen


def render_source(files: Sequence[tuple[str, str]], origin: str = "") -> str:
    """The files as one block for the prompt, each under its path with line numbers."""
    if not files:
        return ""
    parts = [f'<agent_source origin="{origin}">' if origin else "<agent_source>"]
    for path, text in files:
        numbered = "\n".join(
            f"{n:4d}  {line}" for n, line in enumerate(text.splitlines(), start=1)
        )
        parts.append(f'<file path="{path}">\n{numbered}\n</file>')
    parts.append("</agent_source>")
    return "\n".join(parts)
