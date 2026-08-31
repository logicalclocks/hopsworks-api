"""``hops session`` — move a Claude Code session between this machine and a pod.

The mental model is a hand-off. ``push`` hands the session you are in to a
Hopsworks terminal pod (Claude Code is pre-installed in that image); the pod
lands it on its own, so you can close the laptop and keep driving it from the
browser Terminal tab. ``pull`` reclaims it back onto this machine. ``new``
starts a fresh session straight on the pod, ``list`` shows where things are,
``stop`` kills the terminal pod, and ``mirror`` (alias ``attach``) streams the
live pod terminal to your laptop.

The transport is symmetric. Each session is a JSONL that Claude Code stores at
``~/.claude/projects/<cwd-slug>/<session-id>.jsonl``; push uploads it into the
user's private HopsFS home under ``Users/<username>/teleport/<slug>/`` and pull
downloads it back. That home is mode 0700, so a session transcript is readable
only by its owner, not by other project members. Resume is scoped to the
working-directory slug, so both ends must run from a path that hashes to the
same slug for ``claude --resume`` to load.

Every push/new also stages a ``<id>.teleport.json`` manifest. The pod carries a
landing hook that watches the teleport dataset and, when a manifest appears,
resumes the pushed session (or opens a new one) on its own and acks with a
``<id>.landed.json`` sidecar; push waits briefly for that ack and prints the
manual landing steps only when it does not arrive.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import uuid
import webbrowser
from datetime import datetime, timezone
from pathlib import Path


try:
    import termios
    import tty
except ImportError:  # non-POSIX (Windows): raw stdin / --write is unavailable
    termios = None
    tty = None

import click
from hopsworks.cli import git_sync, output, terminal_api
from hopsworks.cli import session as conn
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError


# Where Claude Code keeps per-directory session transcripts on this machine.
_CLAUDE_PROJECTS = Path.home() / ".claude" / "projects"


def _resolve_username() -> str | None:
    """The logged-in user's Hopsworks username, for the ``Users/<username>`` home.

    The external client (a laptop) resolves it at login and stashes it as
    ``_username``. The internal client (running inside a terminal pod or a job)
    does not, but its hdfs identity is ``project__username`` in
    ``HADOOP_USER_NAME`` / ``HDFS_USER``, so peel the project prefix the same way
    the SDK does for the project name. This is what lets ``hops session`` work
    from the pod terminal (re-push, list, pull the baton back), not only the
    laptop.
    """
    username = getattr(client._get_instance(), "_username", None)
    if username:
        return username
    hops_user = os.environ.get("HADOOP_USER_NAME") or os.environ.get("HDFS_USER")
    if hops_user and "__" in hops_user:
        return hops_user.split("__", 1)[1]
    return None


def _teleport_root() -> str:
    """HopsFS dir the session JSONLs are staged under, one subdir per slug.

    The user's private home (``Users/<username>/teleport``, mode 0700) rather
    than the project-wide ``Resources/``: a transcript can carry code, file
    contents, whatever was in the session, so it must not be readable by other
    project members. The terminal pod mounts the same home, so its landing hook
    reads the manifests from there without a project-wide scan.
    """
    username = _resolve_username()
    if not username:
        raise click.ClickException(
            "Could not resolve your Hopsworks username; log in first."
        )
    return f"Users/{username}/teleport"


def _cwd_slug() -> str:
    """Return Claude Code's project-directory slug for the current working dir.

    Claude Code derives the ``~/.claude/projects/<slug>`` folder name by
    replacing every non-alphanumeric character in the absolute cwd with ``-``
    (e.g. ``/Users/lex/x-y`` -> ``-Users-lex-x-y``). We reproduce that mapping
    so we can find this directory's sessions.

    Returns:
        The slug for :func:`Path.cwd`.
    """
    return re.sub(r"[^A-Za-z0-9]", "-", str(Path.cwd()))


def _resolve_local_session(slug: str, session_id: str | None) -> Path:
    """Locate the local session JSONL to push for ``slug``.

    Args:
        slug: The current directory's Claude Code slug.
        session_id: Explicit session id to push, or None to pick the
            most-recently-written session under this slug (the active one).

    Returns:
        Path to the resolved ``<session-id>.jsonl``.

    Raises:
        click.ClickException: When no session directory or file is found, or
            the requested ``session_id`` does not exist here.
    """
    proj_dir = _CLAUDE_PROJECTS / slug
    if not proj_dir.is_dir():
        raise click.ClickException(
            f"No Claude Code sessions found for this directory "
            f"(looked in {proj_dir}). Run this from the directory your "
            f"session is in."
        )
    if session_id:
        candidate = proj_dir / f"{session_id}.jsonl"
        if not candidate.is_file():
            raise click.ClickException(
                f"Session {session_id} not found under {proj_dir}."
            )
        return candidate
    sessions = sorted(
        proj_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not sessions:
        raise click.ClickException(f"No .jsonl sessions under {proj_dir}.")
    return sessions[0]


def _is_missing_path(exc: RestAPIError) -> bool:
    """Whether a dataset-API failure means "that path does not exist".

    A never-pushed slug has no teleport directory, which the backend reports as
    a client error (400/404); callers treat that as an empty listing. Anything
    else (auth, network, server error) is a real failure and must surface.
    """
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status in (400, 404)


def _remote_session_ids(dataset_api, slug: str) -> list[str]:
    """Return the session ids staged in HopsFS for ``slug``, newest-listed last.

    Args:
        dataset_api: The project's dataset API.
        slug: The current directory's Claude Code slug.

    Returns:
        Session ids (JSONL stems) under ``Users/<username>/teleport/<slug>/``.
        Empty when the directory does not exist or holds no transcripts. Any
        other failure (auth, network) surfaces instead of masquerading as
        "nothing staged" and inviting a duplicate push.
    """
    remote_dir = f"{_teleport_root()}/{slug}"
    try:
        entries = dataset_api.list(remote_dir)
    except RestAPIError as exc:
        if _is_missing_path(exc):
            return []
        raise
    return [Path(p).stem for p in entries if str(p).endswith(".jsonl")]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _local_away(slug: str) -> dict[str, str]:
    """Return ``{session_id: project}`` for sessions handed off from this directory.

    Read from the ``<id>.away.json`` markers push leaves behind.
    """
    out: dict[str, str] = {}
    d = _CLAUDE_PROJECTS / slug
    if d.is_dir():
        for m in d.glob("*.away.json"):
            with contextlib.suppress(Exception):
                sid = m.name[: -len(".away.json")]
                out[sid] = json.loads(m.read_text()).get("project", "?")
    return out


def _held_open(path: Path) -> bool | None:
    """Whether a live process holds ``path`` open, or None when undeterminable.

    Asks ``lsof -t`` (pids only, quiet). None when ``lsof`` is absent or fails,
    so the caller can fall back to its mtime heuristic; an empty pid list means
    definitively "no process has it open".
    """
    lsof = shutil.which("lsof")
    if not lsof:
        return None
    try:
        proc = subprocess.run(
            [lsof, "-t", "--", str(path)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return bool(proc.stdout.strip())


def _is_active_session(jsonl: Path) -> bool:
    """Heuristic for "the session you are currently in".

    Active when it is the newest transcript written within the last two minutes,
    or when a live process still holds it open (``lsof``) regardless of recency.
    The open-fd check is what keeps push from renaming a ``claude`` session out
    from under the running process, newest or not; the mtime window covers hosts
    without ``lsof``. When active, the baton is recorded but the local copy is
    left in place.
    """
    proj_dir = jsonl.parent
    newest = max(
        proj_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, default=None
    )
    if newest == jsonl and (time.time() - jsonl.stat().st_mtime) < 120:
        return True
    return _held_open(jsonl) is True


def _write_baton(
    dataset_api, dest_dir: str, sid: str, holder: str, prev_holder: str, lines: int
) -> bool:
    """Write the baton sidecar recording where a session's canonical copy lives.

    Both the laptop CLI and the pod read it; it is the commit point of a
    hand-off. Returns False on failure instead of raising: the caller decides
    how loud to be (the local state it just wrote is valid either way), but a
    silent drop must not masquerade as a recorded hand-off.
    """
    baton = {
        "session_id": sid,
        "holder": holder,
        "since": _now(),
        "prev_holder": prev_holder,
        "transferred_lines": lines,
    }
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / f"{sid}.baton.json"
        p.write_text(json.dumps(baton))
        try:
            dataset_api.upload(local_path=str(p), upload_path=dest_dir, overwrite=True)
        except Exception:  # noqa: BLE001 - caller warns; local state is already valid
            return False
    return True


def _read_remote_json(dataset_api, remote_path: str) -> dict | None:
    """Download a small JSON sidecar from HopsFS and parse it, or None.

    None means the file is absent or unreadable; callers treat that as "no such
    sidecar", not an error.
    """
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "sidecar.json"
        with contextlib.suppress(Exception):
            dataset_api.download(remote_path, local_path=str(local), overwrite=True)
            return json.loads(local.read_text())
    return None


def _read_baton(dataset_api, dest_dir: str, sid: str) -> dict | None:
    """Return the baton for ``sid`` staged under ``dest_dir``, or None.

    None means the baton is genuinely absent (a ``--fork`` push, or a legacy
    transfer), which the caller treats as batonless copy semantics. Any other
    failure (auth, network, corrupt JSON) raises: degrading it to "no baton"
    would silently bypass the ownership gate that the baton exists to enforce.
    """
    remote = f"{dest_dir}/{sid}.baton.json"
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "baton.json"
        try:
            dataset_api.download(remote, local_path=str(local), overwrite=True)
        except RestAPIError as exc:
            if _is_missing_path(exc):
                return None
            raise click.ClickException(
                f"Could not read the session baton: {exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not read the session baton: {exc}"
            ) from exc
        try:
            return json.loads(local.read_text())
        except ValueError as exc:
            raise click.ClickException(
                f"The baton for {sid} is unreadable; refusing to guess ownership. "
                f"Inspect or remove {remote} in the teleport store."
            ) from exc


def _pod_alive(project_id: int) -> bool:
    """Whether the pod still holds a live terminal session for this project.

    ``GET /terminal/session`` answers 200 for an idle terminal too, with a
    descriptor whose ``running`` is false — only that flag distinguishes a live
    pod from a stopped one. A raised call (feature disabled, network) is treated
    as alive on purpose: liveness we cannot confirm must not silently authorise
    stealing the baton, so an unknown state still forces ``--force``.
    """
    try:
        sess = terminal_api.get_session(project_id)
    except Exception:  # noqa: BLE001 - unknown liveness is fail-safe "alive"
        return True
    return bool(sess and sess.get("running"))


def _transcript_relation(local: list[str], remote: list[str], baseline: int) -> str:
    """Classify how two append-only transcripts relate.

    A Claude Code JSONL only ever grows (lines are appended, never rewritten),
    so the relationship is decided by the common line prefix. ``baseline`` is
    the line count recorded in the baton at the last hand-off.

    Returns one of ``same``, ``fast_forward`` (local is a strict prefix of
    remote, take remote), ``local_ahead`` (remote is a strict prefix of local,
    keep local), ``baseline_mismatch`` (they already differ inside the handed-off
    prefix, so they are not one lineage), or ``diverged`` (shared prefix past the
    baseline, tails differ).
    """
    common = 0
    for a, b in zip(local, remote, strict=False):
        if a != b:
            break
        common += 1
    if common == len(local) == len(remote):
        return "same"
    if common == len(local):
        return "fast_forward"
    if common == len(remote):
        return "local_ahead"
    if common < baseline:
        return "baseline_mismatch"
    return "diverged"


def _stamp() -> str:
    """Compact UTC timestamp for parked-transcript sidecar filenames."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _terminal_ui_url(project_id: int) -> str:
    """Deep link that opens the terminal dock on the project's page.

    The web terminal is a route-less dock panel; the front-end reads the
    ``?terminal=open`` query param on load and opens it, so this is the URL
    that lands a teleported session straight in the terminal.
    """
    base = client._get_instance()._base_url.rstrip("/")
    return f"{base}/p/{project_id}?terminal=open"


def _build_manifest(
    session_id: str,
    slug: str,
    mode: str,
    model: str | None,
    prompt: str | None = None,
    git: dict | None = None,
) -> dict:
    """The manifest the pod's landing hook reads to self-resume.

    Carries the original cwd (the slug alone cannot reconstruct it, and the pod
    recreates a path that hashes to the same slug for ``claude --resume``), the
    ``mode`` (``push`` / ``fork`` / ``new``), and an optional ``prompt`` the pod
    feeds to ``claude`` as the session's first instruction.
    """
    manifest = {
        "session_id": session_id,
        "slug": slug,
        "cwd": str(Path.cwd()),
        "host": socket.gethostname(),
        "pushed_at": _now(),
        "mode": mode,
        "model": model,
        "prompt": prompt,
    }
    # Present only when the git-sync gates and consent passed: the pod's landing
    # hook checks the repo out before it resumes the session (HWORKS-3147).
    if git:
        manifest["git"] = git
    return manifest


def _upload_manifest(
    dataset_api, dest_dir: str, session_id: str, manifest: dict
) -> None:
    """Upload the teleport manifest — the last write of a push/new.

    Not best-effort: the manifest is the pod watcher's trigger, so a silent drop
    would stage a session that never lands. A failure is surfaced loudly.

    A prior land's ``.consumed`` marker and ``landed.json`` ack are deliberately
    NOT deleted here. Every marker is versioned by the manifest's ``pushed_at``
    (the pod skips a manifest only when the marker records the same push, and
    ``_await_landing`` accepts only an ack echoing this push), so a re-push
    invalidates them without any delete. Deleting them out-of-band would also
    be a lost-write trap: the pod's FUSE mount caches directory entries, and a
    marker the pod writes into a name the CLI just deleted server-side can land
    in the stale unlinked inode and never become visible.
    """
    with tempfile.TemporaryDirectory() as tmp:
        mpath = Path(tmp) / f"{session_id}.teleport.json"
        mpath.write_text(json.dumps(manifest))
        try:
            dataset_api.upload(
                local_path=str(mpath), upload_path=dest_dir, overwrite=True
            )
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Failed to upload teleport manifest: {exc}"
            ) from exc


# How long push/new waits for the pod's landing ack before falling back to the
# manual landing steps.
_LANDING_POLLS = 8
_LANDING_POLL_SECONDS = 2.0


def _landed(dataset_api, dest_dir: str, session_id: str, pushed_at: str) -> bool:
    """Whether the pod has acked THIS push's landing.

    The ack must echo this manifest's ``pushed_at``: acks are never deleted, so
    a leftover from an earlier land of the same session id would otherwise read
    as an instant (false) landing on a re-push.
    """
    ack = _read_remote_json(dataset_api, f"{dest_dir}/{session_id}.landed.json")
    return bool(ack) and ack.get("pushed_at") == pushed_at


def _await_landing(dataset_api, dest_dir: str, session_id: str, pushed_at: str) -> bool:
    """Poll for the pod's ack of this push, for roughly fifteen seconds.

    The pod's landing hook writes the sidecar right after it resumes (or opens)
    the session; seeing it means the manual landing kit is unnecessary.
    """
    for attempt in range(_LANDING_POLLS):
        if attempt:
            time.sleep(_LANDING_POLL_SECONDS)
        if _landed(dataset_api, dest_dir, session_id, pushed_at):
            return True
    return False


def _launch_pod(project) -> str | None:
    """Start (or reuse) the project's terminal pod; return its ``wsUrl``.

    Best-effort: the terminal feature may be disabled on the cluster, in which
    case we warn and return None (the caller still stages the session and prints
    the manual landing path).
    """
    try:
        started = terminal_api.start_session(project.id)
        output.success("✓ Terminal pod ready for %s", project.name)
        return (started or {}).get("wsUrl")
    except Exception as exc:  # noqa: BLE001 - feature may be disabled on cluster
        output.warn(
            "Terminal pod not started (%s). Open the Terminal tab in the "
            "Hopsworks UI to start it.",
            exc,
        )
        return None


def _scan_slugs(dataset_api) -> list[str]:
    """Return every directory slug staged under the teleport dataset.

    Each push/new writes under ``Users/<username>/teleport/<slug>/``; this lists
    those subdirectories so cross-directory commands (``list --all``, ``pull
    <id>``) can reach sessions this user staged from a different working dir.
    """
    root = _teleport_root()
    try:
        entries = dataset_api.list(root)
    except RestAPIError as exc:
        # No teleport root yet just means nothing was ever staged; any other
        # failure (auth, network) surfaces instead of reading as "no sessions".
        if _is_missing_path(exc):
            return []
        raise
    return [Path(p.rstrip("/")).name for p in entries]


def _locate_session(dataset_api, session_id: str) -> tuple[str, str | None] | None:
    """Find which slug holds ``session_id`` across this user's staged sessions.

    Returns ``(slug, origin_cwd)`` for the first slug that stages this id, with
    ``origin_cwd`` read from its manifest when present, or None when no staged
    slug holds it.
    """
    for slug in _scan_slugs(dataset_api):
        if session_id in _remote_session_ids(dataset_api, slug):
            manifest = _read_remote_json(
                dataset_api,
                f"{_teleport_root()}/{slug}/{session_id}.teleport.json",
            )
            return slug, (manifest or {}).get("cwd")
    return None


@click.group(
    "session",
    epilog="Example: hops session push  →  work in the browser Terminal tab  →  "
    "hops session pull",
)
def session_group() -> None:
    """Move a Claude Code session between this machine and a terminal pod."""


@session_group.command("push")
@click.argument("session_id", required=False)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite a session JSONL already staged for this slug.",
)
@click.option(
    "--fork",
    is_flag=True,
    help="Copy the session instead of handing it off: the local copy stays "
    "canonical, no baton is written. Default is a baton hand-off.",
)
@click.option(
    "--model",
    help="Model the pod should resume the session with (passed to "
    "`claude --resume --model`).",
)
@click.option(
    "--prompt",
    help="An instruction to feed the resumed session as its first input "
    "(passed to `claude` as the prompt).",
)
@click.option(
    "--open/--no-open",
    "open_ui",
    default=True,
    help="Open the terminal in the browser after pushing (default). "
    "--no-open just prints the URL.",
)
@click.pass_context
def push(
    ctx: click.Context,
    session_id: str | None,
    overwrite: bool,
    fork: bool,
    model: str | None,
    prompt: str | None,
    open_ui: bool,
) -> None:
    """Push the current Claude Code session onto a Hopsworks terminal pod.

    Resolves the active session for this directory (or the ``SESSION_ID`` given
    as an argument), uploads its transcript into the project's HopsFS, starts
    the terminal pod (when the feature is enabled on the cluster), and opens the
    terminal in the browser. By default this is a baton hand-off: the pod
    becomes the canonical copy and the local transcript is renamed aside (unless
    it is the session you are currently in, which stays live locally). `--fork`
    keeps a live local copy instead.

    Args:
        ctx: Click context.
        session_id: Explicit session id, or None for the active one.
        overwrite: Re-upload even if a JSONL for this slug already exists.
        fork: Copy instead of hand off; leave the local session canonical.
        model: Model the pod resumes with.
        prompt: First instruction fed to the resumed session, or None.
        open_ui: Open the terminal in the browser after pushing.
    """
    slug = _cwd_slug()
    jsonl = _resolve_local_session(slug, session_id)
    resolved_id = jsonl.stem

    project = conn.get_project(ctx)
    dataset_api = project.get_dataset_api()

    # Git sync runs first: it is the interactive part (consent, key prompt,
    # commit+push offer), so it must finish before any store mutation. A None
    # simply means no git context travels with this push.
    git_ctx = git_sync.maybe_collect(dataset_api, _teleport_root().rsplit("/", 1)[0])

    dest_dir = f"{_teleport_root()}/{slug}"
    # mkdir is best-effort: an existing dir is fine, and a real problem will
    # surface loudly on the upload below.
    with contextlib.suppress(Exception):
        dataset_api.mkdir(dest_dir)
    # Upload a snapshot copy, not the live file: the session may be the one the
    # user is in, still being appended to. The baton's transferred_lines must
    # count exactly the bytes that were shipped — counting the live file after
    # the upload could include lines the pod never received, and pull would then
    # misread a plain divergence as "no common history" (common < baseline).
    with tempfile.TemporaryDirectory() as tmp:
        snapshot = Path(tmp) / jsonl.name
        shutil.copyfile(jsonl, snapshot)
        with snapshot.open(errors="ignore") as f:
            lines = sum(1 for _ in f)
        try:
            dataset_api.upload(
                local_path=str(snapshot), upload_path=dest_dir, overwrite=overwrite
            )
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc):
                raise click.ClickException(
                    "Session already staged for this directory — pass --overwrite "
                    "to replace it."
                ) from exc
            raise click.ClickException(f"Failed to ship session: {exc}") from exc
    output.success("✓ Pushed session %s to %s", resolved_id, project.name)

    # Baton hand-off: record that the canonical copy now lives on the pod. Skip
    # for --fork (deliberate copy). The local rename-aside happens only after
    # the manifest upload succeeds, so a failed push never strands the session
    # (renamed away locally, yet never going to land).
    host = socket.gethostname()
    if not fork and not _write_baton(
        dataset_api,
        dest_dir,
        resolved_id,
        holder=f"pod:{project.name}",
        prev_holder=f"laptop:{host}",
        lines=lines,
    ):
        output.warn(
            "Could not record the baton hand-off; the store does not name the "
            "pod as holder, so a pull from another machine will treat this as "
            "an unowned copy."
        )

    ws_url = _launch_pod(project)

    # Manifest LAST: it is the pod watcher's trigger, so everything it depends on
    # (transcript, baton) must already be staged when it appears. The upload is a
    # hard error, not best-effort — a dropped manifest strands the session
    # (staged, but never landed).
    manifest = _build_manifest(
        resolved_id, slug, "fork" if fork else "push", model, prompt, git=git_ctx
    )
    _upload_manifest(dataset_api, dest_dir, resolved_id, manifest)

    # Only now that the hand-off is fully staged, rename the local transcript
    # aside so it is not resumed by accident — except the session you are in
    # (its file is held open by a live `claude`), which stays live locally.
    if not fork:
        if _is_active_session(jsonl):
            output.warn(
                "This looks like the session you are in, so it stays live on "
                "this machine. The baton points to %s; close it here, then "
                "`hops session stop && hops session pull` to reclaim it "
                "(the pod holds it live, so a plain pull would refuse).",
                project.name,
            )
        else:
            away = jsonl.parent / (jsonl.name + ".away")
            with contextlib.suppress(Exception):
                jsonl.rename(away)
                (jsonl.parent / f"{resolved_id}.away.json").write_text(
                    json.dumps(
                        {"project": project.name, "host": host, "pushed_at": _now()}
                    )
                )

    pod_session_dir = f"~/.claude/projects/{slug}"
    pod_jsonl = f"/hopsfs/{_teleport_root()}/{slug}/{resolved_id}.jsonl"
    landing = [
        f"mkdir -p {pod_session_dir}",
        f"cp {pod_jsonl} {pod_session_dir}/",
        f"claude --resume {resolved_id}",
    ]

    terminal_url = _terminal_ui_url(project.id)
    # Open the browser only in interactive use: JSON mode is for machines, so it
    # just carries the URL. Best-effort — a headless box has no browser.
    opened = False
    if open_ui and not output.JSON_MODE:
        with contextlib.suppress(Exception):
            opened = webbrowser.open(terminal_url)

    if output.JSON_MODE:
        # No poll in machine mode: one instant ack check, the caller re-lists
        # if it wants to wait.
        landed = _landed(dataset_api, dest_dir, resolved_id, manifest["pushed_at"])
        output.print_json(
            {
                "session_id": resolved_id,
                "slug": slug,
                "project": project.name,
                "shipped_to": f"{dest_dir}/{resolved_id}.jsonl",
                "ws_url": ws_url,
                "terminal_url": terminal_url,
                "landed": landed,
                "landing_steps": landing,
            }
        )
        return

    if not opened:
        output.info("Terminal: %s", terminal_url)
    # The landing kit only matters when the pod did NOT self-land, so wait
    # briefly for its ack and keep the happy path to the three ✓ lines. No pod
    # (feature disabled) means no ack will ever come: skip straight to the kit.
    if ws_url and _await_landing(
        dataset_api, dest_dir, resolved_id, manifest["pushed_at"]
    ):
        output.success(
            "✓ Landed on pod %s — open the Terminal tab to resume", project.name
        )
    else:
        output.info("Not landed yet — the pod will pick it up, or land it manually:")
        for step in landing:
            output.info("  %s", step)


@session_group.command("new")
@click.option(
    "--model",
    help="Model the pod should start Claude with (passed to `claude --model`).",
)
@click.option(
    "--prompt",
    help="An instruction to start the session on (passed to `claude` as the "
    "prompt), so the session begins on its task without typing into the tab.",
)
@click.option(
    "--open/--no-open",
    "open_ui",
    default=True,
    help="Open the terminal in the browser after starting (default). "
    "--no-open just prints the URL.",
)
@click.pass_context
def new(
    ctx: click.Context, model: str | None, prompt: str | None, open_ui: bool
) -> None:
    """Start a fresh Claude Code session directly on a terminal pod.

    Unlike ``push`` there is nothing to ship: this stages a ``mode=new`` manifest
    (no transcript) so the pod's landing hook opens a brand-new ``claude`` in a
    directory that hashes to this one's slug, then opens the terminal in the
    browser. Use it to begin work on the pod straight from the laptop. ``--prompt``
    seeds the session with a first instruction, so several `new --prompt` calls
    launch parallel sessions each already working on its own task.

    Args:
        ctx: Click context.
        model: Model the pod starts Claude with.
        prompt: First instruction the session starts on, or None.
        open_ui: Open the terminal in the browser after starting.
    """
    slug = _cwd_slug()
    session_id = str(uuid.uuid4())

    project = conn.get_project(ctx)
    dataset_api = project.get_dataset_api()

    dest_dir = f"{_teleport_root()}/{slug}"
    with contextlib.suppress(Exception):
        dataset_api.mkdir(dest_dir)

    ws_url = _launch_pod(project)

    manifest = _build_manifest(session_id, slug, "new", model, prompt)
    _upload_manifest(dataset_api, dest_dir, session_id, manifest)
    output.success("✓ Staged a new session for %s", project.name)

    terminal_url = _terminal_ui_url(project.id)
    opened = False
    if open_ui and not output.JSON_MODE:
        with contextlib.suppress(Exception):
            opened = webbrowser.open(terminal_url)

    if output.JSON_MODE:
        # No poll in machine mode: one instant ack check, the caller re-lists
        # if it wants to wait.
        landed = _landed(dataset_api, dest_dir, session_id, manifest["pushed_at"])
        output.print_json(
            {
                "session_id": session_id,
                "slug": slug,
                "project": project.name,
                "mode": "new",
                "ws_url": ws_url,
                "terminal_url": terminal_url,
                "landed": landed,
            }
        )
        return
    if not opened:
        output.info("Terminal: %s", terminal_url)
    if ws_url and _await_landing(
        dataset_api, dest_dir, session_id, manifest["pushed_at"]
    ):
        output.success(
            "✓ Landed on pod %s — open the Terminal tab to resume", project.name
        )
    else:
        output.info(
            "Not landed yet — a fresh Claude session will open in the terminal shortly."
        )


@session_group.command("pull")
@click.argument("session_id", required=False)
@click.option(
    "--ours",
    is_flag=True,
    help="On divergence, keep the local transcript (the pod's copy is parked "
    "aside, never lost).",
)
@click.option(
    "--theirs",
    is_flag=True,
    help="On divergence, take the pod's transcript (your local copy is parked "
    "aside, never lost).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Steal the baton from a live pod or another machine. Without it, "
    "pull refuses to reclaim a session a live holder is still writing.",
)
@click.pass_context
def pull(
    ctx: click.Context, session_id: str | None, ours: bool, theirs: bool, force: bool
) -> None:
    """Pull a session staged in HopsFS back onto this machine and take the baton.

    Downloads the transcript from ``Users/<username>/teleport/<slug>/`` into
    ``~/.claude/projects/<slug>/`` and prints the resume command. Given a
    ``SESSION_ID`` the source slug is found across your staged sessions, so you
    can pull a session staged from another directory from anywhere; resume still
    needs a path that hashes to that slug, so the original cwd is printed to
    ``cd`` into. Without an id, pulls this directory's single staged session.

    Reclaiming is baton-aware. If the pod still holds a live terminal session,
    pull refuses unless ``--force`` (you would be stealing from a process that
    is still writing). If both sides advanced the transcript since the hand-off,
    pull refuses until you pick ``--ours`` or ``--theirs``; the losing side is
    parked to a sidecar, never destroyed. A successful pull flips the baton to
    this machine as the very last step, so an interrupted pull leaves the pod
    canonical rather than orphaning the claim here.

    Args:
        ctx: Click context.
        session_id: Explicit session id (located project-wide), or None to pick
            this directory's only staged one.
        ours: On divergence, keep local.
        theirs: On divergence, take the pod's copy.
        force: Steal the baton from a live pod or another machine.
    """
    if ours and theirs:
        raise click.ClickException("--ours and --theirs are mutually exclusive.")

    slug = _cwd_slug()
    project = conn.get_project(ctx)
    dataset_api = project.get_dataset_api()

    # A positional id may name a session staged from another directory: find its
    # slug project-wide so `pull <id>` works from anywhere. Without an id we pull
    # this directory's single staged session.
    origin_cwd: str | None = None
    if session_id:
        located = _locate_session(dataset_api, session_id)
        if located:
            slug, origin_cwd = located
    else:
        staged = _remote_session_ids(dataset_api, slug)
        if not staged:
            raise click.ClickException(
                "No sessions staged for this directory. Sessions are staged "
                "per directory — run from the directory you pushed from, or "
                "`hops session list --all`."
            )
        if len(staged) > 1:
            raise click.ClickException(
                "Multiple sessions staged for this directory; pass a session id "
                "to choose one: " + ", ".join(staged)
            )
        session_id = staged[0]

    dest_dir = f"{_teleport_root()}/{slug}"
    remote_jsonl = f"{dest_dir}/{session_id}.jsonl"
    local_dir = _CLAUDE_PROJECTS / slug
    local_dir.mkdir(parents=True, exist_ok=True)
    local_jsonl = local_dir / f"{session_id}.jsonl"
    away = local_dir / f"{session_id}.jsonl.away"

    baton = _read_baton(dataset_api, dest_dir, session_id)
    host = socket.gethostname()
    me = f"laptop:{host}"

    # --- Ownership gate: --force is required only to take the baton from a
    # holder that may still be writing (a live pod, or another laptop we have
    # no liveness oracle for). A dead pod, or a baton we already hold, is a
    # frictionless reclaim.
    if baton:
        holder = baton.get("holder", "")
        if holder.startswith("pod:"):
            if _pod_alive(project.id) and not force:
                raise click.ClickException(
                    f"The pod still holds a live terminal session for "
                    f"{holder}. Close it there first, or `pull --force` to take "
                    f"the baton anyway (the pod's later writes become orphans)."
                )
        elif holder.startswith("laptop:") and holder != me and not force:
            raise click.ClickException(
                f"Another machine holds this session ({holder}). "
                f"`pull --force` to steal the baton."
            )

    # --- Download the remote transcript to a scratch file so we can compare
    # before touching anything local.
    with tempfile.TemporaryDirectory() as tmp:
        scratch = Path(tmp) / f"{session_id}.jsonl"
        try:
            dataset_api.download(remote_jsonl, local_path=str(scratch), overwrite=True)
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Failed to pull session: {exc}") from exc
        remote_text = scratch.read_text(errors="ignore")
    remote_lines = remote_text.splitlines()

    # The local candidate is the resumable copy if present, else the transcript
    # push renamed aside on hand-off.
    local_src = (
        local_jsonl if local_jsonl.is_file() else away if away.is_file() else None
    )
    local_text = local_src.read_text(errors="ignore") if local_src else ""
    local_lines = local_text.splitlines() if local_src else None

    # --- Decide which content wins (axis: divergence).
    parked: Path | None = None
    take_remote: bool
    if baton is None:
        # Fork / legacy: a batonless copy. Overwrite only when told to, and
        # never mint a baton (the --fork pusher opted out of ownership).
        if local_lines is not None and local_lines != remote_lines and not theirs:
            raise click.ClickException(
                "A different local copy of this session exists. `pull --theirs` "
                "to replace it (your copy is parked aside)."
            )
        if theirs and local_src and local_lines != remote_lines:
            parked = local_dir / f"{session_id}.jsonl.diverged-{_stamp()}"
        take_remote = True
    else:
        baseline = baton.get("transferred_lines") or 0
        if local_lines is None:
            take_remote = True
        else:
            rel = _transcript_relation(local_lines, remote_lines, baseline)
            if rel == "same":
                take_remote = False
            elif rel == "fast_forward":
                take_remote = True
            elif rel == "local_ahead":
                take_remote = False
            elif rel == "diverged":
                if not (ours or theirs):
                    raise click.ClickException(
                        f"Local and pod transcripts diverged since the hand-off "
                        f"(local +{len(local_lines) - baseline}, pod "
                        f"+{len(remote_lines) - baseline} lines). Re-run with "
                        f"--ours to keep yours or --theirs to take the pod's; "
                        f"the other side is parked aside, never lost."
                    )
                take_remote = bool(theirs)
                parked = local_dir / (
                    f"{session_id}.jsonl.diverged-{_stamp()}"
                    if theirs
                    else f"{session_id}.jsonl.remote-{_stamp()}"
                )
            else:  # baseline_mismatch
                if not theirs:
                    raise click.ClickException(
                        "Local and pod transcripts share no common history "
                        "(they differ within the handed-off prefix). `pull "
                        "--theirs` overwrites with the pod's copy; your local "
                        "copy is parked aside."
                    )
                take_remote = True
                parked = local_dir / f"{session_id}.jsonl.diverged-{_stamp()}"

    # --- Apply the decision, then flip the baton LAST (so a crash before the
    # flip leaves the pod canonical instead of orphaning the claim here).
    # The parked sidecar always holds the losing side: local when we take the
    # pod's copy, the pod's copy when we keep local.
    if parked is not None:
        parked.write_text(local_text if take_remote else remote_text)
    if take_remote:
        local_jsonl.write_text(remote_text)
    elif local_src == away:
        away.rename(local_jsonl)

    # The session is resumable locally again: drop the away markers push left.
    for marker in (away, local_dir / f"{session_id}.away.json"):
        with contextlib.suppress(FileNotFoundError):
            marker.unlink()

    manifest: dict = {}
    if baton is not None:
        with local_jsonl.open(errors="ignore") as f:
            final_lines = sum(1 for _ in f)
        if not _write_baton(
            dataset_api,
            dest_dir,
            session_id,
            holder=me,
            prev_holder=baton.get("holder", "?"),
            lines=final_lines,
        ):
            output.warn(
                "The transcript was pulled, but the baton could not be updated: "
                "the store still names %s as holder, so the next pull may "
                "demand --force. Re-run pull to retry the hand-off.",
                baton.get("holder", "?"),
            )
        # A manifest that never landed must not land after this reclaim: a later
        # pod boot would otherwise resume the stale staged copy as a ghost tab.
        # Consuming is how the pod marks "do not land"; mirror it from here with
        # the manifest's own pushed_at as the marker content — markers are
        # versioned by push, and an empty marker would block every future
        # re-push of this id, not just the reclaimed one. An unreadable manifest
        # skips the stamp (nothing to version against). Best-effort, like the
        # baton.
        manifest = (
            _read_remote_json(dataset_api, f"{dest_dir}/{session_id}.teleport.json")
            or {}
        )
        pushed_at = manifest.get("pushed_at")
        if pushed_at:
            with tempfile.TemporaryDirectory() as tmp:
                marker = Path(tmp) / f"{session_id}.teleport.json.consumed"
                marker.write_text(pushed_at)
                with contextlib.suppress(Exception):
                    dataset_api.upload(
                        local_path=str(marker), upload_path=dest_dir, overwrite=True
                    )

    output.success("✓ Pulled session %s to %s", session_id, local_dir)
    if parked is not None:
        output.info("Parked the other side at %s", parked.name)
    # The repo itself is not file transport: with git sync, pod work reaches the
    # laptop through the remote, and only the transcript rides the store.
    if manifest.get("git"):
        output.info(
            "This session synced git state. Pod work flows back through git — "
            "commit and push on the pod, then `git pull` here."
        )

    resume = f"claude --resume {session_id}"
    # The transcript landed under its origin slug; resume only resolves it from a
    # path that hashes to that slug, so when we pulled a session from another
    # directory point the user at its original cwd instead of "here".
    elsewhere = slug != _cwd_slug()
    if output.JSON_MODE:
        output.print_json(
            {
                "session_id": session_id,
                "slug": slug,
                "project": project.name,
                "pulled_to": str(local_jsonl),
                "took": "remote" if take_remote else "local",
                "parked": parked.name if parked else None,
                "baton": me if baton is not None else None,
                "origin_cwd": origin_cwd,
                "resume_step": resume,
            }
        )
        return
    output.info("")
    if elsewhere:
        output.info("This session came from another directory. Resume it from:")
        if origin_cwd:
            output.info("  cd %s && %s", origin_cwd, resume)
        else:
            output.info("  a path whose slug is %s, then: %s", slug, resume)
    else:
        output.info("Resume it here with:")
        output.info("  %s", resume)


def _list_all(dataset_api, project) -> None:
    """Print every session this user has teleported, across all slugs.

    Walks each slug subdirectory under the user's private teleport home and
    reads each manifest for the originating cwd, so a machine that never staged
    a given session can still see it and where it came from.
    """
    rows = []
    for slug in sorted(_scan_slugs(dataset_api)):
        for sid in sorted(_remote_session_ids(dataset_api, slug)):
            manifest = _read_remote_json(
                dataset_api, f"{_teleport_root()}/{slug}/{sid}.teleport.json"
            )
            rows.append(
                {
                    "session_id": sid,
                    "slug": slug,
                    "cwd": (manifest or {}).get("cwd"),
                    "mode": (manifest or {}).get("mode"),
                }
            )
    if output.JSON_MODE:
        output.print_json({"project": project.name, "sessions": rows})
        return
    if not rows:
        output.info("No teleported sessions in project %s.", project.name)
        return
    output.info("Teleported sessions in %s:", project.name)
    for r in rows:
        output.info("  %-40s %s", r["session_id"], r["cwd"] or r["slug"])


@session_group.command("list")
@click.option(
    "--all",
    "all_slugs",
    is_flag=True,
    help="List every session you have teleported in this project, across all "
    "directories, not just this one.",
)
@click.pass_context
def list_sessions(ctx: click.Context, all_slugs: bool) -> None:
    """List sessions for this directory and where each one lives.

    Shows the session ids present locally under ``~/.claude/projects/<slug>/``
    and those staged in your private teleport home, so you can tell what is
    here, what is on the pod side, and what exists in both places. ``--all``
    widens the view to every session you have teleported, across all directories.

    Args:
        ctx: Click context.
        all_slugs: List across all your directories instead of just this one.
    """
    project = conn.get_project(ctx)
    dataset_api = project.get_dataset_api()

    if all_slugs:
        _list_all(dataset_api, project)
        return

    slug = _cwd_slug()
    local_dir = _CLAUDE_PROJECTS / slug
    local_ids = (
        sorted(p.stem for p in local_dir.glob("*.jsonl"))
        if (local_dir.is_dir())
        else []
    )
    away = _local_away(slug)

    remote_ids = sorted(_remote_session_ids(dataset_api, slug))

    def _where(sid: str) -> str:
        if sid in away:
            return f"away → {away[sid]}"
        here, there = sid in local_ids, sid in remote_ids
        return "local+remote" if here and there else "local" if here else "remote"

    all_ids = sorted(set(local_ids) | set(remote_ids) | set(away))
    if output.JSON_MODE:
        output.print_json(
            {
                "slug": slug,
                "project": project.name,
                "sessions": [{"session_id": s, "where": _where(s)} for s in all_ids],
            }
        )
        return
    if not all_ids:
        output.info(
            "No sessions for this directory. Sessions are staged per directory "
            "— run from the directory you pushed from, or `hops session list "
            "--all`."
        )
        return
    output.info("Sessions for this directory:")
    for sid in all_ids:
        output.info("  %-40s %s", sid, _where(sid))


@session_group.command("stop")
@click.pass_context
def stop(ctx: click.Context) -> None:
    """Stop this project's terminal pod (and every session tab in it).

    Closes the running terminal from the laptop, so you never need Kubernetes
    access to shut it down. A no-op when nothing is running.

    Args:
        ctx: Click context.
    """
    project = conn.get_project(ctx)

    # A failed check must not read as "nothing running": surface it instead of
    # skipping the stop on an auth/network error.
    try:
        running = terminal_api.get_session(project.id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not check the terminal session: {exc}"
        ) from exc
    # An idle terminal still answers 200 with a running=false descriptor.
    if not (running and running.get("running")):
        if output.JSON_MODE:
            output.print_json({"project": project.name, "stopped": False})
            return
        output.info("No terminal running for %s.", project.name)
        return

    try:
        terminal_api.stop_session(project.id)
    except Exception as exc:  # noqa: BLE001 - feature may be disabled on cluster
        raise click.ClickException(f"Failed to stop the terminal: {exc}") from exc

    if output.JSON_MODE:
        output.print_json({"project": project.name, "stopped": True})
        return
    output.success("✓ Stopped the terminal for %s", project.name)


# Detach key for `session mirror`: Ctrl-] (GS, 0x1d), the telnet/ssh escape.
_MIRROR_DETACH = b"\x1d"
# Bounded reconnects when the WebSocket drops (proxy idle timeout, token expiry).
_MIRROR_MAX_RETRIES = 5


class _Detach(Exception):
    """Raised to unwind the mirror loop when the user presses the detach key."""


def _ws_url(ws_path: str) -> str:
    """Build the ``wss://`` terminal URL from the REST ``wsUrl`` path.

    The session descriptor carries a host-relative path (``/hopsworks-api/
    terminal/<id>/ws``); the WebSocket host is the same as the REST client's.
    """
    base = client._get_instance()._base_url  # e.g. https://host:port
    scheme, rest = base.split("://", 1)
    ws_scheme = "wss" if scheme == "https" else "ws"
    return f"{ws_scheme}://{rest}{ws_path}"


def _ws_ssl_context(url: str) -> ssl.SSLContext | None:
    """SSL context for the ``wss`` leg, mirroring the REST client's verify mode.

    Returns None for a plain ``ws://`` URL. Honors the client's ``_verify``:
    False disables verification (``--no-verify``), a path loads it as the CA
    bundle, True keeps the system default.
    """
    if not url.startswith("wss://"):
        return None
    ctx = ssl.create_default_context()
    verify = client._get_instance()._verify
    if verify is False:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    elif isinstance(verify, str):
        ctx.load_verify_locations(verify)
    return ctx


def _maybe_warn_size(msg: dict, warned: list[bool]) -> None:
    """Warn once (to stderr) when the pod session is larger than the local tty.

    A mirror never resizes the shared session, so if the writer's terminal is
    bigger than ours the output wraps. We say so once instead of silently
    rendering garbled lines.
    """
    if warned[0]:
        return
    cols, rows = shutil.get_terminal_size((80, 24))
    scols, srows = int(msg.get("cols", 0)), int(msg.get("rows", 0))
    if scols > cols or srows > rows:
        warned[0] = True
        sys.stderr.write(
            f"\r\n[mirror] session is {scols}x{srows}, your terminal is "
            f"{cols}x{rows}; output may wrap. Resize for a clean view.\r\n"
        )
        sys.stderr.flush()


async def _pump(ws, warned: list[bool]) -> None:
    """Stream server frames to stdout until the socket closes or the shell exits.

    Output frames are written raw (they carry ANSI); ``size`` drives the wrap
    warning; every other control frame (``clients``/``windows``/``pong``) is
    ignored — a mirror only renders the byte stream.
    """
    async for message in ws:
        frame = (
            message if isinstance(message, str) else message.decode("utf-8", "replace")
        )
        try:
            msg = json.loads(frame)
        except ValueError:
            sys.stdout.buffer.write(frame.encode("utf-8", "replace"))
            sys.stdout.buffer.flush()
            continue
        kind = msg.get("type")
        if kind == "output":
            sys.stdout.buffer.write(msg.get("data", "").encode("utf-8", "replace"))
            sys.stdout.buffer.flush()
        elif kind == "size":
            _maybe_warn_size(msg, warned)
        elif kind == "exit":
            break


async def _drive(ws, mode: str) -> None:
    """Run one attached mirror session: init, pump output, forward input.

    Sends the init frame (size + role), then races the output pump against the
    detach key. Whenever a raw tty is available the stdin reader is registered
    so Ctrl-] detaches in both modes; in ``ro`` mode every other keystroke is
    discarded, in ``rw`` mode the bytes are forwarded as input frames. The tty
    is always restored on exit.

    In ``rw`` the mirror also drives the shared window's geometry: the init
    frame is ignored when attaching an existing session, so it pushes this
    terminal's size explicitly on attach and again on every SIGWINCH, keeping
    the view rendered at the local size instead of inheriting the driver's.
    ``ro`` never resizes the session (the writer's terminal governs the grid).
    """
    cols, rows = shutil.get_terminal_size((80, 24))
    await ws.send(json.dumps({"cols": cols, "rows": rows, "mode": mode}))

    loop = asyncio.get_running_loop()
    detached = asyncio.Event()
    warned = [False]
    fd = sys.stdin.fileno()
    old_attrs = None
    reader_added = False

    has_tty = termios is not None and sys.stdin.isatty()
    can_write = mode == "rw" and has_tty

    def _read_stdin() -> None:
        try:
            data = os.read(fd, 4096)
        except OSError:
            return
        if not data or _MIRROR_DETACH in data:
            detached.set()
            return
        if not can_write:
            return  # read-only: only the detach key means anything
        loop.create_task(
            ws.send(
                json.dumps({"type": "input", "data": data.decode("utf-8", "replace")})
            )
        )

    def _send_resize() -> None:
        c, r = shutil.get_terminal_size((80, 24))
        loop.create_task(ws.send(json.dumps({"type": "resize", "cols": c, "rows": r})))

    # Raw mode even read-only, so the documented Ctrl-] detach actually fires
    # without waiting for a newline.
    if has_tty:
        old_attrs = termios.tcgetattr(fd)
        tty.setraw(fd)
        loop.add_reader(fd, _read_stdin)
        reader_added = True

    winch_added = False
    if can_write:
        _send_resize()  # apply our geometry now; the init frame was ignored
        with contextlib.suppress(NotImplementedError, ValueError):
            loop.add_signal_handler(signal.SIGWINCH, _send_resize)
            winch_added = True

    try:
        recv_task = loop.create_task(_pump(ws, warned))
        detach_task = loop.create_task(detached.wait())
        done, pending = await asyncio.wait(
            {recv_task, detach_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        if detach_task in done:
            raise _Detach
        recv_task.result()  # re-raise a ConnectionClosed so the caller reconnects
    finally:
        if winch_added:
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(signal.SIGWINCH)
        if reader_added:
            loop.remove_reader(fd)
        if old_attrs is not None:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)


async def _mirror_session(project_id: int, write: bool) -> None:
    """Connect to the project's terminal WS and mirror it, reconnecting on drop.

    Mints a fresh proxy token per connect (they are short-lived and
    non-renewable), so an expiry-driven close just reconnects with a new one.
    A clean detach returns; a lost connection retries with backoff up to
    :data:`_MIRROR_MAX_RETRIES` before giving up.
    """
    # The asyncio client (websockets >= 13) is explicit about the connector and
    # takes additional_headers; the top-level websockets.connect is the legacy
    # asyncio impl with a different header kwarg, so import the new one directly.
    # Lazy on purpose: websockets is the optional `terminal` extra, only mirror
    # needs it.
    try:
        from websockets.asyncio.client import connect
        from websockets.exceptions import ConnectionClosed
    except ImportError as exc:
        raise click.ClickException(
            "hops session mirror requires websockets: pip install 'hopsworks[terminal]'"
        ) from exc

    mode = "rw" if write else "ro"
    attempts = 0
    while True:
        sess = terminal_api.get_session(project_id)
        if not sess or not sess.get("running"):
            raise click.ClickException(
                "No running terminal session for this project. Start one with "
                "`hops session new`, or open the Terminal tab in the Hopsworks UI."
            )
        url = _ws_url(sess["wsUrl"])
        ssl_ctx = _ws_ssl_context(url)
        token = terminal_api.get_proxy_token(project_id)
        try:
            async with connect(
                url,
                ssl=ssl_ctx,
                additional_headers={"Cookie": f"proxy_session={token}"},
                max_size=None,
                open_timeout=20,
            ) as ws:
                attempts = 0
                await _drive(ws, mode)
            return
        except (ConnectionClosed, OSError) as exc:
            attempts += 1
            if attempts > _MIRROR_MAX_RETRIES:
                raise click.ClickException(f"Mirror connection lost: {exc}") from exc
            await asyncio.sleep(min(2**attempts, 8))


@session_group.command("mirror")
@click.option(
    "--write",
    is_flag=True,
    help="Attach read-write (type into the session). Default is read-only: you "
    "see the live output but cannot send input.",
)
@click.pass_context
def mirror(ctx: click.Context, write: bool) -> None:
    """Mirror this project's live terminal session on your laptop.

    Attaches to the running pod terminal over its WebSocket and streams it here
    in real time: read-only by default (observe the driver), or ``--write`` to
    also type. Several clients can attach at once — the terminal header shows a
    presence badge. Detach with Ctrl-] (Ctrl-C without a tty); the session keeps
    running on the pod.

    Args:
        ctx: Click context.
        write: Attach read-write instead of read-only.
    """
    # Fail before logging in: there is nothing machine-readable to produce.
    if output.JSON_MODE:
        raise click.ClickException(
            "`session mirror` is interactive; --json is not supported."
        )
    project = conn.get_project(ctx)
    has_tty = termios is not None and sys.stdin.isatty()
    role = "read-write" if write else "read-only"
    if write and not has_tty:
        output.warn("No interactive tty; --write falls back to read-only.")
        role = "read-only (no tty)"
        # Downgrade for real, not just in the banner: attaching as a writer
        # would count in the server's roster and let this client resize the
        # shared PTY, while its stdin can never actually type.
        write = False
    detach_key = "Ctrl-]" if has_tty else "Ctrl-C"
    output.info(
        "Mirroring %s terminal (%s). Detach with %s.", project.name, role, detach_key
    )
    try:
        asyncio.run(_mirror_session(project.id, write))
    except _Detach:
        pass
    except KeyboardInterrupt:
        pass
    output.success("Detached from %s terminal.", project.name)


# The tmux/docker verb people reach for first; same command, second name.
session_group.add_command(mirror, name="attach")
