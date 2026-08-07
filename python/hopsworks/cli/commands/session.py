"""``hops session`` — move a Claude Code session between this machine and a pod.

You are in a Claude Code session on your laptop. ``hops session push`` ships it
onto a Hopsworks terminal pod (Claude Code is pre-installed in that image), so
you can close the laptop and drive it later from the browser terminal tab.
``hops session pull`` is the mirror: it brings a session that ran on the pod
back down to this machine. ``hops session list`` shows what is staged where.

The transport is symmetric. Each session is a JSONL that Claude Code stores at
``~/.claude/projects/<cwd-slug>/<session-id>.jsonl``; push uploads it into the
project's HopsFS under ``Resources/teleport/<slug>/`` and pull downloads it
back. Resume is scoped to the working-directory slug, so both ends must run
from a path that hashes to the same slug for ``claude --resume`` to load.

Typing ``claude --resume`` into the pod's ``tmux`` over the WebSocket, and the
keep-alive past the 4h reaper, are a later slice; for now push prints the exact
landing steps to run in the terminal.
"""

from __future__ import annotations

import contextlib
import json
import re
import socket
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import click
from hopsworks.cli import output, terminal_api
from hopsworks.cli import session as conn


# Where Claude Code keeps per-directory session transcripts on this machine.
_CLAUDE_PROJECTS = Path.home() / ".claude" / "projects"
# HopsFS dataset dir the session JSONLs are staged under, one subdir per slug.
_TELEPORT_DATASET = "Resources/teleport"


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


def _remote_session_ids(dataset_api, slug: str) -> list[str]:
    """Return the session ids staged in HopsFS for ``slug``, newest-listed last.

    Args:
        dataset_api: The project's dataset API.
        slug: The current directory's Claude Code slug.

    Returns:
        Session ids (JSONL stems) under ``Resources/teleport/<slug>/``. Empty
        when the directory does not exist or holds no transcripts.
    """
    remote_dir = f"{_TELEPORT_DATASET}/{slug}"
    with contextlib.suppress(Exception):
        entries = dataset_api.list(remote_dir)
        return [
            Path(p).stem for p in entries if str(p).endswith(".jsonl")
        ]
    return []


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _local_away(slug: str) -> dict[str, str]:
    """Return ``{session_id: project}`` for sessions handed off from this
    directory, read from the ``<id>.away.json`` markers push leaves behind."""
    out: dict[str, str] = {}
    d = _CLAUDE_PROJECTS / slug
    if d.is_dir():
        for m in d.glob("*.away.json"):
            with contextlib.suppress(Exception):
                sid = m.name[: -len(".away.json")]
                out[sid] = json.loads(m.read_text()).get("project", "?")
    return out


def _is_active_session(jsonl: Path) -> bool:
    """Heuristic for "the session you are currently in": the newest transcript
    for this directory, written to within the last two minutes. A live `claude`
    holds that file open, so pushing it cannot rename it out from under the
    running process; the baton is recorded but the local copy is left in place.
    """
    proj_dir = jsonl.parent
    newest = max(
        proj_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, default=None
    )
    return newest == jsonl and (time.time() - jsonl.stat().st_mtime) < 120


def _write_baton(dataset_api, dest_dir: str, sid: str, holder: str,
                 prev_holder: str, lines: int) -> None:
    """Write the baton sidecar recording where a session's canonical copy lives.

    Both the laptop CLI and the pod read it; it is the commit point of a
    hand-off. Best-effort upload; a failure surfaces on the next read.
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
        with contextlib.suppress(Exception):
            dataset_api.upload(local_path=str(p), upload_path=dest_dir, overwrite=True)


def _read_baton(dataset_api, dest_dir: str, sid: str) -> dict | None:
    """Return the baton for ``sid`` staged under ``dest_dir``, or None.

    None means no baton exists (a ``--fork`` push, or a legacy transfer), which
    the caller treats as batonless copy semantics — not an error.
    """
    remote = f"{dest_dir}/{sid}.baton.json"
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / f"{sid}.baton.json"
        with contextlib.suppress(Exception):
            dataset_api.download(remote, local_path=str(local), overwrite=True)
            return json.loads(local.read_text())
    return None


def _pod_alive(project_id: int) -> bool:
    """Whether the pod still holds a live terminal session for this project.

    Only a definitive ``None`` from the backend counts as dead. A raised call
    (feature disabled, network) is treated as alive on purpose: liveness we
    cannot confirm must not silently authorise stealing the baton, so an
    unknown state still forces ``--force``.
    """
    try:
        return terminal_api.get_session(project_id) is not None
    except Exception:  # noqa: BLE001 - unknown liveness is fail-safe "alive"
        return True


def _transcript_relation(local: list[str], remote: list[str],
                         baseline: int) -> str:
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


@click.group("session")
def session_group() -> None:
    """Move a Claude Code session between this machine and a terminal pod."""


@session_group.command("push")
@click.option(
    "--session",
    "session_id",
    help="Session id to push; defaults to the most recently active one for "
    "this directory.",
)
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
@click.pass_context
def push(ctx: click.Context, session_id: str | None, overwrite: bool,
         fork: bool, model: str | None) -> None:
    """Push the current Claude Code session onto a Hopsworks terminal pod.

    Resolves the active session for this directory, uploads its transcript into
    the project's HopsFS, starts the terminal pod (when the feature is enabled
    on the cluster), and prints how to resume it there. By default this is a
    baton hand-off: the pod becomes the canonical copy and the local transcript
    is renamed aside (unless it is the session you are currently in, which stays
    live locally). `--fork` keeps a live local copy instead.

    Args:
        ctx: Click context.
        session_id: Explicit session id, or None for the active one.
        overwrite: Re-upload even if a JSONL for this slug already exists.
        fork: Copy instead of hand off; leave the local session canonical.
        model: Model the pod resumes with.
    """
    slug = _cwd_slug()
    jsonl = _resolve_local_session(slug, session_id)
    resolved_id = jsonl.stem

    project = conn.get_project(ctx)
    dataset_api = project.get_dataset_api()

    dest_dir = f"{_TELEPORT_DATASET}/{slug}"
    # mkdir is best-effort: an existing dir is fine, and a real problem will
    # surface loudly on the upload below.
    with contextlib.suppress(Exception):
        dataset_api.mkdir(dest_dir)
    try:
        dataset_api.upload(
            local_path=str(jsonl), upload_path=dest_dir, overwrite=overwrite
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Failed to ship session: {exc}") from exc
    output.success("✓ Pushed session %s to %s/", resolved_id, dest_dir)

    marker = (
        f"this session has been pushed from {socket.gethostname()} "
        f"at {datetime.now(timezone.utc).isoformat(timespec='seconds')}"
    )

    # Manifest the pod's landing hook reads to self-resume: it carries the
    # original cwd, which the slug alone cannot reconstruct, so the pod can
    # recreate a path that hashes to the same slug for `claude --resume`.
    manifest = {
        "session_id": resolved_id,
        "slug": slug,
        "cwd": str(Path.cwd()),
        "host": socket.gethostname(),
        "pushed_at": _now(),
        "mode": "fork" if fork else "push",
        "model": model,
    }
    with tempfile.TemporaryDirectory() as tmp:
        mpath = Path(tmp) / f"{resolved_id}.teleport.json"
        mpath.write_text(json.dumps(manifest))
        with contextlib.suppress(Exception):
            dataset_api.upload(
                local_path=str(mpath), upload_path=dest_dir, overwrite=True
            )

    # Baton hand-off: record that the canonical copy now lives on the pod, and
    # rename the local transcript aside so it is not resumed by accident. Skip
    # both for --fork (deliberate copy), and skip the rename for the session
    # you are in (its file is held open by a live `claude`).
    if not fork:
        host = socket.gethostname()
        lines = sum(1 for _ in jsonl.open(errors="ignore"))
        _write_baton(dataset_api, dest_dir, resolved_id,
                     holder=f"pod:{project.name}", prev_holder=f"laptop:{host}",
                     lines=lines)
        if _is_active_session(jsonl):
            output.warn(
                "This looks like the session you are in, so it stays live on "
                "this machine. The baton points to %s; close it here, then "
                "`hops session pull` to reclaim it.", project.name)
        else:
            away = jsonl.parent / (jsonl.name + ".away")
            with contextlib.suppress(Exception):
                jsonl.rename(away)
                (jsonl.parent / f"{resolved_id}.away.json").write_text(json.dumps(
                    {"project": project.name, "host": host, "pushed_at": _now()}))

    ws_url = None
    try:
        started = terminal_api.start_session(project.id)
        ws_url = (started or {}).get("wsUrl")
        output.success("✓ Terminal pod ready for %s", project.name)
    except Exception as exc:  # noqa: BLE001 - feature may be disabled on cluster
        output.warn(
            "Terminal pod not started (%s). Open the Terminal tab in the "
            "Hopsworks UI to start it, then run the landing steps below.",
            exc,
        )

    pod_session_dir = f"~/.claude/projects/{slug}"
    pod_jsonl = f"/hopsfs/{_TELEPORT_DATASET}/{slug}/{resolved_id}.jsonl"
    landing = [
        f"mkdir -p {pod_session_dir}",
        f"cp {pod_jsonl} {pod_session_dir}/",
        f"claude --resume {resolved_id}",
    ]

    if output.JSON_MODE:
        output.print_json(
            {
                "session_id": resolved_id,
                "slug": slug,
                "project": project.name,
                "shipped_to": f"{dest_dir}/{resolved_id}.jsonl",
                "ws_url": ws_url,
                "marker": marker,
                "landing_steps": landing,
            }
        )
        return

    output.info("")
    output.info("Landing kit — run these in the Hopsworks terminal:")
    for step in landing:
        output.info("  %s", step)
    output.info("")
    output.info("Push marker: %s", marker)
    if ws_url:
        output.info("WebSocket: %s", ws_url)


@session_group.command("pull")
@click.option(
    "--session",
    "session_id",
    help="Session id to pull; required when more than one session is staged "
    "for this directory.",
)
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
def pull(ctx: click.Context, session_id: str | None, ours: bool, theirs: bool,
         force: bool) -> None:
    """Pull a session staged in HopsFS back onto this machine and take the baton.

    Downloads the transcript from ``Resources/teleport/<slug>/`` into
    ``~/.claude/projects/<slug>/`` and prints the resume command. Run it from a
    directory that hashes to the same slug as the source, or ``claude
    --resume`` will not find the session.

    Reclaiming is baton-aware. If the pod still holds a live terminal session,
    pull refuses unless ``--force`` (you would be stealing from a process that
    is still writing). If both sides advanced the transcript since the hand-off,
    pull refuses until you pick ``--ours`` or ``--theirs``; the losing side is
    parked to a sidecar, never destroyed. A successful pull flips the baton to
    this machine as the very last step, so an interrupted pull leaves the pod
    canonical rather than orphaning the claim here.

    Args:
        ctx: Click context.
        session_id: Explicit session id, or None to pick the only staged one.
        ours: On divergence, keep local.
        theirs: On divergence, take the pod's copy.
        force: Steal the baton from a live pod or another machine.
    """
    if ours and theirs:
        raise click.ClickException("--ours and --theirs are mutually exclusive.")

    slug = _cwd_slug()
    project = conn.get_project(ctx)
    dataset_api = project.get_dataset_api()

    if not session_id:
        staged = _remote_session_ids(dataset_api, slug)
        if not staged:
            raise click.ClickException(
                f"No sessions staged in HopsFS for this directory "
                f"({_TELEPORT_DATASET}/{slug})."
            )
        if len(staged) > 1:
            raise click.ClickException(
                "Multiple sessions staged for this directory; pass --session "
                "to choose one: " + ", ".join(staged)
            )
        session_id = staged[0]

    dest_dir = f"{_TELEPORT_DATASET}/{slug}"
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
        local_jsonl if local_jsonl.is_file()
        else away if away.is_file()
        else None
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
                    f"{session_id}.jsonl.diverged-{_stamp()}" if theirs
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

    if baton is not None:
        final_lines = sum(1 for _ in local_jsonl.open(errors="ignore"))
        _write_baton(dataset_api, dest_dir, session_id, holder=me,
                     prev_holder=baton.get("holder", "?"), lines=final_lines)

    output.success("✓ Pulled session %s to %s", session_id, local_dir)
    if parked is not None:
        output.info("Parked the other side at %s", parked.name)

    resume = f"claude --resume {session_id}"
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
                "resume_step": resume,
            }
        )
        return
    output.info("")
    output.info("Resume it here with:")
    output.info("  %s", resume)


@session_group.command("list")
@click.pass_context
def list_sessions(ctx: click.Context) -> None:
    """List sessions for this directory and where each one lives.

    Shows the session ids present locally under ``~/.claude/projects/<slug>/``
    and those staged in the project's HopsFS, so you can tell what is here,
    what is on the pod side, and what exists in both places.

    Args:
        ctx: Click context.
    """
    slug = _cwd_slug()
    local_dir = _CLAUDE_PROJECTS / slug
    local_ids = sorted(p.stem for p in local_dir.glob("*.jsonl")) if (
        local_dir.is_dir()
    ) else []
    away = _local_away(slug)

    project = conn.get_project(ctx)
    remote_ids = sorted(_remote_session_ids(project.get_dataset_api(), slug))

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
        output.info("No sessions for this directory (slug %s).", slug)
        return
    output.info("Sessions for slug %s:", slug)
    for sid in all_ids:
        output.info("  %-40s %s", sid, _where(sid))
