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
@click.pass_context
def push(ctx: click.Context, session_id: str | None, overwrite: bool) -> None:
    """Push the current Claude Code session onto a Hopsworks terminal pod.

    Resolves the active session for this directory, uploads its transcript into
    the project's HopsFS, starts the terminal pod (when the feature is enabled
    on the cluster), and prints how to resume it there. The local session is
    left untouched.

    Args:
        ctx: Click context.
        session_id: Explicit session id, or None for the active one.
        overwrite: Re-upload even if a JSONL for this slug already exists.
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
        "pushed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    with tempfile.TemporaryDirectory() as tmp:
        mpath = Path(tmp) / f"{resolved_id}.teleport.json"
        mpath.write_text(json.dumps(manifest))
        with contextlib.suppress(Exception):
            dataset_api.upload(
                local_path=str(mpath), upload_path=dest_dir, overwrite=True
            )

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
    "--overwrite",
    is_flag=True,
    help="Overwrite the local session JSONL if it already exists.",
)
@click.pass_context
def pull(ctx: click.Context, session_id: str | None, overwrite: bool) -> None:
    """Pull a session staged in HopsFS back onto this machine.

    Downloads the transcript from ``Resources/teleport/<slug>/`` into
    ``~/.claude/projects/<slug>/`` and prints the resume command. Run it from a
    directory that hashes to the same slug as the source, or ``claude
    --resume`` will not find the session.

    Args:
        ctx: Click context.
        session_id: Explicit session id, or None to pick the only staged one.
        overwrite: Overwrite the local JSONL if it already exists.
    """
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

    remote_jsonl = f"{_TELEPORT_DATASET}/{slug}/{session_id}.jsonl"
    local_dir = _CLAUDE_PROJECTS / slug
    local_dir.mkdir(parents=True, exist_ok=True)
    local_jsonl = local_dir / f"{session_id}.jsonl"
    try:
        dataset_api.download(
            remote_jsonl, local_path=str(local_jsonl), overwrite=overwrite
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Failed to pull session: {exc}") from exc
    output.success("✓ Pulled session %s to %s", session_id, local_dir)

    resume = f"claude --resume {session_id}"
    if output.JSON_MODE:
        output.print_json(
            {
                "session_id": session_id,
                "slug": slug,
                "project": project.name,
                "pulled_to": str(local_jsonl),
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

    project = conn.get_project(ctx)
    remote_ids = sorted(_remote_session_ids(project.get_dataset_api(), slug))

    def _where(sid: str) -> str:
        here, there = sid in local_ids, sid in remote_ids
        return "local+remote" if here and there else "local" if here else "remote"

    all_ids = sorted(set(local_ids) | set(remote_ids))
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
