"""``hops teleport`` — move the current Claude Code session onto a Hopsworks pod.

The idea: you are in a Claude Code session on your laptop; you run ``hops
teleport`` and the same session continues on a Hopsworks terminal pod (Claude
Code is pre-installed in that image), so you can close the laptop and drive it
later from any browser via the terminal tab.

This first slice does the two halves that need no live terminal pod to verify:

1. resolve the current session — Claude Code stores each session as a JSONL at
   ``~/.claude/projects/<cwd-slug>/<session-id>.jsonl``; the active one is the
   most-recently-written file under this directory's slug;
2. ship it — upload that JSONL into the target project's HopsFS, where the
   terminal pod can read it (the pod mounts HopsFS and symlinks ``~/.claude``
   from the user's HopsFS home).

It then best-effort starts the terminal pod and prints the "landing kit": the
exact commands to resume over there, plus the teleport marker line. Typing
``claude --resume`` into the pod's ``tmux`` over the WebSocket, and the
keep-alive past the 4h reaper, are the next slice.
"""

from __future__ import annotations

import contextlib
import re
import socket
from datetime import datetime, timezone
from pathlib import Path

import click
from hopsworks.cli import output, session, terminal_api


# Where Claude Code keeps per-directory session transcripts on this machine.
_CLAUDE_PROJECTS = Path.home() / ".claude" / "projects"
# HopsFS dataset dir the session JSONLs are uploaded under, one subdir per slug.
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


def _resolve_session(slug: str, session_id: str | None) -> Path:
    """Locate the session JSONL to teleport for ``slug``.

    Args:
        slug: The current directory's Claude Code slug.
        session_id: Explicit session id to teleport, or None to pick the
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
            f"(looked in {proj_dir}). Run teleport from the directory your "
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


@click.command("teleport")
@click.option(
    "--session",
    "session_id",
    help="Session id to teleport; defaults to the most recently active one "
    "for this directory.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite a session JSONL already uploaded for this slug.",
)
@click.pass_context
def teleport_cmd(ctx: click.Context, session_id: str | None, overwrite: bool) -> None:
    """Move the current Claude Code session onto a Hopsworks terminal pod.

    Resolves the active session for the current directory, uploads its
    transcript into the target project's HopsFS, starts the terminal pod (when
    the feature is enabled on the cluster), and prints how to resume it there.

    Args:
        ctx: Click context.
        session_id: Explicit session id, or None for the active one.
        overwrite: Re-upload even if a JSONL for this slug already exists.
    """
    slug = _cwd_slug()
    jsonl = _resolve_session(slug, session_id)
    resolved_id = jsonl.stem

    project = session.get_project(ctx)
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
    output.success("✓ Shipped session %s to %s/", resolved_id, dest_dir)

    marker = (
        f"this session has been teleported from {socket.gethostname()} "
        f"at {datetime.now(timezone.utc).isoformat(timespec='seconds')}"
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
    output.info("Teleport marker: %s", marker)
    if ws_url:
        output.info("WebSocket: %s", ws_url)
