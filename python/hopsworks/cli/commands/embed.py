"""``hops fg embed``: a searchable embedding group over a feature group's text column.

``hops fg embed SOURCE --text COL`` makes or updates the group's Python job
``GROUP_embed``, then the group itself when it needs a new version: fenced,
with that job as its only writer. It starts one execution of the job; the build
runs there (``embed_run``), never in the caller. ``hops fg embed delete GROUP``
runs the job's deletion execution, which fences and deletes the groups, then
deletes the job. ``delete`` is the only subcommand, so any other first word is
the source feature group.
"""

from __future__ import annotations

import hashlib
import os
import re
import shlex
import tempfile
import time
from typing import Any

import click
from hopsworks.cli import embed_run, output, session


_RUNNER_CODE = (
    '"""Runs `hops` with the job arguments: the script of the job `hops fg embed` makes."""\n'
    "import sys\n\n"
    "from hopsworks.cli.main import cli\n\n"
    'cli.main(args=sys.argv[1:], prog_name="hops")\n'
)
_RUN = "run"
MAX_CONCURRENT = "maxConcurrentExecutions"
PLATFORM_CLASS_KEY = "platformClass"
PLATFORM_CLASS = "embed"
SCHEDULES = ("daily", "hourly")
#: The backend's answer to a start of a job being deleted (RESTCodes.JobErrorCode.JOB_DELETING).
JOB_DELETING = "130102"
#: Marks the deletion execution's arguments: `hops fg embed delete GROUP --in-job`.
IN_JOB = "--in-job"
#: Seconds between two reads of the deletion execution's state.
DELETION_POLL_S = 5
#: Interval of the daily default, and the floor when no admin has granted a faster one.
DAY_MINUTES = 1440


class _EmbedGroup(click.Group):
    """`hops fg embed SOURCE ...` runs; `hops fg embed delete GROUP` deletes."""

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        if not args or args[0] not in self.commands or args[0] == _RUN:
            args = [_RUN, *args]
        return super().parse_args(ctx, args)


class _RunCommand(click.Command):
    def format_usage(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        formatter.write_usage(
            ctx.parent.command_path if ctx.parent else ctx.command_path,
            " ".join(self.collect_usage_pieces(ctx)),
        )


@click.group("embed", cls=_EmbedGroup)
def fg_embed_group() -> None:
    """Embed a text column into a searchable feature group."""


@fg_embed_group.command(_RUN, cls=_RunCommand, hidden=True)
@click.argument("source")
@click.option("--version", type=int, help="Version of SOURCE; the latest when omitted.")
@click.option("--text", "text", required=True, help="The text column to embed.")
@click.option("--to", "to", help="The embedding group; SOURCE_TEXT_emb by default.")
@click.option(
    "--embedding-project",
    envvar="HOPSABLE_EMBEDDING_PROJECT",
    show_envvar=True,
    help="Project serving the embedding deployment.",
)
@click.option(
    "--embedding-deployment",
    envvar="HOPSABLE_EMBEDDING_BUILD_DEPLOYMENT",
    show_envvar=True,
    help="The build embedding deployment's name; embedderbuild by default.",
)
@click.option(
    "--embedding-model",
    envvar="HOPSABLE_EMBEDDING_MODEL",
    show_envvar=True,
    help="The model name sent to the deployment.",
)
@click.option(
    "--chunk-chars",
    type=click.IntRange(min=1),
    default=embed_run.CHUNK_CHARS,
    show_default=True,
    help="Characters per chunk.",
)
@click.option(
    "--chunk-overlap",
    type=click.IntRange(min=0),
    default=embed_run.CHUNK_OVERLAP,
    show_default=True,
    help="Characters each chunk repeats from the one before.",
)
@click.option(
    "--schedule",
    "every",
    is_flag=False,
    flag_value="daily",
    help="Also rebuild on a schedule: daily (the default when given alone), hourly, "
    "or a number of minutes. The minute and hour come from the project and group. "
    "Faster than daily needs an admin grant.",
)
@click.option("--env", "environment", help="Python environment of the job.")
@click.option(
    "--build-here",
    is_flag=True,
    hidden=True,
    help="Run the build in this process: what the job does.",
)
@click.option(
    "--to-version",
    type=int,
    hidden=True,
    help="The version of the group the job builds.",
)
@click.option(
    "--readback-timeout",
    type=click.IntRange(min=0),
    default=embed_run.READBACK_S,
    show_default=True,
    help="Seconds a build waits for OnlineFS to pass its fence and for its writes to read back.",
)
@click.pass_context
def fg_embed(
    ctx: click.Context,
    source: str,
    version: int | None,
    text: str,
    to: str | None,
    embedding_project: str | None,
    embedding_deployment: str | None,
    embedding_model: str | None,
    chunk_chars: int,
    chunk_overlap: int,
    every: str | None,
    environment: str | None,
    build_here: bool,
    to_version: int | None,
    readback_timeout: int,
) -> None:
    """Embed a text column of SOURCE into a searchable feature group.

    This command makes the embedding group and its Python job GROUP_embed,
    starts one execution, prints its id and returns; it does not wait. Before
    a step that searches the group, run `hops job wait GROUP_embed --execution
    ID --timeout 600` once: it returns when the build ends and exits 1 with the
    log tail if it failed (`hops job history GROUP_embed` reads the state once).
    A start the platform refuses for capacity exits non-zero
    and starts nothing: run it again after the time it names.

    Each row's text is cut into chunks of --chunk-chars characters that
    overlap by --chunk-overlap; each chunk is a row keyed by (doc_id,
    chunk_ix), doc_id being the source row's primary key, with its text,
    embedding (cosine), content_sha and embedded_at. The model and dimension
    are in the group's description; another model or dimension makes a new
    version. A rerun embeds only changed chunks and deletes the chunks of
    shortened or deleted rows, from both stores. A failed source read changes
    nothing. A source over the supported input (50,000 chunks and 100 MB of
    text unless an admin granted more) is refused with nothing written.

    Only the group's job writes the group, one execution at a time: each run
    takes the group over from every older run, so a stopped or stuck run never
    writes over a newer one.

    The embedding deployment is called with your own credentials. Search it
    from an agent with `hops agent init NAME --reads ... --search GROUP`.
    `hops fg embed delete GROUP` removes the group, its job and its schedule.

    \f
    Args:
        ctx: Click context.
        source: Source feature group.
        version: Source version.
        text: Text column.
        to: Embedding group name.
        embedding_project: Project serving the embedding deployment.
        embedding_deployment: Deployment name.
        embedding_model: Model name.
        chunk_chars: Characters per chunk.
        chunk_overlap: Overlap between chunks.
        every: Schedule of the job.
        environment: Python environment of the job.
        build_here: Run the build in this process.
        to_version: Version of the group the job builds.
        readback_timeout: Seconds a build waits for the fence and the read-back.
    """  # noqa: D301 - click reads the form feed as the end of the help text
    from hopsworks.cli.commands import fg as fg_commands  # noqa: PLC0415
    from hopsworks_agents import embed as embed_client  # noqa: PLC0415

    if chunk_overlap >= chunk_chars:
        raise click.BadParameter(
            "Must be less than --chunk-chars.", param_hint="--chunk-overlap"
        )
    try:
        endpoint = embed_client.endpoint(
            embedding_project, embedding_deployment, embedding_model, build=True
        )
    except ValueError as exc:
        raise click.UsageError(
            f"{exc} Or pass --embedding-project and --embedding-model."
        ) from exc
    src = fg_commands._get_fg(ctx, source, version)
    if text not in [f.name for f in getattr(src, "features", []) or []]:
        raise click.BadParameter(
            f"Feature group '{src.name}' has no column '{text}'.", param_hint="--text"
        )
    group = (to or f"{src.name}_{text}_emb").lower()
    if build_here:
        if to_version is None:
            raise click.ClickException(
                f"This job names no version of {group}: run hops fg embed again to update it. Nothing was written."
            )
        _build(
            ctx, src, text, group, to_version, endpoint, chunk_chars, chunk_overlap,
            readback_timeout,
        )  # fmt: skip
        return

    project = session.get_project(ctx)
    fs = session.get_feature_store(ctx)
    cron = schedule_cron(every, project.id, group) if every else None
    api = project.get_job_api()
    existing = _find_job(api, group)
    try:
        latest = _latest_owned(fs, group)
    except embed_run.EmbedError as exc:
        raise click.ClickException(str(exc)) from exc
    dimension = _probe(endpoint)
    reused = (
        latest
        if latest is not None
        and existing is not None
        and _holds(latest, endpoint[2], dimension)
        and embed_run.fenced_by(latest, existing.id)
        else None
    )
    target = reused.version if reused else (latest.version + 1 if latest else 1)
    args = [
        "fg", "embed", src.name, "--version", str(src.version), "--text", text,
        "--to", group,
        "--embedding-project", endpoint[0],
        "--embedding-deployment", endpoint[1],
        "--embedding-model", endpoint[2],
        "--chunk-chars", str(chunk_chars), "--chunk-overlap", str(chunk_overlap),
        "--readback-timeout", str(readback_timeout),
        "--to-version", str(target),
        "--build-here",
    ]  # fmt: skip
    job = _save_job(project, api, existing, group, src, text, args, environment)
    created = False
    if reused is None:
        _create_group(fs, group, target, src, text, endpoint[2], dimension, job)
        created = True
    execution = _start(job, group)
    if cron:
        _schedule(job, group, cron, execution)

    summary = {
        "group": group,
        "version": target,
        "created": created,
        "job": job.name,
        "execution": getattr(execution, "id", None),
        "schedule": cron,
    }
    if output.JSON_MODE:
        output.print_json(summary)
        return
    output.success(
        "✓ Started execution %s of job %s: it builds %s v%s and is not waited for",
        summary["execution"],
        job.name,
        group,
        target,
    )
    if cron:
        output.info(f"Scheduled job {job.name} ({cron})")


def _probe(endpoint: tuple[str, str, str]) -> int:
    """The model's dimension, from one embedding call: the group is made with it."""
    from hopsworks_agents import embed as embed_client  # noqa: PLC0415

    try:
        vectors = embed_client.embed_texts(
            ["dimension"],
            project=endpoint[0],
            deployment=endpoint[1],
            model=endpoint[2],
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not call the embedding deployment {endpoint[1]} in {endpoint[0]}: {exc}. Nothing was made."
        ) from exc
    return len(vectors[0])


def _latest_owned(fs: Any, group: str) -> Any:
    """The latest version of `group`, which hops fg embed must have made; None when the group does not exist."""
    found = embed_run.versions_of(fs, group)
    if not found:
        return None
    latest = found[-1]
    info = embed_run.described(latest)
    if info is None or info["owner"] != group:
        raise embed_run.EmbedError(
            f"Feature group '{group}' exists and was not made by hops fg embed. Pass --to with another name."
        )
    return latest


def _holds(fg: Any, model: str, dimension: int) -> bool:
    info = embed_run.described(fg) or {}
    return (info.get("model"), info.get("dimension")) == (model, dimension)


def _create_group(fs, group, version, src, text, model, dimension, job) -> Any:
    """A new fenced version of the group, whose only writer is `job`.

    Its Delta table is made with its properties here, before any claim: a
    fenced write never sets them.
    """
    from hsfs.embedding import EmbeddingIndex  # noqa: PLC0415
    from hsfs.feature import Feature  # noqa: PLC0415

    index = EmbeddingIndex(fenced=True)
    index.add_embedding("embedding", dimension, similarity_function_type="cosine")
    try:
        with output.sdk_quiet():
            fg = fs.create_feature_group(
                name=group,
                version=version,
                description=embed_run.description(
                    src.name, src.version, text, model, dimension, group
                ),
                online_enabled=True,
                primary_key=embed_run.KEYS,
                features=[
                    Feature("doc_id", "string"),
                    Feature("chunk_ix", "bigint"),
                    Feature("text", "string"),
                    Feature("embedding", "array<float>"),
                    Feature("content_sha", "string"),
                    Feature("embedded_at", "timestamp"),
                ],
                embedding_index=index,
                statistics_config=False,
            )
            fg.writer_job_id = job.id
            fg.save()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not create {group} v{version}: {exc}. Job {job.name} was updated and nothing was started; "
            "run hops fg embed again."
        ) from exc
    return fg


def _build(
    ctx, src, text, group, version, endpoint, chunk_chars, chunk_overlap, readback_s
) -> None:
    """The build itself: what the job's execution runs."""
    from hopsworks_agents import embed as embed_client  # noqa: PLC0415

    fs = session.get_feature_store(ctx)

    def embed(texts: list[str]) -> list[list[float]]:
        return embed_client.embed_texts(
            texts,
            project=endpoint[0],
            deployment=endpoint[1],
            model=endpoint[2],
        )

    try:
        execution = embed_run.execution_id()
        max_chunks, max_bytes = embed_run.embed_limits()
        plan = embed_run.run(
            fs=fs,
            source=src,
            text=text,
            group=group,
            version=version,
            embed=embed,
            execution=execution,
            size=chunk_chars,
            overlap=chunk_overlap,
            readback_s=readback_s,
            max_chunks=max_chunks,
            max_bytes=max_bytes,
            log=output.info,
        )
    except embed_run.EmbedError as exc:
        raise click.ClickException(str(exc)) from exc
    summary = {
        "group": group,
        "version": plan.version,
        "epoch": plan.epoch,
        "unsettled": plan.unsettled,
        "chunks": plan.desired,
        "embedded": plan.embedded,
        "rewritten": plan.rewritten,
        "deleted": plan.removed,
    }
    if output.JSON_MODE:
        output.print_json(summary)
        return
    output.success(
        "✓ %s v%s: %d chunks, %d embedded, %d rewritten, %d deleted",
        group,
        plan.version,
        plan.desired,
        plan.embedded,
        plan.rewritten,
        plan.removed,
    )


def _owned(thing: Any, group: str) -> bool:
    return (
        (getattr(thing, "description", "") or "")
        .rstrip()
        .endswith(embed_run.OWNER + group)
    )


def _job_name(group: str) -> str:
    return f"{group}_embed"


def _runner(group: str) -> str:
    """The job's script: one per group, so deleting the group deletes it."""
    return f"{embed_run.STATE_DIR}/{group}_embed_job.py"


def _intervals() -> set[int]:
    """Minutes between builds that repeat evenly: a divisor of an hour, or whole hours dividing a day."""
    return {m for m in range(1, 60) if 60 % m == 0} | {
        60 * h for h in range(1, 25) if 24 % h == 0
    }


def schedule_cron(every: str, project_id: Any, group: str) -> str:
    """The Quartz cron of `--schedule`.

    Daily by default. The minute, and the hour of a daily or multi-hour build, come
    from a stable hash of the project id and the group name, so groups do not all fire
    at once. An interval below a day must reach the floor an admin grants in the
    Hopsworks variable platform_jobs_embed_min_interval_minutes; with none set, a day
    is the floor.
    """
    word = every.strip().lower().removeprefix("@")
    minutes = {"daily": DAY_MINUTES, "hourly": 60}.get(
        word, int(word) if word.isdigit() else 0
    )
    if minutes not in _intervals():
        raise click.BadParameter(
            f"Must be daily, hourly, or minutes that divide an hour or are whole hours "
            f"dividing a day, not '{every}'.",
            param_hint="--schedule",
        )
    if minutes < DAY_MINUTES:
        try:
            floor = embed_run.variable_int(embed_run.VAR_MIN_INTERVAL, DAY_MINUTES)
        except embed_run.EmbedError as exc:
            raise click.ClickException(str(exc)) from exc
        if minutes < floor:
            raise click.ClickException(
                f"A build every {minutes} min is faster than this cluster grants "
                f"({floor} min). Use --schedule daily, or ask an admin for a faster floor."
            )
    digest = hashlib.sha256(f"{project_id}/{group}".encode()).digest()
    minute, hour = digest[0] % 60, digest[1] % 24
    if minutes == DAY_MINUTES:
        return f"0 {minute} {hour} * * ?"
    if minutes == 60:
        return f"0 {minute} * * * ?"
    if minutes < 60:
        return f"0 {digest[0] % minutes}/{minutes} * * * ?"
    step = minutes // 60
    return f"0 {minute} {hour % step}/{step} * * ?"


def _find_job(api: Any, group: str) -> Any:
    """The group's job when hops fg embed made it; None when there is none; refused when someone else's job has its name."""
    from hopsworks.cli import versions  # noqa: PLC0415

    name = _job_name(group)
    try:
        existing = api.get_job(name)
    except Exception as exc:  # noqa: BLE001
        if not versions.not_found(exc):
            raise click.ClickException(f"Could not find job '{name}': {exc}") from exc
        return None
    if existing is not None and not _owned(existing, group):
        raise click.ClickException(
            f"Job '{name}' exists and was not made by hops fg embed: it is left alone and nothing was started."
        )
    return existing


def _save_job(project, api, existing, group, src, text, args, environment) -> Any:
    """The group's job, made or updated to run `args`, one execution at a time, in the embed class."""
    from hopsworks.cli.commands.files import makedirs  # noqa: PLC0415

    name = _job_name(group)
    dataset = project.get_dataset_api()
    try:
        makedirs(dataset, embed_run.STATE_DIR)
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, os.path.basename(_runner(group)))
            with open(local, "w", encoding="utf-8") as f:
                f.write(_RUNNER_CODE)
            dataset.upload(local, embed_run.STATE_DIR, overwrite=True)
        config = (
            existing.config if existing is not None else api.get_configuration("PYTHON")
        )
        config["appPath"] = _runner(group)
        config["defaultArgs"] = shlex.join(args)
        config[MAX_CONCURRENT] = 1
        config[PLATFORM_CLASS_KEY] = PLATFORM_CLASS
        if environment:
            config["environmentName"] = environment
        config["description"] = (
            f"Embeds {src.name} v{src.version} {text} into {group}. {embed_run.OWNER}{group}"
        )
        with output.sdk_quiet():
            if existing is None:
                return api.create_job(name=name, config=config)
            return existing.save()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not prepare job '{name}': {exc}. Nothing was started."
        ) from exc


def start_refusal(exc: BaseException, group: str) -> str | None:
    """The refusal of an execution start, from the server's answer; None for any other failure.

    429: the platform's embed capacity (start rate or inventory) is full. 409:
    the job is being deleted (130102), or already has an execution
    (maxConcurrentExecutions is 1).
    """
    from hopsworks_common.client.exceptions import (  # noqa: PLC0415 - SDK loaded on use
        RestAPIError,
    )

    cause: BaseException | None = exc
    while cause is not None and not isinstance(cause, RestAPIError):
        cause = cause.__cause__
    if cause is None:
        return None
    status = cause.response.status_code
    if status == RestAPIError.STATUS_CODE_CONFLICT:
        if str(cause.error_code) == JOB_DELETING:
            return (
                f"{group} is being deleted; nothing was started; "
                f"run hops fg embed delete {group} again to finish it"
            )
        return f"a build of {group} is already running"
    if status == 429:
        after = (cause.response.headers.get("Retry-After") or "").strip()
        when = f"{after} s" if after.isdigit() else "a while"
        return f"embed jobs are at capacity; nothing was started; retry after {when}"
    return None


def _start(job: Any, group: str) -> Any:
    """One execution of the job; a refusal exits non-zero, with no retry."""
    try:
        with output.sdk_quiet():
            return job.run(await_termination=False)
    except Exception as exc:  # noqa: BLE001
        refusal = start_refusal(exc, group)
        if refusal is not None:
            raise click.ClickException(refusal) from exc
        raise click.ClickException(f"Could not start job '{job.name}': {exc}") from exc


def _schedule(job: Any, group: str, cron: str, execution: Any) -> None:
    try:
        job.schedule(cron_expression=cron, max_active_runs=1)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not schedule job '{job.name}': {exc}. "
            f"Execution {getattr(execution, 'id', '?')} was started."
        ) from exc


@fg_embed_group.command("delete")
@click.argument("group")
@click.option("--yes", is_flag=True, help="Skip the interactive confirmation prompt.")
@click.option(
    "--dry-run", is_flag=True, help="List what would be deleted and delete nothing."
)
@click.option(
    "--timeout",
    type=click.IntRange(min=0),
    default=1800,
    show_default=True,
    help="Seconds to wait for the deletion run; run the delete again to keep waiting.",
)
@click.option(
    IN_JOB,
    "in_job",
    is_flag=True,
    hidden=True,
    help="Run the deletion in this process: what the deletion execution does.",
)
@click.pass_context
def fg_embed_delete(
    ctx: click.Context,
    group: str,
    yes: bool,
    dry_run: bool,
    timeout: int,
    in_job: bool,
) -> None:
    """Delete an embedding group, its job and its schedule.

    Only what `hops fg embed` made is deleted: the group's versions and the job
    whose descriptions name the group as their owner, the job's schedule, and
    the job's script under Resources/hopsable-embed. The source feature group
    is untouched. Refused while an agent names the group.

    The deletion runs as an execution of the group's job: from its start the
    job runs no other build, and it takes the group over from every earlier
    run before it deletes the group. The command waits for it, then deletes
    the job. Interrupted or timed out, run the delete again: it picks up where
    it stopped.

    \f
    Args:
        ctx: Click context.
        group: Embedding group name.
        yes: Skip confirmation.
        dry_run: List only.
        timeout: Seconds to wait for the deletion execution.
        in_job: Run the deletion in this process.
    """  # noqa: D301 - click reads the form feed as the end of the help text
    project = session.get_project(ctx)
    fs = session.get_feature_store(ctx)
    api = project.get_job_api()
    if in_job:
        _delete_here(fs, api, group)
        return
    dataset = project.get_dataset_api()
    try:
        found = embed_run.versions_of(fs, group)
    except embed_run.EmbedError as exc:
        raise click.ClickException(str(exc)) from exc
    groups = [fg for fg in found if _owned(fg, group)]
    kept = [fg.version for fg in found if not _owned(fg, group)]
    job = _owned_job(api, group)

    users = _agents_naming(ctx, project, group)
    if users:
        raise click.ClickException(
            f"Agents {', '.join(users)} search '{group}': delete them first "
            "(hops agent delete NAME), then this group."
        )
    # The job's script, and the lease and pending-delete files that runs before the writer fence left.
    paths = [
        path
        for path in [
            _runner(group),
            f"{embed_run.STATE_DIR}/{group}.lease.json",
            *(f"{embed_run.STATE_DIR}/{group}_v{fg.version}" for fg in found),
        ]
        if dataset.exists(path)
    ]
    plan = {
        "group": group,
        "versions": [fg.version for fg in groups],
        "job": getattr(job, "name", None),
        "schedule": bool(job is not None and getattr(job, "job_schedule", None)),
        "files": paths,
        "kept_versions": kept,
    }
    if not groups and job is None and not paths:
        raise click.ClickException(
            f"Nothing made by hops fg embed is named '{group}'."
            + (f" Versions {kept} have no owner mark and are kept." if kept else "")
        )
    if output.JSON_MODE and dry_run:
        output.print_json(plan)
        return
    output.info(
        f"Group {group} versions {plan['versions'] or 'none'}; "
        f"job {plan['job'] or 'none'}"
        f"{' and its schedule' if plan['schedule'] else ''}; files {paths or 'none'}"
        + (f"; versions {kept} are not owned and are kept" if kept else "")
    )
    if dry_run:
        return
    if not yes:
        output.confirm(
            f"Delete these for '{group}'? Their data cannot be restored.",
            f"Deleting the embedding group '{group}'",
        )
    try:
        if job is None:
            # No job, so no run can claim the groups: they are deleted from here.
            for fg in groups:
                fg.delete()
        else:
            plan["deletion_execution"] = _run_deletion(job, group, timeout)
            job.delete()
        for path in paths:
            dataset.remove(path)
    except click.ClickException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not delete embedding group '{group}': {exc}. Run the delete again for what is left."
        ) from exc
    if output.JSON_MODE:
        output.print_json(plan)
        return
    output.success("✓ Deleted embedding group %s", group)


def _owned_job(api: Any, group: str) -> Any:
    """The group's job when hops fg embed made it, else None."""
    from hopsworks.cli import versions  # noqa: PLC0415

    try:
        job = api.get_job(_job_name(group))
    except Exception as exc:  # noqa: BLE001
        if not versions.not_found(exc):
            raise click.ClickException(
                f"Could not find job '{_job_name(group)}': {exc}"
            ) from exc
        return None
    return job if job is not None and _owned(job, group) else None


def _final(execution: Any) -> bool:
    return str(getattr(execution, "final_status", "") or "UNDEFINED") != "UNDEFINED"


def _run_deletion(job: Any, group: str, timeout_s: int) -> int:
    """The id of the deletion execution D once it succeeded: the latest one, else a new one.

    D is started with deletion=true: the backend sets the job's deleting flag
    and admits no other start of the job from then on. A D that already
    succeeded is not run again, one still running is waited for, and a failed
    one is replaced.
    """
    from hopsworks_common.core.execution_api import ExecutionApi  # noqa: PLC0415

    executions_api = ExecutionApi()
    deletions = [
        e for e in executions_api._get_all(job) or [] if IN_JOB in (e.args or "")
    ]
    latest = max(deletions, key=lambda e: e.id) if deletions else None
    if latest is None or (_final(latest) and latest.final_status != "SUCCEEDED"):
        try:
            latest = executions_api._start(
                job,
                args=shlex.join(["fg", "embed", "delete", group, IN_JOB]),
                deletion=True,
            )
        except Exception as exc:  # noqa: BLE001
            refusal = start_refusal(exc, group)
            if refusal is not None and "already running" in refusal:
                refusal = (
                    f"a build of {group} is running and the job now starts no other; "
                    "run the delete again once it has ended"
                )
            raise click.ClickException(
                refusal or f"Could not start the deletion of '{group}': {exc}"
            ) from exc
        output.info(f"Started deletion execution {latest.id} of job {job.name}")
    deadline = time.monotonic() + timeout_s
    while not _final(latest):
        if time.monotonic() >= deadline:
            raise click.ClickException(
                f"The deletion execution {latest.id} of job {job.name} is still running after {timeout_s} s; "
                "run the delete again to keep waiting."
            )
        time.sleep(DELETION_POLL_S)
        latest = executions_api._get(job, latest.id)
    if latest.final_status != "SUCCEEDED":
        raise click.ClickException(
            f"The deletion execution {latest.id} of job {job.name} ended {latest.final_status}: "
            f"read hops job logs {job.name}, then run the delete again."
        )
    return latest.id


def _delete_here(fs: Any, api: Any, group: str) -> None:
    """The deletion execution itself: fence and delete the groups, unschedule the job."""
    job = _owned_job(api, group)
    if job is None:
        raise click.ClickException(
            f"Job '{_job_name(group)}' made by hops fg embed is not found; nothing was deleted."
        )
    try:
        deleted = embed_run.delete_run(
            fs=fs,
            job=job,
            group=group,
            execution=embed_run.execution_id(),
            log=output.info,
        )
    except embed_run.EmbedError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"The deletion of '{group}' stopped: {exc}. Run hops fg embed delete {group} again."
        ) from exc
    output.success("✓ Deleted %s versions %s", group, deleted or "none")


def _agents_naming(ctx: click.Context, project: Any, group: str) -> list[str]:
    """Agents whose description names the group, or whose script quotes its name."""
    from hopsworks.cli.commands import agent as agent_commands  # noqa: PLC0415
    from hsml.core import serving_api  # noqa: PLC0415

    mark = re.compile(re.escape(embed_run.SEARCH + group) + r"(?![A-Za-z0-9_])")
    quoted = (f'"{group}"', f"'{group}'")
    try:
        # get_model_serving() refuses a project without a Models dataset; the serving
        # list itself answers [] there, and a project without Serving has no agents.
        deployments = serving_api.ServingApi()._get_all() or []
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not list agents to check that none searches '{group}': {exc}"
        ) from exc
    dataset = project.get_dataset_api()
    users = []
    for d in deployments:
        if not agent_commands._is_agent(d):
            continue
        if mark.search(getattr(d, "description", "") or ""):
            users.append(d.name)
            continue
        script = agent_commands._predictor_attr(d, "script_file", None)
        if not script:
            continue
        try:
            answer = dataset.read_content(script)
            body = getattr(answer, "content", answer) or b""
            code = (
                body.decode("utf-8", "replace")
                if isinstance(body, bytes)
                else str(body)
            )
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not read the script of agent '{d.name}' to check it does not search '{group}': {exc}"
            ) from exc
        if any(q in code for q in quoted):
            users.append(d.name)
    return users
