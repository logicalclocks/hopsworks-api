"""``hops job`` — Hopsworks jobs (reads + lifecycle writes).

Run, stop, schedule, unschedule, and log retrieval all go through the SDK
``Job``/``Execution`` classes. History uses ``job.get_executions()`` and
follows the same memoized-session pattern as the rest of the CLI.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import click
from hopsworks.cli import output, session


if TYPE_CHECKING:
    from datetime import datetime


def _user_label(value: Any) -> str:
    """Render a Hopsworks user reference (dict / str / object) as one string.

    The REST API returns ``creator`` as a dict (``{username, firstname, ...}``)
    on some endpoints and as a username string on others. Pick the most useful
    field; fall back to ``"-"`` rather than the dict's repr.
    """
    if not value:
        return "-"
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return (
            value.get("username")
            or value.get("email")
            or f"{value.get('firstname', '')} {value.get('lastname', '')}".strip()
            or "-"
        )
    return getattr(value, "username", None) or getattr(value, "email", None) or "-"


def _ts_label(value: Any) -> str:
    """Render a Hopsworks timestamp (epoch ms / ISO string / None) as ISO-8601.

    Job endpoints return ``creationTime`` as epoch milliseconds; some other
    endpoints return ISO strings. Normalize to ISO-8601 UTC for display.
    """
    if value in (None, "", "-"):
        return "-"
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10**12 else value
        from datetime import datetime as _dt  # noqa: PLC0415
        from datetime import timezone as _tz

        return _dt.fromtimestamp(seconds, tz=_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)


@click.group("job")
def job_group() -> None:
    """Job commands."""


@job_group.command("list")
@click.pass_context
def job_list(ctx: click.Context) -> None:
    """List every job defined in the active project.

    Args:
        ctx: Click context.
    """
    project = session.get_project(ctx)
    try:
        jobs = project.get_job_api().get_jobs()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list jobs: {exc}") from exc

    rows = []
    for j in jobs or []:
        rows.append(
            [
                getattr(j, "id", "?"),
                getattr(j, "name", "?"),
                getattr(j, "job_type", "-"),
                _user_label(getattr(j, "creator", None)),
                _ts_label(getattr(j, "creation_time", None)),
            ]
        )
    output.print_table(["ID", "NAME", "TYPE", "CREATOR", "CREATED"], rows)


@job_group.command("info")
@click.argument("name")
@click.pass_context
def job_info(ctx: click.Context, name: str) -> None:
    """Show the configuration of a single job.

    Args:
        ctx: Click context.
        name: Job name.
    """
    project = session.get_project(ctx)
    try:
        job = project.get_job_api().get_job(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Job '{name}' not found: {exc}") from exc
    if job is None:
        raise click.ClickException(f"Job '{name}' not found.")

    if output.JSON_MODE:
        output.print_json(_job_to_dict(job))
        return

    rows = [
        ["ID", getattr(job, "id", "?")],
        ["Name", getattr(job, "name", "?")],
        ["Type", getattr(job, "job_type", "-")],
        ["Creator", _user_label(getattr(job, "creator", None))],
        ["Created", _ts_label(getattr(job, "creation_time", None))],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


def _job_to_dict(job: Any) -> dict[str, Any]:
    config = getattr(job, "config", None)
    creator = getattr(job, "creator", None)
    if creator is not None and not isinstance(creator, (str, dict)):
        creator = {
            k: v
            for k, v in (
                ("username", getattr(creator, "username", None)),
                ("email", getattr(creator, "email", None)),
                ("firstname", getattr(creator, "firstname", None)),
                ("lastname", getattr(creator, "lastname", None)),
            )
            if v is not None
        } or _user_label(creator)
    return {
        "id": getattr(job, "id", None),
        "name": getattr(job, "name", None),
        "type": getattr(job, "job_type", None),
        "creator": creator,
        "creation_time": getattr(job, "creation_time", None),
        "config": config.to_dict() if hasattr(config, "to_dict") else config,
    }


# region Write commands


@job_group.command("create")
@click.argument("name")
@click.option(
    "--type",
    "job_type",
    type=click.Choice(
        ["PYTHON", "PYSPARK", "SPARK", "DOCKER", "FLINK"], case_sensitive=False
    ),
    required=True,
    help="Job type.",
)
@click.option(
    "--app-path", "app_path", required=True, help="HDFS/HopsFS path to the main file."
)
@click.option("--args", "app_args", help="Arguments passed to the program.")
@click.pass_context
def job_create(
    ctx: click.Context,
    name: str,
    job_type: str,
    app_path: str,
    app_args: str | None,
) -> None:
    """Create a new job from a ``type`` + ``--app-path``.

    Args:
        ctx: Click context.
        name: Job name.
        job_type: One of ``PYTHON``/``PYSPARK``/``SPARK``/``DOCKER``/``FLINK``.
        app_path: HopsFS path to the script or JAR.
        app_args: Optional argument string passed to the job.
    """
    project = session.get_project(ctx)
    api = project.get_job_api()
    try:
        config = api.get_configuration(job_type.upper())
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not load default config: {exc}") from exc
    config["appPath"] = app_path
    if app_args:
        config["defaultArgs"] = app_args
    try:
        job = api.create_job(name=name, config=config)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create job: {exc}") from exc
    output.success("✓ Created job %s", getattr(job, "name", name))


_DT_FORMATS = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"]


@job_group.command("run")
@click.argument("name")
@click.option("--args", "app_args", help="Argument string passed to this execution.")
@click.option("--wait", is_flag=True, help="Block until the execution terminates.")
@click.option(
    "--start-time",
    type=click.DateTime(formats=_DT_FORMATS),
    default=None,
    help="ISO timestamp for HOPS_START_TIME (data interval start).",
)
@click.option(
    "--end-time",
    type=click.DateTime(formats=_DT_FORMATS),
    default=None,
    help="ISO timestamp for HOPS_END_TIME (data interval end). Requires --start-time.",
)
@click.pass_context
def job_run(
    ctx: click.Context,
    name: str,
    app_args: str | None,
    wait: bool,
    start_time: datetime | None,
    end_time: datetime | None,
) -> None:
    """Start a new one-off execution of ``name``.

    ``--start-time`` / ``--end-time`` are passed straight through to the
    SDK and surface inside the job as ``HOPS_START_TIME`` /
    ``HOPS_END_TIME``. Useful when you want to nudge a single run to
    consume a specific data window without creating a schedule.

    Args:
        ctx: Click context.
        name: Job name.
        app_args: Argument string passed to this execution.
        wait: Block until the execution terminates.
        start_time: ISO timestamp for ``HOPS_START_TIME``.
        end_time: ISO timestamp for ``HOPS_END_TIME``.
    """
    if end_time is not None and start_time is None:
        raise click.UsageError("--end-time requires --start-time.")
    job = _get_job(ctx, name)
    try:
        execution = job.run(
            args=app_args,
            await_termination=wait,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Run failed: {exc}") from exc

    output.success(
        "✓ Started job %s (execution #%s, state=%s)",
        name,
        getattr(execution, "id", "?"),
        getattr(execution, "state", "?"),
    )
    if output.JSON_MODE:
        output.print_json(_execution_to_dict(execution))


@job_group.command("backfill")
@click.argument("name")
@click.option(
    "--start-time",
    type=click.DateTime(formats=_DT_FORMATS),
    required=True,
    help="ISO timestamp for the start of the backfill window (HOPS_START_TIME).",
)
@click.option(
    "--end-time",
    type=click.DateTime(formats=_DT_FORMATS),
    required=True,
    help="ISO timestamp for the end of the backfill window (HOPS_END_TIME).",
)
@click.option("--args", "app_args", help="Argument string passed to this execution.")
@click.option(
    "--wait/--no-wait",
    "wait",
    default=True,
    show_default=True,
    help="Block until the execution terminates (default: yes).",
)
@click.pass_context
def job_backfill(
    ctx: click.Context,
    name: str,
    start_time: datetime,
    end_time: datetime,
    app_args: str | None,
    wait: bool,
) -> None:
    """Run ``name`` once over an explicit [start_time, end_time] window.

    Same SDK call as ``run`` (which sets ``HOPS_START_TIME`` /
    ``HOPS_END_TIME`` env vars on the execution); the difference is that
    both timestamps are required, signalling intent to backfill a fixed
    historical interval rather than a normal one-off run.

    Args:
        ctx: Click context.
        name: Job name.
        start_time: ISO timestamp for the start of the backfill window.
        end_time: ISO timestamp for the end of the backfill window.
        app_args: Argument string passed to this execution.
        wait: Block until the execution terminates.
    """
    if end_time <= start_time:
        raise click.UsageError("--end-time must be strictly after --start-time.")
    job = _get_job(ctx, name)
    try:
        execution = job.run(
            args=app_args,
            await_termination=wait,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Backfill failed: {exc}") from exc

    output.success(
        "✓ Backfill of %s started (execution #%s, window %s → %s)",
        name,
        getattr(execution, "id", "?"),
        start_time.isoformat(),
        end_time.isoformat(),
    )
    if output.JSON_MODE:
        output.print_json(_execution_to_dict(execution))


@job_group.command("stop")
@click.argument("name")
@click.pass_context
def job_stop(ctx: click.Context, name: str) -> None:
    """Stop the most recent execution of ``name``.

    Args:
        ctx: Click context.
        name: Job name.
    """
    executions = _executions(_get_job(ctx, name))
    if not executions:
        raise click.ClickException(f"No executions for job '{name}'.")
    latest = executions[0]
    try:
        latest.stop()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Stop failed: {exc}") from exc
    output.success("✓ Stopped execution %s of %s", getattr(latest, "id", "?"), name)


@job_group.command("logs")
@click.argument("name")
@click.option(
    "--execution",
    "execution_id",
    type=int,
    help="Specific execution ID; defaults to the most recent.",
)
@click.pass_context
def job_logs(ctx: click.Context, name: str, execution_id: int | None) -> None:
    """Download stdout/stderr logs for a job execution.

    Args:
        ctx: Click context.
        name: Job name.
        execution_id: Specific execution; latest if omitted.
    """
    executions = _executions(_get_job(ctx, name))
    if not executions:
        raise click.ClickException(f"No executions for job '{name}'.")
    target = executions[0]
    if execution_id is not None:
        matches = [e for e in executions if getattr(e, "id", None) == execution_id]
        if not matches:
            raise click.ClickException(f"Execution #{execution_id} not found.")
        target = matches[0]

    try:
        stdout_path, stderr_path = target.download_logs()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not download logs: {exc}") from exc

    if output.JSON_MODE:
        output.print_json({"stdout": stdout_path, "stderr": stderr_path})
        return
    output.info("stdout: %s", stdout_path or "<none>")
    output.info("stderr: %s", stderr_path or "<none>")


@job_group.command("history")
@click.argument("name")
@click.pass_context
def job_history(ctx: click.Context, name: str) -> None:
    """List past executions of a job (newest first).

    Args:
        ctx: Click context.
        name: Job name.
    """
    executions = _executions(_get_job(ctx, name))
    rows = [
        [
            getattr(e, "id", "?"),
            getattr(e, "state", "?"),
            getattr(e, "final_status", "-"),
            getattr(e, "submission_time", "-"),
        ]
        for e in executions
    ]
    output.print_table(["ID", "STATE", "FINAL", "SUBMITTED"], rows)


@job_group.command("schedule")
@click.argument("name")
@click.argument("cron")
@click.option(
    "--start-time",
    type=click.DateTime(formats=_DT_FORMATS),
    help="ISO timestamp for the first trigger (e.g. 2026-04-26T09:00:00).",
)
@click.option(
    "--end-time",
    type=click.DateTime(formats=_DT_FORMATS),
    help="ISO timestamp for the last trigger.",
)
@click.option(
    "--start-offset-seconds",
    "start_offset_seconds",
    type=int,
    default=None,
    help=(
        "Per-fire offset for HOPS_START_TIME (data window start). Negative looks "
        "back from the cron fire (e.g. -3600 = window starts 1h before fire). "
        "Default (omitted) = previous cron fire (last execution time)."
    ),
)
@click.option(
    "--end-offset-seconds",
    "end_offset_seconds",
    type=int,
    default=None,
    help=(
        "Per-fire offset for HOPS_END_TIME (data window end). 0 = cron fire time. "
        "Default (omitted) = cron fire time."
    ),
)
@click.option(
    "--catchup/--no-catchup",
    "catchup",
    default=False,
    show_default=True,
    help="Replay missed fires on recovery (one execution per missed interval).",
)
@click.option(
    "--max-active-runs",
    "max_active_runs",
    type=int,
    default=1,
    show_default=True,
    help="Upper bound on concurrent executions of this job.",
)
@click.option(
    "--max-catchup-runs",
    "max_catchup_runs",
    type=int,
    default=None,
    help="Upper bound on missed intervals to replay during catchup (most-recent wins).",
)
@click.pass_context
def job_schedule(
    ctx: click.Context,
    name: str,
    cron: str,
    start_time: datetime | None,
    end_time: datetime | None,
    start_offset_seconds: int | None,
    end_offset_seconds: int | None,
    catchup: bool,
    max_active_runs: int,
    max_catchup_runs: int | None,
) -> None:
    """Attach (or update) a Quartz cron schedule to ``name``.

    The cron interval is the firing cadence. The data window the job
    consumes per fire is controlled by ``--start-offset-seconds`` /
    ``--end-offset-seconds`` (relative to the fire time). ``--catchup``
    replays missed intervals after an outage; ``--max-catchup-runs``
    caps how many missed intervals are replayed.

    Args:
        ctx: Click context.
        name: Job name.
        cron: Quartz cron expression.
        start_time: ISO timestamp for the first trigger.
        end_time: ISO timestamp for the last trigger.
        start_offset_seconds: Per-fire offset for ``HOPS_START_TIME``.
        end_offset_seconds: Per-fire offset for ``HOPS_END_TIME``.
        catchup: Replay missed fires on recovery.
        max_active_runs: Upper bound on concurrent executions.
        max_catchup_runs: Upper bound on missed intervals to replay during catchup.
    """
    job = _get_job(ctx, name)
    try:
        schedule = job.schedule(
            cron_expression=cron,
            start_time=start_time,
            end_time=end_time,
            catchup=catchup,
            max_active_runs=max_active_runs,
            start_time_offset_seconds=start_offset_seconds,
            end_time_offset_seconds=end_offset_seconds,
            max_catchup_runs=max_catchup_runs,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Schedule failed: {exc}") from exc
    output.success("✓ Scheduled %s (%s)", name, cron)
    if output.JSON_MODE:
        output.print_json(_schedule_to_dict(schedule))


@job_group.command("schedule-info")
@click.argument("name")
@click.pass_context
def job_schedule_info(ctx: click.Context, name: str) -> None:
    """Print the currently attached schedule, if any.

    Args:
        ctx: Click context.
        name: Job name.
    """
    job = _get_job(ctx, name)
    schedule = getattr(job, "job_schedule", None)
    if schedule is None:
        output.info("No schedule attached to %s.", name)
        return
    if output.JSON_MODE:
        output.print_json(_schedule_to_dict(schedule))
        return
    rows = [
        ["Cron", getattr(schedule, "cron_expression", "-")],
        ["Start", getattr(schedule, "start_date_time", "-")],
        ["End", getattr(schedule, "end_date_time", "-")],
        ["Enabled", getattr(schedule, "enabled", "-")],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@job_group.command("unschedule")
@click.argument("name")
@click.pass_context
def job_unschedule(ctx: click.Context, name: str) -> None:
    """Remove the schedule from a job.

    Args:
        ctx: Click context.
        name: Job name.
    """
    job = _get_job(ctx, name)
    try:
        job.unschedule()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Unschedule failed: {exc}") from exc
    output.success("✓ Removed schedule from %s", name)


@job_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def job_delete(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete a job definition (does not affect past execution logs).

    Args:
        ctx: Click context.
        name: Job name.
        yes: Skip confirmation when True.
    """
    job = _get_job(ctx, name)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete job '{name}'?", abort=True)
    try:
        job.delete()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted job %s", name)


def _get_job(ctx: click.Context, name: str) -> Any:
    project = session.get_project(ctx)
    try:
        job = project.get_job_api().get_job(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Job '{name}' not found: {exc}") from exc
    if job is None:
        raise click.ClickException(f"Job '{name}' not found.")
    return job


def _executions(job: Any) -> list[Any]:
    try:
        execs = list(job.get_executions() or [])
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list executions: {exc}") from exc
    execs.sort(key=lambda e: getattr(e, "id", 0), reverse=True)
    return execs


def _execution_to_dict(execution: Any) -> dict[str, Any]:
    return {
        "id": getattr(execution, "id", None),
        "state": getattr(execution, "state", None),
        "final_status": getattr(execution, "final_status", None),
        "submission_time": getattr(execution, "submission_time", None),
    }


def _schedule_to_dict(schedule: Any) -> dict[str, Any]:
    to_dict = getattr(schedule, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    return {
        "cron_expression": getattr(schedule, "cron_expression", None),
        "start_date_time": getattr(schedule, "start_date_time", None),
        "end_date_time": getattr(schedule, "end_date_time", None),
    }
