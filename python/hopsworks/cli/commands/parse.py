"""``hops fg parse``: files under a HopsFS path become a feature group of Markdown pages.

``hops fg parse PATH`` makes or updates the group's Python job ``GROUP_parse``
in the parse class, then the group when it does not exist, and starts one
execution; the parse runs there (``parse_run``), on the document-pipeline
environment, never in the caller. ``hops fg parse delete GROUP`` runs the
job's deletion execution, which deletes the group, then deletes the job.
"""

from __future__ import annotations

import os
import re
import shlex
import tempfile
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import embed_run, output, parse_run, session
from hopsworks.cli.commands import embed


KIND = "parse"
PLATFORM_CLASS = "parse"
ENVIRONMENT = "document-pipeline"
_RUN = "run"
#: Resources of the job's pod: 4 CPU, and 8 GiB over the 5.9 GB peak of a 50-page range.
CORES = 4
MEMORY_MB = 8192


class _ParseGroup(click.Group):
    """`hops fg parse PATH ...` runs; `hops fg parse delete GROUP` deletes."""

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        if not args or args[0] not in self.commands or args[0] == _RUN:
            args = [_RUN, *args]
        return super().parse_args(ctx, args)


@click.group("parse", cls=_ParseGroup)
def fg_parse_group() -> None:
    """Parse documents into a feature group of Markdown pages."""


@fg_parse_group.command(_RUN, cls=embed._RunCommand, hidden=True)
@click.argument("path")
@click.option(
    "--to",
    "to",
    help="The group of pages; the folder or file name plus _pages by default.",
)
@click.option(
    "--ocr",
    type=click.Choice(["auto", "on", "off"]),
    default="auto",
    show_default=True,
    help="Read text from images: auto reads a PDF with OCR when a page has no text layer.",
)
@click.option(
    "--accurate",
    is_flag=True,
    help="Slower table structure; the default keeps every cell's text but may split cells differently.",
)
@click.option(
    "--range-pages",
    type=click.IntRange(min=5),
    default=parse_run.RANGE_PAGES,
    show_default=True,
    help="Pages per conversion of a PDF; lower it for a file that stopped a run.",
)
@click.option(
    "--schedule",
    "every",
    is_flag=False,
    flag_value="daily",
    help="Also parse again on a schedule: daily (the default when given alone), hourly, "
    "or a number of minutes. Faster than daily needs an admin grant.",
)
@click.option(
    "--env", "environment", default=ENVIRONMENT, show_default=True,
    help="Python environment of the job; it must have Docling.",
)  # fmt: skip
@click.option(
    "--parse-here",
    is_flag=True,
    hidden=True,
    help="Run the parse in this process: what the job does.",
)
@click.pass_context
def fg_parse(
    ctx: click.Context,
    path: str,
    to: str | None,
    ocr: str,
    accurate: bool,
    range_pages: int,
    every: str | None,
    environment: str,
    parse_here: bool,
) -> None:
    """Parse the documents under PATH into a feature group of Markdown pages.

    PATH is a file or a folder in the project's files, such as
    Resources/hopsable/uploads (chat attachments) or the dataset a Drive or
    SharePoint sync writes. This command makes the group and its Python job
    GROUP_parse, starts one execution, prints its id and returns; it does not
    wait. Run `hops job wait GROUP_parse --execution ID --timeout 3600` before
    a step that reads the group.

    The group is keyed by (path, page): one row per PDF page or image, one row
    (page 0) per DOCX, PPTX, XLSX, HTML, Markdown, CSV or text file, with its
    text as Markdown, tables as Markdown tables. Embed it for search with
    `hops fg embed GROUP --text text`.

    A rerun parses only files whose content changed and deletes the rows of
    files no longer under PATH. A group parses one PATH: parse another path
    into another group. A run parses up to the cluster's caps (pages and
    minutes per run, a PDF counting its pages and any other file one page)
    and names what it left; the next run goes on. A file over the size cap,
    or one that stops two runs, is skipped and named.

    \f
    Args:
        ctx: Click context.
        path: File or folder in the project.
        to: Group name.
        ocr: auto, on or off.
        accurate: TableFormer ACCURATE instead of FAST.
        range_pages: Pages per conversion.
        every: Schedule of the job.
        environment: Python environment of the job.
        parse_here: Run the parse in this process.
    """
    try:
        root = parse_run.normal_path(path)
    except parse_run.ParseError as exc:
        raise click.BadParameter(str(exc), param_hint="PATH") from exc
    group = (to or default_group(root)).lower()
    fs = session.get_feature_store(ctx)
    if parse_here:
        _parse_here(fs, root, group, ocr, accurate, range_pages)
        return

    project = session.get_project(ctx)
    dataset = project.get_dataset_api()
    if not dataset.exists(root):
        raise click.BadParameter(
            f"{root} is not in project {project.name}.", param_hint="PATH"
        )
    cron = (
        embed.schedule_cron(every, project.id, group, parse_run.VAR_MIN_INTERVAL)
        if every
        else None
    )
    existing = latest_owned(fs, group, root)
    api = project.get_job_api()
    job = _find_job(api, group)
    args = [
        "fg",
        "parse",
        root,
        "--to",
        group,
        "--ocr",
        ocr,
        "--range-pages",
        str(range_pages),
    ]
    if accurate:
        args.append("--accurate")
    args.append("--parse-here")
    job = _save_job(project, api, job, group, root, args, environment)
    created = existing is None
    if created:
        _create_group(fs, group, root)
    execution = embed._start(job, group, kind=KIND)
    if cron:
        embed._schedule(job, group, cron, execution)
    summary = {
        "group": group,
        "path": root,
        "created": created,
        "job": job.name,
        "execution": getattr(execution, "id", None),
        "schedule": cron,
    }
    if output.JSON_MODE:
        output.print_json(summary)
        return
    output.success(
        "✓ Started execution %s of job %s: it parses %s into %s and is not waited for",
        summary["execution"], job.name, root, group,
    )  # fmt: skip
    if cron:
        output.info(f"Scheduled job {job.name} ({cron})")


def default_group(root: str) -> str:
    """The folder or file name, as a feature group name, plus _pages."""
    name = Path(root).stem or "files"
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "files"
    if slug[0].isdigit():
        slug = f"f_{slug}"
    return f"{slug}_pages"


def latest_owned(fs: Any, group: str, root: str) -> Any:
    """The group's latest version when hops fg parse made it for `root`; None when it does not exist."""
    try:
        found = embed_run.versions_of(fs, group)
    except embed_run.EmbedError as exc:
        raise click.ClickException(str(exc)) from exc
    if not found:
        return None
    info = parse_run.described(found[-1])
    if info is None or info["owner"] != group:
        raise click.ClickException(
            f"Feature group '{group}' exists and was not made by hops fg parse. Pass --to with another name."
        )
    if info["path"] != root:
        raise click.ClickException(
            f"{group} parses {info['path']}: parse {root} into another group (--to), "
            f"or delete {group} first (hops fg parse delete {group}). Nothing was changed."
        )
    return found[-1]


def _job_name(group: str) -> str:
    return f"{group}_parse"


def _runner(group: str) -> str:
    return f"{parse_run.STATE_DIR}/{group}_parse_job.py"


def _owned(thing: Any, group: str) -> bool:
    info = parse_run.described(thing)
    return info is not None and info["owner"] == group


def _find_job(api: Any, group: str) -> Any:
    """The group's job when hops fg parse made it; None when there is none; refused when another job has its name."""
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
            f"Job '{name}' exists and was not made by hops fg parse: it is left alone and nothing was started."
        )
    return existing


def _save_job(project, api, existing, group, root, args, environment) -> Any:
    """The group's job, made or updated to run `args`, one execution at a time, in the parse class."""
    from hopsworks.cli.commands.files import makedirs  # noqa: PLC0415

    name = _job_name(group)
    dataset = project.get_dataset_api()
    try:
        makedirs(dataset, parse_run.STATE_DIR)
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, os.path.basename(_runner(group)))
            with open(local, "w", encoding="utf-8") as f:
                f.write(embed._RUNNER_CODE)
            dataset.upload(local, parse_run.STATE_DIR, overwrite=True)
        config = (
            existing.config if existing is not None else api.get_configuration("PYTHON")
        )
        config["appPath"] = _runner(group)
        config["defaultArgs"] = shlex.join(args)
        config[embed.MAX_CONCURRENT] = 1
        config[embed.PLATFORM_CLASS_KEY] = PLATFORM_CLASS
        config["environmentName"] = environment
        resources = config.setdefault("resourceConfig", {})
        resources["cores"] = CORES
        resources["memory"] = MEMORY_MB
        config["description"] = parse_run.description(root, group)
        with output.sdk_quiet():
            if existing is None:
                return api.create_job(name=name, config=config)
            return existing.save()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not prepare job '{name}': {exc}. Nothing was started."
        ) from exc


def _create_group(fs: Any, group: str, root: str) -> Any:
    from hsfs.feature import Feature  # noqa: PLC0415

    try:
        with output.sdk_quiet():
            fg = fs.create_feature_group(
                name=group,
                version=1,
                description=parse_run.description(root, group),
                online_enabled=False,
                primary_key=parse_run.KEYS,
                features=[
                    Feature("path", "string"),
                    Feature("page", "bigint"),
                    Feature("text", "string"),
                    Feature("file_sha256", "string"),
                    Feature("parser", "string"),
                    Feature("parsed_at", "timestamp"),
                ],
                statistics_config=False,
            )
            fg.save()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not create {group}: {exc}. Its job was updated and nothing was started; run hops fg parse again."
        ) from exc
    return fg


def _parse_here(
    fs: Any, root: str, group: str, ocr: str, accurate: bool, range_pages: int
) -> None:
    """The parse itself: what the job's execution runs."""
    from hopsworks.cli import pipeline_data  # noqa: PLC0415

    fg = latest_owned(fs, group, root)
    if fg is None:
        raise click.ClickException(
            f"Feature group '{group}' does not exist: run hops fg parse again to make it. Nothing was written."
        )
    try:
        embed_run.execution_id()
        outcome = parse_run.run(
            fg=fg,
            mount=pipeline_data._mount(),
            root=root,
            group=group,
            conv=parse_run.Converter(accurate, threads=min(os.cpu_count() or 1, CORES)),
            ocr=ocr,
            range_pages=range_pages,
            max_pages=embed_run.variable_int(
                parse_run.VAR_MAX_PAGES, parse_run.MAX_PAGES
            ),
            max_file_mb=embed_run.variable_int(
                parse_run.VAR_MAX_FILE_MB, parse_run.MAX_FILE_MB
            ),
            max_minutes=embed_run.variable_int(
                parse_run.VAR_MAX_MINUTES, parse_run.MAX_MINUTES
            ),
            log=output.info,
        )
    except (parse_run.ParseError, embed_run.EmbedError) as exc:
        raise click.ClickException(str(exc)) from exc
    summary = {"group": group, "path": root, **vars(outcome)}
    if output.JSON_MODE:
        output.print_json(summary)
        return
    output.success(
        "✓ %s: %d files parsed (%d rows), %d unchanged, %d removed (%d rows deleted)",
        group, len(outcome.parsed), outcome.rows, outcome.unchanged, len(outcome.removed),
        outcome.deleted_rows,
    )  # fmt: skip
    for path, why in outcome.skipped.items():
        output.info(f"Not parsed: {path}: {why}")
    if outcome.left:
        output.info(
            f"Stopped by {outcome.stopped_by}; left for the next run: {', '.join(outcome.left)}"
        )


@fg_parse_group.command("delete")
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
@click.option(embed.IN_JOB, "in_job", is_flag=True, hidden=True)
@click.pass_context
def fg_parse_delete(
    ctx: click.Context, group: str, yes: bool, dry_run: bool, timeout: int, in_job: bool
) -> None:
    """Delete a group of pages, its job, its schedule and its run state.

    Only what `hops fg parse` made is deleted: the group's versions and the job
    whose descriptions name the group as their owner, the job's script and its
    state file under Resources/hopsable-parse. The parsed files are untouched;
    so is an embedding group made from this one (hops fg embed delete).

    The deletion runs as an execution of the group's job, so it starts only
    when no parse runs and no parse starts after it. The command waits for it,
    then deletes the job. Interrupted or timed out, run the delete again.

    \f
    Args:
        ctx: Click context.
        group: Group name.
        yes: Skip confirmation.
        dry_run: List only.
        timeout: Seconds to wait for the deletion execution.
        in_job: Run the deletion in this process.
    """
    group = group.lower()
    project = session.get_project(ctx)
    fs = session.get_feature_store(ctx)
    try:
        found = embed_run.versions_of(fs, group)
    except embed_run.EmbedError as exc:
        raise click.ClickException(str(exc)) from exc
    groups = [fg for fg in found if _owned(fg, group)]
    if in_job:
        for fg in groups:
            fg.delete()
        output.success(
            "✓ Deleted %s versions %s", group, [fg.version for fg in groups] or "none"
        )
        return
    api = project.get_job_api()
    job = _find_job(api, group)
    dataset = project.get_dataset_api()
    paths = [
        p
        for p in (_runner(group), f"{parse_run.STATE_DIR}/{group}.json")
        if dataset.exists(p)
    ]
    plan = {
        "group": group,
        "versions": [fg.version for fg in groups],
        "job": getattr(job, "name", None),
        "files": paths,
        "kept_versions": [fg.version for fg in found if not _owned(fg, group)],
    }
    if not groups and job is None and not paths:
        raise click.ClickException(f"Nothing made by hops fg parse is named '{group}'.")
    if output.JSON_MODE and dry_run:
        output.print_json(plan)
        return
    output.info(
        f"Group {group} versions {plan['versions'] or 'none'}; job {plan['job'] or 'none'}; "
        f"files {paths or 'none'}"
    )
    if dry_run:
        return
    if not yes:
        output.confirm(
            f"Delete these for '{group}'? Their data cannot be restored.",
            f"Deleting the group of pages '{group}'",
        )
    try:
        if job is None:
            for fg in groups:
                fg.delete()
        else:
            plan["deletion_execution"] = embed._run_deletion(
                job, group, timeout, kind=KIND
            )
            job.delete()
        for p in paths:
            dataset.remove(p)
    except click.ClickException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not delete '{group}': {exc}. Run the delete again for what is left."
        ) from exc
    if output.JSON_MODE:
        output.print_json(plan)
        return
    output.success("✓ Deleted group of pages %s", group)
