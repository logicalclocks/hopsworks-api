"""``hops notebook``: write and run Jupyter notebooks on the user's own Jupyter server.

Notebooks are ``.ipynb`` files in the project, named by their path in it (``Users/<username>/x.ipynb``,
the user's Jupyter home unless they changed it in Hopsworks). ``run`` writes one cell, runs it on the notebook's kernel and saves the
outputs, so the notebook opens in the user's JupyterLab exactly as the run left it. ``update``
rewrites several cells in one save and runs none.
"""

from __future__ import annotations

import json
import sys
import time

import click
from hopsworks.cli import notebook_api, output, session


OUTPUT_LIMIT = 8000


@click.group("notebook")
def notebook_group() -> None:
    """Notebooks: cells run on the user's Jupyter kernel."""


@notebook_group.command("start")
@click.option(
    "--env",
    help="The Python environment to run in, kept in the Jupyter settings. Default: the saved one.",
)
@click.option(
    "--timeout", default=300, show_default=True, help="Seconds to wait for the server."
)
@click.pass_context
def notebook_start(ctx: click.Context, env: str | None, timeout: int) -> None:
    """Start the Jupyter server, or check the running one answers.

    Prints the Python environment its kernels run in and the folder for notebooks.

    \f
    Args:
        ctx: Click context.
        env: Python environment to start the server in.
        timeout: Seconds to wait for the server to answer.
    """
    project_id = _project_id(ctx)
    try:
        notebook_api.connect(project_id, timeout, env)
        setup = notebook_api.setup(project_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not start Jupyter: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(
            {"running": True, "environment": setup.environment, "home": setup.home}
        )
        return
    output.success(
        "✓ Jupyter is running in the environment %s. Write notebooks under %s/",
        setup.environment,
        setup.home,
    )


@notebook_group.command("stop")
@click.pass_context
def notebook_stop(ctx: click.Context) -> None:
    """Stop the Jupyter server and its kernels.

    \f
    Args:
        ctx: Click context.
    """
    project_id = _project_id(ctx)
    try:
        if notebook_api.server(project_id) is None:
            output.info("Jupyter is not running.")
            return
        notebook_api.stop(project_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not stop Jupyter: {exc}") from exc
    output.success("✓ Jupyter stopped")


@notebook_group.command("run")
@click.argument("path")
@click.option(
    "--code",
    required=True,
    help="The cell's source; '-' reads it from stdin.",
)
@click.option(
    "--cell",
    type=click.IntRange(min=1),
    help="Replace cell N (as `hops notebook info` numbers them) instead of adding one.",
)
@click.option("--markdown", is_flag=True, help="Write a markdown cell; nothing runs.")
@click.option(
    "--timeout",
    default=600,
    show_default=True,
    help="Seconds before the cell is interrupted.",
)
@click.pass_context
def notebook_run(
    ctx: click.Context,
    path: str,
    code: str,
    cell: int | None,
    markdown: bool,
    timeout: int,
) -> None:
    """Add a cell to a notebook and run it, saving its outputs.

    PATH is the notebook's path in the project, e.g. `Users/<username>/churn.ipynb`; the notebook is
    created when missing.
    Every run of one notebook uses the same kernel, so variables carry over between cells.

    \f
    Args:
        ctx: Click context.
        path: Notebook path in the project.
        code: Cell source, or "-" for stdin.
        cell: 1-based index of the cell to replace.
        markdown: Write a markdown cell instead of a code cell.
        timeout: Seconds before a running cell is interrupted.
    """
    if not path.endswith(".ipynb"):
        raise click.BadParameter("A notebook path ends in .ipynb.", param_hint="PATH")
    text = sys.stdin.read() if code == "-" else code
    project_id = _project_id(ctx)
    try:
        # A markdown cell needs the notebook only, not its kernel.
        jupyter = (notebook_api.attach if markdown else notebook_api.connect)(
            project_id, 300
        )
        project_path = path.strip("/")
        path = notebook_api.in_jupyter(path, notebook_api.root(project_id))
        notebook = jupyter.read(path)
        if notebook is None and cell is not None:
            raise click.ClickException(f"Notebook '{project_path}' not found.")
        kernel_name = None
        if notebook is not None:
            kernel_name = notebook.get("metadata", {}).get("kernelspec", {}).get("name")
        jupyter_session = None if markdown else jupyter.session(path, kernel_name)
        if notebook is None:
            notebook = notebook_api.new_notebook(
                jupyter.kernelspec(
                    jupyter_session["kernel"]["name"] if jupyter_session else None
                )
            )
        new = notebook_api.new_cell("markdown" if markdown else "code", text)
        cells = notebook["cells"]
        if cell is None:
            cells.append(new)
            index = len(cells)
        elif cell > len(cells):
            raise click.BadParameter(
                f"The notebook has {len(cells)} cells.", param_hint="--cell"
            )
        elif _edited_by_user(cells[cell - 1]):
            raise click.ClickException(
                f"Cell {cell} of '{project_path}' was edited by the user. "
                "Ask the user before replacing it, or add a new cell."
            )
        else:
            # The same cell, rewritten: an open notebook view keeps it where it is.
            new = notebook_api.rewrite_cell(cells[cell - 1], new["cell_type"], text)
            cells[cell - 1] = new
            index = cell
        jupyter.write(path, notebook)
    except click.ClickException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not write notebook '{path}': {exc}") from exc

    if markdown:
        _report(project_path, index, None, [], True)
        return
    try:
        # A kernel still starting may close a socket before it takes the cell: nothing ran, so
        # the cell is sent again, a few times at most.
        for attempt in range(3):
            try:
                run = jupyter.execute(jupyter_session, new["id"], text, timeout)
                break
            except notebook_api.Unreached:
                if attempt == 2:
                    raise
                time.sleep(2)
                jupyter_session = jupyter.session(path, kernel_name)
        outputs, count, finished, execution, _ = run
        # The user may have edited the notebook in JupyterLab meanwhile: save onto the latest.
        latest = jupyter.read(path) or notebook
        for c in latest["cells"]:
            if c.get("id") == new["id"]:
                c["outputs"] = outputs
                c["execution_count"] = count
                c["metadata"] = {**c.get("metadata", {}), "execution": execution}
        jupyter.write(path, latest)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not run cell {index} of '{path}': {exc}"
        ) from exc
    _report(project_path, index, count, outputs, finished)
    if not finished:
        raise click.ClickException(
            f"Cell {index} was still running after {timeout} s and was interrupted. "
            "Run it again with a longer --timeout, or split it."
        )
    error = next((o for o in outputs if o["output_type"] == "error"), None)
    if error:
        raise click.ClickException(
            f"Cell {index} raised {error['ename']}: {error['evalue']}"
        )


@notebook_group.command("update")
@click.argument("path")
@click.pass_context
def notebook_update(ctx: click.Context, path: str) -> None:
    """Rewrite several cells in one save, from JSON on stdin; none runs.

    Each edit is {"cell": N, "markdown": "<text>"} or {"cell": N, "code": "<text>"}, N as
    `hops notebook info` numbers the cells:

    \b
    hops notebook update Users/alice/churn.ipynb <<'EOF'
    [{"cell": 2, "markdown": "## Churn by plan"},
     {"cell": 4, "code": "df.groupby('plan').churned.mean()"}]
    EOF

    A cell keeps its id, its place and its metadata, and a markdown or raw cell that stays that type
    keeps its attachments. A code cell loses its outputs, run count and run timing: run it with
    `hops notebook run PATH --cell N --code -`. A cell the user edited in Hopsable is refused, and
    then no cell changes.

    \f
    Args:
        ctx: Click context.
        path: Notebook path in the project.
    """
    edits = _edits(sys.stdin.read())
    project_path = path.strip("/")
    project_id = _project_id(ctx)
    try:
        jupyter = notebook_api.attach(project_id, 300)
        path = notebook_api.in_jupyter(path, notebook_api.root(project_id))
        notebook = jupyter.read(path)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not read notebook '{project_path}': {exc}"
        ) from exc
    if notebook is None:
        raise click.ClickException(f"Notebook '{project_path}' not found.")
    cells = notebook["cells"]
    beyond = [n for n, _, _ in edits if n > len(cells)]
    if beyond:
        raise click.UsageError(
            f"The notebook has {len(cells)} cells; there is no cell {beyond[0]}."
        )
    refused = [n for n, _, _ in edits if _edited_by_user(cells[n - 1])]
    if refused:
        cells_were = "Cell %s was" if len(refused) == 1 else "Cells %s were"
        raise click.ClickException(
            f"{cells_were % ', '.join(map(str, refused))} edited by the user in '{project_path}'; "
            "no cell was changed. Ask the user before replacing them, or leave them out."
        )
    for n, kind, text in edits:
        # The same cell, rewritten: an open notebook view keeps it where it is.
        cells[n - 1] = notebook_api.rewrite_cell(cells[n - 1], kind, text)
    try:
        jupyter.write(path, notebook)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not write notebook '{project_path}': {exc}"
        ) from exc
    numbers = [n for n, _, _ in edits]
    code = [n for n, kind, _ in edits if kind == "code"]
    if output.JSON_MODE:
        output.print_json({"path": project_path, "cells": numbers, "not_run": code})
        return
    output.success("✓ Cells %s of %s saved", ", ".join(map(str, numbers)), project_path)
    if code:
        output.info(
            "Code cells %s not run: `hops notebook run %s --cell N --code -` runs one.",
            ", ".join(map(str, code)),
            project_path,
        )


def _edits(raw: str) -> list[tuple[int, str, str]]:
    """The edits from stdin as (cell number, cell type, text), checked before anything is read."""
    try:
        edits = json.loads(raw)
    except ValueError as exc:
        raise click.UsageError(f"The edits on stdin are not JSON: {exc}") from exc
    if not isinstance(edits, list) or not edits:
        raise click.UsageError("Give at least one edit, as a JSON list on stdin.")
    checked = []
    for edit in edits:
        kinds = (
            [k for k in ("markdown", "code") if k in edit]
            if isinstance(edit, dict)
            else []
        )
        cell = edit.get("cell") if isinstance(edit, dict) else None
        if (
            type(cell) is not int
            or cell < 1
            or len(kinds) != 1
            or not isinstance(edit[kinds[0]], str)
        ):
            raise click.UsageError(
                'Each edit is {"cell": N, "markdown": "<text>"} or {"cell": N, "code": "<text>"}, '
                f"N from 1; got {json.dumps(edit)[:200]}"
            )
        checked.append((cell, kinds[0], edit[kinds[0]]))
    numbers = [n for n, _, _ in checked]
    twice = next((n for n in numbers if numbers.count(n) > 1), None)
    if twice is not None:
        raise click.UsageError(f"Cell {twice} is edited twice; give it once.")
    return checked


@notebook_group.command("info")
@click.argument("path")
@click.option(
    "--source",
    is_flag=True,
    help="Print every cell's whole text, with its number, type and id, instead of the table.",
)
@click.pass_context
def notebook_info(ctx: click.Context, path: str, source: bool) -> None:
    """List a notebook's cells, their outputs and its kernel state.

    \f
    Args:
        ctx: Click context.
        path: Notebook path in the project.
        source: Print the cells' text instead.
    """
    project_id = _project_id(ctx)
    if source:
        _print_source(project_id, path)
        return
    try:
        jupyter = notebook_api.connect(project_id, 300)
        setup = notebook_api.setup(project_id)
        path = notebook_api.in_jupyter(path, setup.root)
        notebook = jupyter.read(path)
        existing = jupyter.find_session(path)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not find notebook '{path}': {exc}") from exc
    if notebook is None:
        raise click.ClickException(f"Notebook '{path}' not found.")
    kernel = existing["kernel"].get("execution_state", "?") if existing else "none"
    output.info("Environment: %s", setup.environment)
    output.info("Kernel: %s", kernel)
    rows = []
    for i, c in enumerate(notebook["cells"], 1):
        first = notebook_api.source(c).strip().split("\n", 1)[0]
        rows.append(
            [
                i,
                c["cell_type"],
                c.get("execution_count") or "-",
                len(c.get("outputs", [])) if c["cell_type"] == "code" else "-",
                first[:60],
            ]
        )
    output.print_table(["CELL", "TYPE", "RUN", "OUTPUTS", "SOURCE"], rows)


def _print_source(project_id: int, path: str) -> None:
    project_path = path.strip("/")
    try:
        jupyter = notebook_api.attach(project_id, 300)
        notebook = jupyter.read(
            notebook_api.in_jupyter(path, notebook_api.root(project_id))
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not read notebook '{project_path}': {exc}"
        ) from exc
    if notebook is None:
        raise click.ClickException(f"Notebook '{project_path}' not found.")
    cells = [
        {
            "cell": i,
            "id": c.get("id", ""),
            "type": c["cell_type"],
            "source": notebook_api.source(c),
        }
        for i, c in enumerate(notebook["cells"], 1)
    ]
    if output.JSON_MODE:
        output.print_json(cells)
        return
    for c in cells:
        sys.stdout.write(f"[cell {c['cell']}] {c['type']} {c['id']}\n{c['source']}\n\n")


def _report(
    project_path: str,
    index: int,
    count: int | None,
    outputs: list[dict],
    finished: bool,
) -> None:
    text = notebook_api.describe(outputs, OUTPUT_LIMIT)
    if output.JSON_MODE:
        output.print_json(
            {
                "path": project_path,
                "cell": index,
                "execution_count": count,
                "finished": finished,
                "output": text,
            }
        )
        return
    run = f" ran as In [{count}]" if count is not None else " saved"
    output.success("✓ Cell %d of %s%s", index, project_path, run)
    if text:
        sys.stdout.write(text + "\n")


def _edited_by_user(cell: dict) -> bool:
    # Hopsable's notebook view marks the cells the user changed there.
    return (cell.get("metadata") or {}).get("hopsable", {}).get("editedBy") == "user"


def _project_id(ctx: click.Context) -> int:
    return session.get_project(ctx).id
