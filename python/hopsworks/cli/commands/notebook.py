"""``hops notebook``: write and run Jupyter notebooks on the user's own Jupyter server.

Notebooks are ``.ipynb`` files in the project, named by their path in it (``Users/<username>/x.ipynb``,
the user's Jupyter home unless they changed it in Hopsworks). ``run`` writes one cell, runs it on the notebook's kernel and saves the
outputs, so the notebook opens in the user's JupyterLab exactly as the run left it.
"""

from __future__ import annotations

import sys
import time

import click
from hopsworks.cli import notebook_api, output, session
from hopsworks_common import events


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
    # started, then done or failed with the cell, so a UI can show the notebook from its first run.
    with events.tracked(
        "notebook", "update", long=True, path=path.strip("/")
    ) as change:
        _run(ctx, path, code, cell, markdown, timeout, change)


def _run(
    ctx: click.Context,
    path: str,
    code: str,
    cell: int | None,
    markdown: bool,
    timeout: int,
    change: events.Change,
) -> None:
    text = sys.stdin.read() if code == "-" else code
    project_id = _project_id(ctx)
    try:
        jupyter = notebook_api.connect(project_id, 300)
        root = notebook_api.setup(project_id).root
        project_path = path.strip("/")
        path = notebook_api.in_jupyter(path, root)
        notebook = jupyter.read(path)
        if notebook is None and cell is not None:
            raise click.ClickException(f"Notebook '{project_path}' not found.")
        kernel_name = None
        if notebook is not None:
            kernel_name = notebook.get("metadata", {}).get("kernelspec", {}).get("name")
        jupyter_session = jupyter.session(path, kernel_name)
        if notebook is None:
            notebook = notebook_api.new_notebook(
                jupyter.kernelspec(jupyter_session["kernel"]["name"])
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
            new["id"] = cells[cell - 1].get("id", new["id"])
            cells[cell - 1] = new
            index = cell
        jupyter.write(path, notebook)
        change.detail = {"cell": new["id"], "number": index}
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
        outputs, count, finished, execution, msg_id = run
        change.detail["msg_id"] = msg_id
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


@notebook_group.command("info")
@click.argument("path")
@click.pass_context
def notebook_info(ctx: click.Context, path: str) -> None:
    """List a notebook's cells, their outputs and its kernel state.

    \f
    Args:
        ctx: Click context.
        path: Notebook path in the project.
    """
    project_id = _project_id(ctx)
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
