"""``hops transformation`` — list and register transformation functions.

``list`` enumerates built-in and user-defined transformations. ``create``
registers every ``@udf``-decorated function declared in either an inline
Python expression or a file; we validate via ``ast`` so a syntax error in
the source is reported before any network call.
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output, session


@click.group("transformation")
def transformation_group() -> None:
    """Transformation function commands."""


@transformation_group.command("list")
@click.pass_context
def transformation_list(ctx: click.Context) -> None:
    """List every transformation function in the feature store.

    Args:
        ctx: Click context.
    """
    fs = session.get_feature_store(ctx)
    try:
        tfs = fs.get_transformation_functions()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list transformations: {exc}") from exc

    rows = []
    for tf in tfs or []:
        udf = getattr(tf, "hopsworks_udf", None) or tf
        rows.append(
            [
                getattr(tf, "id", "?"),
                getattr(udf, "function_name", None) or getattr(tf, "name", "?"),
                getattr(tf, "version", "-"),
                _format_output_type(udf),
            ]
        )
    output.print_table(["ID", "NAME", "VERSION", "OUTPUT TYPE"], rows)


@transformation_group.command("create")
@click.option(
    "--file",
    "file_path",
    type=click.Path(exists=True),
    help="Python file containing one or more @udf-decorated functions.",
)
@click.option(
    "--code",
    help='Inline @udf source, e.g. "@udf(float)\\ndef x(c): return c * 2".',
)
@click.option(
    "--version",
    "version",
    type=int,
    default=1,
    show_default=True,
    help=(
        "Initial version. The backend rejects null versions with a 500, "
        "so we default to 1 instead of letting it raise."
    ),
)
@click.pass_context
def transformation_create(
    ctx: click.Context, file_path: str | None, code: str | None, version: int
) -> None:
    """Register every ``@udf``-decorated function from the source.

    Either ``--file`` or ``--code`` is required. The source is validated via
    ``ast.parse`` before import so syntax errors surface with a clear message.
    Each ``@udf``-decorated function is imported in-process and passed to
    ``fs.create_transformation_function()`` individually so a single
    pipeline file can declare all its transformations together.

    Args:
        ctx: Click context.
        file_path: Python source path.
        code: Inline Python source.
        version: Initial version for the registered transformation functions.
    """
    if not file_path and not code:
        raise click.UsageError("Provide either --file or --code.")
    if file_path and code:
        raise click.UsageError("Provide --file or --code, not both.")

    source = Path(file_path).read_text() if file_path else (code or "")
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise click.ClickException(f"Invalid Python source: {exc}") from exc

    fn_names = _udf_names(tree)
    if not fn_names:
        raise click.ClickException("No @udf-decorated function found in source.")

    fs = session.get_feature_store(ctx)
    registered: list[dict[str, Any]] = []
    for fn_name in fn_names:
        udf = _load_udf(source, fn_name, origin=file_path or "<inline>")
        try:
            tf = fs.create_transformation_function(
                transformation_function=udf, version=version
            )
            tf.save()
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not create transformation '{fn_name}': {exc}"
            ) from exc
        version = getattr(tf, "version", "?")
        registered.append({"name": fn_name, "version": version})
        output.success("✓ Created transformation %s v%s", fn_name, version)

    if output.JSON_MODE:
        output.print_json(registered)


def _udf_names(tree: ast.Module) -> list[str]:
    """Return every ``@udf``-decorated function name in source order.

    Args:
        tree: Parsed AST of the source.

    Returns:
        Function names in the order they appear in the source.
    """
    names: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for deco in node.decorator_list:
            target = deco.func if isinstance(deco, ast.Call) else deco
            name = getattr(target, "id", None) or getattr(target, "attr", None)
            if name == "udf":
                names.append(node.name)
                break
    return names


def _load_udf(source: str, fn_name: str, origin: str) -> Any:
    """Execute ``source`` in an isolated namespace and return the named function.

    We import ``hsfs.hopsworks_udf`` up-front so the ``@udf`` decorator is in
    the module globals the user's source expects.

    Args:
        source: Python source text.
        fn_name: Function to pull out of the namespace post-execution.
        origin: File path or ``"<inline>"`` used in error messages.

    Returns:
        The decorated callable.
    """
    spec = importlib.util.spec_from_loader(f"hops_cli_udf_{fn_name}", loader=None)
    if spec is None:
        raise click.ClickException("Could not build an import spec for the UDF.")
    module = importlib.util.module_from_spec(spec)
    from hsfs.hopsworks_udf import udf  # noqa: PLC0415

    module.__dict__["udf"] = udf
    try:
        exec(compile(source, origin, "exec"), module.__dict__)  # noqa: S102
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"UDF import failed: {exc}") from exc
    fn = module.__dict__.get(fn_name)
    if fn is None:
        raise click.ClickException(f"Function '{fn_name}' disappeared after import.")
    return fn


def _format_output_type(udf: Any) -> str:
    otype = getattr(udf, "output_type", None) or getattr(udf, "return_type", None)
    if otype is None:
        return "-"
    if isinstance(otype, type):
        return otype.__name__
    return str(otype)
