"""``hops fv`` — feature view read and write commands.

Listing feature views is a REST-only operation (the SDK's ``get_feature_views``
requires a name). Creation supports the ``--join`` mini-grammar shared with
``hops fg derive`` and an ``--transform fn:col`` form that attaches either
built-in or custom transformation functions by name.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
from hopsworks.cli import joinspec, output, session


@click.group("fv")
def fv_group() -> None:
    """Feature view commands."""


@fv_group.command("list")
@click.pass_context
def fv_list(ctx: click.Context) -> None:
    """List every feature view in the active project's feature store.

    Args:
        ctx: Click context.
    """
    fs = session.get_feature_store(ctx)
    items = _list_feature_views(fs)
    rows = []
    for item in items:
        rows.append(
            [
                item.get("id", "?"),
                item.get("name", "?"),
                item.get("version", "?"),
                ", ".join(item.get("labels", []) or []) or "-",
                output.first_line(item.get("description"), empty=""),
            ]
        )
    output.print_table(["ID", "NAME", "VERSION", "LABELS", "DESCRIPTION"], rows)


@fv_group.command("info")
@click.argument("name")
@click.option("--version", type=int, help="Feature view version; defaults to latest.")
@click.pass_context
def fv_info(ctx: click.Context, name: str, version: int | None) -> None:
    """Show metadata for a single feature view.

    Args:
        ctx: Click context.
        name: Feature view name.
        version: Specific version to inspect.
    """
    fs = session.get_feature_store(ctx)
    try:
        fv = fs.get_feature_view(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature view '{name}' not found: {exc}") from exc

    if output.JSON_MODE:
        output.print_json(_fv_to_dict(fv))
        return

    rows = [
        ["ID", getattr(fv, "id", "?")],
        ["Name", getattr(fv, "name", "?")],
        ["Version", getattr(fv, "version", "?")],
        ["Labels", ", ".join(getattr(fv, "labels", []) or []) or "-"],
        ["Description", output.first_line(getattr(fv, "description", ""))],
    ]
    output.print_table(["FIELD", "VALUE"], rows)

    features = getattr(fv, "features", []) or []
    if features:
        output.info("")
        output.info("Features:")
        rows = [
            [
                getattr(f, "name", "?"),
                getattr(f, "type", "?"),
                "yes" if getattr(f, "label", False) else "",
            ]
            for f in features
        ]
        output.print_table(["NAME", "TYPE", "LABEL"], rows)


def _list_feature_views(fs: Any) -> list[dict[str, Any]]:
    """Fetch all feature views via raw REST.

    The SDK's ``get_feature_views(name)`` requires a name, so for the
    ``fv list`` case we call the underlying endpoint directly with the same
    authenticated client the SDK already set up.

    Args:
        fs: Feature store object; used only to read the feature store ID.

    Returns:
        A list of dicts with at least ``id``/``name``/``version`` keys.
    """
    from hopsworks_common.core import rest

    fs_id = getattr(fs, "id", None)
    if fs_id is None:
        return []
    try:
        payload = rest.send_request(
            "GET", rest.project_path("featurestores", fs_id, "featureview")
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list feature views: {exc}") from exc
    items = payload.get("items") if isinstance(payload, dict) else payload
    return items or []


def _fv_to_dict(fv: Any) -> dict[str, Any]:
    return {
        "id": getattr(fv, "id", None),
        "name": getattr(fv, "name", None),
        "version": getattr(fv, "version", None),
        "labels": list(getattr(fv, "labels", []) or []),
        "description": getattr(fv, "description", None),
        "features": [
            {
                "name": getattr(f, "name", None),
                "type": getattr(f, "type", None),
                "label": getattr(f, "label", False),
            }
            for f in getattr(fv, "features", []) or []
        ],
    }


# region Write commands


@fv_group.command("create")
@click.argument("name")
@click.option(
    "--feature-group",
    "base_fg",
    required=True,
    help="Base feature group on the left of the join (name[:version]).",
)
@click.option(
    "--join",
    "joins",
    multiple=True,
    help='Join spec, repeatable: "<fg>[:<ver>] <INNER|LEFT|RIGHT|FULL> <on>[=<right_on>] [prefix]".',
)
@click.option(
    "--transform",
    "transforms",
    multiple=True,
    help='Attach a transformation function, repeatable: "function_name:column".',
)
@click.option("--labels", help="Comma-separated label columns.")
@click.option("--version", type=int, help="Feature view version.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def fv_create(
    ctx: click.Context,
    name: str,
    base_fg: str,
    joins: tuple[str, ...],
    transforms: tuple[str, ...],
    labels: str | None,
    version: int | None,
    description: str,
) -> None:
    """Create a feature view from one or more feature groups.

    Args:
        ctx: Click context.
        name: Feature view name.
        base_fg: Base feature group ``name[:version]``.
        joins: Repeatable join specs.
        transforms: Repeatable ``function_name:column`` specs.
        labels: Comma-separated label columns.
        version: Feature view version.
        description: Free-form description.
    """
    fs = session.get_feature_store(ctx)
    base_name, base_ver = _split_name_version(base_fg)
    try:
        base = fs.get_feature_group(base_name, version=base_ver)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Base FG '{base_fg}' not found: {exc}") from exc

    query = base.select_all()
    for raw in joins:
        try:
            spec = joinspec.parse(raw)
        except joinspec.JoinSpecError as exc:
            raise click.BadParameter(str(exc), param_hint="--join") from exc
        try:
            other = fs.get_feature_group(spec.fg_name, version=spec.version)
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Joined FG '{spec.fg_name}' not found: {exc}"
            ) from exc
        query = query.join(
            other.select_all(),
            on=[spec.on] if not spec.right_on else None,
            left_on=[spec.on] if spec.right_on else None,
            right_on=[spec.right_on] if spec.right_on else None,
            join_type=spec.join_type.lower(),
            prefix=spec.prefix,
        )

    tf_objs = _resolve_transforms(fs, transforms)
    label_list = [lbl.strip() for lbl in (labels or "").split(",") if lbl.strip()]

    try:
        fv = fs.create_feature_view(
            name=name,
            query=query,
            version=version,
            description=description,
            labels=label_list or None,
            transformation_functions=tf_objs or None,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create feature view: {exc}") from exc

    output.success(
        "✓ Created feature view %s v%s",
        getattr(fv, "name", name),
        getattr(fv, "version", "?"),
    )


@fv_group.command("get")
@click.argument("name")
@click.option("--version", type=int, help="Feature view version.")
@click.option(
    "--entry",
    required=True,
    help='Comma-separated primary-key bindings, e.g. "id=42,region=eu".',
)
@click.pass_context
def fv_get(ctx: click.Context, name: str, version: int | None, entry: str) -> None:
    """Look up a single feature vector from the online store.

    Args:
        ctx: Click context.
        name: Feature view name.
        version: Feature view version.
        entry: Comma-separated ``key=value`` pairs for the primary keys.
    """
    fs = session.get_feature_store(ctx)
    try:
        fv = fs.get_feature_view(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature view '{name}' not found: {exc}") from exc

    entry_dict = _parse_entry(entry)
    try:
        vector = fv.get_feature_vector(entry=entry_dict, return_type="list")
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Lookup failed: {exc}") from exc

    if output.JSON_MODE:
        output.print_json({"entry": entry_dict, "vector": list(vector or [])})
        return
    output.info("%s", vector)


@fv_group.command("read")
@click.argument("name")
@click.option("--version", type=int, help="Feature view version.")
@click.option(
    "--n", type=int, default=100, show_default=True, help="Max rows to fetch."
)
@click.option(
    "--output",
    "output_file",
    type=click.Path(),
    help="Write to a file (parquet/csv/json).",
)
@click.option("--start-time", "start_time", help="ISO timestamp lower bound.")
@click.option("--end-time", "end_time", help="ISO timestamp upper bound.")
@click.pass_context
def fv_read(
    ctx: click.Context,
    name: str,
    version: int | None,
    n: int,
    output_file: str | None,
    start_time: str | None,
    end_time: str | None,
) -> None:
    """Batch-read from the offline feature store.

    Args:
        ctx: Click context.
        name: Feature view name.
        version: Feature view version.
        n: Max rows rendered when no ``--output`` file is given.
        output_file: Optional destination path (parquet/csv/json).
        start_time: ISO timestamp lower bound.
        end_time: ISO timestamp upper bound.
    """
    fs = session.get_feature_store(ctx)
    try:
        fv = fs.get_feature_view(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature view '{name}' not found: {exc}") from exc

    try:
        df = fv.get_batch_data(
            start_time=start_time,
            end_time=end_time,
            dataframe_type="pandas",
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Batch read failed: {exc}") from exc

    if output_file:
        _write_dataframe(df, output_file)
        output.success("✓ Wrote %d rows to %s", len(df), output_file)
        return

    df = df.head(n)
    if output.JSON_MODE:
        output.print_json(df.to_dict(orient="records"))
        return
    columns = list(df.columns)
    rows = [[row[c] for c in columns] for _, row in df.iterrows()]
    output.print_table(columns, rows)


@fv_group.command("delete")
@click.argument("name")
@click.option("--version", type=int, help="Feature view version.")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.option("--force", is_flag=True, help="Delete even when training data exists.")
@click.pass_context
def fv_delete(
    ctx: click.Context,
    name: str,
    version: int | None,
    yes: bool,
    force: bool,
) -> None:
    """Delete a feature view and its training datasets.

    Args:
        ctx: Click context.
        name: Feature view name.
        version: Feature view version.
        yes: Skip confirmation when True.
        force: Pass ``force=True`` to the SDK.
    """
    fs = session.get_feature_store(ctx)
    try:
        fv = fs.get_feature_view(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature view '{name}' not found: {exc}") from exc

    if not yes and not output.JSON_MODE:
        click.confirm(
            f"Delete feature view '{name}' v{getattr(fv, 'version', '?')}?",
            abort=True,
        )
    try:
        fv.delete(force=force) if force else fv.delete()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted feature view %s", name)


# region Helpers


def _split_name_version(spec: str) -> tuple[str, int | None]:
    if ":" not in spec:
        return spec.strip(), None
    name, _, ver = spec.partition(":")
    try:
        return name.strip(), int(ver)
    except ValueError as exc:
        raise click.BadParameter(
            f"Invalid version in '{spec}': {exc}", param_hint="--feature-group"
        ) from exc


def _parse_entry(entry: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in entry.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        key, _, value = item.partition("=")
        # Try to coerce to int or float; leave as string otherwise.
        key, value = key.strip(), value.strip()
        try:
            out[key] = int(value)
            continue
        except ValueError:
            pass
        try:
            out[key] = float(value)
            continue
        except ValueError:
            out[key] = value
    return out


def _resolve_transforms(fs: Any, transforms: tuple[str, ...]) -> list[Any]:
    if not transforms:
        return []
    resolved: list[Any] = []
    for spec in transforms:
        if ":" not in spec:
            raise click.BadParameter(
                f"Transform '{spec}' must be 'function:column'.",
                param_hint="--transform",
            )
        fn_name, _, col = spec.partition(":")
        try:
            fn = fs.get_transformation_function(name=fn_name.strip())
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Transformation '{fn_name}' not found: {exc}"
            ) from exc
        # The SDK pattern for attaching a TF to a column is calling the
        # HopsworksUdf with the column name as input.
        try:
            resolved.append(fn(col.strip()))
        except TypeError:
            resolved.append(fn)
    return resolved


def _write_dataframe(df: Any, path: str) -> None:
    suffix = Path(path).suffix.lower()
    if suffix == ".parquet":
        df.to_parquet(path, index=False)
    elif suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix in (".json", ".jsonl"):
        df.to_json(path, orient="records", lines=(suffix == ".jsonl"))
    else:
        raise click.BadParameter(
            f"Unsupported output format: {suffix}. Use .parquet, .csv, or .json.",
            param_hint="--output",
        )
