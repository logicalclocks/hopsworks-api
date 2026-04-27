"""``hops fg`` — feature group commands (read + write).

Covers the full feature-group surface: ``list``, ``info``, ``preview``,
``features`` (reads) plus ``create``, ``create-external``, ``insert``,
``derive``, ``delete``, ``stats``, ``search``, ``keywords``/``add-keyword``/
``remove-keyword`` (writes). All operations go through the SDK in-process so
Hopsworks domain logic is invoked directly, with no subprocess hop.
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import joinspec, output, session


@click.group("fg")
def fg_group() -> None:
    """Feature group commands."""


@fg_group.command("list")
@click.pass_context
def fg_list(ctx: click.Context) -> None:
    """List all feature groups in the active project's feature store.

    Args:
        ctx: Click context.
    """
    fs = session.get_feature_store(ctx)
    fgs = fs.get_feature_groups()
    rows = []
    for fg in fgs:
        rows.append(
            [
                getattr(fg, "id", "?"),
                getattr(fg, "name", "?"),
                getattr(fg, "version", "?"),
                _fg_type_label(fg),
                "yes" if getattr(fg, "online_enabled", False) else "no",
            ]
        )
    output.print_table(["ID", "NAME", "VERSION", "TYPE", "ONLINE"], rows)


@fg_group.command("info")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version; defaults to latest.")
@click.pass_context
def fg_info(ctx: click.Context, name: str, version: int | None) -> None:
    """Print metadata for a single feature group.

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Specific version to inspect; latest if omitted.
    """
    fg = _get_fg(ctx, name, version)
    if output.JSON_MODE:
        output.print_json(_fg_to_dict(fg))
        return
    rows = [
        ["ID", getattr(fg, "id", "?")],
        ["Name", getattr(fg, "name", "?")],
        ["Version", getattr(fg, "version", "?")],
        ["Type", _fg_type_label(fg)],
        ["Online", "yes" if getattr(fg, "online_enabled", False) else "no"],
        ["Primary key", ", ".join(getattr(fg, "primary_key", []) or []) or "-"],
        ["Event time", getattr(fg, "event_time", None) or "-"],
        ["Description", output.first_line(getattr(fg, "description", ""))],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@fg_group.command("preview")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version; defaults to latest.")
@click.option("--n", type=int, default=10, show_default=True, help="Number of rows.")
@click.option(
    "--online", is_flag=True, help="Read from the online store (default: offline)."
)
@click.pass_context
def fg_preview(
    ctx: click.Context, name: str, version: int | None, n: int, online: bool
) -> None:
    """Show the first ``n`` rows of a feature group.

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Specific version to read.
        n: Number of rows to fetch.
        online: When True, read from the online store.
    """
    fg = _get_fg(ctx, name, version)
    try:
        df = fg.read(online=online, dataframe_type="pandas").head(n)
    except Exception as exc:  # noqa: BLE001 - SDK raises a bag of types
        raise click.ClickException(f"Could not read feature group: {exc}") from exc

    if output.JSON_MODE:
        output.print_json(df.to_dict(orient="records"))
        return

    columns = list(df.columns)
    rows = [[row[c] for c in columns] for _, row in df.iterrows()]
    output.print_table(columns, rows)


@fg_group.command("features")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version; defaults to latest.")
@click.pass_context
def fg_features(ctx: click.Context, name: str, version: int | None) -> None:
    """List the schema of a feature group (name, type, primary key).

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Specific version to inspect.
    """
    fg = _get_fg(ctx, name, version)
    rows = []
    for f in getattr(fg, "features", []) or []:
        rows.append(
            [
                getattr(f, "name", "?"),
                getattr(f, "type", "?"),
                "yes" if getattr(f, "primary", False) else "",
                "yes" if getattr(f, "partition", False) else "",
                getattr(f, "description", "") or "",
            ]
        )
    output.print_table(["NAME", "TYPE", "PK", "PARTITION", "DESCRIPTION"], rows)


def _get_fg(ctx: click.Context, name: str, version: int | None) -> Any:
    fs = session.get_feature_store(ctx)
    try:
        return fs.get_feature_group(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature group '{name}' not found: {exc}") from exc


def _fg_type_label(fg: Any) -> str:
    """Best-effort one-word type label: ``stream``, ``external``, ``cached``.

    Args:
        fg: A FeatureGroup, ExternalFeatureGroup, or SpineGroup instance.

    Returns:
        A short label suitable for table output.
    """
    cls = type(fg).__name__.lower()
    if "external" in cls:
        return "external"
    if "spine" in cls:
        return "spine"
    if getattr(fg, "stream", False):
        return "stream"
    return "cached"


def _fg_to_dict(fg: Any) -> dict[str, Any]:
    return {
        "id": getattr(fg, "id", None),
        "name": getattr(fg, "name", None),
        "version": getattr(fg, "version", None),
        "type": _fg_type_label(fg),
        "online_enabled": getattr(fg, "online_enabled", False),
        "primary_key": list(getattr(fg, "primary_key", []) or []),
        "event_time": getattr(fg, "event_time", None),
        "description": getattr(fg, "description", None),
        "features": [
            {
                "name": getattr(f, "name", None),
                "type": getattr(f, "type", None),
                "primary": getattr(f, "primary", False),
                "partition": getattr(f, "partition", False),
                "description": getattr(f, "description", None),
            }
            for f in getattr(fg, "features", []) or []
        ],
    }


# region Write commands


@fg_group.command("create")
@click.argument("name")
@click.option(
    "--version",
    type=int,
    help="Feature group version; backend auto-assigns if omitted.",
)
@click.option(
    "--primary-key",
    "primary_key",
    help="Comma-separated primary-key column names.",
)
@click.option(
    "--features",
    "features_spec",
    help='Comma-separated schema, e.g. "id:bigint,amount:double".',
)
@click.option("--event-time", "event_time", help="Name of the event-time column.")
@click.option(
    "--partition-key",
    "partition_key",
    help="Comma-separated partition column names.",
)
@click.option("--online", is_flag=True, help="Enable the online feature store.")
@click.option(
    "--embedding",
    "embedding_spec",
    help='Embedding feature spec, "col:dim[:metric]". Auto-enables online.',
)
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def fg_create(
    ctx: click.Context,
    name: str,
    version: int | None,
    primary_key: str | None,
    features_spec: str | None,
    event_time: str | None,
    partition_key: str | None,
    online: bool,
    embedding_spec: str | None,
    description: str,
) -> None:
    """Register a new feature group in the feature store.

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Version; auto-assigned when omitted.
        primary_key: Comma-separated primary keys.
        features_spec: Comma-separated ``name:type`` pairs.
        event_time: Event-time column name.
        partition_key: Comma-separated partition keys.
        online: Whether to enable the online store.
        embedding_spec: ``col:dim[:metric]`` for a vector feature.
        description: Free-form description.
    """
    fs = session.get_feature_store(ctx)
    pk_list = _split_csv(primary_key)
    partition_list = _split_csv(partition_key)
    features = _build_features(features_spec)
    embedding_index = None
    if embedding_spec:
        embedding_index, emb_feature = _build_embedding(embedding_spec)
        features.append(emb_feature)
        online = True

    try:
        fg = fs.create_feature_group(
            name=name,
            version=version,
            description=description,
            online_enabled=online,
            primary_key=pk_list or None,
            partition_key=partition_list or None,
            event_time=event_time,
            features=features or None,
            embedding_index=embedding_index,
        )
        fg.save()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create feature group: {exc}") from exc

    output.success(
        "✓ Created feature group %s v%s",
        getattr(fg, "name", name),
        getattr(fg, "version", "?"),
    )
    if output.JSON_MODE:
        output.print_json(_fg_to_dict(fg))


@fg_group.command("create-external")
@click.argument("name")
@click.option(
    "--connector", "connector_name", required=True, help="Storage connector name."
)
@click.option(
    "--query", "query", required=True, help="SQL query backing this feature group."
)
@click.option("--version", type=int, help="Feature group version.")
@click.option("--primary-key", "primary_key", help="Comma-separated primary keys.")
@click.option("--event-time", "event_time", help="Event-time column.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def fg_create_external(
    ctx: click.Context,
    name: str,
    connector_name: str,
    query: str,
    version: int | None,
    primary_key: str | None,
    event_time: str | None,
    description: str,
) -> None:
    """Register an external feature group backed by a storage connector.

    Args:
        ctx: Click context.
        name: Feature group name.
        connector_name: Storage connector to query.
        query: SQL backing the feature group.
        version: Version; auto-assigned when omitted.
        primary_key: Comma-separated primary keys.
        event_time: Event-time column.
        description: Free-form description.
    """
    fs = session.get_feature_store(ctx)
    try:
        connector = fs.get_data_source(connector_name).storage_connector
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Connector '{connector_name}' not found: {exc}"
        ) from exc

    try:
        fg = fs.create_external_feature_group(
            name=name,
            storage_connector=connector,
            query=query,
            version=version,
            description=description,
            primary_key=_split_csv(primary_key) or None,
            event_time=event_time,
        )
        fg.save()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create external FG: {exc}") from exc

    output.success(
        "✓ Created external feature group %s v%s",
        getattr(fg, "name", name),
        getattr(fg, "version", "?"),
    )


@fg_group.command("insert")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version; defaults to latest.")
@click.option(
    "--file",
    "file_path",
    type=click.Path(exists=True),
    help="CSV or JSON file to load.",
)
@click.option(
    "--generate", type=int, help="Generate N rows of synthetic data and insert."
)
@click.option(
    "--online", is_flag=True, help="Write to online store only (skip offline backfill)."
)
@click.pass_context
def fg_insert(
    ctx: click.Context,
    name: str,
    version: int | None,
    file_path: str | None,
    generate: int | None,
    online: bool,
) -> None:
    """Insert rows into a feature group from a file, stdin, or synthetic data.

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Feature group version.
        file_path: CSV or JSON file path. When omitted, read JSON from stdin.
        generate: Generate N synthetic rows instead.
        online: When True, pass ``start_offline_materialization=False``.
    """
    fg = _get_fg(ctx, name, version)

    try:
        import pandas as pd  # noqa: PLC0415 - pandas import cost is high
    except ImportError as exc:  # pragma: no cover - pandas is a base dep
        raise click.ClickException("pandas is required for hops fg insert.") from exc

    if generate is not None and generate > 0:
        df = _generate_dataframe(fg, generate)
    elif file_path:
        path = Path(file_path)
        df = pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_json(path)
    else:
        data = sys.stdin.read()
        if not data.strip():
            raise click.ClickException(
                "No data provided. Pass --file, --generate N, or pipe JSON on stdin."
            )
        df = pd.DataFrame(json.loads(data))

    write_options = {"start_offline_materialization": False} if online else None

    try:
        job, _ = fg.insert(df, write_options=write_options)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Insert failed: {exc}") from exc

    output.success("✓ Inserted %d rows into %s", len(df), getattr(fg, "name", name))
    if job is not None and not output.JSON_MODE:
        output.info("Materialization job: %s", getattr(job, "name", "?"))
    if output.JSON_MODE:
        output.print_json({"rows": len(df), "feature_group": getattr(fg, "name", name)})


@fg_group.command("derive")
@click.argument("name")
@click.option("--base", "base_fg", required=True, help="Base feature group name.")
@click.option(
    "--join",
    "joins",
    multiple=True,
    help='Join spec, repeatable: "<fg>[:<ver>] <INNER|LEFT|RIGHT|FULL> <on>[=<right_on>] [prefix]".',
)
@click.option(
    "--primary-key", "primary_key", required=True, help="Comma-separated primary keys."
)
@click.option(
    "--event-time", "event_time", help="Event-time column for the derived FG."
)
@click.option("--online", is_flag=True, help="Enable online store for the derived FG.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def fg_derive(
    ctx: click.Context,
    name: str,
    base_fg: str,
    joins: tuple[str, ...],
    primary_key: str,
    event_time: str | None,
    online: bool,
    description: str,
) -> None:
    """Create a new feature group derived from joining existing ones.

    Args:
        ctx: Click context.
        name: Derived feature group name.
        base_fg: Base feature group on the left side of the join.
        joins: One or more join specs.
        primary_key: Comma-separated primary keys.
        event_time: Event-time column for the derived FG.
        online: Whether to enable online storage.
        description: Free-form description.
    """
    fs = session.get_feature_store(ctx)
    try:
        base = fs.get_feature_group(base_fg)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Base FG '{base_fg}' not found: {exc}") from exc

    query = base.select_all()
    parents = [base]
    for raw in joins:
        spec = _parse_join(raw)
        try:
            other = fs.get_feature_group(spec.fg_name, version=spec.version)
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Joined FG '{spec.fg_name}' not found: {exc}"
            ) from exc
        parents.append(other)
        query = query.join(
            other.select_all(),
            on=[spec.on] if not spec.right_on else None,
            left_on=[spec.on] if spec.right_on else None,
            right_on=[spec.right_on] if spec.right_on else None,
            join_type=spec.join_type.lower(),
            prefix=spec.prefix,
        )

    try:
        fg = fs.create_feature_group(
            name=name,
            description=description or f"Derived from {base_fg}",
            primary_key=_split_csv(primary_key) or None,
            event_time=event_time,
            online_enabled=online,
            parents=parents,
        )
        fg.insert(query.read())
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not derive feature group: {exc}") from exc

    output.success("✓ Derived feature group %s from %s", name, base_fg)


@fg_group.command("delete")
@click.argument("name")
@click.option(
    "--version", type=int, help="Feature group version; required when multiple exist."
)
@click.option("--yes", is_flag=True, help="Skip the interactive confirmation prompt.")
@click.pass_context
def fg_delete(ctx: click.Context, name: str, version: int | None, yes: bool) -> None:
    """Delete a feature group and all its data.

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Specific version to delete.
        yes: Skip confirmation when True.
    """
    fg = _get_fg(ctx, name, version)
    if not yes and not output.JSON_MODE:
        click.confirm(
            f"Delete feature group '{name}' v{getattr(fg, 'version', '?')}? "
            "This wipes all offline and online data.",
            abort=True,
        )
    try:
        fg.delete()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted feature group %s", name)


@fg_group.command("stats")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version.")
@click.option(
    "--compute",
    is_flag=True,
    help="Trigger recomputation; otherwise read cached stats.",
)
@click.pass_context
def fg_stats(ctx: click.Context, name: str, version: int | None, compute: bool) -> None:
    """Show or recompute statistics for a feature group.

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Feature group version.
        compute: When True, trigger a recomputation job.
    """
    fg = _get_fg(ctx, name, version)
    if compute:
        try:
            fg.compute_statistics()
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Stats compute failed: {exc}") from exc
        output.success("✓ Triggered statistics compute for %s", name)
        return

    try:
        stats = fg.statistics
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not read stats: {exc}") from exc

    if output.JSON_MODE:
        to_dict = getattr(stats, "to_dict", None)
        output.print_json(to_dict() if callable(to_dict) else {"stats": str(stats)})
        return
    output.info("%s", stats)


@fg_group.command("search")
@click.argument("name")
@click.option(
    "--vector", required=True, help='Comma-separated query vector, e.g. "0.1,0.2,0.3".'
)
@click.option("--col", help="Embedding column name; inferred when only one exists.")
@click.option(
    "--k", type=int, default=5, show_default=True, help="Number of neighbors."
)
@click.option("--version", type=int, help="Feature group version.")
@click.pass_context
def fg_search(
    ctx: click.Context,
    name: str,
    vector: str,
    col: str | None,
    k: int,
    version: int | None,
) -> None:
    """Run a KNN similarity search against a feature group's embedding column.

    Args:
        ctx: Click context.
        name: Feature group name.
        vector: Comma-separated query vector.
        col: Embedding column; inferred when omitted.
        k: Number of neighbors to return.
        version: Feature group version.
    """
    fg = _get_fg(ctx, name, version)
    try:
        query = [float(x) for x in vector.split(",") if x.strip()]
    except ValueError as exc:
        raise click.BadParameter(
            f"Invalid vector: {exc}", param_hint="--vector"
        ) from exc

    try:
        results = fg.find_neighbors(embedding=query, col=col, k=k)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"KNN search failed: {exc}") from exc

    if output.JSON_MODE:
        output.print_json(
            [{"score": score, "row": list(row)} for score, row in results or []]
        )
        return
    rows = [
        [f"{score:.4f}", ", ".join(str(v) for v in row)] for score, row in results or []
    ]
    output.print_table(["SCORE", "ROW"], rows)


@fg_group.command("keywords")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version.")
@click.pass_context
def fg_keywords(ctx: click.Context, name: str, version: int | None) -> None:
    """Show all tags attached to a feature group (aka keywords).

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Feature group version.
    """
    fg = _get_fg(ctx, name, version)
    try:
        tags = fg.get_tags()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not read tags: {exc}") from exc

    if output.JSON_MODE:
        output.print_json({k: _tag_value(v) for k, v in (tags or {}).items()})
        return
    rows = [[k, _tag_value(v)] for k, v in (tags or {}).items()]
    output.print_table(["KEYWORD", "VALUE"], rows)


@fg_group.command("add-keyword")
@click.argument("name")
@click.argument("keyword")
@click.option("--value", default="true", help="Tag value; defaults to 'true'.")
@click.option("--version", type=int, help="Feature group version.")
@click.pass_context
def fg_add_keyword(
    ctx: click.Context, name: str, keyword: str, value: str, version: int | None
) -> None:
    """Attach a keyword (tag) to a feature group.

    Args:
        ctx: Click context.
        name: Feature group name.
        keyword: Tag name.
        value: Tag value.
        version: Feature group version.
    """
    fg = _get_fg(ctx, name, version)
    try:
        fg.add_tag(name=keyword, value=value)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not add keyword: {exc}") from exc
    output.success("✓ Added keyword '%s' to %s", keyword, name)


@fg_group.command("remove-keyword")
@click.argument("name")
@click.argument("keyword")
@click.option("--version", type=int, help="Feature group version.")
@click.pass_context
def fg_remove_keyword(
    ctx: click.Context, name: str, keyword: str, version: int | None
) -> None:
    """Remove a keyword (tag) from a feature group.

    Args:
        ctx: Click context.
        name: Feature group name.
        keyword: Tag name.
        version: Feature group version.
    """
    fg = _get_fg(ctx, name, version)
    try:
        fg.delete_tag(keyword)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not remove keyword: {exc}") from exc
    output.success("✓ Removed keyword '%s' from %s", keyword, name)


# region Helpers


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _build_features(spec: str | None) -> list[Any]:
    """Parse ``--features`` into a list of SDK ``Feature`` objects.

    Args:
        spec: ``name:type`` pairs separated by commas, or None.

    Returns:
        A list of ``hsfs.feature.Feature`` instances.
    """
    if not spec:
        return []
    from hsfs.feature import Feature  # noqa: PLC0415

    out = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise click.BadParameter(
                f"Feature '{item}' must be 'name:type'.",
                param_hint="--features",
            )
        col, dtype = item.split(":", 1)
        out.append(Feature(col.strip(), dtype.strip()))
    return out


def _build_embedding(spec: str) -> tuple[Any, Any]:
    """Parse ``--embedding`` into an ``(EmbeddingIndex, Feature)`` pair.

    Args:
        spec: ``col:dim[:metric]`` where metric defaults to ``cosine``.

    Returns:
        Tuple of the EmbeddingIndex to attach to the FG and the array feature.
    """
    parts = spec.split(":")
    if len(parts) < 2:
        raise click.BadParameter(
            f"Embedding '{spec}' must be 'col:dim[:metric]'.",
            param_hint="--embedding",
        )
    col = parts[0].strip()
    try:
        dim = int(parts[1])
    except ValueError as exc:
        raise click.BadParameter(f"Embedding dim must be int: {exc}") from exc
    metric = parts[2].strip() if len(parts) >= 3 else "cosine"
    from hsfs.embedding import EmbeddingFeature, EmbeddingIndex  # noqa: PLC0415
    from hsfs.feature import Feature  # noqa: PLC0415

    index = EmbeddingIndex()
    index.add_embedding(
        EmbeddingFeature(name=col, dimension=dim, similarity_function_type=metric)
    )
    feature = Feature(col, "array<float>")
    return index, feature


def _generate_dataframe(fg: Any, n: int) -> Any:
    """Build a synthetic DataFrame matching the feature group schema.

    Values are drawn from simple distributions keyed by the declared SDK type
    so that an ``insert`` smoke-test does not require real data.

    Args:
        fg: Feature group with a populated ``features`` list.
        n: Number of rows to generate.

    Returns:
        A pandas DataFrame.
    """
    import pandas as pd  # noqa: PLC0415

    data: dict[str, list[Any]] = {}
    for feat in getattr(fg, "features", []) or []:
        name = getattr(feat, "name", None)
        if not name:
            continue
        dtype = str(getattr(feat, "type", "") or "").lower()
        data[name] = [_synth_value(dtype, i) for i in range(n)]
    return pd.DataFrame(data)


def _synth_value(dtype: str, i: int) -> Any:
    if "bigint" in dtype or dtype.startswith("int"):
        return i + 1
    if "double" in dtype or "float" in dtype:
        return round(random.random() * 100, 2)  # noqa: S311 - synthetic data only
    if "bool" in dtype:
        return bool(i % 2)
    if "timestamp" in dtype or "date" in dtype:
        import pandas as pd  # noqa: PLC0415

        return pd.Timestamp("2026-01-01") + pd.Timedelta(days=i)
    return f"val_{i}"


def _tag_value(tag: Any) -> Any:
    """Normalize an SDK ``Tag`` object (or primitive) to a JSON-friendly value."""
    if hasattr(tag, "value"):
        return tag.value
    return tag


def _parse_join(raw: str) -> joinspec.JoinSpec:
    try:
        return joinspec.parse(raw)
    except joinspec.JoinSpecError as exc:
        raise click.BadParameter(str(exc), param_hint="--join") from exc
