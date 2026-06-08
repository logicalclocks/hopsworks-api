"""``hops td`` — list, materialize, and retrieve training datasets.

The underlying SDK API produces two-sided tuples for labelled views
(``features_df, labels_df``); when no labels are declared the second value is
``None`` and we render just the feature frame.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
from hopsworks.cli import lineage, output, session


@click.group("td")
def td_group() -> None:
    """Training dataset commands."""


@td_group.command("list")
@click.argument("feature_view")
@click.argument("fv_version", type=int, required=False)
@click.option(
    "--version",
    "version_opt",
    type=int,
    help="Feature view version; same as the positional FV_VERSION. Defaults to latest.",
)
@click.pass_context
def td_list(
    ctx: click.Context,
    feature_view: str,
    fv_version: int | None,
    version_opt: int | None,
) -> None:
    """List training dataset versions generated from a feature view.

    The FV version can be given positionally (``td list <fv> 2``, matching
    ``td compute``) or via ``--version``; the positional form wins.

    Args:
        ctx: Click context.
        feature_view: Feature view name.
        fv_version: Feature view version (positional, optional).
        version_opt: Feature view version (``--version``, optional).
    """
    version = fv_version if fv_version is not None else version_opt
    fs = session.get_feature_store(ctx)
    try:
        fv = fs.get_feature_view(feature_view, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Feature view '{feature_view}' not found: {exc}"
        ) from exc

    try:
        tds = fv.get_training_datasets()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list training datasets: {exc}") from exc

    rows = []
    for td in tds or []:
        rows.append(
            [
                getattr(td, "version", "?"),
                getattr(td, "data_format", "-"),
                getattr(td, "coalesce", "-"),
                _format_splits(td),
            ]
        )
    output.print_table(["VERSION", "FORMAT", "COALESCE", "SPLITS"], rows)


@td_group.command("compute")
@click.argument("feature_view")
@click.argument("fv_version", type=int)
@click.option("--split", help='Train/test split, e.g. "train:0.8,test:0.2".')
@click.option("--start-time", "start_time", help="ISO timestamp lower bound.")
@click.option("--end-time", "end_time", help="ISO timestamp upper bound.")
@click.option("--description", default="", help="Training dataset description.")
@click.option(
    "--format",
    "data_format",
    default="parquet",
    show_default=True,
    help="Output format.",
)
@click.pass_context
def td_compute(
    ctx: click.Context,
    feature_view: str,
    fv_version: int,
    split: str | None,
    start_time: str | None,
    end_time: str | None,
    description: str,
    data_format: str,
) -> None:
    """Materialize a training dataset from a feature view.

    Args:
        ctx: Click context.
        feature_view: Feature view name.
        fv_version: Feature view version.
        split: Comma-separated ``name:ratio`` split definitions.
        start_time: ISO timestamp lower bound.
        end_time: ISO timestamp upper bound.
        description: Free-form description.
        data_format: Output format (parquet, csv, tfrecord, ...).
    """
    fv = _get_fv(ctx, feature_view, fv_version)
    splits = _parse_splits(split)

    try:
        if splits:
            td_version, _ = fv.create_train_test_split(
                test_size=splits.get("test") or splits.get("validation") or 0.2,
                description=description,
                data_format=data_format,
                start_time=start_time,
                end_time=end_time,
            )
        else:
            td_version, _ = fv.create_training_data(
                description=description,
                data_format=data_format,
                start_time=start_time or "",
                end_time=end_time or "",
            )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not materialize training data: {exc}"
        ) from exc

    output.success(
        "✓ Materialized training dataset v%s for %s", td_version, feature_view
    )
    if output.JSON_MODE:
        output.print_json(
            {"feature_view": feature_view, "td_version": td_version, "splits": splits}
        )


@td_group.command("read")
@click.argument("feature_view")
@click.argument("fv_version", type=int)
@click.option(
    "--td-version",
    "td_version",
    type=int,
    required=True,
    help="Training dataset version to retrieve.",
)
@click.option(
    "--split", help='Split to read when the TD was created with a split, e.g. "train".'
)
@click.option("--output", "output_file", type=click.Path(), help="Write to a file.")
@click.option(
    "--n", type=int, default=100, show_default=True, help="Max rows to render."
)
@click.pass_context
def td_read(
    ctx: click.Context,
    feature_view: str,
    fv_version: int,
    td_version: int,
    split: str | None,
    output_file: str | None,
    n: int,
) -> None:
    """Retrieve a previously materialized training dataset.

    Args:
        ctx: Click context.
        feature_view: Feature view name.
        fv_version: Feature view version.
        td_version: Training dataset version.
        split: Optional split name.
        output_file: Optional file to write the dataframe to.
        n: Max rows rendered when no ``--output`` file is given.
    """
    fv = _get_fv(ctx, feature_view, fv_version)
    try:
        if split:
            features, labels = fv.get_train_test_split(
                training_dataset_version=td_version
            )
            df = (
                features.get(split) or labels.get(split)
                if isinstance(features, dict)
                else features
            )
        else:
            features, labels = fv.get_training_data(training_dataset_version=td_version)
            df = features
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not read training data: {exc}") from exc

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


@td_group.command("delete")
@click.argument("feature_view")
@click.argument("fv_version", type=int)
@click.option(
    "--td-version", "td_version", type=int, help="Specific TD version to delete."
)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def td_delete(
    ctx: click.Context,
    feature_view: str,
    fv_version: int,
    td_version: int | None,
    yes: bool,
) -> None:
    """Delete a training dataset (single version or all).

    Args:
        ctx: Click context.
        feature_view: Feature view name.
        fv_version: Feature view version.
        td_version: Specific TD version; deletes all when omitted.
        yes: Skip confirmation when True.
    """
    fv = _get_fv(ctx, feature_view, fv_version)
    if not yes and not output.JSON_MODE:
        target = f"v{td_version}" if td_version else "ALL versions"
        click.confirm(
            f"Delete training dataset {target} for {feature_view} v{fv_version}?",
            abort=True,
        )
    try:
        if td_version is not None:
            fv.delete_training_dataset(training_dataset_version=td_version)
        else:
            fv.delete_all_training_datasets()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted training dataset for %s", feature_view)


@td_group.command("lineage")
@click.argument("feature_view")
@click.argument("fv_version", type=int, required=False)
@click.option(
    "--version",
    "version_opt",
    type=int,
    help="Feature view version; same as the positional FV_VERSION. Defaults to latest.",
)
@click.option(
    "--td-version",
    type=int,
    help="Training dataset version to trace downstream models for.",
)
@click.pass_context
def td_lineage(
    ctx: click.Context,
    feature_view: str,
    fv_version: int | None,
    version_opt: int | None,
    td_version: int | None,
) -> None:
    """Show lineage for training datasets of a feature view.

    Training datasets inherit their upstream lineage from the parent feature
    view (its parent feature groups).
    Downstream covers models trained on the dataset; pass ``--td-version`` to
    scope downstream models to a single training dataset version.

    Args:
        ctx: Click context.
        feature_view: Feature view name.
        fv_version: Feature view version (positional, optional).
        version_opt: Feature view version (``--version``, optional).
        td_version: Training dataset version to scope downstream models.
    """
    version = fv_version if fv_version is not None else version_opt
    fv = _get_fv(ctx, feature_view, version)
    td_suffix = f" td v{td_version}" if td_version is not None else ""
    label = (
        f"training datasets of feature view "
        f"{getattr(fv, 'name', feature_view)} v{getattr(fv, 'version', '?')}{td_suffix}"
    )
    sections = [
        (
            "upstream",
            "parent_feature_group",
            lineage.fetch(fv.get_parent_feature_groups),
        ),
        (
            "downstream",
            "model",
            lineage.fetch(
                lambda: fv.get_models_provenance(training_dataset_version=td_version)
            ),
        ),
    ]
    lineage.render(label, sections)


def _get_fv(ctx: click.Context, name: str, version: int | None) -> Any:
    fs = session.get_feature_store(ctx)
    try:
        return fs.get_feature_view(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature view '{name}' not found: {exc}") from exc


def _format_splits(td: Any) -> str:
    """Render every split as ``name:pct`` for the SPLITS column.

    Distinguishes a multi-split TD from a single-split one. Metadata only (no
    data read); row counts would need materializing each split.
    """
    splits = getattr(td, "splits", None) or []
    if not splits:
        return "-"
    parts = []
    for s in splits:
        name = getattr(s, "name", "?")
        pct = getattr(s, "percentage", None)
        parts.append(f"{name}:{pct:g}" if pct is not None else str(name))
    return "/".join(parts)


def _parse_splits(spec: str | None) -> dict[str, float]:
    if not spec:
        return {}
    out: dict[str, float] = {}
    for item in spec.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        name, _, ratio = item.partition(":")
        try:
            out[name.strip()] = float(ratio)
        except ValueError as exc:
            raise click.BadParameter(
                f"Invalid split ratio '{ratio}': {exc}", param_hint="--split"
            ) from exc
    return out


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
