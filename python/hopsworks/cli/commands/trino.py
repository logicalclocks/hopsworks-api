"""``hops trino`` — run SQL against the project's Trino coordinator.

Trino in Hopsworks is exposed via ``project.get_trino_api()``. That class
handles host resolution (internal service-discovery vs external loadbalancer),
fetches the per-project user's password from the Hopsworks Secrets storage,
and configures TLS, so the CLI just wraps the resulting DBAPI connection.

Defaults: ``--catalog iceberg`` (the project's offline feature-group store)
and ``--schema <projectName>``. Override per-command, or pass ``--catalog
hive`` / ``--catalog system`` for the built-in catalogs.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

import click
from hopsworks.cli import output, session


if TYPE_CHECKING:
    from collections.abc import Iterator


# Soft cap on rows pulled back into the terminal. ``--limit 0`` removes it.
_DEFAULT_LIMIT = 10_000

# The DBAPI cursor's fetchmany() page size; only affects internal pacing,
# not what the user sees.
_FETCH_BATCH = 1_000


@click.group("trino")
def trino_group() -> None:
    """Trino commands (query, catalogs, schemas, tables)."""


def _api(ctx: click.Context) -> Any:
    return session.get_project(ctx).get_trino_api()


def _project_schema(ctx: click.Context) -> str:
    """The project name, lowercased — Trino schema convention in Hopsworks."""
    return session.get_project(ctx).name.lower()


def _connect(ctx: click.Context, catalog: str | None, schema: str | None) -> Any:
    """Open a DBAPI connection with CLI-friendly defaults applied."""
    api = _api(ctx)
    cat = catalog or "iceberg"
    sch = schema or _project_schema(ctx)
    try:
        return api.connect(catalog=cat, schema=sch)
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Trino connect failed: {exc}") from exc


def _column_names(cursor: Any) -> list[str]:
    """Pull column names from a DBAPI cursor.description."""
    desc = getattr(cursor, "description", None) or []
    return [str(c[0]) for c in desc]


def _iter_rows(cursor: Any, limit: int) -> Iterator[tuple[Any, ...]]:
    """Yield rows up to ``limit`` (0 = unlimited)."""
    pulled = 0
    while True:
        batch = cursor.fetchmany(_FETCH_BATCH)
        if not batch:
            return
        for row in batch:
            if limit and pulled >= limit:
                return
            yield row
            pulled += 1


def _render_rows(headers: list[str], rows: list[tuple[Any, ...]]) -> None:
    """Print rows as a table, or a list-of-dicts when in JSON mode."""
    if output.JSON_MODE:
        output.print_json(
            [dict(zip(headers, [_jsonable(v) for v in r], strict=False)) for r in rows]
        )
        return
    output.print_table(
        [h.upper() for h in headers],
        [[_stringify(v) for v in r] for r in rows],
    )


def _jsonable(value: Any) -> Any:
    """Coerce DBAPI-returned scalars to JSON-friendly types.

    The trino driver returns Python scalars for most types, but Decimal
    serialises through the existing ``default=str`` of ``output.print_json``.
    Bytes don't, so we hex-encode them.
    """
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _execute(
    ctx: click.Context,
    sql: str,
    catalog: str | None,
    schema: str | None,
    limit: int,
) -> None:
    """Run ``sql`` and render the result set."""
    sql = sql.strip().rstrip(";")
    if not sql:
        raise click.ClickException("Empty SQL.")
    conn = _connect(ctx, catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description is None:
            # Statement that returned no rowset (e.g. CREATE/INSERT). Most users
            # don't run these from the CLI, but report the rowcount if the
            # driver provides one.
            affected = getattr(cur, "rowcount", -1)
            if not output.JSON_MODE:
                output.success(f"OK (affected rows: {affected})")
            else:
                output.print_json({"affected_rows": affected})
            return
        headers = _column_names(cur)
        rows = list(_iter_rows(cur, limit))
        _render_rows(headers, rows)
        if limit and len(rows) >= limit and not output.JSON_MODE:
            output.warn(
                f"Output truncated to {limit} rows. Pass --limit 0 for unlimited."
            )
    finally:
        # close-time errors are non-fatal
        with contextlib.suppress(Exception):
            conn.close()


# region: query -----------------------------------------------------------


@trino_group.command("query")
@click.argument("sql", required=False)
@click.option(
    "-f",
    "--file",
    "sql_file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Read SQL from a file.",
)
@click.option(
    "-i",
    "--stdin",
    "from_stdin",
    is_flag=True,
    help="Read SQL from standard input.",
)
@click.option(
    "--catalog",
    "catalog",
    default=None,
    help="Trino catalog (default: iceberg).",
)
@click.option(
    "--schema",
    "schema",
    default=None,
    help="Trino schema (default: project name lowercased).",
)
@click.option(
    "--limit",
    "limit",
    type=int,
    default=_DEFAULT_LIMIT,
    show_default=True,
    help="Soft cap on rows printed; 0 means unlimited.",
)
@click.pass_context
def trino_query(
    ctx: click.Context,
    sql: str | None,
    sql_file: str | None,
    from_stdin: bool,
    catalog: str | None,
    schema: str | None,
    limit: int,
) -> None:
    """Run an ad-hoc SQL query.

    Provide the SQL as a positional argument, or use ``-f`` to read from a
    file, or ``-i`` to read from stdin (useful in shell pipelines).

    Args:
        ctx: Click context.
        sql: SQL string passed positionally.
        sql_file: Path to a file containing SQL.
        from_stdin: Read SQL from standard input.
        catalog: Trino catalog override.
        schema: Trino schema override.
        limit: Soft cap on rows printed.
    """
    sources = [bool(sql), bool(sql_file), bool(from_stdin)]
    if sum(sources) != 1:
        raise click.UsageError(
            "Provide exactly one of: positional SQL, --file, or --stdin."
        )
    if sql_file:
        with open(sql_file, encoding="utf-8") as fh:
            sql = fh.read()
    elif from_stdin:
        sql = click.get_text_stream("stdin").read()
    assert sql is not None  # narrowed by the count-check above
    _execute(ctx, sql, catalog, schema, limit)


# region: discovery shortcuts ---------------------------------------------


@trino_group.command("catalogs")
@click.pass_context
def trino_catalogs(ctx: click.Context) -> None:
    """List Trino catalogs visible to the current user.

    Args:
        ctx: Click context.
    """
    _execute(ctx, "SHOW CATALOGS", catalog=None, schema=None, limit=0)


@trino_group.command("schemas")
@click.argument("catalog")
@click.pass_context
def trino_schemas(ctx: click.Context, catalog: str) -> None:
    """List schemas in CATALOG.

    Args:
        ctx: Click context.
        catalog: Trino catalog name.
    """
    _execute(ctx, f"SHOW SCHEMAS FROM {catalog}", catalog=None, schema=None, limit=0)


@trino_group.command("tables")
@click.argument("target")
@click.pass_context
def trino_tables(ctx: click.Context, target: str) -> None:
    """List tables in TARGET (``<catalog>.<schema>`` or just ``<schema>``).

    When TARGET has no dot the active project's default catalog is assumed.

    Args:
        ctx: Click context.
        target: ``<catalog>.<schema>`` or just ``<schema>``.
    """
    if "." in target:
        cat, sch = target.split(".", 1)
        sql = f"SHOW TABLES FROM {cat}.{sch}"
        _execute(ctx, sql, catalog=None, schema=None, limit=0)
        return
    sql = f"SHOW TABLES FROM {target}"
    _execute(ctx, sql, catalog=None, schema=None, limit=0)


@trino_group.command("describe")
@click.argument("table_ref")
@click.pass_context
def trino_describe(ctx: click.Context, table_ref: str) -> None:
    """Describe TABLE_REF (``<catalog>.<schema>.<table>`` or ``<schema>.<table>``).

    Output is one row per column with name, type, extra-info, and comment.

    Args:
        ctx: Click context.
        table_ref: ``<catalog>.<schema>.<table>`` or ``<schema>.<table>``.
    """
    parts = table_ref.split(".")
    if len(parts) not in (2, 3):
        raise click.UsageError(
            "TABLE_REF must be <catalog>.<schema>.<table> or <schema>.<table>."
        )
    _execute(ctx, f"DESCRIBE {table_ref}", catalog=None, schema=None, limit=0)


# region: connection info -------------------------------------------------


@trino_group.command("info")
@click.pass_context
def trino_info(ctx: click.Context) -> None:
    """Show the host/port/user the CLI would connect with.

    Args:
        ctx: Click context.
    """
    api = _api(ctx)
    try:
        host = api.get_host()
        port = api.get_port()
        user, _ = api.get_basic_auth()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Trino info failed: {exc}") from exc
    payload = {"host": host, "port": port, "user": user}
    if output.JSON_MODE:
        output.print_json(payload)
        return
    output.print_table(
        ["FIELD", "VALUE"],
        [[k.upper(), str(v)] for k, v in payload.items()],
    )
