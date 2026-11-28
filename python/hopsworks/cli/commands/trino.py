"""``hops trino`` — run SQL against the project's Trino coordinator.

Trino in Hopsworks is exposed via ``project.get_trino_api()``. That class
handles host resolution (internal service-discovery vs external loadbalancer),
fetches the per-project user's password from the Hopsworks Secrets storage,
and configures TLS, so the CLI just wraps the resulting DBAPI connection.

Defaults: ``--catalog delta`` (the format feature groups are created in) and
``--schema <project>_featurestore`` (the project's feature store), so
``hops sql "SELECT * FROM <fg>_<version>"`` reads a feature group. Override
per-command, e.g. ``--catalog iceberg`` for Iceberg feature groups, or
``--catalog hive`` / ``--catalog system`` for the built-in catalogs.
"""

from __future__ import annotations

import contextlib
import time
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


def _featurestore_schema(ctx: click.Context) -> str:
    """The project's feature store, the Trino schema its feature groups live in."""
    return session.get_feature_store(ctx).name


def _connect(ctx: click.Context, catalog: str | None, schema: str | None) -> Any:
    """Open a DBAPI connection with CLI-friendly defaults applied."""
    api = _api(ctx)
    cat = catalog or "delta"
    sch = schema or _featurestore_schema(ctx)
    # hops sql verifies the Trino coordinator's TLS certificate by default,
    # independent of the REST/login default (which is off). An explicit
    # --verify/--no-verify or HOPSWORKS_HOSTNAME_VERIFICATION still wins, so a
    # cluster whose Trino load balancer serves a self-signed or IP-SAN-mismatched
    # certificate stays reachable with --no-verify (the trino driver otherwise
    # raises an SSL CERTIFICATE_VERIFY_FAILED). Forwarding it here matters because
    # the flag reaches hopsworks.login() but never the Trino connection on its own.
    cfg = session.get_config(ctx)
    verify = cfg.hostname_verification if cfg.hostname_verification_explicit else True
    try:
        return api.connect(catalog=cat, schema=sch, verify=verify)
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
    help="Trino catalog (default: delta).",
)
@click.option(
    "--schema",
    "schema",
    default=None,
    help="Trino schema (default: the project's feature store).",
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


# ``hops trino sql ...`` is a natural guess (the skill calls it "Trino SQL",
# and the top-level alias is ``hops sql``). Without this it failed with a bare
# "No such command 'sql'". Register the same command under both names.
trino_group.add_command(trino_query, name="sql")


# region: discovery shortcuts ---------------------------------------------


@trino_group.command("catalogs")
@click.pass_context
def trino_catalogs(ctx: click.Context) -> None:
    """List Trino catalogs visible to the current user.

    Args:
        ctx: Click context.
    """
    _execute(ctx, "SHOW CATALOGS", catalog=None, schema=None, limit=0)


def _quoted(ref: str) -> str:
    """Quote each part of a dotted name, since catalog and store names may start with a digit.

    A name the caller already quoted is used as given.
    """
    if '"' in ref:
        return ref
    return ".".join(f'"{part}"' for part in ref.split("."))


@trino_group.command("schemas")
@click.argument("catalog")
@click.pass_context
def trino_schemas(ctx: click.Context, catalog: str) -> None:
    """List schemas in CATALOG.

    Args:
        ctx: Click context.
        catalog: Trino catalog name.
    """
    _execute(
        ctx, f"SHOW SCHEMAS FROM {_quoted(catalog)}", catalog=None, schema=None, limit=0
    )


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
    _execute(
        ctx, f"SHOW TABLES FROM {_quoted(target)}", catalog=None, schema=None, limit=0
    )


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
    _execute(ctx, f"DESCRIBE {_quoted(table_ref)}", catalog=None, schema=None, limit=0)


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


# region: catalogs ----------------------------------------------------------

# A catalog applied without a restart (dynamic Trino) is SYNCED within one backend
# poll (1 min) plus its CREATE CATALOG; the wait covers two polls.
_CATALOG_WAIT_S = 150
_CATALOG_POLL_S = 2


@trino_group.group("catalog")
def trino_catalog_group() -> None:
    """Trino catalogs: query a data source through Trino."""


def _catalog_api(ctx: click.Context) -> Any:
    return session.get_project(ctx).get_trino_catalog_api()


def _print_catalog(catalog: dict[str, Any]) -> None:
    if output.JSON_MODE:
        output.print_json(catalog)
        return
    fields = ("name", "connectorType", "scope", "status", "operation", "failureReason")
    output.print_table(
        ["FIELD", "VALUE"],
        [[f.upper(), str(catalog.get(f) or "-")] for f in fields],
    )


def _wait_synced(api: Any, name: str, timeout_s: int) -> dict[str, Any]:
    """Poll the catalog until Trino has it (SYNCED) or refused it (FAILED), within timeout_s."""
    deadline = time.monotonic() + timeout_s
    while True:
        catalog = api.get_catalog(name)
        if catalog.get("status") in ("SYNCED", "FAILED"):
            return catalog
        if time.monotonic() >= deadline:
            return catalog
        time.sleep(_CATALOG_POLL_S)


@trino_catalog_group.command("create")
@click.argument("data_source")
@click.option(
    "--name",
    default=None,
    help="Catalog name, with the prefix of the suggested one (default: the suggested name).",
)
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help=f"Wait up to {_CATALOG_WAIT_S} s until Trino answers on the catalog.",
)
@click.pass_context
def trino_catalog_create(
    ctx: click.Context, data_source: str, name: str | None, wait: bool
) -> None:
    """Create a Trino catalog from DATA_SOURCE.

    Query it as `"<catalog>".<schema>.<table>`.

    Credentials come from the data source; none are passed here. On a Trino with dynamic
    catalogs the catalog answers within about a minute. On a static one it answers after the
    next Trino restart, whose time this prints.

    Args:
        ctx: Click context.
        data_source: Data source name in the project's feature store.
        name: Catalog name; the suggested one when unset.
        wait: Wait until the catalog answers.
    """
    api = _catalog_api(ctx)
    fs_id = session.get_feature_store(ctx).id
    template = api.get_catalog_template(data_source, fs_id)
    if not template.get("supported"):
        raise click.ClickException(
            f"Data source {data_source} cannot be a Trino catalog: {template.get('reason') or 'unsupported type'}."
        )
    catalog = api.create_catalog(
        name or template["suggestedName"],
        template["connectorType"],
        template["properties"],
        data_source_name=data_source,
        featurestore_id=fs_id,
        scope=template.get("scope"),
    )
    # Named before any further call, so a failure below still says which catalog exists now.
    output.info("Created Trino catalog %s.", catalog["name"])
    capabilities = api.get_capabilities()
    if not capabilities.get("dynamicCatalogs"):
        _print_catalog(catalog)
        output.info(
            "Trino here loads catalogs at a restart: %s answers after the next one (%s).",
            catalog["name"],
            capabilities.get("nextScheduledRestart")
            or "none scheduled; ask an administrator",
        )
        return
    if wait:
        catalog = _wait_synced(api, catalog["name"], _CATALOG_WAIT_S)
    _print_catalog(catalog)
    if catalog.get("status") == "FAILED":
        raise click.ClickException(
            f"Trino refused catalog {catalog['name']}: {catalog.get('failureReason') or 'no reason given'}."
        )
    if catalog.get("status") == "SYNCED":
        output.success(
            "Catalog %s answers: hops trino schemas %s",
            catalog["name"],
            catalog["name"],
        )
    elif wait:
        raise click.ClickException(
            f"Catalog {catalog['name']} is still {catalog.get('status')} after {_CATALOG_WAIT_S} s."
        )


@trino_catalog_group.command("info")
@click.argument("name")
@click.pass_context
def trino_catalog_info(ctx: click.Context, name: str) -> None:
    """Show catalog NAME: its connector, scope and status.

    Args:
        ctx: Click context.
        name: Catalog name.
    """
    _print_catalog(_catalog_api(ctx).get_catalog(name))


@trino_catalog_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Delete without asking.")
@click.pass_context
def trino_catalog_delete(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete catalog NAME. Its data source stays.

    Args:
        ctx: Click context.
        name: Catalog name.
        yes: Skip the confirmation.
    """
    if not yes:
        output.confirm(f"Delete Trino catalog {name}?")
    _catalog_api(ctx).delete_catalog(name)
    output.success("Deleted Trino catalog %s", name)
