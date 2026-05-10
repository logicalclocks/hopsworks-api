"""``hops datasource`` — storage connector read + write commands.

The SDK does not expose a ``create_storage_connector``; we POST straight to
``/featurestores/{id}/storageconnectors`` via the authenticated REST client.
``databases``/``tables``/``preview`` delegate to the SDK's ``DataSource``
methods where available.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


@click.group("datasource")
def datasource_group() -> None:
    """Data source (storage connector) commands."""


@datasource_group.command("list")
@click.pass_context
def connector_list(ctx: click.Context) -> None:
    """List all storage connectors in the active project.

    Args:
        ctx: Click context.
    """
    fs = session.get_feature_store(ctx)
    items = _list_connectors(fs)
    rows = []
    for c in items:
        rows.append(
            [
                c.get("id", "?"),
                c.get("name", "?"),
                c.get("storageConnectorType", c.get("connectorType", "?")),
                output.first_line(c.get("description"), empty=""),
            ]
        )
    output.print_table(["ID", "NAME", "TYPE", "DESCRIPTION"], rows)


@datasource_group.command("info")
@click.argument("name")
@click.pass_context
def connector_info(ctx: click.Context, name: str) -> None:
    """Show details for a single storage connector.

    Args:
        ctx: Click context.
        name: Connector name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Connector '{name}' not found: {exc}") from exc

    sc = getattr(ds, "storage_connector", ds)
    if output.JSON_MODE:
        to_dict = getattr(sc, "to_dict", None)
        payload = to_dict() if callable(to_dict) else _connector_to_dict(sc)
        output.print_json(payload)
        return

    rows = [
        ["ID", getattr(sc, "id", "?")],
        ["Name", getattr(sc, "name", "?")],
        ["Type", type(sc).__name__],
        ["Description", output.first_line(getattr(sc, "description", ""))],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


def _list_connectors(fs: Any) -> list[dict[str, Any]]:
    from hopsworks_common.core import rest

    fs_id = getattr(fs, "id", None)
    if fs_id is None:
        return []
    try:
        payload = rest.send_request(
            "GET", rest.project_path("featurestores", fs_id, "storageconnectors")
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list connectors: {exc}") from exc
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("items") or []
    return []


def _connector_to_dict(sc: Any) -> dict[str, Any]:
    return {
        "id": getattr(sc, "id", None),
        "name": getattr(sc, "name", None),
        "type": type(sc).__name__,
        "description": getattr(sc, "description", None),
    }


# region Write commands


@datasource_group.group("create")
def connector_create() -> None:
    """Create a storage connector (subcommand per backend)."""


@connector_create.command("jdbc")
@click.argument("name")
@click.option("--url", required=True, help="JDBC connection URL.")
@click.option("--user", help='Connection user, stored as "user".')
@click.option("--password", help='Connection password, stored as "password".')
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_jdbc(
    ctx: click.Context,
    name: str,
    url: str,
    user: str | None,
    password: str | None,
    description: str,
) -> None:
    """Register a JDBC connector.

    Args:
        ctx: Click context.
        name: Connector name.
        url: JDBC connection URL.
        user: Optional user.
        password: Optional password.
        description: Description.
    """
    args = []
    if user:
        args.append({"name": "user", "value": user})
    if password:
        args.append({"name": "password", "value": password})
    body = {
        "name": name,
        "storageConnectorType": "JDBC",
        "connectionString": url,
        "description": description,
        "arguments": args,
    }
    _create_connector(ctx, body)


@connector_create.command("s3")
@click.argument("name")
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--access-key", help="AWS access key ID.")
@click.option("--secret-key", help="AWS secret access key.")
@click.option("--region", help="AWS region.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_s3(
    ctx: click.Context,
    name: str,
    bucket: str,
    access_key: str | None,
    secret_key: str | None,
    region: str | None,
    description: str,
) -> None:
    """Register an S3 connector.

    Args:
        ctx: Click context.
        name: Connector name.
        bucket: S3 bucket.
        access_key: AWS access key.
        secret_key: AWS secret key.
        region: AWS region.
        description: Description.
    """
    body = {
        "name": name,
        "storageConnectorType": "S3",
        "bucket": bucket,
        "description": description,
    }
    if access_key:
        body["accessKey"] = access_key
    if secret_key:
        body["secretKey"] = secret_key
    if region:
        body["region"] = region
    _create_connector(ctx, body)


@connector_create.command("snowflake")
@click.argument("name")
@click.option("--url", required=True, help="Snowflake account URL.")
@click.option("--user", required=True, help="User name.")
@click.option("--password", required=True, help="Password.")
@click.option("--database", required=True, help="Database name.")
@click.option("--schema", "db_schema", required=True, help="Schema name.")
@click.option("--warehouse", required=True, help="Warehouse name.")
@click.option("--role", help="Role to assume.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_snowflake(
    ctx: click.Context,
    name: str,
    url: str,
    user: str,
    password: str,
    database: str,
    db_schema: str,
    warehouse: str,
    role: str | None,
    description: str,
) -> None:
    """Register a Snowflake connector.

    Args:
        ctx: Click context.
        name: Connector name.
        url: Snowflake account URL.
        user: User.
        password: Password.
        database: Database.
        db_schema: Schema.
        warehouse: Warehouse.
        role: Optional role.
        description: Description.
    """
    body = {
        "name": name,
        "storageConnectorType": "SNOWFLAKE",
        "url": url,
        "user": user,
        "password": password,
        "database": database,
        "schema": db_schema,
        "warehouse": warehouse,
        "description": description,
    }
    if role:
        body["role"] = role
    _create_connector(ctx, body)


@connector_create.command("bigquery")
@click.argument("name")
@click.option("--project-id", "project_id", required=True, help="GCP project ID.")
@click.option("--dataset", help="BigQuery dataset.")
@click.option("--key-path", "key_path", help="Path to the service-account JSON key.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_bigquery(
    ctx: click.Context,
    name: str,
    project_id: str,
    dataset: str | None,
    key_path: str | None,
    description: str,
) -> None:
    """Register a BigQuery connector.

    Args:
        ctx: Click context.
        name: Connector name.
        project_id: GCP project ID.
        dataset: Optional BigQuery dataset.
        key_path: Optional service-account key path.
        description: Description.
    """
    body = {
        "name": name,
        "storageConnectorType": "BIGQUERY",
        "queryProject": project_id,
        "description": description,
    }
    if dataset:
        body["dataset"] = dataset
    if key_path:
        body["keyPath"] = key_path
    _create_connector(ctx, body)


@datasource_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def connector_delete(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete a storage connector.

    Args:
        ctx: Click context.
        name: Connector name.
        yes: Skip confirmation when True.
    """
    fs = session.get_feature_store(ctx)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete connector '{name}'?", abort=True)

    from hopsworks_common.core import rest

    try:
        rest.send_request(
            "DELETE",
            rest.project_path(
                "featurestores", getattr(fs, "id", None), "storageconnectors", name
            ),
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted connector %s", name)


@datasource_group.command("databases")
@click.argument("name")
@click.pass_context
def connector_databases(ctx: click.Context, name: str) -> None:
    """List databases visible through a connector.

    Args:
        ctx: Click context.
        name: Connector name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        databases = ds.get_databases()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list databases: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(databases)
        return
    output.print_table(["DATABASE"], [[db] for db in databases or []])


@datasource_group.command("tables")
@click.argument("name")
@click.option("--database", help="Database to list tables from.")
@click.pass_context
def connector_tables(ctx: click.Context, name: str, database: str | None) -> None:
    """List tables in a database reachable through a connector.

    Args:
        ctx: Click context.
        name: Connector name.
        database: Database name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        tables = ds.get_tables(database=database)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list tables: {exc}") from exc

    rows = [
        [
            getattr(t, "table", None) or "?",
            getattr(t, "database", None) or database or "-",
        ]
        for t in tables or []
    ]
    output.print_table(["TABLE", "DATABASE"], rows)


@datasource_group.command("preview")
@click.argument("name")
@click.pass_context
def connector_preview(ctx: click.Context, name: str) -> None:
    """Fetch a small data preview through a connector.

    Args:
        ctx: Click context.
        name: Connector name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        data = ds.get_data()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Preview failed: {exc}") from exc
    if output.JSON_MODE:
        to_dict = getattr(data, "to_dict", None)
        output.print_json(to_dict() if callable(to_dict) else str(data))
        return
    click.echo(str(data))


@datasource_group.command("infer-metadata")
@click.argument("name")
@click.argument("table")
@click.option("--database", help="Database the table lives in (connector-dependent).")
@click.pass_context
def connector_infer_metadata(
    ctx: click.Context, name: str, table: str, database: str | None
) -> None:
    """Use platform intelligence to infer feature metadata for a table.

    Calls the same LLM-backed endpoint as the "Infer metadata" button in the
    UI: suggests a renamed feature name, Hopsworks type, and description per
    column, plus a primary key and event time. Use this before mounting an
    external table as an external feature group, or before creating a new
    feature group from it.

    Args:
        ctx: Click context.
        name: Connector name.
        table: Table name to infer metadata for.
        database: Database that contains the table.
    """
    from hopsworks_common.client.exceptions import PlatformIntelligenceException

    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        tables = ds.get_tables(database=database) or []
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list tables: {exc}") from exc

    match = next((t for t in tables if getattr(t, "table", None) == table), None)
    if match is None:
        raise click.ClickException(
            f"Table '{table}' not found in connector '{name}'"
            + (f" / database '{database}'" if database else "")
        )

    try:
        inferred = match.infer_metadata()
    except PlatformIntelligenceException as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Infer metadata failed: {exc}") from exc

    if output.JSON_MODE:
        output.print_json(inferred.to_dict())
        return

    rows = [
        [f.original_name, f.new_name, f.type, output.first_line(f.description)]
        for f in inferred.features
    ]
    output.print_table(["ORIGINAL", "NEW", "TYPE", "DESCRIPTION"], rows)
    if inferred.suggested_primary_key:
        click.echo(
            f"Suggested primary key: {', '.join(inferred.suggested_primary_key)}"
        )
    if inferred.suggested_event_time:
        click.echo(f"Suggested event time: {inferred.suggested_event_time}")


def _create_connector(ctx: click.Context, body: dict[str, Any]) -> None:
    fs = session.get_feature_store(ctx)
    from hopsworks_common.core import rest

    try:
        rest.send_request(
            "POST",
            rest.project_path(
                "featurestores", getattr(fs, "id", None), "storageconnectors"
            ),
            json_body=body,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Create failed: {exc}") from exc
    output.success("✓ Created connector %s", body["name"])
