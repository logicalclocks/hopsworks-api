"""Secrets management commands"""

import click
import hopsworks
from pathlib import Path
from hopsworks_cli.auth import get_connection
from hopsworks_cli.output import OutputFormatter, print_success, print_error, print_warning
from hopsworks_cli.utils.exceptions import ResourceNotFoundError


@click.group()
def secrets():
    """Manage secrets in the project"""
    pass


@secrets.command("list")
@click.pass_context
def list_secrets(ctx):
    """List all secrets (does not show values)"""
    try:
        get_connection(ctx)
        secrets_api = hopsworks.get_secrets_api()
        secrets_list = secrets_api.get_secrets()

        if not secrets_list:
            print("No secrets found")
            return

        # Format secret data (without values for security)
        secrets_data = []
        for secret in secrets_list:
            secret_info = {
                "name": secret.name,
                "owner": getattr(secret, "owner", "N/A"),
                "scope": getattr(secret, "scope", "N/A"),
                "visibility": getattr(secret, "visibility", "N/A"),
            }
            secrets_data.append(secret_info)

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(secrets_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to list secrets: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@secrets.command("get")
@click.argument("name")
@click.option("--owner", help="Secret owner (defaults to current user)")
@click.option("--show-value", is_flag=True, help="Show the secret value (use with caution)")
@click.pass_context
def get_secret(ctx, name, owner, show_value):
    """Get secret metadata or value"""
    try:
        get_connection(ctx)
        secrets_api = hopsworks.get_secrets_api()

        if show_value:
            # Get the secret value
            print_warning("⚠️  Displaying secret value - use with caution!")
            secret_value = secrets_api.get(name, owner=owner)

            if not secret_value:
                raise ResourceNotFoundError(f"Secret '{name}' not found")

            # Output the secret value
            if ctx.obj["output_format"] == "json":
                import json
                click.echo(json.dumps({"name": name, "value": secret_value}))
            else:
                click.echo(f"Secret value for '{name}':")
                click.echo(secret_value)
        else:
            # Get secret metadata only
            secret = secrets_api.get_secret(name, owner=owner)

            if not secret:
                raise ResourceNotFoundError(f"Secret '{name}' not found")

            # Build secret information (no value)
            secret_data = {
                "name": secret.name,
                "owner": getattr(secret, "owner", "N/A"),
                "scope": getattr(secret, "scope", "N/A"),
                "visibility": getattr(secret, "visibility", "N/A"),
            }

            # Output formatted results
            formatter = OutputFormatter()
            output = formatter.format(secret_data, ctx.obj["output_format"])
            click.echo(output)

    except Exception as e:
        print_error(f"Failed to get secret: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@secrets.command("create")
@click.argument("name")
@click.option("--value", help="Secret value (not recommended, use --value-file instead)")
@click.option("--value-file", type=click.Path(exists=True), help="Path to file containing secret value")
@click.option("--project", help="Project scope (optional)")
@click.pass_context
def create_secret(ctx, name, value, value_file, project):
    """Create a new secret"""
    try:
        # Validate inputs
        if not value and not value_file:
            print_error("Either --value or --value-file must be provided")
            ctx.exit(1)

        if value and value_file:
            print_error("Cannot provide both --value and --value-file")
            ctx.exit(1)

        # Get secret value
        if value_file:
            secret_value = Path(value_file).read_text().strip()
        else:
            secret_value = value
            if not value:
                # Prompt for value if not provided
                secret_value = click.prompt("Enter secret value", hide_input=True)

        get_connection(ctx)
        secrets_api = hopsworks.get_secrets_api()

        click.echo(f"Creating secret '{name}'...")

        # Create the secret
        secrets_api.create_secret(name, secret_value, project=project)

        print_success(f"Secret '{name}' created successfully")

        # Show confirmation (no value)
        secret_data = {
            "name": name,
            "scope": project if project else "user",
            "status": "created",
        }

        formatter = OutputFormatter()
        output = formatter.format(secret_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to create secret: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@secrets.command("delete")
@click.argument("name")
@click.option("--owner", help="Secret owner (defaults to current user)")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def delete_secret(ctx, name, owner, yes):
    """Delete a secret"""
    try:
        get_connection(ctx)
        secrets_api = hopsworks.get_secrets_api()

        # Verify secret exists
        secret = secrets_api.get_secret(name, owner=owner)
        if not secret:
            raise ResourceNotFoundError(f"Secret '{name}' not found")

        # Confirm deletion
        if not yes:
            if not click.confirm(f"Are you sure you want to delete secret '{name}'?"):
                click.echo("Deletion cancelled")
                return

        # Delete the secret
        secrets_api.delete(name, owner=owner)

        print_success(f"Secret '{name}' deleted successfully")

    except Exception as e:
        print_error(f"Failed to delete secret: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@secrets.command("update")
@click.argument("name")
@click.option("--value", help="New secret value (not recommended, use --value-file instead)")
@click.option("--value-file", type=click.Path(exists=True), help="Path to file containing new secret value")
@click.pass_context
def update_secret(ctx, name, value, value_file):
    """Update an existing secret"""
    try:
        # Validate inputs
        if not value and not value_file:
            print_error("Either --value or --value-file must be provided")
            ctx.exit(1)

        if value and value_file:
            print_error("Cannot provide both --value and --value-file")
            ctx.exit(1)

        # Get secret value
        if value_file:
            secret_value = Path(value_file).read_text().strip()
        else:
            secret_value = value
            if not value:
                # Prompt for value if not provided
                secret_value = click.prompt("Enter new secret value", hide_input=True)

        get_connection(ctx)
        secrets_api = hopsworks.get_secrets_api()

        # Verify secret exists
        secret = secrets_api.get_secret(name)
        if not secret:
            raise ResourceNotFoundError(f"Secret '{name}' not found")

        click.echo(f"Updating secret '{name}'...")

        # Delete and recreate (Hopsworks API pattern)
        secrets_api.delete(name)
        secrets_api.create_secret(name, secret_value)

        print_success(f"Secret '{name}' updated successfully")

    except Exception as e:
        print_error(f"Failed to update secret: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)
