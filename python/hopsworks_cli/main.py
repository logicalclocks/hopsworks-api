"""Hopsworks CLI - Main entry point"""

import click
import sys
from hopsworks_cli.config import Config, DEFAULT_HOST
from hopsworks_cli.commands.projects import projects
from hopsworks_cli.commands.feature_groups import feature_groups
from hopsworks_cli.commands.models import models
from hopsworks_cli.commands.deployments import deployments
from hopsworks_cli.commands.jobs import jobs
from hopsworks_cli.commands.secrets import secrets
from hopsworks_cli.output import print_error
from hopsworks_common.client.exceptions import RestAPIError


def handle_exception(ctx, exception):
    """Global exception handler"""
    if isinstance(exception, RestAPIError):
        print_error(f"API Error: {exception}")
        if ctx.obj.get("verbose"):
            raise
        sys.exit(1)
    elif isinstance(exception, Exception):
        print_error(f"Error: {exception}")
        if ctx.obj.get("verbose"):
            raise
        sys.exit(1)


@click.group()
@click.version_option(version="0.1.0", prog_name="hopsworks-cli")
@click.option(
    "--host",
    envvar="HOPSWORKS_HOST",
    help="Hopsworks hostname",
)
@click.option(
    "--port",
    envvar="HOPSWORKS_PORT",
    type=int,
    help="Hopsworks port [default: 443]",
)
@click.option(
    "--project",
    envvar="HOPSWORKS_PROJECT",
    help="Project name",
)
@click.option(
    "--api-key",
    envvar="HOPSWORKS_API_KEY",
    help="API key value (not recommended, use --api-key-file instead)",
)
@click.option(
    "--api-key-file",
    envvar="HOPSWORKS_API_KEY_FILE",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to API key file",
)
@click.option(
    "--profile",
    help="Configuration profile name",
)
@click.option(
    "--output",
    "output_format",
    type=click.Choice(["json", "table", "yaml"], case_sensitive=False),
    default="table",
    help="Output format [default: table]",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose output with full stack traces",
)
@click.option(
    "--no-verify",
    is_flag=True,
    help="Disable SSL certificate verification",
)
@click.option(
    "--trust-store-path",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to custom trust store",
)
@click.option(
    "--engine",
    type=click.Choice(["python", "spark", "training", "spark-no-metastore", "spark-delta"]),
    default="python",
    help="Execution engine [default: python]",
)
@click.pass_context
def cli(
    ctx,
    host,
    port,
    project,
    api_key,
    api_key_file,
    profile,
    output_format,
    verbose,
    no_verify,
    trust_store_path,
    engine,
):
    """
    Hopsworks CLI - Manage Hopsworks resources from the command line

    \b
    Examples:
      # List all models in a project
      hopsworks-cli --project my_project models list

      # Get best performing model
      hopsworks-cli models best my_model --metric accuracy --direction max

      # Start a deployment
      hopsworks-cli deployments start my_deployment --wait

      # Download a model
      hopsworks-cli models download my_model --version 1 --output-dir ./models

    \b
    Configuration:
      The CLI can be configured using:
      1. Command-line arguments (highest priority)
      2. Environment variables (HOPSWORKS_*)
      3. Configuration file (~/.hopsworks/config.yaml)
      4. Default values (lowest priority)

    \b
    For more information, visit: https://docs.hopsworks.ai
    """
    # Initialize context object
    ctx.ensure_object(dict)

    # Load configuration
    config = Config()

    # Use profile if specified, otherwise use defaults
    if profile:
        try:
            profile_obj = config.get_profile(profile)
            # CLI args override profile settings
            host = host or profile_obj.host
            port = port or profile_obj.port
            project = project or profile_obj.project
            api_key_file = api_key_file or profile_obj.api_key_file
            trust_store_path = trust_store_path or profile_obj.trust_store_path
            engine = engine or profile_obj.engine
        except Exception as e:
            print_error(f"Failed to load profile '{profile}': {e}")
            sys.exit(1)

    # Set defaults if not provided
    host = host or DEFAULT_HOST
    port = port or 443

    # Validate required authentication
    if not api_key and not api_key_file:
        print_error(
            "Authentication required: provide --api-key-file or --api-key\n"
            "You can also set HOPSWORKS_API_KEY or HOPSWORKS_API_KEY_FILE environment variables"
        )
        sys.exit(1)

    # Store configuration in context
    ctx.obj["config"] = config
    ctx.obj["host"] = host
    ctx.obj["port"] = port
    ctx.obj["project"] = project
    ctx.obj["api_key"] = api_key
    ctx.obj["api_key_file"] = api_key_file
    ctx.obj["output_format"] = output_format
    ctx.obj["verbose"] = verbose
    ctx.obj["hostname_verification"] = not no_verify
    ctx.obj["trust_store_path"] = trust_store_path
    ctx.obj["engine"] = engine
    ctx.obj["error_handler"] = handle_exception


# Register command groups
cli.add_command(projects)
cli.add_command(feature_groups)
cli.add_command(models)
cli.add_command(deployments)
cli.add_command(jobs)
cli.add_command(secrets)


def main():
    """Entry point for the CLI"""
    try:
        cli(obj={})
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
