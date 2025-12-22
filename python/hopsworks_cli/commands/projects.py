"""Project management commands"""

import click
import hopsworks
from hopsworks_common import client
from hopsworks_cli.auth import get_connection
from hopsworks_cli.output import OutputFormatter, print_success, print_error
from hopsworks_cli.config import Config
from hopsworks_cli.utils.exceptions import ResourceNotFoundError


@click.group()
def projects():
    """Manage Hopsworks projects"""
    pass


@projects.command("list")
@click.pass_context
def list_projects(ctx):
    """List all accessible projects"""
    try:
        # First authenticate (without a project)
        get_connection(ctx, require_project=False)

        # Then get the connection object to access get_projects()
        connection = client.get_connection()
        projects_list = connection.get_projects()

        if not projects_list:
            print("No projects found")
            return

        # Format project data
        projects_data = []
        for project in projects_list:
            project_info = {
                "name": project.name,
                "id": project.id,
                "owner": getattr(project, "owner", "N/A"),
                "created": str(project.created) if hasattr(project, "created") else "N/A",
                "description": getattr(project, "description", ""),
            }
            projects_data.append(project_info)

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(projects_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to list projects: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@projects.command("get")
@click.argument("name")
@click.pass_context
def get_project(ctx, name):
    """Get details of a specific project"""
    try:
        # First authenticate (without a project)
        get_connection(ctx, require_project=False)

        # Then get the connection object to access get_project()
        connection = client.get_connection()
        project = connection.get_project(name)

        if not project:
            raise ResourceNotFoundError(f"Project '{name}' not found")

        # Build detailed project information
        project_data = {
            "name": project.name,
            "id": project.id,
            "owner": getattr(project, "owner", "N/A"),
            "created": str(project.created) if hasattr(project, "created") else "N/A",
            "description": getattr(project, "description", ""),
        }

        # Add additional metadata if available
        if hasattr(project, "retention_period"):
            project_data["retention_period"] = project.retention_period

        if hasattr(project, "payment_tier"):
            project_data["payment_tier"] = project.payment_tier

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(project_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get project: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@projects.command("create")
@click.argument("name")
@click.option("--description", help="Project description")
@click.pass_context
def create_project(ctx, name, description):
    """Create a new project"""
    try:
        # First, connect without a project
        get_connection(ctx, require_project=False)

        click.echo(f"Creating project '{name}'...")

        # Create the project using the hopsworks module API
        project = hopsworks.create_project(name, description=description)

        print_success(f"Project '{name}' created successfully")

        # Show project details
        project_data = {
            "name": project.name,
            "id": project.id,
            "owner": getattr(project, "owner", "N/A"),
            "created": str(project.created) if hasattr(project, "created") else "N/A",
            "description": getattr(project, "description", ""),
        }

        formatter = OutputFormatter()
        output = formatter.format(project_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to create project: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@projects.command("use")
@click.argument("name")
@click.option("--profile", help="Configuration profile to update (uses default if not specified)")
@click.pass_context
def use_project(ctx, name, profile):
    """Set a project as the default for a profile"""
    try:
        # First authenticate (without a project)
        get_connection(ctx, require_project=False)

        # Verify the project exists
        connection = client.get_connection()
        project = connection.get_project(name)

        if not project:
            raise ResourceNotFoundError(f"Project '{name}' not found")

        # Load config and update the profile
        config = Config()

        # Determine which profile to update
        profile_name = profile or ctx.obj.get("profile") or "default"

        # Get or create the profile
        try:
            profile_obj = config.get_profile(profile_name)
        except:
            # Profile doesn't exist, create a new one
            from hopsworks_cli.config import Profile
            profile_obj = Profile(
                host=ctx.obj.get("host"),
                port=ctx.obj.get("port"),
                project=name,
                api_key_file=ctx.obj.get("api_key_file"),
            )

        # Update the project
        profile_obj.project = name

        # Save the updated profile
        config.profiles[profile_name] = profile_obj
        config.save()

        print_success(f"Set '{name}' as the default project for profile '{profile_name}'")

        # Show updated profile
        profile_data = {
            "profile": profile_name,
            "project": name,
            "host": profile_obj.host,
            "port": profile_obj.port,
        }

        formatter = OutputFormatter()
        output = formatter.format(profile_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to set default project: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)
