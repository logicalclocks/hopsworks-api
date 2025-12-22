"""Feature Group management commands"""

import click
from hopsworks_cli.auth import get_connection, get_feature_store
from hopsworks_cli.output import OutputFormatter, print_success, print_error
from hopsworks_cli.utils.exceptions import ResourceNotFoundError


@click.group("feature-groups")
def feature_groups():
    """Manage feature groups in the Feature Store"""
    pass


@feature_groups.command("list")
@click.option("--name", help="Filter by feature group name")
@click.pass_context
def list_feature_groups(ctx, name):
    """List all feature groups in the project"""
    try:
        fs = get_feature_store(ctx)

        # Try to get feature groups
        try:
            result = fs.get_feature_groups(name=name)
        except TypeError as e:
            # Known issue: HSFS library sometimes can't parse feature group responses
            # This typically happens due to version mismatches or response format changes
            error_msg = str(e)
            if "missing 3 required positional arguments" in error_msg:
                if ctx.obj.get("verbose"):
                    print_error(f"HSFS parsing error: {e}")

                print_error("\nUnable to list feature groups due to an HSFS library compatibility issue.")
                print_error("This is a known issue where the feature group response format is incompatible")
                print_error("with the installed HSFS version.\n")
                print_error("Workarounds:")
                print_error("  1. Access feature groups through the Hopsworks UI")
                print_error("  2. Use the Python SDK directly in a script:")
                print_error("     ```python")
                print_error("     import hopsworks")
                print_error("     project = hopsworks.login()")
                print_error("     fs = project.get_feature_store()")
                print_error("     # Access feature groups programmatically")
                print_error("     ```")
                return
            else:
                # Re-raise if it's a different TypeError
                raise

        # Handle different return types
        if not result:
            print("No feature groups found")
            return

        # Extract feature groups list
        if isinstance(result, dict) and "items" in result:
            feature_groups_list = result["items"]
        elif hasattr(result, '__iter__') and not isinstance(result, str):
            feature_groups_list = list(result)
        else:
            # Single feature group
            feature_groups_list = [result]

        if not feature_groups_list:
            print("No feature groups found")
            return

        # Format feature group data
        fg_data = []
        for fg in feature_groups_list:
            fg_info = {
                "name": fg.name,
                "version": fg.version,
                "id": fg.id,
                "online_enabled": getattr(fg, "online_enabled", False),
                "deprecated": getattr(fg, "deprecated", False),
            }

            # Add event time if available
            if hasattr(fg, "event_time") and fg.event_time:
                fg_info["event_time"] = fg.event_time

            fg_data.append(fg_info)

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(fg_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to list feature groups: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@feature_groups.command("get")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version (defaults to latest)")
@click.pass_context
def get_feature_group(ctx, name, version):
    """Get details of a specific feature group"""
    try:
        fs = get_feature_store(ctx)

        # Get the feature group
        try:
            if version:
                fg = fs.get_feature_group(name, version=version)
            else:
                fg = fs.get_feature_group(name)
        except TypeError:
            # Handle case where feature group doesn't exist
            raise ResourceNotFoundError(f"Feature group '{name}' not found")

        if not fg:
            raise ResourceNotFoundError(f"Feature group '{name}' not found")

        # Build detailed feature group information
        fg_data = {
            "name": fg.name,
            "version": fg.version,
            "id": fg.id,
            "featurestore_id": fg.featurestore_id,
            "online_enabled": getattr(fg, "online_enabled", False),
            "deprecated": getattr(fg, "deprecated", False),
            "location": getattr(fg, "location", "N/A"),
        }

        # Add event time if available
        if hasattr(fg, "event_time") and fg.event_time:
            fg_data["event_time"] = fg.event_time

        # Add primary key info
        if hasattr(fg, "primary_key") and fg.primary_key:
            fg_data["primary_key"] = fg.primary_key

        # Add description if available
        if hasattr(fg, "description") and fg.description:
            fg_data["description"] = fg.description

        # Add statistics config
        if hasattr(fg, "statistics_config"):
            fg_data["statistics_enabled"] = getattr(fg.statistics_config, "enabled", False)

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(fg_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get feature group: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@feature_groups.command("schema")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version (defaults to latest)")
@click.pass_context
def get_feature_group_schema(ctx, name, version):
    """Show the schema of a feature group"""
    try:
        fs = get_feature_store(ctx)

        # Get the feature group
        try:
            if version:
                fg = fs.get_feature_group(name, version=version)
            else:
                fg = fs.get_feature_group(name)
        except TypeError:
            # Handle case where feature group doesn't exist
            raise ResourceNotFoundError(f"Feature group '{name}' not found")

        if not fg:
            raise ResourceNotFoundError(f"Feature group '{name}' not found")

        # Get features (schema)
        features = fg.features

        if not features:
            print(f"Feature group '{name}' has no features defined")
            return

        # Format schema data
        schema_data = []
        for feature in features:
            feature_info = {
                "name": feature.name,
                "type": feature.type,
            }

            # Add description if available
            if hasattr(feature, "description") and feature.description:
                feature_info["description"] = feature.description

            # Add if it's a primary key
            if hasattr(fg, "primary_key") and fg.primary_key and feature.name in fg.primary_key:
                feature_info["primary_key"] = True

            # Add if it's the event time
            if hasattr(fg, "event_time") and fg.event_time == feature.name:
                feature_info["event_time"] = True

            schema_data.append(feature_info)

        # Output formatted results
        click.echo(f"\nSchema for feature group: {name} (version {fg.version})")
        click.echo("=" * 60)

        formatter = OutputFormatter()
        output = formatter.format(schema_data, ctx.obj["output_format"])
        click.echo(output)

        # Show summary
        click.echo(f"\nTotal features: {len(features)}")
        if hasattr(fg, "primary_key") and fg.primary_key:
            click.echo(f"Primary key(s): {', '.join(fg.primary_key)}")
        if hasattr(fg, "event_time") and fg.event_time:
            click.echo(f"Event time: {fg.event_time}")

    except Exception as e:
        print_error(f"Failed to get feature group schema: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)
