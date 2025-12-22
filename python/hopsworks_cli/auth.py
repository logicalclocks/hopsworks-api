"""Authentication helpers for CLI"""

from typing import Optional
import hopsworks
from hopsworks_cli.utils.exceptions import AuthenticationError


def get_connection(ctx, project: Optional[str] = None, require_project: bool = True):
    """
    Get Hopsworks connection using context parameters

    # Arguments
        ctx: Click context object
        project: Project name (overrides context project)
        require_project: If True, validates that a project is specified

    # Returns
        Hopsworks connection/project object

    # Raises
        AuthenticationError: If authentication fails
    """
    try:
        # Get parameters from context
        host = ctx.obj.get("host")
        port = ctx.obj.get("port")
        project_name = project or ctx.obj.get("project")
        api_key = ctx.obj.get("api_key")
        api_key_file = ctx.obj.get("api_key_file")
        hostname_verification = False
        trust_store_path = ctx.obj.get("trust_store_path")
        engine = ctx.obj.get("engine", "python")

        # For some commands (like listing projects), a project is not required
        # In those cases, we pass None to hopsworks.login()
        if not require_project:
            project_name = None

        # Login to Hopsworks
        connection = hopsworks.login(
            host=host,
            port=port,
            project=project_name,
            api_key_value=api_key,
            api_key_file=api_key_file,
            hostname_verification=hostname_verification,
            trust_store_path=trust_store_path,
            engine=engine,
        )

        return connection

    except Exception as e:
        raise AuthenticationError(f"Failed to connect to Hopsworks: {e}")


def get_model_registry(ctx):
    """
    Get model registry from connection

    # Arguments
        ctx: Click context object

    # Returns
        Model registry object
    """
    connection = get_connection(ctx)
    return connection.get_model_registry()


def get_model_serving(ctx):
    """
    Get model serving from connection

    # Arguments
        ctx: Click context object

    # Returns
        Model serving object
    """
    connection = get_connection(ctx)
    return connection.get_model_serving()


def get_feature_store(ctx, name: Optional[str] = None):
    """
    Get feature store from connection

    # Arguments
        ctx: Click context object
        name: Feature store name (optional)

    # Returns
        Feature store object
    """
    connection = get_connection(ctx)
    return connection.get_feature_store(name=name)
