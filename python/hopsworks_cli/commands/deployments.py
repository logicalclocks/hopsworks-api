"""Model deployment management commands"""

import click
import time
from hopsworks_cli.auth import get_model_serving
from hopsworks_cli.output import OutputFormatter, print_success, print_error, print_warning
from hopsworks_cli.utils.exceptions import ResourceNotFoundError


@click.group()
def deployments():
    """Manage model deployments"""
    pass


@deployments.command("list")
@click.pass_context
def list_deployments(ctx):
    """List all model deployments"""
    try:
        ms = get_model_serving(ctx)
        deployments_list = ms.get_deployments()

        if not deployments_list:
            print("No deployments found")
            return

        # Format deployment data
        deployments_data = []
        for deployment in deployments_list:
            deployment_info = {
                "name": deployment.name,
                "model": (
                    f"{deployment.model_name}:{deployment.model_version}"
                    if hasattr(deployment, "model_name")
                    else "N/A"
                ),
                "status": getattr(deployment, "state", "N/A"),
                "created": (
                    str(deployment.created) if hasattr(deployment, "created") else "N/A"
                ),
            }

            # Add predictor info if available
            if hasattr(deployment, "predictor"):
                predictor = deployment.predictor
                deployment_info["instances"] = getattr(predictor, "requested_instances", "N/A")

            deployments_data.append(deployment_info)

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(deployments_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to list deployments: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@deployments.command("get")
@click.argument("name")
@click.pass_context
def get_deployment(ctx, name):
    """Get details of a specific deployment"""
    try:
        ms = get_model_serving(ctx)
        deployment = ms.get_deployment(name)

        if not deployment:
            raise ResourceNotFoundError(f"Deployment '{name}' not found")

        # Build detailed deployment information
        deployment_data = {
            "name": deployment.name,
            "model_name": getattr(deployment, "model_name", "N/A"),
            "model_version": getattr(deployment, "model_version", "N/A"),
            "status": getattr(deployment, "state", "N/A"),
            "created": str(deployment.created) if hasattr(deployment, "created") else "N/A",
        }

        # Add predictor details
        if hasattr(deployment, "predictor") and deployment.predictor:
            predictor = deployment.predictor
            deployment_data.update({
                "predictor_script": getattr(predictor, "script_file", "N/A"),
                "requested_instances": getattr(predictor, "requested_instances", "N/A"),
            })

            # Add resources if available
            if hasattr(predictor, "resources"):
                resources = predictor.resources
                deployment_data["resources"] = {
                    "cores": getattr(resources, "cores", "N/A"),
                    "memory": getattr(resources, "memory", "N/A"),
                    "gpus": getattr(resources, "gpus", 0),
                }

        # Add transformer details if exists
        if hasattr(deployment, "transformer") and deployment.transformer:
            deployment_data["has_transformer"] = True
            transformer = deployment.transformer
            deployment_data["transformer_script"] = getattr(
                transformer, "script_file", "N/A"
            )
        else:
            deployment_data["has_transformer"] = False

        # Add inference logger if exists
        if hasattr(deployment, "inference_logger") and deployment.inference_logger:
            deployment_data["inference_logging"] = "enabled"
        else:
            deployment_data["inference_logging"] = "disabled"

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(deployment_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get deployment: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@deployments.command("start")
@click.argument("name")
@click.option("--wait", is_flag=True, help="Wait for deployment to be running")
@click.option("--timeout", type=int, default=300, help="Timeout in seconds (default: 300)")
@click.pass_context
def start_deployment(ctx, name, wait, timeout):
    """Start a model deployment"""
    try:
        ms = get_model_serving(ctx)
        deployment = ms.get_deployment(name)

        if not deployment:
            raise ResourceNotFoundError(f"Deployment '{name}' not found")

        # Check current state
        current_state = getattr(deployment, "state", None)
        if current_state and "running" in current_state.lower():
            print_warning(f"Deployment '{name}' is already running")
            return

        click.echo(f"Starting deployment '{name}'...")

        # Start the deployment
        deployment.start()

        print_success(f"Deployment '{name}' start command sent")

        if wait:
            click.echo(f"Waiting for deployment to be running (timeout: {timeout}s)...")
            start_time = time.time()

            while time.time() - start_time < timeout:
                # Refresh deployment status
                deployment = ms.get_deployment(name)
                current_state = getattr(deployment, "state", "unknown")

                if "running" in current_state.lower():
                    print_success(
                        f"Deployment '{name}' is now running "
                        f"(took {int(time.time() - start_time)}s)"
                    )
                    break
                elif "failed" in current_state.lower():
                    print_error(f"Deployment '{name}' failed to start")
                    ctx.exit(1)

                time.sleep(5)
            else:
                print_warning(
                    f"Deployment '{name}' did not reach running state within {timeout}s"
                )

        # Show final status
        deployment_data = {
            "name": deployment.name,
            "status": getattr(deployment, "state", "N/A"),
            "model": f"{deployment.model_name}:{deployment.model_version}"
            if hasattr(deployment, "model_name")
            else "N/A",
        }

        formatter = OutputFormatter()
        output = formatter.format(deployment_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to start deployment: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@deployments.command("stop")
@click.argument("name")
@click.option("--wait", is_flag=True, help="Wait for deployment to be stopped")
@click.option("--timeout", type=int, default=300, help="Timeout in seconds (default: 300)")
@click.pass_context
def stop_deployment(ctx, name, wait, timeout):
    """Stop a model deployment"""
    try:
        ms = get_model_serving(ctx)
        deployment = ms.get_deployment(name)

        if not deployment:
            raise ResourceNotFoundError(f"Deployment '{name}' not found")

        # Check current state
        current_state = getattr(deployment, "state", None)
        if current_state and "stopped" in current_state.lower():
            print_warning(f"Deployment '{name}' is already stopped")
            return

        click.echo(f"Stopping deployment '{name}'...")

        # Stop the deployment
        deployment.stop()

        print_success(f"Deployment '{name}' stop command sent")

        if wait:
            click.echo(f"Waiting for deployment to be stopped (timeout: {timeout}s)...")
            start_time = time.time()

            while time.time() - start_time < timeout:
                # Refresh deployment status
                deployment = ms.get_deployment(name)
                current_state = getattr(deployment, "state", "unknown")

                if "stopped" in current_state.lower():
                    print_success(
                        f"Deployment '{name}' is now stopped "
                        f"(took {int(time.time() - start_time)}s)"
                    )
                    break

                time.sleep(5)
            else:
                print_warning(
                    f"Deployment '{name}' did not reach stopped state within {timeout}s"
                )

        # Show final status
        deployment_data = {
            "name": deployment.name,
            "status": getattr(deployment, "state", "N/A"),
        }

        formatter = OutputFormatter()
        output = formatter.format(deployment_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to stop deployment: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@deployments.command("status")
@click.argument("name")
@click.pass_context
def deployment_status(ctx, name):
    """Get the status of a deployment"""
    try:
        ms = get_model_serving(ctx)
        deployment = ms.get_deployment(name)

        if not deployment:
            raise ResourceNotFoundError(f"Deployment '{name}' not found")

        # Build status information
        status_data = {
            "name": deployment.name,
            "status": getattr(deployment, "state", "N/A"),
            "model": f"{deployment.model_name}:{deployment.model_version}"
            if hasattr(deployment, "model_name")
            else "N/A",
        }

        # Add predictor state if available
        if hasattr(deployment, "predictor") and deployment.predictor:
            predictor = deployment.predictor
            if hasattr(predictor, "state"):
                status_data["predictor_state"] = predictor.state
            if hasattr(predictor, "available_instances"):
                status_data["available_instances"] = predictor.available_instances
            if hasattr(predictor, "requested_instances"):
                status_data["requested_instances"] = predictor.requested_instances

        # Add transformer state if exists
        if hasattr(deployment, "transformer") and deployment.transformer:
            transformer = deployment.transformer
            if hasattr(transformer, "state"):
                status_data["transformer_state"] = transformer.state

        # Determine overall health
        state = getattr(deployment, "state", "").lower()
        if "running" in state:
            print_success(f"Deployment '{name}' is healthy and running")
        elif "stopped" in state:
            print_warning(f"Deployment '{name}' is stopped")
        elif "failed" in state:
            print_error(f"Deployment '{name}' has failed")
        else:
            print_warning(f"Deployment '{name}' is in state: {state}")

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(status_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get deployment status: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)
