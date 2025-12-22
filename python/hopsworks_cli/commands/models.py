"""Model management commands"""

import click
from pathlib import Path
from hopsworks_cli.auth import get_model_registry
from hopsworks_cli.output import OutputFormatter, print_success, print_error
from hopsworks_cli.utils.exceptions import ResourceNotFoundError


@click.group()
def models():
    """Manage models in the model registry"""
    pass


@models.command("list")
@click.option("--name", help="Filter by model name (all versions)")
@click.pass_context
def list_models(ctx, name):
    """List models in the model registry"""
    try:
        mr = get_model_registry(ctx)

        if name:
            # Get all versions of a specific model
            models_list = mr.get_models(name)
        else:
            # Get all models - this may not be directly supported
            # We'll need to handle this based on available API
            try:
                # Try to get all models (if API supports it)
                models_list = mr.get_models()
            except Exception:
                print_error(
                    "Listing all models is not supported. Please specify --name to list versions of a specific model."
                )
                ctx.exit(1)

        if not models_list:
            print("No models found")
            return

        # Format model data
        models_data = []
        for model in models_list:
            model_info = {
                "name": model.name,
                "version": model.version,
                "framework": getattr(model, "framework", "N/A"),
                "created": str(model.created) if hasattr(model, "created") else "N/A",
            }

            # Add metrics if available
            if hasattr(model, "training_metrics") and model.training_metrics:
                # Show first few metrics
                metrics_str = ", ".join(
                    [f"{k}={v:.4f}" if isinstance(v, float) else f"{k}={v}"
                     for k, v in list(model.training_metrics.items())[:3]]
                )
                model_info["metrics"] = metrics_str
            else:
                model_info["metrics"] = "N/A"

            models_data.append(model_info)

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(models_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to list models: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@models.command("get")
@click.argument("name")
@click.option("--version", type=int, required=True, help="Model version")
@click.pass_context
def get_model(ctx, name, version):
    """Get details of a specific model"""
    try:
        mr = get_model_registry(ctx)
        model = mr.get_model(name, version)

        if not model:
            raise ResourceNotFoundError(f"Model '{name}' version {version} not found")

        # Build detailed model information
        model_data = {
            "name": model.name,
            "version": model.version,
            "description": getattr(model, "description", "N/A"),
            "framework": getattr(model, "framework", "N/A"),
            "created": str(model.created) if hasattr(model, "created") else "N/A",
            "model_schema": str(model.model_schema) if hasattr(model, "model_schema") else "N/A",
        }

        # Add training metrics
        if hasattr(model, "training_metrics") and model.training_metrics:
            model_data["training_metrics"] = model.training_metrics

        # Add input/output example if available
        if hasattr(model, "input_example") and model.input_example:
            model_data["has_input_example"] = True
        else:
            model_data["has_input_example"] = False

        # Add program and environment
        if hasattr(model, "program"):
            model_data["program"] = model.program
        if hasattr(model, "environment"):
            model_data["environment"] = model.environment

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(model_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get model: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@models.command("best")
@click.argument("name")
@click.option("--metric", required=True, help="Metric name to compare")
@click.option(
    "--direction",
    type=click.Choice(["max", "min"], case_sensitive=False),
    required=True,
    help="Direction to optimize (max or min)",
)
@click.pass_context
def get_best_model(ctx, name, metric, direction):
    """Get the best performing model based on a metric"""
    try:
        mr = get_model_registry(ctx)
        model = mr.get_best_model(name, metric, direction)

        if not model:
            raise ResourceNotFoundError(
                f"No model found for '{name}' with metric '{metric}'"
            )

        # Build model information
        model_data = {
            "name": model.name,
            "version": model.version,
            "framework": getattr(model, "framework", "N/A"),
            "created": str(model.created) if hasattr(model, "created") else "N/A",
            f"best_metric_{metric}": (
                model.training_metrics.get(metric, "N/A")
                if hasattr(model, "training_metrics") and model.training_metrics
                else "N/A"
            ),
        }

        # Add all metrics
        if hasattr(model, "training_metrics") and model.training_metrics:
            model_data["all_metrics"] = model.training_metrics

        print_success(
            f"Best model: {model.name} v{model.version} "
            f"({metric}={model_data.get(f'best_metric_{metric}', 'N/A')})"
        )

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(model_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get best model: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@models.command("download")
@click.argument("name")
@click.option("--version", type=int, required=True, help="Model version")
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    help="Output directory for downloaded model",
)
@click.pass_context
def download_model(ctx, name, version, output_dir):
    """Download a model from the model registry"""
    try:
        mr = get_model_registry(ctx)
        model = mr.get_model(name, version)

        if not model:
            raise ResourceNotFoundError(f"Model '{name}' version {version} not found")

        # Set output directory
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            download_path = output_dir
        else:
            download_path = None

        click.echo(f"Downloading model {name} v{version}...")

        # Download the model
        local_path = model.download(download_path)

        print_success(f"Model downloaded to: {local_path}")

        # Show download details
        download_data = {
            "name": model.name,
            "version": model.version,
            "local_path": str(local_path),
            "framework": getattr(model, "framework", "N/A"),
        }

        formatter = OutputFormatter()
        output = formatter.format(download_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to download model: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@models.command("info")
@click.argument("name")
@click.option("--version", type=int, required=True, help="Model version")
@click.pass_context
def model_info(ctx, name, version):
    """Get comprehensive information about a model including metrics and metadata"""
    try:
        mr = get_model_registry(ctx)
        model = mr.get_model(name, version)

        if not model:
            raise ResourceNotFoundError(f"Model '{name}' version {version} not found")

        # Build comprehensive model information
        info = {
            "Basic Information": {
                "name": model.name,
                "version": model.version,
                "description": getattr(model, "description", "N/A"),
                "framework": getattr(model, "framework", "N/A"),
                "created": str(model.created) if hasattr(model, "created") else "N/A",
            }
        }

        # Training metrics
        if hasattr(model, "training_metrics") and model.training_metrics:
            info["Training Metrics"] = model.training_metrics

        # Model schema
        if hasattr(model, "model_schema") and model.model_schema:
            info["Model Schema"] = str(model.model_schema)

        # Input/Output examples
        info["Examples"] = {
            "has_input_example": bool(
                hasattr(model, "input_example") and model.input_example
            ),
            "has_output_example": bool(
                hasattr(model, "output_example") and model.output_example
            ),
        }

        # Tags
        if hasattr(model, "tags") and model.tags:
            info["Tags"] = model.tags

        # For table format, flatten the structure
        if ctx.obj["output_format"] == "table":
            flat_data = []
            for section, data in info.items():
                if isinstance(data, dict):
                    for key, value in data.items():
                        flat_data.append(
                            {"section": section, "property": key, "value": str(value)}
                        )
                else:
                    flat_data.append(
                        {"section": section, "property": "value", "value": str(data)}
                    )
            output_data = flat_data
        else:
            output_data = info

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(output_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get model info: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)
