"""Job management commands"""

import click
import json
from pathlib import Path
from hopsworks_cli.auth import get_connection
from hopsworks_cli.output import OutputFormatter, print_success, print_error
from hopsworks_cli.utils.exceptions import ResourceNotFoundError


@click.group()
def jobs():
    """Manage jobs in the project"""
    pass


@jobs.command("list")
@click.pass_context
def list_jobs(ctx):
    """List all jobs in the project"""
    try:
        connection = get_connection(ctx)
        job_api = connection.get_job_api()
        jobs_list = job_api.get_jobs()

        if not jobs_list:
            print("No jobs found")
            return

        # Format job data
        jobs_data = []
        for job in jobs_list:
            job_info = {
                "name": job.name,
                "type": getattr(job, "job_type", "N/A"),
                "created": str(job.created) if hasattr(job, "created") else "N/A",
            }

            # Add last execution info if available
            if hasattr(job, "executions") and job.executions:
                last_execution = job.executions[0]
                job_info["last_execution"] = getattr(last_execution, "state", "N/A")
                job_info["last_execution_time"] = str(last_execution.submission_time) if hasattr(last_execution, "submission_time") else "N/A"
            else:
                job_info["last_execution"] = "Never run"
                job_info["last_execution_time"] = "N/A"

            jobs_data.append(job_info)

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(jobs_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to list jobs: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@jobs.command("get")
@click.argument("name")
@click.pass_context
def get_job(ctx, name):
    """Get details of a specific job"""
    try:
        connection = get_connection(ctx)
        job_api = connection.get_job_api()
        job = job_api.get_job(name)

        if not job:
            raise ResourceNotFoundError(f"Job '{name}' not found")

        # Build detailed job information
        job_data = {
            "name": job.name,
            "type": getattr(job, "job_type", "N/A"),
            "created": str(job.created) if hasattr(job, "created") else "N/A",
            "creator": getattr(job, "creator", "N/A"),
        }

        # Add job configuration details
        if hasattr(job, "config"):
            config = job.config
            job_data["config"] = {
                "app_path": getattr(config, "app_path", "N/A"),
                "main_class": getattr(config, "main_class", "N/A") if hasattr(config, "main_class") else "N/A",
            }

        # Add schedule if exists
        if hasattr(job, "job_schedule") and job.job_schedule:
            schedule = job.job_schedule
            job_data["schedule"] = {
                "enabled": getattr(schedule, "enabled", False),
                "cron_expression": getattr(schedule, "cron_expression", "N/A"),
            }
        else:
            job_data["schedule"] = "No schedule"

        # Add execution history
        if hasattr(job, "executions") and job.executions:
            job_data["executions_count"] = len(job.executions)
            recent = job.executions[:3]  # Last 3 executions
            job_data["recent_executions"] = [
                {
                    "id": exec.id if hasattr(exec, "id") else "N/A",
                    "state": getattr(exec, "state", "N/A"),
                    "submission_time": str(exec.submission_time) if hasattr(exec, "submission_time") else "N/A",
                }
                for exec in recent
            ]
        else:
            job_data["executions_count"] = 0
            job_data["recent_executions"] = []

        # Output formatted results
        formatter = OutputFormatter()
        output = formatter.format(job_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to get job: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@jobs.command("create")
@click.argument("name")
@click.option("--config-file", type=click.Path(exists=True), required=True, help="Path to job configuration JSON file")
@click.pass_context
def create_job(ctx, name, config_file):
    """Create or update a job from a configuration file"""
    try:
        connection = get_connection(ctx)
        job_api = connection.get_job_api()

        # Load configuration from file
        config_path = Path(config_file)
        with open(config_path, "r") as f:
            config = json.load(f)

        click.echo(f"Creating/updating job '{name}' from {config_file}...")

        # Create or update the job
        job = job_api.create_job(name, config)

        print_success(f"Job '{name}' created/updated successfully")

        # Show job details
        job_data = {
            "name": job.name,
            "type": getattr(job, "job_type", "N/A"),
            "created": str(job.created) if hasattr(job, "created") else "N/A",
        }

        formatter = OutputFormatter()
        output = formatter.format(job_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to create job: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@jobs.command("run")
@click.argument("name")
@click.option("--wait", is_flag=True, help="Wait for job execution to complete")
@click.option("--timeout", type=int, default=600, help="Timeout in seconds when waiting (default: 600)")
@click.option("--args", help="Job arguments as JSON string")
@click.pass_context
def run_job(ctx, name, wait, timeout, args):
    """Run a job"""
    try:
        connection = get_connection(ctx)
        job_api = connection.get_job_api()
        job = job_api.get_job(name)

        if not job:
            raise ResourceNotFoundError(f"Job '{name}' not found")

        click.echo(f"Running job '{name}'...")

        # Parse arguments if provided
        job_args = None
        if args:
            try:
                job_args = json.loads(args)
            except json.JSONDecodeError as e:
                print_error(f"Invalid JSON for job arguments: {e}")
                ctx.exit(1)

        # Run the job
        if job_args:
            execution = job.run(args=job_args)
        else:
            execution = job.run()

        execution_id = execution.id if hasattr(execution, "id") else "N/A"
        print_success(f"Job '{name}' started. Execution ID: {execution_id}")

        if wait:
            import time
            click.echo(f"Waiting for execution to complete (timeout: {timeout}s)...")
            start_time = time.time()

            while time.time() - start_time < timeout:
                # Refresh job to get latest execution status
                job = job_api.get_job(name)
                if job.executions:
                    latest_exec = job.executions[0]
                    state = getattr(latest_exec, "state", "unknown").lower()

                    if state in ["finished", "succeeded", "success"]:
                        print_success(
                            f"Job execution completed successfully "
                            f"(took {int(time.time() - start_time)}s)"
                        )
                        break
                    elif state in ["failed", "killed", "error"]:
                        print_error(f"Job execution failed with state: {state}")
                        ctx.exit(1)

                time.sleep(5)
            else:
                print_error(f"Job execution did not complete within {timeout}s")
                ctx.exit(1)

        # Show execution details
        execution_data = {
            "job_name": name,
            "execution_id": execution_id,
            "state": getattr(execution, "state", "N/A"),
            "submission_time": str(execution.submission_time) if hasattr(execution, "submission_time") else "N/A",
        }

        formatter = OutputFormatter()
        output = formatter.format(execution_data, ctx.obj["output_format"])
        click.echo(output)

    except Exception as e:
        print_error(f"Failed to run job: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)


@jobs.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def delete_job(ctx, name, yes):
    """Delete a job"""
    try:
        connection = get_connection(ctx)
        job_api = connection.get_job_api()
        job = job_api.get_job(name)

        if not job:
            raise ResourceNotFoundError(f"Job '{name}' not found")

        # Confirm deletion
        if not yes:
            if not click.confirm(f"Are you sure you want to delete job '{name}'?"):
                click.echo("Deletion cancelled")
                return

        # Delete the job
        job.delete()

        print_success(f"Job '{name}' deleted successfully")

    except Exception as e:
        print_error(f"Failed to delete job: {e}")
        if ctx.obj.get("verbose"):
            raise
        ctx.exit(1)
