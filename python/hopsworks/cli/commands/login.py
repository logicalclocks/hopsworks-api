"""``hops login`` — scripted, flag-based authentication.

Use this when you already have an API key (CI scripts, headless machines
without a browser). For an interactive first-time install, prefer
``hops setup``.
"""

from __future__ import annotations

import click
from hopsworks.cli import auth, config, output


@click.command("login")
@click.option("--host", "host_flag", help="Hopsworks host URL.")
@click.option(
    "--api-key",
    "api_key_flag",
    help="API key value. If omitted, you will be prompted.",
)
@click.option("--project", "project_flag", help="Default project to attach to.")
@click.option(
    "--save-key/--no-save-key",
    default=True,
    help="Persist the API key to ~/.hops.toml. On by default; use --no-save-key to skip.",
)
def login_cmd(
    host_flag: str | None,
    api_key_flag: str | None,
    project_flag: str | None,
    save_key: bool,
) -> None:
    """Authenticate non-interactively with an existing API key.

    The key is validated against the host before it is saved. If validation
    fails, the config file is left untouched so the user can retry.

    Args:
        host_flag: Value of ``--host``; prompted if neither flag, env, nor config has it.
        api_key_flag: Value of ``--api-key``; prompted with hidden input if absent.
        project_flag: Value of ``--project``; may be None.
        save_key: When True, persist the validated key to ``~/.hops.toml``.
    """
    cfg = config.load(
        flag_host=host_flag, flag_api_key=api_key_flag, flag_project=project_flag
    )

    host = cfg.host or click.prompt("Hopsworks host", default=config.DEFAULT_HOST)
    host = auth.normalize_host(host)
    api_key = cfg.api_key or click.prompt("API key", hide_input=True)
    project = cfg.project or project_flag

    try:
        project_obj = auth.verify(host=host, api_key_value=api_key, project=project)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Login failed: {exc}") from exc

    project_name = getattr(project_obj, "name", project or None)

    if save_key:
        cfg.host = host
        cfg.api_key = api_key
        cfg.project = project_name
        config.save(cfg)
        output.info("Saved credentials to %s", config.CONFIG_PATH)

    output.success(
        "✓ Connected to %s as %s",
        host.replace("https://", "").replace("http://", ""),
        project_name or "(no project)",
    )
