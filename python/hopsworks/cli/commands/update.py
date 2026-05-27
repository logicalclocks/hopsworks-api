"""``hops update`` — print pip upgrade instructions.

We intentionally do not self-update. The CLI ships inside the ``hopsworks``
pip package, and the right command to run depends on how the user installed
it (user venv, conda env, system-managed venv, Hopsworks-managed
``/srv/hops/venv``, docker image). We detect the install mode and suggest the
right command; the user runs it themselves.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
from hopsworks.cli import output


@click.command("update")
def update_cmd() -> None:
    """Print the command to upgrade the ``hopsworks`` package."""
    install_mode, hint = _describe_install()
    current = _current_version()

    if output.JSON_MODE:
        output.print_json(
            {
                "install_mode": install_mode,
                "version": current,
                "upgrade_command": hint,
            }
        )
        return

    output.info("hops is installed from the hopsworks PyPI package.")
    output.info("Install mode: %s", install_mode)
    output.info("Current version: %s", current)
    output.info("")
    output.info("To upgrade, run:")
    click.echo(f"  {hint}")


def _describe_install() -> tuple[str, str]:
    """Classify the Python environment and return an appropriate pip command.

    Returns:
        ``(mode, command)``. ``mode`` is human-readable; ``command`` is the
        literal upgrade line to print.
    """
    venv = sys.prefix
    if venv == sys.base_prefix:
        # No virtualenv — almost certainly a system Python or conda base.
        return "system Python (no venv active)", (
            "pip install --user --upgrade hopsworks   # or use pipx/uv"
        )

    # Compare against the unresolved prefix so we recognize the Linux-only
    # "/srv/hops/venv" path even on Windows test runners, where Path.resolve()
    # would drive-prefix it.
    if venv.replace("\\", "/").startswith("/srv/hops"):
        return "Hopsworks-managed venv (/srv/hops/venv)", (
            "# Ask your Hopsworks admin to rebuild the image — the venv is baked in.\n"
            "# Alternatively, create a user venv and `pip install --upgrade hopsworks` there."
        )

    try:
        venv_path = Path(venv).resolve()
    except OSError:
        venv_path = Path(venv)

    if os.environ.get("CONDA_PREFIX") and Path(os.environ["CONDA_PREFIX"]) == venv_path:
        return "conda env", "pip install --upgrade hopsworks"

    if _is_editable():
        return "editable install (git checkout)", "git pull && uv sync --project python"

    return f"virtualenv ({venv_path})", "pip install --upgrade hopsworks"


def _is_editable() -> bool:
    """Return True when ``hopsworks`` was installed with ``pip install -e``.

    Uses ``importlib.metadata`` so we don't force the SDK import just to
    answer the question.
    """
    try:
        from importlib.metadata import distribution
    except ImportError:  # pragma: no cover - 3.10+ stdlib
        return False
    try:
        dist = distribution("hopsworks")
    except Exception:  # noqa: BLE001
        return False
    direct_url = dist.read_text("direct_url.json") or ""
    return (
        '"editable": true' in direct_url.lower()
        or '"editable":true' in direct_url.lower()
    )


def _current_version() -> str:
    try:
        from hopsworks_common.version import __version__

        return __version__
    except Exception:  # noqa: BLE001
        return "unknown"
