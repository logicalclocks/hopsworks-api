"""Config file and environment resolution for the ``hops`` CLI.

Config is stored in ``~/.hops.toml`` (Modal-style) under a ``[default]`` table.
Resolution precedence, highest first: CLI flags, ``HOPSWORKS_*`` env vars,
legacy ``REST_ENDPOINT``/``HOPSWORKS_API_KEY``/``PROJECT_NAME`` env vars kept
for back-compat with existing Hopsworks terminal pods, the TOML file, and the
SDK's SaaS key cache at ``~/.{username}_hopsworks_app/.hw_api_key``.

Internal mode is detected when ``REST_ENDPOINT`` is set and
``$SECRETS_DIR/token.jwt`` is readable — in that case the SDK authenticates
from the JWT directly and the CLI never writes ``~/.hops.toml``.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import sys
from pathlib import Path
from typing import Any


if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - 3.10 path
    import tomli as tomllib

import tomli_w


_logger = logging.getLogger(__name__)

DEFAULT_HOST = "https://c.app.hopsworks.ai"
CONFIG_PATH = Path.home() / ".hops.toml"
LEGACY_YAML_PATH = Path.home() / ".hops" / "config"


@dataclasses.dataclass
class HopsConfig:
    """Resolved CLI configuration for a single invocation.

    Fields are populated by ``load()`` from the precedence chain documented at
    module level.
    The ``internal`` flag is set when the process is running inside a Hopsworks
    pod and auth uses the mounted JWT instead of an API key.
    """

    host: str | None = None
    api_key: str | None = None
    api_key_name: str | None = None
    project: str | None = None
    project_id: int | None = None
    feature_store_id: int | None = None
    jwt_token: str | None = None
    internal: bool = False

    def mode(self) -> str:
        """Describe which auth mode the CLI is operating in.

        Returns:
            ``"internal"`` when running inside a Hopsworks pod, else ``"external"``.
        """
        return "internal" if self.internal else "external"

    def is_authenticated(self) -> bool:
        """Check whether enough credentials are available to call ``hopsworks.login()``.

        Internal mode counts as authenticated regardless of cached fields:
        the SDK will read ``REST_ENDPOINT`` and ``$SECRETS_DIR/token.jwt``
        from the pod environment when invoked with no args. External mode
        needs both ``host`` and a non-empty API key.

        Returns:
            True when the SDK can reach a usable login configuration.
        """
        if self.internal:
            return True
        if not self.host:
            return False
        return bool(self.api_key)


def _detect_internal() -> tuple[bool, str | None]:
    """Detect the in-pod internal mode and return ``(is_internal, jwt_or_none)``.

    Mirrors the Go CLI's ``config.Load()`` detection so existing terminal pods
    keep working without change.
    """
    if not os.environ.get("REST_ENDPOINT"):
        return False, None
    secrets_dir = os.environ.get("SECRETS_DIR")
    if not secrets_dir:
        return False, None
    token_path = Path(secrets_dir) / "token.jwt"
    try:
        return True, token_path.read_text().strip()
    except OSError:
        return True, None


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, tomllib.TOMLDecodeError) as exc:
        _logger.warning("Could not read %s: %s", path, exc)
        return {}


def _migrate_legacy_yaml() -> dict[str, Any]:
    """Convert a Go-CLI ``~/.hops/config`` YAML into TOML on first run.

    We only perform the migration once and only when the TOML file is absent so
    that users who deliberately pruned their config do not get it resurrected.
    We do not require PyYAML because the file's schema is trivially
    hand-parseable — a few top-level ``key: value`` pairs.
    """
    if CONFIG_PATH.exists() or not LEGACY_YAML_PATH.exists():
        return {}

    data: dict[str, Any] = {}
    try:
        for raw in LEGACY_YAML_PATH.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            key, _, value = line.partition(":")
            value = value.strip().strip('"').strip("'")
            if not value:
                continue
            if value.isdigit():
                data[key.strip()] = int(value)
            else:
                data[key.strip()] = value
    except OSError as exc:
        _logger.warning("Could not migrate %s: %s", LEGACY_YAML_PATH, exc)
        return {}

    remapped = {
        "host": data.get("host"),
        "api_key": data.get("api_key"),
        "project": data.get("project"),
        "project_id": data.get("project_id"),
        "feature_store_id": data.get("feature_store_id"),
    }
    remapped = {k: v for k, v in remapped.items() if v is not None}
    if remapped:
        try:
            save(HopsConfig(**remapped))
            sys.stderr.write(
                f"Migrated {LEGACY_YAML_PATH} -> {CONFIG_PATH}. "
                "You can safely delete the old file.\n"
            )
        except OSError as exc:
            _logger.warning("Could not write migrated config: %s", exc)
    return remapped


def load(
    flag_host: str | None = None,
    flag_api_key: str | None = None,
    flag_project: str | None = None,
    profile: str = "default",
) -> HopsConfig:
    """Resolve configuration from flags, environment, and ``~/.hops.toml``.

    Args:
        flag_host: Value of the ``--host`` CLI flag, if set.
        flag_api_key: Value of the ``--api-key`` CLI flag, if set.
        flag_project: Value of the ``--project`` CLI flag, if set.
        profile: TOML table to read from; defaults to ``default``.

    Returns:
        A fully-resolved ``HopsConfig``. Fields may still be ``None`` when the
        user has not yet run ``hops setup`` — callers should check
        ``is_authenticated()`` before attempting SDK calls.
    """
    internal, jwt = _detect_internal()
    cfg = HopsConfig(internal=internal, jwt_token=jwt)

    if internal:
        cfg.host = os.environ.get("REST_ENDPOINT") or None
        cfg.project = os.environ.get("PROJECT_NAME") or None
        project_id = os.environ.get("HOPSWORKS_PROJECT_ID")
        if project_id and project_id.isdigit():
            cfg.project_id = int(project_id)
        return _apply_overrides(cfg, flag_host, flag_api_key, flag_project)

    _migrate_legacy_yaml()
    file_data = _read_toml(CONFIG_PATH).get(profile, {})
    if isinstance(file_data, dict):
        cfg.host = file_data.get("host") or None
        cfg.api_key = file_data.get("api_key") or None
        cfg.api_key_name = file_data.get("api_key_name") or None
        cfg.project = file_data.get("project") or None
        cfg.project_id = file_data.get("project_id")
        cfg.feature_store_id = file_data.get("feature_store_id")

    env_host = os.environ.get("HOPSWORKS_HOST") or os.environ.get("REST_ENDPOINT")
    env_api_key = os.environ.get("HOPSWORKS_API_KEY")
    env_project = os.environ.get("HOPSWORKS_PROJECT") or os.environ.get("PROJECT_NAME")
    env_project_id = os.environ.get("HOPSWORKS_PROJECT_ID")

    if env_host:
        cfg.host = env_host
    if env_api_key:
        cfg.api_key = env_api_key
    if env_project:
        cfg.project = env_project
    if env_project_id and env_project_id.isdigit():
        cfg.project_id = int(env_project_id)

    return _apply_overrides(cfg, flag_host, flag_api_key, flag_project)


def _apply_overrides(
    cfg: HopsConfig,
    flag_host: str | None,
    flag_api_key: str | None,
    flag_project: str | None,
) -> HopsConfig:
    if flag_host:
        cfg.host = flag_host
    if flag_api_key:
        cfg.api_key = flag_api_key
    if flag_project:
        cfg.project = flag_project
    return cfg


def save(cfg: HopsConfig, profile: str = "default") -> None:
    """Atomically write ``cfg`` into ``~/.hops.toml`` under ``[profile]``.

    The file is written with mode ``0600`` because it holds an API key.
    We preserve other profiles so that a user with multiple hosts configured
    only sees the selected one updated.

    Args:
        cfg: The config object to persist.
        profile: TOML table to write into.
    """
    existing = _read_toml(CONFIG_PATH)
    payload = {
        k: v
        for k, v in {
            "host": cfg.host,
            "api_key": cfg.api_key,
            "api_key_name": cfg.api_key_name,
            "project": cfg.project,
            "project_id": cfg.project_id,
            "feature_store_id": cfg.feature_store_id,
        }.items()
        if v is not None
    }
    existing[profile] = payload

    tmp = CONFIG_PATH.with_suffix(CONFIG_PATH.suffix + ".tmp")
    # Open with O_CREAT|O_WRONLY|O_TRUNC and mode 0o600 so the secret is
    # never world-readable even between open() and chmod().
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        tomli_w.dump(existing, f)
    os.replace(tmp, CONFIG_PATH)


def clear(profile: str = "default") -> None:
    """Delete ``[profile]`` from the config file; drop the file if it becomes empty.

    Args:
        profile: TOML table to remove.
    """
    existing = _read_toml(CONFIG_PATH)
    if profile in existing:
        del existing[profile]
    if not existing:
        CONFIG_PATH.unlink(missing_ok=True)
        return
    tmp = CONFIG_PATH.with_suffix(CONFIG_PATH.suffix + ".tmp")
    # Open with O_CREAT|O_WRONLY|O_TRUNC and mode 0o600 so the secret is
    # never world-readable even between open() and chmod().
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        tomli_w.dump(existing, f)
    os.replace(tmp, CONFIG_PATH)
