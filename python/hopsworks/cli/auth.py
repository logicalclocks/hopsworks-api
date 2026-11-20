"""Thin wrapper around ``hopsworks.login()`` for the CLI.

Importing ``hopsworks`` transitively imports ``hsfs``/``hsml`` which adds ~1s
of startup cost, so every function here defers that import until the caller
actually needs a live SDK connection.
The module also exposes a ``normalize_host()`` helper shared by the token-flow
commands and ``hops login``.
"""

from __future__ import annotations

import io
import logging
import sys
import threading
import urllib.parse
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:  # pragma: no cover - import only for type hints
    from hopsworks.project import Project


_logger = logging.getLogger(__name__)


def normalize_host(host: str) -> str:
    """Return ``host`` as ``https://<netloc>`` with trailing API paths stripped.

    The SDK stores REST endpoints as ``https://host/hopsworks-api/api``; the
    user may paste any of the above forms. Stripping them here keeps URL
    construction in other modules a simple ``f"{host}/hopsworks-api/api/..."``.

    Args:
        host: Raw host string as typed by the user or loaded from config.

    Returns:
        A canonical ``scheme://netloc`` form with no trailing path.
    """
    if "://" not in host:
        host = "https://" + host
    parsed = urllib.parse.urlparse(host.strip())
    netloc = parsed.netloc or parsed.path
    base = f"{parsed.scheme or 'https'}://{netloc}"
    return base.rstrip("/")


def api_base(host: str) -> str:
    """Return the ``/hopsworks-api/api`` base URL for a given host.

    Args:
        host: Raw or normalized host string.

    Returns:
        A full REST base URL ready for endpoint concatenation.
    """
    return normalize_host(host) + "/hopsworks-api/api"


def login(
    host: str,
    api_key_value: str | None = None,
    project: str | None = None,
    engine: str | None = None,
    internal: bool = False,
    hostname_verification: bool = False,
) -> Project:
    """Call ``hopsworks.login()`` with a stable argument shape.

    Separated from ``hopsworks.login`` so the CLI can swap in test doubles and
    so the heavy SDK import stays lazy.

    Internal-mode behaviour: when ``internal`` is true (the CLI is running
    inside a Hopsworks pod), call ``hopsworks.login()`` with no host/port/key
    arguments so the SDK's own env-var detection (``REST_ENDPOINT`` +
    ``$SECRETS_DIR/token.jwt``) takes the auth path. Passing ``host``/``port``
    explicitly works in current SDK versions because it ignores them when
    ``REST_ENDPOINT`` is set, but relying on that is a footgun: a future SDK
    refactor that honours the explicit args would silently break in-pod auth.

    Args:
        host: Hopsworks host; ``https://`` prefix and trailing paths tolerated.
            Defaults port to 443 for ``https://`` (or no scheme), 80 for
            ``http://``.
        api_key_value: API key; omit to let the SDK pick up its own env/cache.
        project: Default project to attach to.
        engine: SDK engine; defaults to ``python``. Left unset, the SDK picks
            ``spark`` whenever pyspark is importable and then needs a Spark
            session configured for Hopsworks, which a laptop never has.
        internal: When True, call ``hopsworks.login()`` with no args so the
            SDK reads its in-pod credentials from environment variables.
        hostname_verification: Whether to verify Hopsworks' TLS certificate;
            False skips verification (the SDK default). Passed straight through
            to ``hopsworks.login(hostname_verification=...)``.

    Returns:
        The authenticated SDK ``Project`` object.
    """
    import hopsworks  # noqa: PLC0415 - intentionally lazy

    engine = engine or "python"
    if internal:
        # Let the SDK do its own ``REST_ENDPOINT`` + ``$SECRETS_DIR/token.jwt``
        # discovery. ``project``/``engine`` still apply at the SDK layer.
        kwargs: dict[str, Any] = {"hostname_verification": hostname_verification}
        if project:
            kwargs["project"] = project
        if engine:
            kwargs["engine"] = engine
        return _quiet_login(hopsworks, kwargs)

    hostname, port = _host_port(host)
    kwargs = {
        "host": hostname,
        "port": port,
        "hostname_verification": hostname_verification,
    }
    if api_key_value:
        kwargs["api_key_value"] = api_key_value
    if project:
        kwargs["project"] = project
    if engine:
        kwargs["engine"] = engine
    return _quiet_login(hopsworks, kwargs)


def _host_port(host: str) -> tuple[str, int]:
    """Split a host as the user gives it into the SDK's hostname and port: 443 for https or no scheme, 80 for http."""
    parsed = urllib.parse.urlparse(host if "://" in host else "https://" + host)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc or parsed.path
    return netloc.split(":")[0], parsed.port or (80 if scheme == "http" else 443)


# RESTCodes.ApiKeyErrorCode: the key carries none of the scopes the endpoint accepts.
NO_VALID_SCOPE = 320004


def is_scope_refusal(exc: BaseException) -> bool:
    """Whether ``exc`` is Hopsworks refusing an API key for its scopes."""
    from hopsworks_common.client.exceptions import RestAPIError  # noqa: PLC0415

    return (
        isinstance(exc, RestAPIError)
        and getattr(exc.response, "status_code", None) == 403
        and getattr(exc, "error_code", None) == NO_VALID_SCOPE
    )


def login_serving(
    host: str,
    api_key_value: str,
    project: str,
    project_id: int | None = None,
    hostname_verification: bool = False,
) -> Any:
    """Connect for the model serving endpoints only, with a key that may hold just the SERVING scope.

    ``hopsworks.login`` reads the project by name, the username, the client
    certificates and the serving settings, which need the PROJECT scope, and
    ``get_model_serving`` checks the Models dataset, which needs DATASET_VIEW.
    The serving endpoints themselves take SERVING. This connects without a
    project, then sets it from its id: read by name when the key may, else
    ``project_id`` (``HOPSWORKS_PROJECT_ID``, or the saved config of the same
    project). No certificates are downloaded; serving is REST only.

    Args:
        host: Hopsworks host; ``https://`` prefix and trailing paths tolerated.
        api_key_value: API key.
        project: Project name.
        project_id: The project's id, used when the key cannot read the project by name.
        hostname_verification: Whether to verify Hopsworks' TLS certificate.

    Returns:
        The SDK ``ModelServing`` handle of the project.

    Raises:
        click.ClickException: When the project id cannot be known.
    """
    import click  # noqa: PLC0415
    import hopsworks  # noqa: PLC0415 - intentionally lazy
    from hopsworks_common import client  # noqa: PLC0415
    from hsml.model_serving import ModelServing  # noqa: PLC0415

    hostname, port = _host_port(host)
    connection = hopsworks.connection(
        host=hostname,
        port=port,
        api_key_value=api_key_value,
        hostname_verification=hostname_verification,
        engine="python",
    )
    _client = client._get_instance()
    namespace = None
    by_name = True
    try:
        info = _client._get_project_info(project)
        project_id = info["projectId"]
        namespace = info.get("namespace")
    except Exception as exc:
        if not is_scope_refusal(exc):
            raise
        by_name = False
    if not by_name and project_id is None:
        # Raised outside the handler: the CLI would otherwise append its
        # generic missing-scope advice to this one.
        raise click.ClickException(
            f"This API key cannot read project '{project}' by name (it has no "
            "PROJECT scope). Give the project's id: HOPSWORKS_PROJECT_ID=<id>."
        )
    _client._project_name = project
    _client._project_id = str(project_id)
    connection._project = project
    try:
        connection._load_serving_defaults()
    except Exception as exc:
        if not is_scope_refusal(exc):
            raise
        # The serving settings are variables, which take the PROJECT scope.
        # Without them there is no Istio client, so predictions go through
        # Hopsworks, which serves every key with SERVING.
        client._set_kserve_installed(False)
        client._set_serving_num_instances_limits([-1, -1])
        client._set_knative_domain("")
        connection._serving_defaults_loaded = True
    return ModelServing(project, int(project_id), project_namespace=namespace)


class _ThreadSilencedStdout(io.TextIOBase):
    """Stdout that discards writes from threads that asked for silence.

    ``contextlib.redirect_stdout`` swaps ``sys.stdout`` for the whole process, so
    a login in a background thread (``hops factory run`` logs in while the user answers
    its first question) would swallow the main thread's prompts.
    """

    _silenced = threading.local()

    def __init__(self, real: Any) -> None:
        self.real = real

    def write(self, text: str) -> int:
        if getattr(self._silenced, "on", False):
            return len(text)
        return self.real.write(text)

    def flush(self) -> None:
        self.real.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.real, name)


def _quiet_login(hopsworks: Any, kwargs: dict[str, Any]) -> Project:
    """``hopsworks.login`` without its ``print`` banner.

    The SDK prints "Logged in to project, explore it here <url>" to stdout on
    every login. A CLI reports through ``output.*`` and its stdout carries the
    command payload (``--json``, pipelines), so the banner is discarded, for
    the calling thread only.
    """
    if not isinstance(sys.stdout, _ThreadSilencedStdout):
        sys.stdout = _ThreadSilencedStdout(sys.stdout)
    _ThreadSilencedStdout._silenced.on = True
    try:
        return hopsworks.login(**kwargs)
    finally:
        _ThreadSilencedStdout._silenced.on = False


def verify(
    host: str,
    api_key_value: str,
    project: str | None = None,
    hostname_verification: bool = False,
) -> Project:
    """Run a minimal login to confirm ``api_key_value`` works against ``host``.

    Raises whatever ``hopsworks.login()`` raises on failure so the caller can
    surface a specific error message to the user.

    Args:
        host: Hopsworks host.
        api_key_value: API key to validate.
        project: Optional project to attach to.
        hostname_verification: Whether to verify Hopsworks' TLS certificate.

    Returns:
        The authenticated SDK ``Project`` object.
    """
    return login(
        host=host,
        api_key_value=api_key_value,
        project=project,
        hostname_verification=hostname_verification,
    )
