#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from hopsworks_apigen import public
from hopsworks_common import client, usage, util
from hopsworks_common.client.exceptions import RestAPIError


if TYPE_CHECKING:
    from hopsworks_common import app


_GIT_PROVIDER_ALIASES = {
    "github": "GitHub",
    "gitlab": "GitLab",
    "bitbucket": "BitBucket",
}


@public("hopsworks.core.app_api.AppApi")
class AppApi:
    # Mirrors PythonAppKind in the backend.
    # Every kind but CUSTOM is launched from its app file.
    APP_KINDS = ("STREAMLIT", "CUSTOM", "FLASK", "GRADIO", "NODEJS")
    APP_KIND_LABELS = {
        "STREAMLIT": "Streamlit",
        "CUSTOM": "custom",
        "FLASK": "Flask",
        "GRADIO": "Gradio",
        "NODEJS": "Node.js",
    }
    APP_KIND_EXTENSIONS = {
        "STREAMLIT": (".py",),
        "FLASK": (".py",),
        "GRADIO": (".py",),
        "NODEJS": (".js", ".mjs", ".cjs", ".ts", ".mts"),
    }

    @classmethod
    def _check_entrypoint_extension(
        cls, app_kind: str, path: str, parameter: str
    ) -> None:
        extensions = cls.APP_KIND_EXTENSIONS.get(app_kind)
        if extensions and not path.lower().endswith(extensions):
            raise ValueError(
                f"{parameter} must be a {', '.join(extensions)} file for {cls.APP_KIND_LABELS[app_kind]} apps: {path}"
            )

    def __init__(self):
        self._log = logging.getLogger(__name__)

    @public
    @usage._method_logger
    def get_apps(self) -> list[app.App]:
        """Get all apps in the project.

        Returns:
            List of App objects.
        """
        from hopsworks_common import app

        _client = client._get_instance()
        path_params = ["project", _client._project_id, "apps"]
        headers = {"content-type": "application/json"}
        response = _client._send_request("GET", path_params, headers=headers)
        return app.App.from_response_json_list(response)

    @public
    @usage._method_logger
    def get_app(self, name: str) -> app.App | None:
        """Get an app by name.

        Parameters:
            name: Name of the app.

        Returns:
            App object, or ``None`` if no app with that name exists, so a
            reuse-if-exists guard does not have to catch a 404.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error other than a 404.
        """
        from hopsworks_common import app

        _client = client._get_instance()
        path_params = ["project", _client._project_id, "apps", name]
        headers = {"content-type": "application/json"}
        try:
            response = _client._send_request("GET", path_params, headers=headers)
        except RestAPIError as err:
            if (
                getattr(err.response, "status_code", None)
                == RestAPIError.STATUS_CODE_NOT_FOUND
            ):
                return None
            raise
        return app.App.from_response_json(response)

    @public
    @usage._method_logger
    def create_app(
        self,
        name: str,
        app_path: str | None = None,
        environment: str = "python-app-pipeline",
        memory: int = 2048,
        cores: float = 1.0,
        env_vars: dict[str, str] | None = None,
        app_kind: str = "STREAMLIT",
        entrypoint_command: str | None = None,
        app_port: int | None = None,
        description: str | None = None,
        git_url: str | None = None,
        git_provider: str | None = None,
        git_branch: str | None = None,
        git_auto_redeploy: bool = False,
        entrypoint_script: str | None = None,
        app_base_path: str | None = None,
        readiness_probe_path: str | None = None,
        db_access: bool = True,
        idle_scale_to_zero: bool | None = None,
        idle_cooldown_seconds: int | None = None,
        cold_start_timeout_seconds: int | None = None,
    ) -> app.App:
        """Create a new app.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()
            apps = project.get_app_api()

            app = apps.create_app(
                "my_dashboard",
                app_path="Resources/app.py",
            )

            app.run()
            print(app.app_url)
            ```

        Parameters:
            name: Name of the app.
            app_path: Path to the app file in HopsFS.
            environment: Python environment name (default: "python-app-pipeline").
            memory: Memory in MB (default: 2048).
            cores: CPU cores (default: 1.0).
            env_vars: Per-runtime env vars applied when the app is started.
                These override account-level env vars for this app's executions.
            app_kind: What the app is, which decides how it is started.
                ``STREAMLIT``, ``FLASK`` and ``GRADIO`` run the given Python file with their framework.
                ``NODEJS`` runs a JavaScript or TypeScript file with Node.js, and the app reads its port from ``process.env.PORT``.
                ``CUSTOM`` runs ``entrypoint_command``.
            entrypoint_command: Startup command for ``CUSTOM`` apps.
                The other kinds generate theirs from the app file.
            app_port: Port the app listens on, for every kind but ``STREAMLIT``.
                Flask is started on it; Gradio and Node.js are told it through ``GRADIO_SERVER_PORT`` and ``PORT``.
                Unset leaves the backend's default.
            description: Optional app description.
            git_url: Optional Git repository URL. When set, the app is cloned on
                every start.
            git_provider: Git provider for git-backed apps (GitHub, GitLab or
                BitBucket).
            git_branch: Optional branch to clone for git-backed apps.
            git_auto_redeploy: Roll the app to the branch HEAD whenever a new commit is pushed.
                Only valid for git-backed apps.
                The running app keeps serving until the new version is ready.
            entrypoint_script: The app file relative to the repository root, for git
                repository apps of every kind but ``CUSTOM``.
            app_base_path: Public mount path for the app, for example ``/`` or
                ``/myapp``.
            readiness_probe_path: Optional readiness probe path to use instead of
                the platform default.

            db_access: Give the app access to the project's online feature store
                database. Defaults to True: starting the app creates the database
                if the project does not have one yet and injects the `MYSQL_USER`,
                `MYSQL_PASSWORD_SECRET_NAME`, `MYSQL_DB`, `MYSQL_HOST` and `MYSQL_PORT`
                environment variables into the app container, the same way agent
                deployments get them. Set to False for apps that do not need the
                database. Independently of this flag, when Trino is enabled on the
                cluster every app gets `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`,
                `TRINO_PASSWORD_SECRET_NAME` and `TRINO_SCHEMA` to read the offline
                feature group tables with SQL (the catalog is the feature group's
                format, `delta` or `hudi`, chosen by the app).
            idle_scale_to_zero: Scale the app to zero when idle.
                The app's pod goes away once no request has been in flight for `idle_cooldown_seconds` and comes back on the next request, which is held for up to `cold_start_timeout_seconds`.
                The app shows the `IDLE` state meanwhile, and opening it is what wakes it.
                On by default wherever the cluster has KEDA (`None` leaves the choice to the cluster); pass `False` for an app that must stay up, and `True` to insist, which fails on a cluster without KEDA.
                An open WebSocket, such as a Streamlit session in an open tab, counts as in flight, so the app stays up while someone has it open.
            idle_cooldown_seconds: Seconds without a request before the app scales to zero.
                Turns the option on when given; the default is the cluster's `pythonapp_idle_cooldown_seconds` (21600, six hours).
            cold_start_timeout_seconds: Seconds the first request waits for the app to wake.
                Turns the option on when given; the default and ceiling is the cluster's `pythonapp_cold_start_timeout_max_seconds` (300). The ingress read timeout (60 unless raised) may fail the waking request earlier; the pod keeps starting and the next request finds it ready.

        Returns:
            The created App object.
        """
        _client = client._get_instance()

        app_kind_name = str(getattr(app_kind, "name", app_kind) or "STREAMLIT").upper()
        app_path = self._trim_to_none(app_path)
        entrypoint_command = self._trim_to_none(entrypoint_command)
        git_url = self._trim_to_none(git_url)
        git_provider = self._normalize_git_provider(git_provider)
        git_branch = self._trim_to_none(git_branch)
        entrypoint_script = self._trim_to_none(entrypoint_script)
        app_base_path = self._trim_to_none(app_base_path)
        readiness_probe_path = self._trim_to_none(readiness_probe_path)
        git_repo_app = bool(git_url)
        if app_kind_name not in self.APP_KINDS:
            raise ValueError(
                f"Unknown app_kind {app_kind_name!r}; one of {', '.join(self.APP_KINDS)}."
            )
        # Every kind but CUSTOM is started from its app file with a generated command.
        file_launched_app = app_kind_name != "CUSTOM"
        kind_label = self.APP_KIND_LABELS[app_kind_name]

        # Mirrors PythonAppJobValidator: the backend rejects the flag without a git
        # source. Fail here so the caller gets the reason instead of a REST error.
        if git_auto_redeploy and not git_repo_app:
            raise ValueError(
                "git_auto_redeploy is only supported for Git repository apps."
            )
        # The backend ignores the two timeouts with the option off; saying so here beats
        # a silent no-op. Given without the flag, they mean the option is wanted.
        has_idle_timeouts = (
            idle_cooldown_seconds is not None or cold_start_timeout_seconds is not None
        )
        if idle_scale_to_zero is False and has_idle_timeouts:
            raise ValueError(
                "idle_cooldown_seconds and cold_start_timeout_seconds are only used with idle_scale_to_zero on."
            )
        if idle_scale_to_zero is None and has_idle_timeouts:
            idle_scale_to_zero = True

        if file_launched_app:
            if entrypoint_command:
                raise ValueError(
                    f"entrypoint_command is only used for custom apps; {kind_label} apps are started from the app file."
                )
            if git_repo_app:
                if not git_provider:
                    raise ValueError(
                        "git_provider is required for Git repository apps."
                    )
                if not entrypoint_script:
                    raise ValueError(
                        f"entrypoint_script is required for {kind_label} Git repository apps."
                    )
                self._check_entrypoint_extension(
                    app_kind_name, entrypoint_script, "entrypoint_script"
                )
            elif not app_path:
                raise ValueError(f"app_path is required for {kind_label} apps.")
            elif entrypoint_script:
                raise ValueError(
                    "entrypoint_script is only used for Git repository apps."
                )
            else:
                self._check_entrypoint_extension(app_kind_name, app_path, "app_path")
        else:
            if not entrypoint_command:
                raise ValueError("entrypoint_command is required for custom apps.")
            if git_repo_app and not git_provider:
                raise ValueError("git_provider is required for Git repository apps.")
            if entrypoint_script:
                raise ValueError(
                    "entrypoint_script is only used for Git repository apps of the other kinds."
                )

        if app_path and not git_repo_app:
            app_path = util._convert_to_abs(app_path, _client._project_name)
            if not app_path.startswith("hdfs://"):
                app_path = "hdfs://" + app_path

        config = {
            "type": "pythonAppJobConfiguration",
            "appName": name,
            # Root routing, as the UI creates apps: the proxy strips the browser mount,
            # so the app serves at its base path. Without it the backend falls back to
            # the compatibility prefix and forwards /hopsworks-api/pythonapp/... as is.
            "proxyPathMode": "ROOT",
            "resourceConfig": {
                "memory": memory,
                "cores": cores,
                "gpus": 0,
                "shmSize": 128,
            },
        }
        if app_path and not git_repo_app:
            config["appPath"] = app_path
        config["appKind"] = app_kind_name
        config["environmentName"] = environment
        if git_repo_app:
            config["gitUrl"] = git_url
            config["gitProvider"] = git_provider
            config["gitAutoRedeploy"] = bool(git_auto_redeploy)
            if git_branch:
                config["gitBranch"] = git_branch
            if file_launched_app:
                config["entrypointScript"] = entrypoint_script
        if not file_launched_app and entrypoint_command:
            config["entrypointCommand"] = entrypoint_command
        if app_kind_name != "STREAMLIT" and app_port is not None:
            config["appPort"] = app_port
        if description is not None:
            config["description"] = description
        if app_base_path is not None:
            config["appBasePath"] = app_base_path
        if readiness_probe_path is not None:
            config["readinessProbePath"] = readiness_probe_path
        config["dbAccess"] = bool(db_access)
        # Unset is left out: the backend turns the option on wherever KEDA is installed.
        if idle_scale_to_zero is not None:
            config["idleScaleToZero"] = bool(idle_scale_to_zero)
        if idle_scale_to_zero:
            if idle_cooldown_seconds is not None:
                config["idleCooldownSeconds"] = int(idle_cooldown_seconds)
            if cold_start_timeout_seconds is not None:
                config["coldStartTimeoutSeconds"] = int(cold_start_timeout_seconds)

        path_params = ["project", _client._project_id, "jobs", name]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps(config)
        )

        created = self.get_app(name)
        # env_vars is a runtime-only override applied at start time; the backend
        # has no app-config field for it, so attach it to the returned object.
        created._env_vars = dict(env_vars) if env_vars else None
        return created

    def _start(self, app_name: str, env_vars: dict[str, str] | None = None):
        """Start an app execution.

        When ``env_vars`` is provided, POSTs a JSON body with ``envVars`` so the
        backend applies the runtime override; otherwise falls back to the legacy
        text/plain POST that Jersey dispatches to the no-body start handler.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
        ]
        if env_vars:
            headers = {"content-type": "application/json"}
            body = {"envVars": dict(env_vars)}
            return _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(body)
            )
        headers = {"content-type": "text/plain"}
        return _client._send_request("POST", path_params, headers=headers)

    def _stop(self, app_name: str, execution_id: int):
        """Stop an app execution."""
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
            execution_id,
            "status",
        ]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps({"state": "stopped"})
        )

    def _redeploy(self, app_name: str):
        """Redeploy a running app."""
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "apps",
            app_name,
            "redeploy",
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request("POST", path_params, headers=headers)

    def _set_public(self, app_name: str, enabled: bool):
        """Enable or disable public (no-login) access for an app.

        On enable the response carries the share token (data-owner only); the
        caller builds the share URL from it.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "apps",
            app_name,
            "public",
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "POST", path_params, headers=headers, data=json.dumps({"enabled": enabled})
        )

    def _tables_path(self, app_name: str, *rest: str) -> list:
        _client = client._get_instance()
        return ["project", _client._project_id, "apps", app_name, "tables", *rest]

    def _get_tables(self, app_name: str) -> list[dict]:
        """List the tables the app registered."""
        return client._get_instance()._send_request("GET", self._tables_path(app_name))

    def _register_table(
        self, app_name: str, table_name: str, definition: str | None = None
    ) -> dict:
        """Register a table of the app, and create it on disk from `definition` when given; idempotent."""
        headers = {"content-type": "application/json"}
        body = {"name": table_name}
        if definition is not None:
            body["definition"] = definition
        return client._get_instance()._send_request(
            "POST",
            self._tables_path(app_name),
            headers=headers,
            data=json.dumps(body),
        )

    def _alter_table(self, app_name: str, table_name: str, alteration: str) -> dict:
        """Change a table of the app with `alteration`, what follows ALTER TABLE."""
        headers = {"content-type": "application/json"}
        return client._get_instance()._send_request(
            "POST",
            self._tables_path(app_name, table_name, "alter"),
            headers=headers,
            data=json.dumps({"alteration": alteration}),
        )

    def _get_table(self, app_name: str, table_name: str) -> dict:
        """Get one registered table with its columns."""
        return client._get_instance()._send_request(
            "GET", self._tables_path(app_name, table_name)
        )

    def _preview_table(self, app_name: str, table_name: str, limit: int) -> dict:
        """Get up to `limit` rows of a registered table."""
        return client._get_instance()._send_request(
            "GET",
            self._tables_path(app_name, table_name, "preview"),
            query_params={"limit": limit},
        )

    def _get_log(self, app_name: str, execution_id: int, log_type: str) -> dict:
        """Get stdout or stderr log metadata for an app execution."""
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
            execution_id,
            "log",
            log_type,
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers) or {}

    def _delete(self, app_name: str):
        """Delete an app."""
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
        ]
        _client._send_request("DELETE", path_params)

    def _trim_to_none(self, value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        trimmed = value.strip()
        return trimmed or None

    def _normalize_git_provider(self, git_provider: str | None) -> str | None:
        if hasattr(git_provider, "git_provider"):
            git_provider = git_provider.git_provider
        provider = self._trim_to_none(git_provider)
        if not provider:
            return None
        normalized = _GIT_PROVIDER_ALIASES.get(provider.lower())
        return normalized or provider
