#
#   Copyright 2022 Logical Clocks AB
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

import os
import posixpath
import re
import tempfile
import zipfile
from typing import TYPE_CHECKING

from hopsworks_apigen import public
from hopsworks_common import usage, util
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.constants import INFERENCE_ENDPOINTS as IE
from hopsworks_common.constants import PREDICTOR_STATE
from hopsworks_common.core import dataset_api as _dataset_api
from hopsworks_common.core import environment_api as _environment_api
from hsml.core import serving_api
from hsml.deployment import Deployment
from hsml.predictor import Predictor
from hsml.transformer import Transformer


try:
    import tomllib
except ImportError:
    import tomli as tomllib


_AGENT_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]+$")


if TYPE_CHECKING:
    from hsml.inference_batcher import InferenceBatcher
    from hsml.inference_endpoint import InferenceEndpoint
    from hsml.inference_logger import InferenceLogger
    from hsml.model import Model
    from hsml.resources import PredictorResources
    from hsml.scaling_config import PredictorScalingConfig, TransformerScalingConfig


@public
class ModelServing:
    DEFAULT_VERSION = 1

    def __init__(
        self,
        project_name: str,
        project_id: int,
        **kwargs,
    ):
        self._project_name = project_name
        self._project_id = project_id

        self._serving_api = serving_api.ServingApi()

    @public
    @usage.method_logger
    def get_deployment_by_id(self, id: int) -> Deployment | None:
        """Get a deployment by id from Model Serving.

        Getting a deployment from Model Serving means getting its metadata handle so you can subsequently operate on it (e.g., start or stop).

        Example:
            ```python
            # login and get Hopsworks Model Serving handle using .login() and .get_model_serving()

            # get a deployment by id
            my_deployment = ms.get_deployment_by_id(1)
            ```

        Parameters:
            id: Id of the deployment to get.

        Returns:
            The deployment metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If unable to retrieve deployment from model serving.
        """
        return self._serving_api.get_by_id(id)

    @public
    @usage.method_logger
    def get_deployment(self, name: str = None) -> Deployment | None:
        """Get a deployment by name from Model Serving.

        Example:
            ```python
            # login and get Hopsworks Model Serving handle using .login() and .get_model_serving()

            # get a deployment by name
            my_deployment = ms.get_deployment('deployment_name')
            ```

        Getting a deployment from Model Serving means getting its metadata handle
        so you can subsequently operate on it (e.g., start or stop).

        Parameters:
            name: Name of the deployment to get.

        Returns:
            The deployment metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If unable to retrieve deployment from model serving.
        """
        if name is None and ("DEPLOYMENT_NAME" in os.environ):
            name = os.environ["DEPLOYMENT_NAME"]
        return self._serving_api.get(name)

    @public
    @usage.method_logger
    def get_deployments(
        self, model: Model = None, status: str = None
    ) -> list[Deployment]:
        """Get all deployments from model serving.

        Example:
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            list_deployments = ms.get_deployment(my_model)

            for deployment in list_deployments:
                print(deployment.get_state())
            ```

        Parameters:
            model: Filter by model served in the deployments
            status: Filter by status of the deployments

        Returns:
            A list of deployments.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If unable to retrieve deployments from model serving.
        """
        model_name = model._get_default_serving_name() if model is not None else None
        if status is not None:
            self._validate_deployment_status(status)

        return self._serving_api.get_all(model_name, status)

    def _validate_deployment_status(self, status):
        statuses = list(util.get_members(PREDICTOR_STATE, prefix="STATUS"))
        status = status.upper()
        if status not in statuses:
            raise ValueError(
                "Deployment status '{}' is not valid. Possible values are '{}'".format(
                    status, ", ".join(statuses)
                )
            )
        return status

    @public
    def get_inference_endpoints(self) -> list[InferenceEndpoint]:
        """Get all inference endpoints available in the current project.

        Returns:
            Inference endpoints for model inference
        """
        return self._serving_api.get_inference_endpoints()

    @public
    @usage.method_logger
    def create_predictor(
        self,
        model: Model,
        name: str | None = None,
        artifact_version: str
        | None = None,  # deprecated, kept for backward compatibility
        serving_tool: str | None = None,
        script_file: str | None = None,
        config_file: str | None = None,
        resources: PredictorResources | dict | None = None,
        inference_logger: InferenceLogger | dict | str | None = None,
        inference_batcher: InferenceBatcher | dict | None = None,
        transformer: Transformer | dict | None = None,
        api_protocol: str | None = IE.API_PROTOCOL_REST,
        environment: str | None = None,
        scaling_configuration: PredictorScalingConfig | dict | None = None,
        env_vars: dict | None = None,
        vllm_variant: str | None = None,
        vllm_image_tag: str | None = None,
    ) -> Predictor:
        """Create a Predictor metadata object.

        Example:
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            my_predictor = ms.create_predictor(my_model)

            my_deployment = my_predictor.deploy()
            ```

        Note: Lazy
            This method is lazy and does not persist any metadata or deploy any model on its own.
            To create a deployment using this predictor, call the `deploy()` method.

        Parameters:
            model: Model to be deployed.
            name: Name of the predictor.
            artifact_version: (**Deprecated**) Version number of the model artifact to deploy, `CREATE` to create a new model artifact or `MODEL-ONLY` to reuse the shared artifact containing only the model files.
            serving_tool: Serving tool used to deploy the model server.
            script_file: Path to a custom predictor script implementing the Predict class.
            config_file: Model server configuration file to be passed to the model deployment.
                It can be accessed via `CONFIG_FILE_PATH` environment variable from a predictor script.
                For LLM deployments without a predictor script, this file is used to configure the vLLM engine.
            resources: Resources to be allocated for the predictor.
            inference_logger: Inference logger configuration.
            inference_batcher: Inference batcher configuration.
            transformer: Transformer to be deployed together with the predictor.
            api_protocol: API protocol to be enabled in the deployment (i.e., 'REST' or 'GRPC').
            environment: The project Python environment to use
            scaling_configuration: Scaling configuration for the predictor.
            env_vars: Environment variables to set on the predictor.
            vllm_variant: vLLM image variant for vLLM deployments. One of `'VLLM'` or `'VLLM_OMNI'`. Ignored for non-vLLM model servers.
            vllm_image_tag: vLLM image tag override. `None` uses the cluster default; if set, it should match one of the tags made available by a cluster administrator. Ignored for non-vLLM model servers.

        Returns:
            The predictor metadata object.
        """
        if name is None:
            name = model._get_default_serving_name()

        return Predictor.for_model(
            model,
            name=name,
            serving_tool=serving_tool,
            script_file=script_file,
            config_file=config_file,
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            transformer=transformer,
            api_protocol=api_protocol,
            environment=environment,
            scaling_configuration=scaling_configuration,
            env_vars=env_vars,
            vllm_variant=vllm_variant,
            vllm_image_tag=vllm_image_tag,
        )

    @public
    @usage.method_logger
    def create_transformer(
        self,
        script_file: str | None = None,
        resources: PredictorResources | dict | None = None,
        scaling_configuration: TransformerScalingConfig | dict | None = None,
        env_vars: dict | None = None,
    ) -> Transformer:
        """Create a Transformer metadata object.

        Example:
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Dataset API instance
            dataset_api = project.get_dataset_api()

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            # create my_transformer.py Python script
            class Transformer(object):
                def __init__(self):
                    ''' Initialization code goes here '''
                    pass

                def preprocess(self, inputs):
                    ''' Transform the requests inputs here. The object returned by this method will be used as model input to make predictions. '''
                    return inputs

                def postprocess(self, outputs):
                    ''' Transform the predictions computed by the model before returning a response '''
                    return outputs

            uploaded_file_path = dataset_api.upload("my_transformer.py", "Resources", overwrite=True)
            transformer_script_path = os.path.join("/Projects", project.name, uploaded_file_path)

            my_transformer = ms.create_transformer(script_file=uploaded_file_path)

            # or

            from hsml.transformer import Transformer

            my_transformer = Transformer(script_file)
            ```

        Example: Create a deployment with the transformer
            ```python

            my_predictor = ms.create_predictor(transformer=my_transformer)
            my_deployment = my_predictor.deploy()

            # or
            my_deployment = ms.create_deployment(my_predictor, transformer=my_transformer)
            my_deployment.save()
            ```

        Note: Lazy
            This method is lazy and does not persist any metadata or deploy any transformer. To create a deployment using this transformer, set it in the `predictor.transformer` property.

        Parameters:
            script_file: Path to a custom predictor script implementing the Transformer class.
            resources: Resources to be allocated for the transformer.
            scaling_configuration: Scaling configuration for the transformer.
            env_vars: Environment variables to set on the transformer.

        Returns:
            The transformer metadata object.
        """
        return Transformer(
            script_file=script_file,
            resources=resources,
            scaling_configuration=scaling_configuration,
            env_vars=env_vars,
        )

    @public
    @usage.method_logger
    def create_endpoint(
        self,
        name: str,
        script_file: str,
        description: str | None = None,
        resources: PredictorResources | dict | None = None,
        inference_logger: InferenceLogger | dict | str | None = None,
        inference_batcher: InferenceBatcher | dict | None = None,
        api_protocol: str | None = IE.API_PROTOCOL_REST,
        environment: str | None = None,
        scaling_configuration: PredictorScalingConfig | dict | None = None,
        env_vars: dict | None = None,
    ) -> Predictor:
        """Create an Entrypoint metadata object.

        Example:
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            ms = project.get_model_serving()

            my_endpoint = ms.create_entrypoint(name="feature_server", entrypoint_file="feature_server.py")

            my_deployment = my_endpoint.deploy()
            ```

        Note: Lazy
            This method is lazy and does not persist any metadata or deploy any endpoint on its own.
            To create a deployment using this endpoint, call the `deploy()` method.

        Parameters:
            name: Name of the endpoint.
            script_file: Path to a custom script file implementing a HTTP server.
            description: Description of the endpoint.
            resources: Resources to be allocated for the predictor.
            inference_logger: Inference logger configuration.
            inference_batcher: Inference batcher configuration.
            api_protocol: API protocol to be enabled in the deployment (i.e., 'REST' or 'GRPC').
            environment: The project Python environment to use
            scaling_configuration: Scaling configuration for the predictor.
            env_vars: Environment variables to set on the predictor.

        Returns:
            The predictor metadata object.
        """
        return Predictor.for_server(
            name=name,
            script_file=script_file,
            description=description,
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            api_protocol=api_protocol,
            environment=environment,
            scaling_configuration=scaling_configuration,
            env_vars=env_vars,
        )

    @public
    @usage.method_logger
    def deploy_agent(
        self,
        entry: str,
        name: str | None = None,
        requirements: str | None = None,
        environment: str | None = None,
        upload_dir: str = "Resources/agents",
        description: str | None = None,
        resources: PredictorResources | dict | None = None,
        inference_logger: InferenceLogger | dict | str | None = None,
        inference_batcher: InferenceBatcher | dict | None = None,
        api_protocol: str | None = IE.API_PROTOCOL_REST,
        scaling_configuration: PredictorScalingConfig | dict | None = None,
    ) -> Deployment:
        """Deploy a Python script or package as an agent.

        The agent is created on first call and updated on subsequent calls.
        Each call uploads the latest local code, refreshes the Python environment, and rewrites the deployment's predictor metadata to reflect the arguments passed in — including any unspecified arguments, which fall back to their defaults.
        The deployment's running state is left untouched; call `start()` after the first deploy and `restart()` to roll a running agent onto the new code.
        Works the same whether invoked from outside or inside a Hopsworks cluster.

        Pass either a `.py` script or a directory containing a `pyproject.toml`.
        For a script, the file is uploaded and run directly.
        For a package, a wheel is built locally with the project's PEP 517 backend, uploaded, and installed; a small runner module invokes the package via `runpy.run_module`.

        ```python
        ms = project.get_model_serving()

        agent = ms.deploy_agent(entry="my_agent.py")
        agent.start() # or agent.restart()

        # iterate: edit code locally, push, then roll the running agent onto it
        agent = ms.deploy_agent(entry="my_agent.py")
        agent.restart()
        ```

        Parameters:
            entry: Local path to a `.py` script or to a directory containing `pyproject.toml`.
            name:
                Name of the deployment, also used as the default Python environment name.
                Defaults to the basename of `entry` (without the `.py` extension for scripts).
                Must match `[A-Za-z0-9_-]+`.
            requirements: Local path to a `requirements.txt` to install into the environment.
            environment:
                Name of the Python environment to use; defaults to `name`.
                Created if it does not exist.
                Must match `[A-Za-z0-9_-]+`.
            upload_dir: Directory in the Hopsworks Filesystem under which agent files are placed; the agent gets its own subdirectory `<upload_dir>/<name>`.
            description: Description of the deployment.
            resources: Resources to be allocated for the predictor.
            inference_logger: Inference logger configuration.
            inference_batcher: Inference batcher configuration.
            api_protocol: API protocol to be enabled in the deployment (i.e., 'REST' or 'GRPC').
            scaling_configuration: Scaling configuration for the predictor.

        Returns:
            The deployment metadata object.

        Raises:
            ValueError: If `entry` is neither a `.py` file nor a directory with `pyproject.toml`, or if `name`/`environment` contain characters outside `[A-Za-z0-9_-]`.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        entry_abs = os.path.abspath(entry)
        is_script = os.path.isfile(entry_abs) and entry_abs.endswith(".py")
        is_package = os.path.isdir(entry_abs) and os.path.isfile(
            os.path.join(entry_abs, "pyproject.toml")
        )
        if not (is_script or is_package):
            raise ValueError(
                f"entry must be a .py file or a directory containing pyproject.toml: {entry}"
            )

        if name is None:
            name = os.path.basename(entry_abs)
            if is_script:
                name = os.path.splitext(name)[0]
        _validate_agent_identifier(name, "name")

        env_name = environment or name
        if environment is not None:
            _validate_agent_identifier(environment, "environment")

        agent_dir = f"{_normalize_upload_dir(upload_dir)}/{name}"

        ds_api = _dataset_api.DatasetApi()
        env_api = _environment_api.EnvironmentApi()

        _ensure_dataset_dir(ds_api, agent_dir)

        env = env_api.get_environment(env_name) or env_api.create_environment(
            env_name, base_environment_name="python-agent-pipeline"
        )

        if is_script:
            script_file = ds_api.upload(entry_abs, agent_dir, overwrite=True)
        else:
            script_file = _build_and_install_package(ds_api, env, entry_abs, agent_dir)
        # The serving backend expects the script path under /Projects/<proj>/...
        script_file = util.convert_to_abs(script_file, self._project_name)

        if requirements is not None:
            requirements_abs = os.path.abspath(requirements)
            if not os.path.isfile(requirements_abs):
                raise ValueError(
                    "requirements must be a path to an existing file: "
                    + requirements_abs
                )
            req_remote = ds_api.upload(requirements_abs, agent_dir, overwrite=True)
            env.install_requirements(req_remote)

        predictor = Predictor.for_server(
            name=name,
            script_file=script_file,
            description=description,
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            api_protocol=api_protocol,
            environment=env_name,
            scaling_configuration=scaling_configuration,
        )

        existing = self.get_deployment(name)
        if existing is not None:
            # Preserve identity and server-assigned version so save() updates the
            # existing record instead of creating a new one.
            predictor._id = existing.predictor.id
            predictor._version = existing.predictor.version
            existing.predictor = predictor
            existing.description = description
            existing.save()
            return existing

        return predictor.deploy()

    @public
    @usage.method_logger
    def create_deployment(
        self,
        predictor: Predictor,
        name: str | None = None,
        environment: str | None = None,
    ) -> Deployment:
        """Create a Deployment metadata object.

        Example:
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            my_predictor = ms.create_predictor(my_model)

            my_deployment = ms.create_deployment(my_predictor)
            my_deployment.save()
            ```

        Example: Using the model object
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            my_deployment = my_model.deploy()

            my_deployment.get_state().describe()
            ```

        Example: Using the Model Serving handle
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            my_predictor = ms.create_predictor(my_model)

            my_deployment = my_predictor.deploy()

            my_deployment.get_state().describe()
            ```

        Note: Lazy
            This method is lazy and does not persist any metadata or deploy any model. To create a deployment, call the `save()` method.

        Parameters:
            predictor: predictor to be used in the deployment
            name: name of the deployment
            environment: (**Deprecated**) The project Python environment to use. This argument will be ignored, use the argument `environment` in the `create_predictor()` or `create_endpoint()` methods instead.

        Returns:
            The deployment metadata object.
        """
        return Deployment(predictor=predictor, name=name)

    @public
    @property
    def project_name(self):
        """Name of the project in which Model Serving is located."""
        return self._project_name

    @public
    @property
    def project_path(self):
        """Path of the project the registry is connected to."""
        return f"/Projects/{self._project_name}"

    @public
    @property
    def project_id(self):
        """Id of the project in which Model Serving is located."""
        return self._project_id

    def __repr__(self):
        return f"ModelServing(project: {self._project_name!r})"


def _validate_agent_identifier(value: str, field: str) -> None:
    """Reject identifiers that would be unsafe to interpolate into a dataset path.

    `name` and `environment` flow into both the upload subdirectory and predictor metadata,
    so disallowing path separators and traversal segments protects against accidental writes
    outside `<upload_dir>/<name>`.
    """
    if not _AGENT_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{field} must match {_AGENT_IDENTIFIER_RE.pattern!r}, got {value!r}"
        )


def _normalize_upload_dir(value: str) -> str:
    """Normalize `upload_dir` and ensure it stays within the project's dataset root.

    Rejects empty input, absolute paths, and any traversal that resolves to the parent of
    the project root (e.g. `Resources/../..`).
    Returns the normalized form for use in path interpolation.
    """
    if not value:
        raise ValueError("upload_dir must not be empty")
    normalized = posixpath.normpath(value)
    if normalized == ".." or normalized.startswith(("/", "../")):
        raise ValueError(
            f"upload_dir must be a relative path that stays within the project, got {value!r}"
        )
    return normalized


def _ensure_dataset_dir(ds_api, path: str) -> None:
    """Create `path` in the Hopsworks Filesystem if missing, creating parents as needed."""
    if ds_api.exists(path):
        return
    parent, _, _ = path.rpartition("/")
    if parent:
        _ensure_dataset_dir(ds_api, parent)
    ds_api.mkdir(path)


def _build_and_install_package(ds_api, env, package_dir: str, agent_dir: str) -> str:
    """Build a wheel from `package_dir`, upload it, install it into `env`, and upload a runner script.

    Returns the remote path of the runner script to use as `script_file`.
    """
    from build import ProjectBuilder
    from build.env import DefaultIsolatedEnv

    pkg_name = _read_package_name(package_dir)

    with tempfile.TemporaryDirectory() as build_dir:
        # Use an isolated env so the project's PEP 517 backend (hatchling, setuptools, …)
        # is installed on the fly rather than required up-front in the caller's env.
        with DefaultIsolatedEnv() as build_env:
            builder = ProjectBuilder.from_isolated_env(build_env, package_dir)
            build_env.install(builder.build_system_requires)
            build_env.install(builder.get_requires_for_build("wheel"))
            # `build` returns the wheel filename relative to build_dir on some versions and
            # an absolute path on others; os.path.join leaves an absolute result untouched.
            wheel_local = os.path.join(build_dir, builder.build("wheel", build_dir))

        # The distribution name (`pkg_name`) is what pip uses to install/uninstall, but it
        # may not be a valid Python identifier (e.g. "my-agent"). The runner needs the
        # importable module name, which we read from the built wheel's contents.
        module_name = _find_runnable_module(wheel_local)

        wheel_remote = ds_api.upload(wheel_local, agent_dir, overwrite=True)

        # Force a reinstall: pip skips a same-version wheel, so we uninstall first.
        # On first deploy the package is not installed yet — that 404 is expected.
        try:
            env.uninstall(pkg_name)
        except RestAPIError as e:
            if e.response.status_code != 404:
                raise

        env.install_wheel(wheel_remote)

        runner_local = os.path.join(build_dir, "runner.py")
        with open(runner_local, "w") as f:
            f.write(
                f"import runpy\nrunpy.run_module({module_name!r}, run_name='__main__')\n"
            )
        return ds_api.upload(runner_local, agent_dir, overwrite=True)


def _find_runnable_module(wheel_path: str) -> str:
    """Return the top-level package in the wheel that exposes a `__main__.py`.

    Used to generate the runner's `runpy.run_module(<name>, ...)` call.
    The wheel's contents are authoritative (they reflect what's actually installable and
    importable), so this works even when the distribution name differs from the importable
    name (e.g. `my-agent` distribution containing a `my_agent` package).
    """
    with zipfile.ZipFile(wheel_path) as wheel:
        candidates = sorted(
            {
                parts[0]
                for name in wheel.namelist()
                for parts in [name.split("/")]
                if len(parts) == 2 and parts[1] == "__main__.py"
            }
        )
    if len(candidates) == 1:
        return candidates[0]
    wheel_basename = os.path.basename(wheel_path)
    if not candidates:
        raise ValueError(
            f"{wheel_basename} has no top-level package with `__main__.py`; "
            "agents deployed as packages must be runnable via `python -m`."
        )
    raise ValueError(
        f"{wheel_basename} has multiple top-level packages with `__main__.py` "
        f"({', '.join(candidates)}); cannot determine the agent entry point."
    )


def _read_package_name(package_dir: str) -> str:
    """Read `[project].name` from the package's `pyproject.toml`."""
    with open(os.path.join(package_dir, "pyproject.toml"), "rb") as f:
        pyproject = tomllib.load(f)
    project = pyproject.get("project") or {}
    pkg_name = project.get("name")
    if not isinstance(pkg_name, str):
        raise ValueError(
            f"Cannot read [project].name as a static string from {package_dir}/pyproject.toml"
        )
    return pkg_name
