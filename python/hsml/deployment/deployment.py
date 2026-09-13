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
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common import client, usage, util
from hopsworks_common.client.exceptions import ModelServingException
from hsml.constants import DEPLOYABLE_COMPONENT, MODEL_SERVING, PREDICTOR_STATE
from hsml.core import model_api, serving_api
from hsml.deployment import predictor as predictor_mod
from hsml.engine import serving_engine


if TYPE_CHECKING:
    from collections.abc import Iterator

    from hsfs.core.feature_monitoring_config import FeatureMonitoringConfig
    from hsml.client.istio.utils.infer_type import InferInput
    from hsml.deployment.inference_batcher import InferenceBatcher
    from hsml.deployment.inference_logger import InferenceLogger
    from hsml.deployment.logging_config import DeploymentLoggingConfig
    from hsml.deployment.predictor_state import PredictorState
    from hsml.deployment.resources import Resources
    from hsml.deployment.scaling_config import PredictorScalingConfig
    from hsml.deployment.schema import DeploymentSchema
    from hsml.deployment.tracing_config import DeploymentTracingConfig
    from hsml.deployment.transformer import Transformer


@public
class Deployment:
    NOT_FOUND_ERROR_CODE = 240000
    """Metadata object representing a deployment in Model Serving."""

    def __init__(
        self,
        predictor,
        name: str | None = None,
        description: str | None = None,
        project_namespace: str = None,
        **kwargs,
    ):
        self._predictor = predictor
        self._description = description
        self._project_namespace = project_namespace

        if self._predictor is None:
            raise ModelServingException("A predictor is required")
        if not isinstance(self._predictor, predictor_mod.Predictor):
            raise ValueError(
                "The predictor provided is not an instance of the Predictor class"
            )

        if name is not None:
            self._predictor.name = name

        if self._description is None:
            self._description = self._predictor.description
        else:
            self._description = self._predictor.description = description

        self._serving_api = serving_api.ServingApi()
        self._serving_engine = serving_engine.ServingEngine()
        self._model_api = model_api.ModelApi()
        self._grpc_channel = None
        self._model_registry_id = None

    @public
    @usage._method_logger
    def save(self, await_update: int | None = 600):
        """Persist this deployment including the predictor and metadata to Model Serving.

        Parameters:
            await_update: If the deployment is running, awaiting time (seconds) for the running instances to be updated.
                          If the running instances are not updated within this timespan, the call to this method returns while
                          the update in the background.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        self._serving_engine._save(self, await_update)

    @public
    @usage._method_logger
    def start(self, await_running: int | None = 600):
        """Start the deployment.

        Parameters:
            await_running: Awaiting time (seconds) for the deployment to start.
                           If the deployment has not started within this timespan, the call to this method returns while
                           it deploys in the background.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        self._serving_engine._start(self, await_status=await_running)

    @public
    @usage._method_logger
    def stop(self, await_stopped: int | None = 600):
        """Stop the deployment.

        Parameters:
            await_stopped: Awaiting time (seconds) for the deployment to stop.
                           If the deployment has not stopped within this timespan, the call to this method returns while
                           it stopping in the background.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        self._serving_engine._stop(self, await_status=await_stopped)

    @public
    @usage._method_logger
    def restart(
        self,
        await_stopped: int | None = 600,
        await_running: int | None = 600,
    ) -> None:
        """Restart the deployment so it picks up the latest code and environment state.

        If the deployment is already stopped, it is started in place.

        Parameters:
            await_stopped: Awaiting time (seconds) for the deployment to stop.
            await_running: Awaiting time (seconds) for the deployment to start again.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        if not self.is_stopped():
            self.stop(await_stopped=await_stopped)
        self.start(await_running=await_running)

    @public
    @usage._method_logger
    def delete(self, force: bool = False):
        """Delete the deployment.

        Parameters:
            force:
                Force the deletion of the deployment.
                If the deployment is running, it will be stopped and deleted automatically.

        Warning:
            A call to this method does not ask for a second confirmation.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        self._serving_engine._delete(self, force)

    @public
    @usage._method_logger
    def add_tag(self, name: str, value: Any):
        """Attach a tag to a deployment.

        A tag consists of a <name,value> pair.
        Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        Parameters:
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to add the tag.
        """
        self._serving_engine._set_tag(self, name, value)

    @public
    @usage._method_logger
    def delete_tag(self, name: str):
        """Delete a tag attached to a deployment.

        Parameters:
            name: Name of the tag to be removed.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to delete the tag.
        """
        self._serving_engine._delete_tag(self, name)

    @public
    def get_tag(self, name: str) -> Any | None:
        """Get the value of a tag attached to a deployment.

        Parameters:
            name: Name of the tag to get.

        Returns:
            tag value, or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to retrieve the tag.
        """
        return self._serving_engine._get_tag(self, name)

    @public
    def get_tags(self) -> dict[str, Any]:
        """Retrieve all tags attached to a deployment.

        Returns:
            Dictionary of tag name/values.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to retrieve the tags.
        """
        return self._serving_engine._get_tags(self)

    @public
    @property
    def missing_mandatory_tags(self) -> list[dict[str, Any]]:
        """Mandatory tags configured for deployments that this deployment is missing.

        Populated from the backend response.
        Empty when all mandatory deployment tags are set.
        """
        return self._predictor._missing_mandatory_tags

    @public
    def get_state(self) -> PredictorState:
        """Get the current state of the deployment.

        Returns:
            The state of the deployment.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        return self._serving_engine._get_state(self)

    @public
    def is_created(self) -> bool:
        """Check whether the deployment is created.

        Returns:
            Whether the deployment is created or not.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        return (
            self._serving_engine._get_state(self).status
            != PREDICTOR_STATE.STATUS_CREATING
        )

    @public
    def is_running(self, or_idle: bool = True, or_updating: bool = True) -> bool:
        """Check whether the deployment is ready to handle inference requests.

        Parameters:
            or_idle: Whether the idle state is considered as running (default is True).
            or_updating: Whether the updating state is considered as running (default is True).

        Returns:
            Whether the deployment is ready or not.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        status = self._serving_engine._get_state(self).status
        return (
            status == PREDICTOR_STATE.STATUS_RUNNING
            or (or_idle and status == PREDICTOR_STATE.STATUS_IDLE)
            or (or_updating and status == PREDICTOR_STATE.STATUS_UPDATING)
        )

    @public
    def is_stopped(self, or_created: bool = True) -> bool:
        """Check whether the deployment is stopped.

        Parameters:
            or_created: Whether the creating and created state is considered as stopped (default is True).

        Returns:
            Whether the deployment is stopped or not.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        status = self._serving_engine._get_state(self).status
        return status == PREDICTOR_STATE.STATUS_STOPPED or (
            or_created
            and (
                status == PREDICTOR_STATE.STATUS_CREATING
                or status == PREDICTOR_STATE.STATUS_CREATED
            )
        )

    @public
    def predict(
        self,
        data: dict | InferInput = None,
        inputs: list | dict = None,
        validate: bool = True,
    ) -> dict:
        """Send inference requests to the deployment.

        One of data or inputs parameters must be set.
        Setting both raises `ModelServingException`.
        When the deployment has a schema and the protocol is REST, the rows are
        encoded and validated against it before the request is sent; the pod
        validates again regardless.

        Parameters:
            data: Payload dictionary for the inference request including the model input(s).
            inputs: Model inputs used in the inference requests.
            validate: Whether to validate the rows against `schema` before sending.

        Returns:
            Inference response.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.

        Examples:
            ```python
            # login into Hopsworks using hopsworks.login()

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            # retrieve deployment by name
            my_deployment = ms.get_deployment("my_deployment")

            # (optional) retrieve model input example
            my_model = project.get_model_registry()  \
                              .get_model(my_deployment.model_name, my_deployment.model_version)

            # make predictions using model inputs (single or batch)
            predictions = my_deployment.predict(inputs=my_model.input_example)

            # or using more sophisticated inference request payloads
            data = { "instances": [ my_model.input_example ], "key2": "value2" }
            predictions = my_deployment.predict(data)
            ```
        """
        return self._serving_engine._predict(self, data, inputs, validate=validate)

    @public
    def get_model(self):
        """Retrieve the metadata object for the model being used by this deployment, or `None` when it has no model."""
        if not self.has_model:
            return None
        return self._model_api._get(
            self.model_name, self.model_version, self.model_registry_id
        )

    @public
    def get_feature_view(self, init: bool = False) -> Any:
        """Retrieve the feature view this deployment serves, or `None`.

        A feature view deployment names its view in the deployment env vars; a
        model deployment resolves it through the model's provenance.

        Parameters:
            init: Whether to initialise the view for serving with the deployment's training dataset version.

        Returns:
            The feature view, or `None` when the deployment has neither a feature view nor a model with one.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If no project connection is available to reach the feature store.
        """
        feature_view = None
        if self.has_feature_view:
            # Same route as hsml.core.explicit_provenance: reach the feature
            # store through the hopsworks project so hsml never imports hsfs.
            import hopsworks

            if not hopsworks._connected_project:
                from hopsworks_common.client.exceptions import FeatureStoreException

                raise FeatureStoreException(
                    "Resolving the feature view of a deployment needs a project "
                    "connection; use hopsworks.login() first."
                )
            feature_store = hopsworks._connected_project.get_feature_store()
            feature_view = feature_store.get_feature_view(
                self.feature_view_name, self.feature_view_version
            )
        elif self.has_model:
            model = self.get_model()
            feature_view = model.get_feature_view(init=False) if model else None
        if feature_view is not None and init:
            feature_view.init_serving(
                training_dataset_version=self.training_dataset_version
            )
        return feature_view

    @public
    def commit_feature_logs(self, wait: bool = False) -> list[Any]:
        """Run the commit job of the feature view this deployment logs through.

        For a view on the `"job"` transport this commits every chunk that reached HopsFS, including what a stopped or killed replica left in the staging directory; for a `"realtime"` view it runs the materialization job.

        Parameters:
            wait: Whether to wait for the job to finish.

        Returns:
            The jobs that were started.

        Raises:
            hopsworks.client.exceptions.ModelServingException: If the deployment serves no feature view with logging enabled.
        """
        feature_view = self.get_feature_view(init=False)
        if feature_view is None or not getattr(feature_view, "logging_enabled", False):
            raise ModelServingException(
                f"Deployment '{self.name}' serves no feature view with logging enabled."
            )
        return feature_view.materialize_log(wait=wait)

    @public
    def reinfer_schema(self) -> DeploymentSchema:
        """Re-infer the deployment schema from the current feature view and mark it pending.

        Use after enabling logging or changing the view; `save()` then
        publishes the new schema as a new revision. Passed features are kept.

        Example:
            ```python
            feature_view.enable_logging(extra_log_columns={"channel": "string"})

            deployment = ms.get_deployment("fraud")
            deployment.reinfer_schema()  # `channel` becomes an optional request field
            deployment.save()  # publishes the schema as a new revision
            ```

        Returns:
            The re-inferred schema, also set on the deployment.

        Raises:
            hopsworks.client.exceptions.ModelServingException: If the deployment is not served by the default predictor and has no schema to refine.
        """
        from hsml.deployment.schema import (
            OUTPUT_FEATURE_VECTORS,
            OUTPUT_PREDICTIONS,
            _infer_deployment_schema,
        )

        current = self.schema
        if not self._predictor.default_predictor and current is None:
            raise ModelServingException(
                f"Deployment '{self.name}' has no schema to re-infer: it is not served "
                "by the default predictor and none was set. Set deployment.schema "
                "explicitly instead."
            )
        feature_view = self.get_feature_view(init=False)
        if feature_view is None:
            raise ModelServingException(
                f"Deployment '{self.name}' has no feature view to infer a schema from."
            )
        passed = [f.name for f in current.passed_features] if current else None
        output_kind = (
            OUTPUT_FEATURE_VECTORS if not self.has_model else OUTPUT_PREDICTIONS
        )
        output_columns = None
        if current and output_kind == OUTPUT_PREDICTIONS:
            output_columns = current.output.get("columns")
        self.schema = _infer_deployment_schema(
            feature_view,
            passed_features=passed,
            training_dataset_version=self.training_dataset_version,
            output_kind=output_kind,
            output_columns=output_columns,
        )
        return self.schema

    @public
    def create_feature_monitoring(
        self,
        name: str,
        description: str | None = None,
        start_date_time: int | str | None = None,
        end_date_time: int | str | None = None,
        cron_expression: str | None = "0 0 12 ? * * *",
    ) -> Any:
        """Create a feature monitoring config on the logging feature group of a feature view deployment.

        Model deployments use `create_model_monitoring()` instead. Finish the
        returned builder with a detection window, a reference window or value,
        a comparison, and `save()`.

        Parameters:
            name: Name of the feature monitoring configuration.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression: Cron expression scheduling the job (UTC, Quartz).

        Returns:
            A `FeatureMonitoringConfig` builder.

        Raises:
            hopsworks.client.exceptions.ModelServingException: If this is a model deployment, or the feature view has no logging enabled.
        """
        if self.has_model:
            raise ModelServingException(
                "Model deployments are monitored per model version; use "
                "create_model_monitoring() instead."
            )
        feature_view = self.get_feature_view(init=False)
        if feature_view is None or not feature_view.logging_enabled:
            raise ModelServingException(
                f"Deployment '{self.name}' serves a feature view without logging enabled; "
                "call feature_view.enable_logging() first."
            )
        logging_fg = feature_view.feature_logging.get_feature_group(transformed=True)
        return logging_fg.create_feature_monitoring(
            name=name,
            description=description,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    @public
    def get_monitoring_configs(self) -> list[FeatureMonitoringConfig]:
        """Get the feature monitoring configurations for the model deployed by this deployment.

        For a model deployment these are the configs filtered by the model
        version; for a feature view deployment the configs on the feature
        view's logging feature group. Delegates to the underlying model's
        ``get_monitoring_configs`` method.

        Example:
            ```python

            import hopsworks

            project = hopsworks.login()

            ms = project.get_model_serving()
            my_deployment = ms.get_deployment("my_deployment")

            fm_configs = my_deployment.get_monitoring_configs()
            ```

        Returns:
            List of `FeatureMonitoringConfig` objects for the deployed model version.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        if self.has_model:
            return self.get_model().get_monitoring_configs()
        feature_view = self.get_feature_view(init=False)
        if feature_view is None or not feature_view.logging_enabled:
            return []
        logging_fg = feature_view.feature_logging.get_feature_group(transformed=True)
        configs = logging_fg.get_feature_monitoring_configs()
        return configs or []

    @public
    def create_model_monitoring(
        self,
        name: str,
        description: str | None = None,
        start_date_time: int | str | None = None,
        end_date_time: int | str | None = None,
        cron_expression: str | None = "0 0 12 ? * * *",
    ) -> FeatureMonitoringConfig:
        """Create a model monitoring config bound to this deployment's model.

        Resolves the model's parent feature view from the registered provenance and
        delegates to ``feature_view.create_model_monitoring`` with this deployment's
        ``model_name`` and ``model_version`` already filled in. The resulting config
        targets the FV's logging feature group, filters by this model+version, and
        defaults the reference training dataset to the version that was used to train
        the model.

        Experimental:
            Public API is subject to change, this feature is not suitable for production use-cases.

        Example:
            ```python3
            my_deployment = ms.get_deployment(name="my_deployment")
            my_deployment.create_model_monitoring(
                name="psi_drift",
            ).with_detection_window(
                time_offset="1d", window_length="1d",
            ).with_reference_training_dataset(  # defaults to model's TD version
            ).compare_on_distribution(
                feature_name="amount", metric="PSI", threshold=0.2,
            ).save()
            ```

        Parameters:
            name: Name of the feature monitoring configuration.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression: Cron expression scheduling the FM job (UTC, Quartz).

        Raises:
            hopsworks.client.exceptions.ModelServingException: If the deployment's
                model has no parent feature view recorded in its provenance.

        Returns:
            A ``FeatureMonitoringConfig`` builder. Call ``with_detection_window``,
            ``with_reference_*``, ``compare_on``/``compare_on_distribution``, and
            ``save()`` to register it.
        """
        if not self.has_model:
            raise ModelServingException(
                f"Deployment '{self.name}' serves a feature view without a model; use "
                "create_feature_monitoring() instead."
            )
        model_meta = self.get_model()
        fv = model_meta.get_feature_view(init=False)
        if fv is None:
            raise ModelServingException(
                f"Cannot create model monitoring for deployment '{self.name}': "
                f"model '{self.model_name}' v{self.model_version} has no parent "
                "feature view recorded in its provenance."
            )
        return fv.create_model_monitoring(
            name=name,
            model_name=self.model_name,
            model_version=self.model_version,
            description=description,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    @public
    @usage._method_logger
    def download_artifact_files(self, local_path: str | None = None):
        """Download the artifact files served by the deployment.

        Parameters:
            local_path: Path where to download the artifact files in the local filesystem.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        return self._serving_engine._download_artifact_files(
            self, local_path=local_path
        )

    @public
    def get_logs(self, component: str = "predictor", tail: int = 10):
        """Prints the deployment logs of the predictor or transformer.

        .. note::
            Legacy: this method **prints to stdout and returns ``None``**.
            New code (and any agent / scripted use) should call
            :py:meth:`read_logs` for a string return value or
            :py:meth:`tail_logs` for incremental streaming.

        Parameters:
            component: Deployment component to get the logs from (e.g., predictor or transformer).
            tail: Number of most recent lines to retrieve from the logs.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        # validate component
        components = list(util._get_members(DEPLOYABLE_COMPONENT))
        if component not in components:
            raise ValueError(
                "Component '{}' is not valid. Possible values are '{}'".format(
                    component, ", ".join(components)
                )
            )

        logs = self._serving_engine._get_logs(self, component, tail)
        if logs is not None:
            for log in logs:
                print(log, end="\n\n")

    @public
    def read_logs(
        self,
        component: str = "predictor",
        tail: int = 100,
        source: str = "opensearch",
        since: str | None = None,
        until: str | None = None,
        pod: str | None = None,
    ) -> str:
        r"""Return deployment logs as a single plain-text string.

        Programmatic counterpart to :py:meth:`get_logs`. Suitable for
        agents and scripts: never prints, never short-circuits on
        deployment state. The default ``source="opensearch"`` reads the
        project's serving index and works for stopped or restarted
        deployments — :py:meth:`get_logs` only reads live pod stdout and
        returns ``None`` when the deployment isn't running.

        Parameters:
            component: ``predictor`` or ``transformer``.
            tail: Most-recent lines to retrieve. Capped server-side.
            source: ``opensearch`` (historical, default) or ``kubernetes``
                (live pod-tailing; only works while running).
            since: ISO-8601 lower bound on log timestamp. Ignored on the
                Kubernetes path.
            until: ISO-8601 upper bound on log timestamp. Ignored on the
                Kubernetes path.
            pod: Restrict to one instance / container name.

        Returns:
            The joined logs as plain text. Empty string when there are no
            matching lines; ``==> <instance> <==\\n`` block headers when
            multiple instances are present.
        """
        components = list(util._get_members(DEPLOYABLE_COMPONENT))
        if component not in components:
            raise ValueError(
                "Component '{}' is not valid. Possible values are '{}'".format(
                    component, ", ".join(components)
                )
            )
        return self._serving_engine._read_logs(
            self,
            component=component,
            tail=tail,
            source=source,
            since=since,
            until=until,
            pod=pod,
        )

    @public
    def tail_logs(
        self,
        component: str = "predictor",
        interval: float = 2.0,
        source: str = "opensearch",
        since: str | None = "now",
        timeout: float | None = None,
        stop_on_status: str | None = None,
    ) -> Iterator[str]:
        """Yield only newly observed log chunks as plain text.

        Client-side polling, not server-streaming: each tick calls
        :py:meth:`read_logs` with a moving cursor and yields the portion
        not already seen. Deduplication uses the OpenSearch ``timestamp``
        + ``doc_id`` pair; a content-hash fallback covers the Kubernetes
        path.

        Example::

            for chunk in dep.tail_logs(timeout=120):
                print(chunk, end="")

        Parameters:
            component: ``predictor`` or ``transformer``.
            interval: Seconds between polls.
            source: ``opensearch`` (default) or ``kubernetes``.
            since: ``"now"`` to start from the current instant (default),
                or an ISO-8601 timestamp to replay from a specific point.
            timeout: Stop after this many seconds. ``None`` runs forever.
            stop_on_status: Stop when ``deployment.get_state().status``
                matches this string (e.g. ``"Stopped"``).

        Yields:
            Plain-text log chunks containing only newly observed content.
        """
        components = list(util._get_members(DEPLOYABLE_COMPONENT))
        if component not in components:
            raise ValueError(
                "Component '{}' is not valid. Possible values are '{}'".format(
                    component, ", ".join(components)
                )
            )
        yield from self._serving_engine._tail_logs(
            self,
            component=component,
            interval=interval,
            source=source,
            since=since,
            timeout=timeout,
            stop_on_status=stop_on_status,
        )

    @public
    def get_url(self):
        """Get url to the deployment in Hopsworks."""
        path = (
            "/p/"
            + str(client._get_instance()._project_id)
            + "/deployments/"
            + str(self.id)
        )
        return util._get_hostname_replaced_url(path)

    @public
    def get_endpoint_url(self) -> str | None:
        """Get the base endpoint URL for this deployment.

        Returns the base URL that can be used with external HTTP clients.
        This is the path-based routing base endpoint without any protocol-specific
        suffixes like `:predict` or `/v1`.

        If Istio client is not available, returns `None`.

        Returns:
            Base endpoint URL, or `None` if unavailable.

        Examples:
            ```python
            deployment = ms.get_deployment("my_deployment")
            url = deployment.get_endpoint_url()
            # url = "https://host:port/v1/project/name"
            ```
        """
        return self._predictor.get_endpoint_url()

    @public
    def get_openai_url(self) -> str | None:
        """Get the OpenAI-compatible API URL for vLLM deployments.

        Returns the URL for OpenAI-compatible API endpoints (e.g., /v1/chat/completions).
        This method only returns a URL for LLM (vLLM) deployments.

        Returns:
            OpenAI-compatible URL (base URL + "/v1"), or `None` if not a LLM deployment.

        Examples:
            ```python
            deployment = ms.get_deployment("my_llm_deployment")
            url = deployment.get_openai_url()
            # url = "https://host:port/v1/project/name/v1"
            # Then use: url + "/chat/completions"
            ```
        """
        return self._predictor.get_openai_url()

    @public
    def get_inference_url(self) -> str | None:
        """Get the KServe inference URL for standard model deployments.

        Returns the full URL with `:predict` suffix for KServe inference protocol.
        This method only returns a URL for standard model deployments (non-vLLM,
        with a model attached).

        If Istio client is not available, falls back to Hopsworks REST API path.

        Returns:
            Inference URL with `:predict` suffix, or `None` if not a standard model deployment.

        Examples:
            ```python
            deployment = ms.get_deployment("my_deployment")
            url = deployment.get_inference_url()
            # Use with any HTTP client
            import requests
            response = requests.post(url, json={"instances": [[1, 2, 3]]})
            ```
        """
        return self._predictor.get_inference_url()

    @public
    def describe(self):
        """Print a JSON description of the deployment."""
        util._pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        predictors = predictor_mod.Predictor.from_response_json(json_dict)
        if isinstance(predictors, list):
            return [
                cls._from_predictor(predictor_instance)
                for predictor_instance in predictors
            ]
        return cls._from_predictor(predictors)

    @classmethod
    def _from_predictor(cls, predictor_instance):
        return Deployment(
            predictor=predictor_instance,
            name=predictor_instance._name,
            description=predictor_instance._description,
        )

    def update_from_response_json(self, json_dict):
        self._predictor.update_from_response_json(json_dict)
        self.__init__(
            predictor=self._predictor,
            name=self._predictor._name,
            description=self._predictor._description,
        )
        return self

    def json(self):
        return self._predictor.json()

    def to_dict(self):
        return self._predictor.to_dict()

    # Deployment

    @public
    @property
    def id(self):
        """Id of the deployment."""
        return self._predictor.id

    @public
    @property
    def name(self):
        """Name of the deployment."""
        return self._predictor.name

    @name.setter
    def name(self, name: str):
        self._predictor.name = name

    @public
    @property
    def version(self):
        """Version of the deployment."""
        return self._predictor.version

    @public
    @property
    def description(self):
        """Description of the deployment."""
        return self._description

    @description.setter
    def description(self, description: str):
        self._description = description

    @public
    @property
    def has_model(self):
        """Whether the deployment has a model associated."""
        return self.model_name is not None and self.model_version is not None

    @public
    @property
    def has_feature_view(self) -> bool:
        """Whether this deployment serves a feature view without a model."""
        return self._predictor.has_feature_view

    @public
    @property
    def feature_view_name(self) -> str | None:
        """Name of the feature view served by a feature view deployment."""
        return self._predictor.feature_view_name

    @public
    @property
    def feature_view_version(self) -> int | None:
        """Version of the feature view served by a feature view deployment."""
        return self._predictor.feature_view_version

    @public
    @property
    def training_dataset_version(self) -> int | None:
        """Training dataset version whose statistics the deployment's transformations use."""
        version = (self._predictor.env_vars or {}).get(
            MODEL_SERVING.TRAINING_DATASET_VERSION_ENV_VAR
        )
        if version is not None:
            return int(version)
        if self.has_model:
            model = self.get_model()
            return model.training_dataset_version if model else None
        return None

    @public
    @property
    def schema(self):
        """Deployment schema, or `None`; see [`Predictor.schema`][hsml.deployment.predictor.Predictor.schema]."""
        return self._predictor.schema

    @schema.setter
    def schema(self, schema):
        self._predictor.schema = schema

    @public
    @property
    def schema_id(self) -> str | None:
        """Id of the schema this deployment's revision serves."""
        return self._predictor.schema_id

    @public
    @property
    def predictor(self):
        """Predictor used in the deployment."""
        return self._predictor

    @predictor.setter
    def predictor(self, predictor):
        self._predictor = predictor

    @public
    @property
    def requested_instances(self):
        """Total number of requested instances in the deployment."""
        return self._predictor.requested_instances

    # Single predictor

    @public
    @property
    def model_name(self):
        """Name of the model deployed by the predictor."""
        return self._predictor.model_name

    @model_name.setter
    def model_name(self, model_name: str):
        self._predictor.model_name = model_name

    @public
    @property
    def model_path(self):
        """Model path deployed by the predictor."""
        return self._predictor.model_path

    @model_path.setter
    def model_path(self, model_path: str):
        self._predictor.model_path = model_path

    @public
    @property
    def model_version(self):
        """Model version deployed by the predictor."""
        return self._predictor.model_version

    @model_version.setter
    def model_version(self, model_version: int):
        self._predictor.model_version = model_version

    @public
    @property
    def artifact_version(self):
        """Artifact version deployed by the predictor.

        Warning: Deprecated
            Artifact versions are deprecated in favor of deployment versions.
        """
        return self._predictor.version

    @artifact_version.setter
    def artifact_version(self, version: int | str):
        pass  # do nothing, kept for backward compatibility

    @public
    @property
    def artifact_files_path(self):
        """Path of the artifact files deployed by the predictor."""
        return self._predictor.artifact_files_path

    @public
    @property
    def artifact_path(self):
        """Path of the model artifact deployed by the predictor.

        Warning: Deprecated
            Artifact versions are deprecated in favor of deployment versions.
        """
        return self.artifact_files_path

    @public
    @property
    def model_server(self):
        """Model server ran by the predictor."""
        return self._predictor.model_server

    @model_server.setter
    def model_server(self, model_server: str):
        self._predictor.model_server = model_server

    @public
    @property
    def serving_tool(self):
        """Serving tool used to run the model server."""
        return self._predictor.serving_tool

    @serving_tool.setter
    def serving_tool(self, serving_tool: str):
        self._predictor.serving_tool = serving_tool

    @public
    @property
    def script_file(self):
        """Script file used by the predictor."""
        return self._predictor.script_file

    @script_file.setter
    def script_file(self, script_file: str):
        self._predictor.script_file = script_file

    @public
    @property
    def config_file(self):
        """Model server configuration file passed to the model deployment.

        It can be accessed via `CONFIG_FILE_PATH` environment variable from a predictor or transformer script.
        For LLM deployments without a predictor script, this file is used to configure the vLLM engine.
        """
        return self._predictor.config_file

    @config_file.setter
    def config_file(self, config_file: str):
        self._predictor.config_file = config_file

    @public
    @property
    def resources(self):
        """Resource configuration for the predictor."""
        return self._predictor.resources

    @resources.setter
    def resources(self, resources: Resources):
        self._predictor.resources = resources

    @public
    @property
    def inference_logger(self):
        """Configuration of the inference logger attached to this predictor."""
        return self._predictor.inference_logger

    @inference_logger.setter
    def inference_logger(self, inference_logger: InferenceLogger):
        self._predictor.inference_logger = inference_logger

    @public
    @property
    def inference_batcher(self):
        """Configuration of the inference batcher attached to this predictor."""
        return self._predictor.inference_batcher

    @inference_batcher.setter
    def inference_batcher(self, inference_batcher: InferenceBatcher):
        self._predictor.inference_batcher = inference_batcher

    @public
    @property
    def transformer(self):
        """Transformer configured in the predictor."""
        return self._predictor.transformer

    @transformer.setter
    def transformer(self, transformer: Transformer):
        self._predictor.transformer = transformer

    @public
    @property
    def tracing(self):
        """Tracing configuration attached to this deployment."""
        return self._predictor.tracing

    @tracing.setter
    def tracing(self, tracing: DeploymentTracingConfig | dict | None):
        self._predictor.tracing = tracing

    @public
    @property
    def feature_logging(self):
        """Feature logging configuration attached to this deployment.

        Edit its fields and call `save()`; a running deployment applies them after `restart()`.
        """
        return self._predictor.feature_logging

    @feature_logging.setter
    def feature_logging(self, feature_logging: DeploymentLoggingConfig | dict | None):
        self._predictor.feature_logging = feature_logging

    @public
    @property
    def model_registry_id(self):
        """Model Registry Id of the deployment."""
        return self._model_registry_id

    @model_registry_id.setter
    def model_registry_id(self, model_registry_id: int):
        self._model_registry_id = model_registry_id

    @public
    @property
    def created_at(self):
        """Created at date of the predictor."""
        return self._predictor.created_at

    @public
    @property
    def creator(self):
        """Creator of the predictor."""
        return self._predictor.creator

    @public
    @property
    def api_protocol(self):
        """API protocol enabled in the deployment (e.g., HTTP or GRPC)."""
        return self._predictor.api_protocol

    @api_protocol.setter
    def api_protocol(self, api_protocol: str):
        self._predictor.api_protocol = api_protocol

    @public
    @property
    def environment(self):
        """Name of inference environment."""
        return self._predictor.environment

    @environment.setter
    def environment(self, environment: str):
        self._predictor.environment = environment

    @public
    @property
    def env_vars(self):
        """Environment variables of the predictor."""
        return self._predictor.env_vars

    @env_vars.setter
    def env_vars(self, env_vars: dict[str, str] | None):
        self._predictor.env_vars = env_vars

    @public
    @property
    def project_namespace(self):
        """Name of the Kubernetes namespace the project is in."""
        return self._predictor.project_namespace

    @project_namespace.setter
    def project_namespace(self, project_namespace: str):
        self._predictor.project_namespace = project_namespace

    @public
    @property
    def project_name(self):
        """Name of the project the deployment belongs to."""
        return self._predictor._project_name

    @project_name.setter
    def project_name(self, project_name: str):
        self._predictor._project_name = project_name

    @public
    @property
    def scaling_configuration(self):
        """Scaling configuration for the deployment."""
        return self._predictor.scaling_configuration

    @scaling_configuration.setter
    def scaling_configuration(self, scaling_configuration: PredictorScalingConfig):
        self._predictor.scaling_configuration = scaling_configuration

    def __repr__(self):
        desc = (
            f", description: {self._description!r}"
            if self._description is not None
            else ""
        )
        return f"Deployment(name: {self._predictor._name!r}" + desc + ")"
