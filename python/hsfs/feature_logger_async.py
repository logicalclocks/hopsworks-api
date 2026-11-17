#
#   Copyright 2025 Hopsworks AB
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

import base64
import itertools
import json
import logging
import threading
import uuid
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common.core.feature_logging_async import _AsyncLogWorker
from hopsworks_common.core.feature_logging_buffer import _positive_env
from hsfs.core.feature_logging_client import (
    _get_instance as get_feature_logging_client,
)
from hsfs.core.feature_logging_client import (
    _init_client,
)
from hsfs.core.kafka_engine import _encode_row, _get_writer_function
from hsfs.feature_logger import FeatureLogger


if TYPE_CHECKING:
    from hsfs.feature_view import FeatureView


_logger = logging.getLogger(__name__)


class EventEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            # Convert bytes to a base64-encoded string
            return base64.b64encode(obj).decode("utf-8")
        if isinstance(obj, datetime):
            obj = obj.replace(tzinfo=timezone.utc)
            return obj.isoformat()
        if isinstance(obj, date):
            # Convert to days since Unix epoch
            epoch = date(1970, 1, 1)
            return (obj - epoch).days
        return super().default(obj)


@public
class AsyncFeatureLogger(FeatureLogger):
    def __init__(
        self,
        project_id,
        source,
        namespace,
        deployment_name,
        max_concurrent_tasks=5,
        feature_logger_config: dict[str, Any] | None = None,
        max_queue_size: int = 1000,
    ):
        self._max_concurrent_tasks = max_concurrent_tasks
        self._feature_view: FeatureView = None
        self._project_id = project_id
        self._source = source
        self._namespace = namespace
        self._deployment_name = deployment_name
        self._workers = []  # List to keep track of worker coroutines

        # Initialize workers in another so that we don't cause any issues with the event loop's running in the main thread.
        self._async_worker_thread = _AsyncLogWorker(
            max_queue_size=max_queue_size,
            on_drop=self._on_drop,
            close_client=lambda: get_feature_logging_client()._close(),
        )
        self._max_event_bytes = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_MAX_EVENT_BYTES", 8 * 1024 * 1024
        )
        self._stats_lock = threading.Lock()
        self._stats = dict.fromkeys(("submitted", "sent", "failed", "dropped"), 0)

        self._feature_logger_config = feature_logger_config
        if self._feature_logger_config is None:
            self._feature_logger_config = {}
        # set pool size equals to the number of concurrent tasks
        if "pool_size" not in self._feature_logger_config:
            self._feature_logger_config["pool_size"] = max_concurrent_tasks
        self._feature_encoders = {}

    @public
    def log(
        self,
        untransformed_features: list[dict] = None,
        transformed_features: list[dict] = None,
    ):
        if not untransformed_features:
            untransformed_features = []
        if not transformed_features:
            transformed_features = []

        for untransformed_feature, transformed_feature in itertools.zip_longest(
            untransformed_features, transformed_features
        ):
            self._count("submitted")
            self._async_worker_thread._submit_task(
                (untransformed_feature, transformed_feature)
            )

    def _count(self, outcome, rows=1):
        with self._stats_lock:
            self._stats[outcome] += rows
            total = self._stats[outcome]
            snapshot = dict(self._stats)
        if outcome in ("failed", "dropped") and (total == rows or total % 100 == 0):
            _logger.error("Feature logging %s; counters=%s", outcome, snapshot)

    def _on_drop(self, task, reason, rows=1):
        self._count("dropped", rows)

    async def _send_events(self, task):
        try:
            untransformed_feature = task[0]
            transformed_feature = task[1]

            events = []
            for transformed, feature_vector in [
                (False, untransformed_feature),
                (True, transformed_feature),
            ]:
                if feature_vector:
                    events.append(
                        self._create_cloud_event(
                            self._feature_view.feature_logging.get_feature_group(
                                transformed
                            ),
                            self._avro_encode_features(
                                self._feature_encoders[transformed][0],
                                self._feature_encoders[transformed][1],
                                feature_vector,
                            ),
                        )
                    )

            payload = json.dumps(events, cls=EventEncoder).encode("utf-8")
            if len(payload) > self._max_event_bytes:
                self._on_drop(task, "event byte limit")
                return
            await get_feature_logging_client()._post(
                payload,
                headers=self._create_cloud_headers(),
            )
            self._count("sent")
        except Exception:  # noqa: BLE001 - delivery must not stop the logging worker
            self._count("failed")

    def _create_cloud_headers(self):
        return {
            "content-type": "application/json",
            "ce-specversion": "1.0",
            "ce-id": str(uuid.uuid4()),
            "ce-time": datetime.now(timezone.utc).isoformat(),
            "ce-type": "serving.hops.works.logging.features",
            "ce-source": self._source or "http://localhost:8099",
        }

    def _create_cloud_event(self, fg, features):
        return {
            "projectId": self._project_id,
            "featureGroupId": fg.id,
            "topicName": fg._online_topic_name,
            "subjectId": fg.subject["id"],
            "data": features,
        }

    def init(self, feature_view: FeatureView) -> None:
        self._feature_view = feature_view
        self._init_kafka_resource(feature_view)
        _init_client(self._feature_logger_config)

        # Start worker thread after initializing the client.
        self._async_worker_thread._initialize_workers(
            self._max_concurrent_tasks, self._send_events
        )
        self._async_worker_thread.start()

    def _init_kafka_resource(self, feature_view):
        for transformed in [True, False]:
            fg = feature_view.feature_logging.get_feature_group(transformed)
            feature_writers, writer = _get_writer_function(fg)
            self._feature_encoders[transformed] = (feature_writers, writer)

    def _avro_encode_features(self, complex_feature_encoder, feature_encoder, features):
        return _encode_row(complex_feature_encoder, feature_encoder, features)

    @public
    def close(self, timeout: float | None = None) -> bool:
        """Stop admission and drain accepted rows within the shutdown deadline.

        Parameters:
            timeout: Maximum time to drain, or the configured shutdown limit.

        Returns:
            Whether all accepted rows finished before the deadline.
        """
        drained = self._async_worker_thread._close(timeout)
        _logger.info("Feature logging counters at shutdown: %s", self._stats)
        return drained
