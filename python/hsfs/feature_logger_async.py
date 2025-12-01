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
import asyncio
import base64
import itertools
import json
import logging
import threading
import traceback
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional, Tuple

from hsfs.core.feature_logging_client import (
    get_instance as get_feature_logging_client,
)
from hsfs.core.feature_logging_client import (
    init_client,
)
from hsfs.core.kafka_engine import encode_row, get_writer_function
from hsfs.feature_logger import FeatureLogger
from hsfs.feature_view import FeatureView


_logger = logging.getLogger(__name__)


class EventEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            # Convert bytes to a base64-encoded string
            return base64.b64encode(obj).decode("utf-8")
        elif isinstance(obj, datetime):
            obj = obj.replace(tzinfo=timezone.utc)
            return obj.isoformat()
        elif isinstance(obj, date):
            # Convert to days since Unix epoch
            epoch = datetime.date(1970, 1, 1)
            return (obj - epoch).days


class AsyncWorkerThread(threading.Thread):
    """
    Thread class to run an asyncio event loop in a separate thread. The event loop is used to run async workers that processes logs.
    """

    def __init__(
        self, group=None, target=None, name=None, args=..., kwargs=None, *, daemon=None
    ):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self._event_loop = asyncio.new_event_loop()
        self._workers = []  # List to keep track of worker coroutines
        self._tasks_queue = asyncio.Queue()  # Queue to manage tasks

        self._stop_event = (
            threading.Event()
        )  # Stop event to stop input of new tasks after a close has been called.

    def submit_task(self, task: Tuple[dict, dict]):
        """
        Function to submit a task to the queue from a different thread so that it can be processed by workers.

        Parameters:
            task: Tuple that contains untransformed and transformed features to be logged.
        """
        if self._stop_event.is_set():
            _logger.error("Cannot submit task. Workers are stopped.")
        else:
            asyncio.run_coroutine_threadsafe(
                self._tasks_queue.put(task), self._event_loop
            )

    def _initialize_workers(self, num_workers: int, worker_function: Callable):
        """
        Function to initialize workers as tasks in the event loop.

        Parameters:
            num_worker: Number of workers to be initialized.
            worker_function: Function to be run by the workers.
        """
        for _ in range(num_workers):
            worker = self._event_loop.create_task(self._worker(worker_function))
            self._workers.append(worker)

    def run(self):
        """
        Thread functions that runs the event loop in the thread and close the event loop with feature logging after the loop is stopped.
        """
        asyncio.set_event_loop(self._event_loop)

        # Run the event loop
        self._event_loop.run_forever()

        # Closing the feature logging client inside the thread.
        self._event_loop.run_until_complete(get_feature_logging_client().close())

        # Close the event loop
        self._event_loop.close()

    async def _worker(self, worker_function: Callable):
        """
        Function to run the worker function in the event loop, until a None has been submitted to the queue.

        Parameters:
            worker_function: `Callable`. Function to be run by the workers.
        """
        while True:
            task = await self._tasks_queue.get()
            if task is None:
                # Poison pill means shutdown
                self._tasks_queue.task_done()
                break
            await worker_function(task)
            self._tasks_queue.task_done()

    def close(self):
        """
        Function to stop any more tasks from being submitted and start the graceful stop of the thread.
        """
        # Stop any more tasks from being submitted using the stop event.
        self._stop_event.set()

        # Stop the event loop
        asyncio.run_coroutine_threadsafe(self.finalize_event_loop(), self._event_loop)

    async def finalize_event_loop(self):
        """
        Function that gracefully stops the event loop by stopping the workers and waiting for all tasks to be processed.
        """
        # Stop workers
        for _ in range(len(self._workers)):
            await self._tasks_queue.put(None)  # Poison pill to stop workers

        # Wait until all tasks in the queue are processed
        await self._tasks_queue.join()

        # Wait until all tasks in the queue are processed
        await asyncio.gather(*self._workers)

        # Stop the event loop
        self._event_loop.stop()


class AsyncFeatureLogger(FeatureLogger):
    def __init__(
        self,
        project_id,
        source,
        namespace,
        deployment_name,
        max_concurrent_tasks=5,
        feature_logger_config: Optional[dict[str, Any]] = None,
    ):
        self._max_concurrent_tasks = max_concurrent_tasks
        self._feature_view: FeatureView = None
        self._project_id = project_id
        self._source = source
        self._namespace = namespace
        self._deployment_name = deployment_name
        self._workers = []  # List to keep track of worker coroutines

        # Initialize workers in another so that we don't cause any issues with the event loop's running in the main thread.
        self._async_worker_thread = AsyncWorkerThread()

        self._feature_logger_config = feature_logger_config
        if self._feature_logger_config is None:
            self._feature_logger_config = {}
        # set pool size equals to the number of concurrent tasks
        if "pool_size" not in self._feature_logger_config:
            self._feature_logger_config["pool_size"] = max_concurrent_tasks
        self._feature_encoders = {}

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
            try:
                self._async_worker_thread.submit_task(
                    (untransformed_feature, transformed_feature)
                )
            except asyncio.QueueFull:
                _logger.error("Queue is full. Failed to log features.")

    async def _send_events(self, task):
        try:
            untransformed_feature = task[0]
            transformed_feature = task[1]

            events = []
            ce_id = str(uuid.uuid4())
            ce_time = datetime.now(timezone.utc).isoformat()
            for transformed, feature_vector in [
                (False, untransformed_feature),
                (True, transformed_feature),
            ]:
                if feature_vector:
                    events.append(
                        self._create_cloud_event(
                            ce_id,
                            ce_time,
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

            responses = await get_feature_logging_client().post(
                json.dumps(events, cls=EventEncoder),
                headers=self._create_cloud_headers(),
            )
            _logger.debug(f"Feature logging events sent successfully: {responses}")
        except Exception as e:
            _logger.error(f"Failed to send events: {e}")
            traceback.print_exc()

    def _create_cloud_headers(self):
        return {
            "content-type": "application/cloudevents-batch+json; charset=UTF-8",
            "ce-specversion": "1.0",
            "ce-type": "serving.hops.works.logging.features",
            "ce-source": self._source or "http://localhost:8099",
        }

    def _create_cloud_event(self, ce_id, ce_time, fg, features):
        return {
            "specversion": "1.0",
            "type": "serving.hops.works.logging.features",
            "source": self._source,
            "id": ce_id,
            "time": ce_time,
            "datacontenttype": "application/json",
            "endpoint": "",
            "inferenceservicename": self._deployment_name,
            "namespace": self._namespace,
            "projectId": self._project_id,
            "featureGroupId": fg.id,
            "topicName": fg._online_topic_name,
            "subjectId": fg.subject["id"],
            "data": features,
        }

    def init(self, feature_view: FeatureView) -> None:
        self._feature_view = feature_view
        self._init_kafka_resource(feature_view)
        init_client(self._feature_logger_config)

        # Start worker thread after initializing the client.
        self._async_worker_thread._initialize_workers(
            self._max_concurrent_tasks, self._send_events
        )
        self._async_worker_thread.start()

    def _init_kafka_resource(self, feature_view):
        for transformed in [True, False]:
            fg = feature_view.feature_logging.get_feature_group(transformed)
            feature_writers, writer = get_writer_function(fg)
            self._feature_encoders[transformed] = (feature_writers, writer)

    def _avro_encode_features(self, complex_feature_encoder, feature_encoder, features):
        return encode_row(complex_feature_encoder, feature_encoder, features)

    def close(self):
        """
        Close the async feature logger.
        """
        # Close the async worker thread
        self._async_worker_thread.close()
