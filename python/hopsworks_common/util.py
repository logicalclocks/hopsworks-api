#
#   Copyright 2020 Logical Clocks AB
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

import asyncio
import inspect
import itertools
import json
import os
import queue
import re
import shutil
import sys
import threading
import time
import warnings
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Literal
from urllib.parse import urljoin, urlparse

import humps
from hopsworks_aliases import public
from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException, JobException
from hopsworks_common.constants import MODEL, PREDICTOR, Default
from hopsworks_common.core.constants import HAS_PANDAS
from hopsworks_common.git_file_status import GitFileStatus
from six import string_types


if HAS_PANDAS:
    import pandas as pd


FEATURE_STORE_NAME_SUFFIX = "_featurestore"


if TYPE_CHECKING:
    from hsfs import feature_group


@public("hopsworks.util", "hsml.util")
class Encoder(json.JSONEncoder):
    def default(self, o: Any) -> dict[str, Any]:
        try:
            return o.to_dict()
        except AttributeError:
            return super().default(o)


@public("hsml.util")
class NumpyEncoder(json.JSONEncoder):
    """Special json encoder for numpy types.

    Note that some numpy types doesn't have native python equivalence, hence json.dumps will raise TypeError.
    In this case, you'll need to convert your numpy types into its closest python equivalence.
    """

    def convert(self, obj):
        import base64

        import numpy as np

        def encode_binary(x):
            return base64.encodebytes(x).decode("ascii")

        if isinstance(obj, np.ndarray):
            if obj.dtype == np.object:
                return [self.convert(x)[0] for x in obj.tolist()]
            if obj.dtype == np.bytes_:
                return np.vectorize(encode_binary)(obj), True
            return obj.tolist(), True

        if isinstance(obj, datetime) or (HAS_PANDAS and isinstance(obj, pd.Timestamp)):
            return obj.isoformat(), True
        if isinstance(obj, (bytes, bytearray)):
            return encode_binary(obj), True
        if isinstance(obj, np.generic):
            return obj.item(), True
        if isinstance(obj, np.datetime64):
            return np.datetime_as_string(obj), True
        return obj, False

    def default(self, obj):  # pylint: disable=E0202
        res, converted = self.convert(obj)
        if converted:
            return res
        return super().default(obj)


VALID_EMBEDDING_TYPE = {
    "array<int>",
    "array<bigint>",
    "array<float>",
    "array<double>",
}


@public("hopsworks.util")
def validate_embedding_feature_type(embedding_index, schema):
    if not embedding_index or not schema:
        return
    feature_type_map = {feat.name: feat.type for feat in schema}
    for embedding in embedding_index.get_embeddings():
        feature_type = feature_type_map.get(embedding.name)
        if feature_type not in VALID_EMBEDDING_TYPE:
            raise FeatureStoreException(
                f"Provide feature `{embedding.name}` has type `{feature_type}`, "
                f"but requires one of the following: {', '.join(VALID_EMBEDDING_TYPE)}"
            )


@public("hopsworks.util")
def autofix_feature_name(name: str, warn: bool = False) -> str:
    # replace spaces with underscores and enforce lower case
    if warn and contains_uppercase(name):
        warnings.warn(
            f"The feature name `{name}` contains upper case letters. "
            "Feature names are sanitized to lower case in the feature store.",
            stacklevel=1,
        )
    if warn and contains_whitespace(name):
        warnings.warn(
            f"The feature name `{name}` contains spaces. "
            "Feature names are sanitized to use underscore '_' in the feature store.",
            stacklevel=1,
        )
    return name.lower().replace(" ", "_")


def contains_uppercase(name: str) -> bool:
    return any(re.finditer("[A-Z]", name))


def contains_whitespace(name: str) -> bool:
    return " " in name


@public("hopsworks.util")
def feature_group_name(
    feature_group,  #  FeatureGroup | ExternalFeatureGroup | SpineGroup
) -> str:
    return feature_group.name + "_" + str(feature_group.version)


@public("hopsworks.util")
def append_feature_store_suffix(name: str) -> str:
    name = name.lower()
    if name.endswith(FEATURE_STORE_NAME_SUFFIX):
        return name
    return name + FEATURE_STORE_NAME_SUFFIX


@public("hopsworks.util")
def strip_feature_store_suffix(name: str) -> str:
    name = name.lower()
    if name.endswith(FEATURE_STORE_NAME_SUFFIX):
        return name[: -1 * len(FEATURE_STORE_NAME_SUFFIX)]
    return name


@public("hopsworks.util")
def get_dataset_type(path: str) -> Literal["HIVEDB", "DATASET"]:
    if re.match(r"^(?:hdfs://|)/apps/hive/warehouse/*", path):
        return "HIVEDB"
    return "DATASET"


@public("hopsworks.util")
def check_timestamp_format_from_date_string(input_date: str) -> tuple[str, str]:
    date_format_patterns = {
        r"^([0-9]{4})([0-9]{2})([0-9]{2})$": "%Y%m%d",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M%S",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{3})$": "%Y%m%d%H%M%S%f",
        r"^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{6})Z$": "ISO",
    }
    normalized_date = (
        input_date.replace("/", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(":", "")
        .replace(".", "")
    )

    date_format = None
    for pattern in date_format_patterns:
        date_format_pattern = re.match(pattern, normalized_date)
        if date_format_pattern:
            date_format = date_format_patterns[pattern]
            break

    if date_format is None:
        raise ValueError(
            "Unable to identify format of the provided date value : " + input_date
        )

    return normalized_date, date_format


@public("hopsworks.util")
def get_timestamp_from_date_string(input_date: str) -> int:
    norm_input_date, date_format = check_timestamp_format_from_date_string(input_date)
    try:
        if date_format != "ISO":
            date_time = datetime.strptime(norm_input_date, date_format)
        else:
            date_time = datetime.fromisoformat(input_date[:-1])
    except ValueError as err:
        raise ValueError(
            "Unable to parse the normalized input date value : "
            + norm_input_date
            + " with format "
            + date_format
        ) from err
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=timezone.utc)
    return int(float(date_time.timestamp()) * 1000)


@public("hopsworks.util")
def get_hudi_datestr_from_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp / 1000, timezone.utc).strftime(
        "%Y%m%d%H%M%S%f"
    )[:-3]


@public("hopsworks.util")
def get_delta_datestr_from_timestamp(timestamp: int) -> str:
    # It does not work to add the Z in the strftime function
    return (
        datetime.fromtimestamp(timestamp / 1000, timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]
        + "Z"
    )


@public("hopsworks.util")
def convert_event_time_to_timestamp(
    event_time: str
    | pd._libs.tslibs.timestamps.Timestamp
    | datetime
    | date
    | int
    | None,
) -> int | None:
    if not event_time:
        return None
    if isinstance(event_time, str):
        return get_timestamp_from_date_string(event_time)
    if hasattr(event_time, "to_pydatetime"):
        # only pandas Timestamp has to_pydatetime method out of the accepted event_time types
        # convert to unix epoch time in milliseconds.
        event_time = event_time.to_pydatetime()
        # convert to unix epoch time in milliseconds.
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        return int(event_time.timestamp() * 1000)
    if isinstance(event_time, datetime):
        # convert to unix epoch time in milliseconds.
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        return int(event_time.timestamp() * 1000)
    if isinstance(event_time, date):
        # convert to unix epoch time in milliseconds.
        event_time = datetime(*event_time.timetuple()[:7])
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        return int(event_time.timestamp() * 1000)
    if isinstance(event_time, int):
        if event_time == 0:
            raise ValueError("Event time should be greater than 0.")
        # jdbc supports timestamp precision up to second only.
        if len(str(event_time)) <= 10:
            event_time = event_time * 1000
        return event_time
    raise ValueError(
        "Given event time should be in `datetime`, `date`, `str` or `int` type"
    )


@public("hopsworks.util")
def get_hostname_replaced_url(sub_path: str) -> str:
    """Construct and return an url with public hopsworks hostname and sub path.

    Parameters:
        sub_path: URL sub-path after base URL.

    Returns:
        href url.
    """
    href = urljoin(client.get_instance()._base_url, sub_path)
    url_parsed = client.get_instance().replace_public_host(urlparse(href))
    return url_parsed.geturl()


@public("hopsworks.util")
def verify_attribute_key_names(
    feature_group_obj,  #  FeatureGroup | ExternalFeatureGroup | SpineGroup
    external_feature_group: bool = False,
) -> None:
    feature_names = {feat.name for feat in feature_group_obj.features}
    if feature_group_obj.primary_key:
        diff = set(feature_group_obj.primary_key) - feature_names
        if diff:
            raise FeatureStoreException(
                f"Provided primary key(s) {','.join(diff)} doesn't exist in feature dataframe"
            )

    if (
        feature_group_obj.event_time
        and feature_group_obj.event_time not in feature_names
    ):
        raise FeatureStoreException(
            f"Provided event_time feature {feature_group_obj.event_time} doesn't exist in feature dataframe"
        )

    if not external_feature_group:
        if feature_group_obj.partition_key:
            diff = set(feature_group_obj.partition_key) - feature_names
            if diff:
                raise FeatureStoreException(
                    f"Provided partition key(s) {','.join(diff)} doesn't exist in feature dataframe"
                )

        if (
            feature_group_obj.hudi_precombine_key
            and feature_group_obj.hudi_precombine_key not in feature_names
        ):
            raise FeatureStoreException(
                f"Provided hudi precombine key {feature_group_obj.hudi_precombine_key} "
                f"doesn't exist in feature dataframe"
            )


@public("hopsworks.util")
def get_job_url(href: str) -> str:
    """Use the endpoint returned by the API to construct the UI url for jobs.

    Parameters:
        href: the endpoint returned by the API
    """
    url = urlparse(href)
    url_splits = url.path.split("/")
    project_id = url_splits[4]
    job_name = url_splits[6]
    ui_url = url._replace(path=f"p/{project_id}/jobs/named/{job_name}/executions")
    ui_url = client.get_instance().replace_public_host(ui_url)
    return ui_url.geturl()


@public("hopsworks.util")
def _loading_animation(message: str, stop_event: threading.Event) -> None:
    for char in itertools.cycle([".", "..", "...", ""]):
        if stop_event.is_set():
            break
        print(f"{message}{char}   ", end="\r")
        time.sleep(0.5)


@public("hopsworks.util")
def run_with_loading_animation(message: str, func: Callable, *args, **kwargs) -> Any:
    stop_event = threading.Event()
    t = threading.Thread(
        target=_loading_animation,
        args=(
            message,
            stop_event,
        ),
    )
    t.daemon = True
    t.start()
    start = time.time()
    end = None

    try:
        result = func(*args, **kwargs)
        end = time.time()
        return result
    finally:
        # Stop the animation and print the "Finished Querying" message
        stop_event.set()
        t.join()
        if not end:
            print(f"\rError: {message}           ", end="\n")
        else:
            print(f"\rFinished: {message} ({(end - start):.2f}s) ", end="\n")


@public("hopsworks.util")
def get_feature_group_url(feature_store_id: int, feature_group_id: int) -> str:
    sub_path = (
        "/p/"
        + str(client.get_instance()._project_id)
        + "/fs/"
        + str(feature_store_id)
        + "/fg/"
        + str(feature_group_id)
    )
    return get_hostname_replaced_url(sub_path)


@public("hopsworks.util")
def is_runtime_notebook():
    return "ipykernel" in sys.modules


@public("hopsworks.util", "hsml.util")
class VersionWarning(Warning):
    pass


@public("hsml.util")
class ProvenanceWarning(Warning):
    pass


@public("hopsworks.util")
class JobWarning(Warning):
    pass


@public("hopsworks.util")
class StorageWarning(Warning):
    pass


@public("hopsworks.util")
class StatisticsWarning(Warning):
    pass


@public("hopsworks.util")
class ValidationWarning(Warning):
    pass


@public("hopsworks.util")
class FeatureGroupWarning(Warning):
    pass


@public("hopsworks.util")
def convert_to_abs(path, current_proj_name):
    abs_project_prefix = f"/Projects/{current_proj_name}"
    if not path.startswith(abs_project_prefix):
        return abs_project_prefix + "/" + path
    return path


def convert_to_project_rel_path(path, current_proj_name):
    abs_project_prefix = f"/Projects/{current_proj_name}"
    if path.startswith(abs_project_prefix):
        return path.replace(abs_project_prefix, "")
    return path


@public("hopsworks.util")
def validate_job_conf(config, project_name):
    # User is required to set the appPath programmatically after getting the configuration
    if (
        config["type"] != "dockerJobConfiguration"
        and config["type"] != "flinkJobConfiguration"
        and "appPath" not in config
    ):
        raise JobException("'appPath' not set in job configuration")
    if "appPath" in config and not config["appPath"].startswith("hdfs://"):
        config["appPath"] = "hdfs://" + convert_to_abs(config["appPath"], project_name)

    # If PYSPARK application set the mainClass, if SPARK validate there is a mainClass set
    if config["type"] == "sparkJobConfiguration":
        if config["appPath"].endswith(".py") or config["appPath"].endswith(".ipynb"):
            config["mainClass"] = "org.apache.spark.deploy.PythonRunner"
        elif "mainClass" not in config:
            raise JobException("'mainClass' needs to be set in the job configuration")

    return config


@public("hopsworks.util")
def convert_git_status_to_files(files):
    # Convert GitFileStatus to list of file paths
    if isinstance(files[0], GitFileStatus):
        tmp_files = []
        for file_status in files:
            tmp_files.append(file_status.file)
        files = tmp_files

    return files


@public("hopsworks.util")
def is_interactive():
    import __main__ as main

    return not hasattr(main, "__file__")


# Model registry

# - schema and types


@public("hsml.util")
def set_model_class(model):
    from hsml.llm.model import Model as LLMModel
    from hsml.model import Model as BaseModel
    from hsml.python.model import Model as PyModel
    from hsml.sklearn.model import Model as SkLearnModel
    from hsml.tensorflow.model import Model as TFModel
    from hsml.torch.model import Model as TorchModel

    if "href" in model:
        _ = model.pop("href")
    if "type" in model:  # backwards compatibility
        _ = model.pop("type")
    if "tags" in model:
        _ = model.pop("tags")  # tags are always retrieved from backend

    if "framework" not in model:
        return BaseModel(**model)

    framework = model.pop("framework")
    if framework == MODEL.FRAMEWORK_TENSORFLOW:
        return TFModel(**model)
    if framework == MODEL.FRAMEWORK_TORCH:
        return TorchModel(**model)
    if framework == MODEL.FRAMEWORK_SKLEARN:
        return SkLearnModel(**model)
    if framework == MODEL.FRAMEWORK_PYTHON:
        return PyModel(**model)
    if framework == MODEL.FRAMEWORK_LLM:
        return LLMModel(**model)
    raise ValueError(f"framework {str(framework)} is not a supported framework")


@public("hsml.util")
def input_example_to_json(input_example):
    import numpy as np

    if isinstance(input_example, np.ndarray):
        if input_example.size > 0:
            return _handle_tensor_input(input_example)
        raise ValueError(
            f"input_example of type {type(input_example)} can not be empty"
        )
    if isinstance(input_example, dict):
        return _handle_dict_input(input_example)
    return _handle_dataframe_input(input_example)


@public("hsml.util")
def _handle_tensor_input(input_tensor):
    return input_tensor.tolist()


@public("hsml.util")
def _handle_dataframe_input(input_ex):
    if HAS_PANDAS and isinstance(input_ex, pd.DataFrame):
        if not input_ex.empty:
            return input_ex.iloc[0].tolist()
        raise ValueError(f"input_example of type {type(input_ex)} can not be empty")
    if HAS_PANDAS and isinstance(input_ex, pd.Series):
        if not input_ex.empty:
            return input_ex.tolist()
        raise ValueError(f"input_example of type {type(input_ex)} can not be empty")
    if isinstance(input_ex, list):
        if len(input_ex) > 0:
            return input_ex
        raise ValueError(f"input_example of type {type(input_ex)} can not be empty")
    raise TypeError(f"{type(input_ex)} is not a supported input example type")


@public("hsml.util")
def _handle_dict_input(input_ex):
    return input_ex


# - artifacts


@public("hsml.util")
def compress(archive_out_path, archive_name, path_to_archive):
    if os.path.isdir(path_to_archive):
        return shutil.make_archive(
            os.path.join(archive_out_path, archive_name), "gztar", path_to_archive
        )
    return shutil.make_archive(
        os.path.join(archive_out_path, archive_name),
        "gztar",
        os.path.dirname(path_to_archive),
        os.path.basename(path_to_archive),
    )


@public("hsml.util")
def decompress(archive_file_path, extract_dir=None):
    return shutil.unpack_archive(archive_file_path, extract_dir=extract_dir)


# - export models


@public("hsml.util")
def validate_metrics(metrics):
    if metrics is not None:
        if not isinstance(metrics, dict):
            raise TypeError(
                f"provided metrics is of instance {type(metrics)}, expected a dict"
            )

        for metric in metrics:
            # Validate key is a string
            if not isinstance(metric, string_types):
                raise TypeError(
                    f"provided metrics key is of instance {type(metric)}, expected a string"
                )
            # Validate value is a number
            try:
                float(metrics[metric])
            except ValueError as err:
                raise ValueError(
                    f"{str(metrics[metric])} is not a number, only numbers can be attached as metadata for models."
                ) from err


# Model serving


@public("hsml.util")
def get_predictor_for_model(model, **kwargs):
    from hsml.llm.model import Model as LLMModel
    from hsml.llm.predictor import Predictor as vLLMPredictor
    from hsml.model import Model as BaseModel
    from hsml.predictor import Predictor as BasePredictor
    from hsml.python.model import Model as PyModel
    from hsml.python.predictor import Predictor as PyPredictor
    from hsml.sklearn.model import Model as SkLearnModel
    from hsml.sklearn.predictor import Predictor as SkLearnPredictor
    from hsml.tensorflow.model import Model as TFModel
    from hsml.tensorflow.predictor import Predictor as TFPredictor
    from hsml.torch.model import Model as TorchModel
    from hsml.torch.predictor import Predictor as TorchPredictor

    if not isinstance(model, BaseModel):
        raise ValueError(
            f"model is of type {type(model)}, but an instance of {BaseModel} class is expected"
        )

    if type(model) is TFModel:
        return TFPredictor(**kwargs)
    if type(model) is TorchModel:
        return TorchPredictor(**kwargs)
    if type(model) is SkLearnModel:
        return SkLearnPredictor(**kwargs)
    if type(model) is PyModel:
        return PyPredictor(**kwargs)
    if type(model) is LLMModel:
        return vLLMPredictor(**kwargs)
    if type(model) is BaseModel:
        return BasePredictor(  # python as default framework and model server
            model_framework=MODEL.FRAMEWORK_PYTHON,
            model_server=PREDICTOR.MODEL_SERVER_PYTHON,
            **kwargs,
        )
    raise TypeError(
        f"model is of type {type(model)}, but an instance of {BaseModel} class is expected"
    )


def get_predictor_for_server(name: str, script_file: str, **kwargs):
    from hsml.python.endpoint import Endpoint as PyEndpoint

    return PyEndpoint(name=name, script_file=script_file, **kwargs)


# General


@public("hsml.util")
def pretty_print(obj):
    if isinstance(obj, list):
        for logs in obj:
            pretty_print(logs)
    else:
        json_decamelized = humps.decamelize(obj.to_dict())
        print(json.dumps(json_decamelized, indent=4, sort_keys=True))


@public("hsml.util")
def get_members(cls, prefix=None):
    for m in inspect.getmembers(cls, lambda m: not (inspect.isroutine(m))):
        n = m[0]  # name
        if (prefix is not None and n.startswith(prefix)) or (
            prefix is None and not (n.startswith("__") and n.endswith("__"))
        ):
            yield m[1]  # value


# - json


def extract_field_from_json(obj, fields, default=None, as_instance_of=None):
    if isinstance(fields, list):
        for field in fields:
            value = extract_field_from_json(obj, field, default, as_instance_of)
            if value is not None:
                break
    else:
        value = obj.pop(fields) if fields in obj else default
        if as_instance_of is not None:
            if isinstance(value, list):
                # if the field is a list, get all obj
                value = [
                    get_obj_from_json(obj=subvalue, cls=as_instance_of)
                    for subvalue in value
                ]
            else:
                # otherwise, get single obj
                value = get_obj_from_json(obj=value, cls=as_instance_of)
    return value


@public("hsml.util")
def get_obj_from_json(obj, cls):
    if obj is not None:
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, dict):
            return cls.from_json(obj)
        if isinstance(obj, Default):
            return cls()
        raise ValueError(
            f"Object of type {type(obj)} cannot be converted to class {cls}"
        )
    return obj


@public("hsml.util")
def feature_view_to_json(obj):
    if obj is None:
        return None
    import importlib.util

    if importlib.util.find_spec("hsfs"):
        from hsfs import feature_view

        if isinstance(obj, feature_view.FeatureView):
            import json

            import humps

            return humps.camelize(json.loads(obj.json()))
    return None


def generate_fully_qualified_feature_name(
    feature_group: feature_group.FeatureGroup, feature_name: str
):
    """Generate the fully qualified feature name for a feature.

    The fully qualified name is created by concatenating the project name, feature group name, feature group version and feature name.
    """
    return f"{feature_group._get_project_name()}_{feature_group.name}_{feature_group.version}_{feature_name}"


class AsyncTask:
    """Generic class to represent an async task.

    Args:
        func (Callable): The function to run asynchronously.
        requires_connection_pool (bool): Whether the task requires a connection pool.
        **kwargs: Key word arguments to be passed to the functions.

    Properties:
        result (Any): The result of the async task.
        event (threading.Event): The event that will be set when the async task is finished.
    """

    def __init__(
        self,
        task_function: Callable,
        task_args: tuple = (),
        requires_connection_pool=None,
        **kwargs,
    ):
        self.task_function = task_function
        self.task_args = task_args
        self.task_kwargs = kwargs
        self._event: threading.Event = threading.Event()
        self._result: Any = None
        self._requires_connection_pool = requires_connection_pool

    @property
    def result(self) -> Any:
        """The result of the async task."""
        return self._result

    @result.setter
    def result(self, value) -> None:
        self._result = value

    @property
    def event(self) -> threading.Event:
        """The event that will be set when the async task is finished."""
        return self._event

    @event.setter
    def event(self, value) -> None:
        self._event = value

    @property
    def requires_connection_pool(self) -> bool:
        """Whether the task requires a connection pool."""
        return self._requires_connection_pool


class AsyncTaskThread(threading.Thread):
    """Generic thread class that can be used to run async tasks in a separate thread.

    The thread will create its own event loop and run submitted tasks in that loop.

    The thread also store and fetches a connection pool that can be used by the async tasks.

    Parameters:
        connection_pool_initializer (Callable): A function that initializes a connection pool.
        connection_pool_params (Tuple): The parameters to pass to the connection pool initializer.
        *thread_args: Arguments to be passed to the thread.
        **thread_kwargs: Key word arguments to be passed to the thread.
        event_loop (asyncio.AbstractEventLoop): The event loop used by the thread.
        task_queue (queue.Queue[AsyncTask]): The queue used to submit tasks to the thread.
        connection_pool: The connection pool used
    """

    def __init__(
        self,
        connection_pool_initializer: Callable = None,
        connection_test: Callable = None,
        connection_pool_params: tuple = (),
        *thread_args,
        **thread_kwargs,
    ):
        super().__init__(*thread_args, **thread_kwargs)
        self._task_queue: queue.Queue[AsyncTask] = queue.Queue()
        self._event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self.stop_event = threading.Event()
        self._connection_pool_initializer: Callable = connection_pool_initializer
        self._connection_test_function: Callable = connection_test
        self._connection_pool_params: tuple = connection_pool_params
        self._connection_pool = None
        self.daemon = True  # Setting the thread as a daemon thread by default, so it will be terminated when the main thread is terminated.

    async def execute_task(self):
        """Execute the async tasks for the queue."""
        asyncio.set_event_loop(self._event_loop)

        while not self.stop_event.is_set():
            # Fetch a task from the queue.
            task = self.task_queue.get()
            # Run the task in the event loop and get the result
            try:
                if task.requires_connection_pool:
                    # Try checking connection to mysql and refresh it if required before running the task.
                    try:
                        await self._connection_test_function(self._connection_pool)
                    except Exception as e:
                        raise e

                    task.result = await task.task_function(
                        *task.task_args,
                        **task.task_kwargs,
                        connection_pool=self.connection_pool,
                    )
                else:
                    task.result = await task.task_function(
                        *task.task_args, **task.task_kwargs
                    )

                # Unblock the task, so the submit function can return the result.
                task.event.set()
            except Exception as e:
                task.result = e
                task.event.set()

    def stop(self):
        """Stop the thread and close the event loop."""
        self.stop_event.set()
        self._event_loop.stop()
        self._event_loop.close()

    def run(self):
        """Execute the async tasks for the queue."""
        asyncio.set_event_loop(self._event_loop)
        # Initialize the connection pool by using loop.run_until_complete to make sure the connection pool is initialized before the event loop starts running forever.
        if self._connection_pool_initializer:
            self._connection_pool = self._event_loop.run_until_complete(
                self._connection_pool_initializer(*self._connection_pool_params)
            )
        self._event_loop.create_task(self.execute_task())
        try:
            self._event_loop.run_forever()
        except Exception as e:
            print(
                f"An error occurred in the async task thread the event loop has been closed: {str(e)}"
            )
            self._event_loop.stop()
            self._event_loop.close()
            # raise e
        finally:
            self._event_loop.close()

    def submit(self, task: AsyncTask):
        """Submit a async task to the thread and block until the execution of the function is completed."""
        # Submit a task to the queue.
        self.task_queue.put(task)
        # Block the execution until the task is finished.
        task.event.wait()

        if isinstance(task.result, Exception):
            raise task.result
        # Return the result of the task.
        return task.result

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        """The event loop used by the thread."""
        return self._event_loop

    @property
    def task_queue(self) -> queue.Queue[AsyncTask]:
        """The queue used to submit tasks to the thread."""
        return self._task_queue

    @property
    def connection_pool(self):
        """The connection pool used by the thread."""
        return self._connection_pool
