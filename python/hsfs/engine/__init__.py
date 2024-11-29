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

from typing import TypeVar, Union

import hopsworks_common.connection
from hsfs.client import exceptions
from hsfs.engine import spark, spark_no_metastore


_engine = None
_engine_type = None


def init(engine_type: str) -> None:
    global _engine, _engine_type
    python_types = ["python", "training"]
    if _engine_type != engine_type:
        if engine_type in python_types and _engine_type in python_types:
            _engine_type = engine_type
        else:
            stop()
    if not _engine:
        if engine_type == "spark":
            _engine = spark.Engine()
        elif engine_type == "hive":
            raise ValueError(
                "Hive engine is not supported in hopsworks client version >= 4.0."
            )
        elif engine_type == "spark-no-metastore" or engine_type == "spark-delta":
            _engine = spark_no_metastore.Engine()
        elif engine_type in python_types:
            try:
                from hsfs.engine import python
            except ImportError as err:
                raise exceptions.FeatureStoreException(
                    "Trying to instantiate Python as engine, but 'python' extras are "
                    "missing in HSFS installation. Install with `pip install "
                    "hsfs[python]`."
                ) from err
            _engine = python.Engine()
        if _engine:
            _engine_type = engine_type


def get_instance() -> (
    Union[spark.Engine, spark_no_metastore.Engine, TypeVar("python.Engine")]
):
    init(hopsworks_common.connection._hsfs_engine_type)
    return _engine


# Used for testing
def set_instance(
    engine_type: str,
    engine: Union[spark.Engine, spark_no_metastore.Engine, TypeVar("python.Engine")],
) -> None:
    global _engine
    hopsworks_common.connection._hsfs_engine_type = engine_type
    _engine = engine


def get_type() -> str:
    if _engine:
        return hopsworks_common.connection._hsfs_engine_type
    raise Exception("Couldn't find execution engine. Try reconnecting to Hopsworks.")


def stop() -> None:
    global _engine
    from hsfs.core import arrow_flight_client

    _engine = None
    arrow_flight_client.close()
