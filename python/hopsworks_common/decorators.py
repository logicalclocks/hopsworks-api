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

import functools
import importlib
import os

from hopsworks_common.core.constants import (
    HAS_CONFLUENT_KAFKA,
    HAS_GREAT_EXPECTATIONS,
    HAS_POLARS,
    confluent_kafka_not_installed_message,
    great_expectations_not_installed_message,
    polars_not_installed_message,
)


def not_connected(fn):
    @functools.wraps(fn)
    def if_not_connected(inst, *args, **kwargs):
        if inst._connected:
            raise HopsworksConnectionError
        return fn(inst, *args, **kwargs)

    return if_not_connected


def connected(fn):
    @functools.wraps(fn)
    def if_connected(inst, *args, **kwargs):
        if not inst._connected:
            raise NoHopsworksConnectionError
        return fn(inst, *args, **kwargs)

    return if_connected


def catch_not_found(*class_import_paths, fallback_return=None):
    def decorator(f):
        @functools.wraps(f)
        def g(*args, **kwds):
            # Needs to be imported inside function to avoid circular dependency
            from hopsworks.client.exceptions import RestAPIError
            not_found_error_codes = []
            for class_import_path in class_import_paths:
                class_index = class_import_path.rfind(".")
                module_path = class_import_path[:class_index]
                module = importlib.import_module(module_path)
                object_class = getattr(module, class_import_path[class_index + 1 :])
                not_found_error_codes.append(object_class.NOT_FOUND_ERROR_CODE)
            try:
                return f(*args, **kwds)
            except RestAPIError as e:
                if (
                    e.response.status_code in [400, 404]
                    and e.response.json().get("errorCode", "") in not_found_error_codes
                ):
                    return fallback_return
                else:
                    raise e

        return g

    return decorator


class HopsworksConnectionError(Exception):
    """Thrown when attempted to change connection attributes while connected."""

    def __init__(self):
        super().__init__(
            "Connection is currently in use. Needs to be closed for modification."
        )


class NoHopsworksConnectionError(Exception):
    """Thrown when attempted to perform operation on connection while not connected."""

    def __init__(self):
        super().__init__(
            "Connection is not active. Needs to be connected for feature store operations."
        )


if os.environ.get("HOPSWORKS_RUN_WITH_TYPECHECK", False):
    from typeguard import typechecked
else:
    from typing import TypeVar

    _T = TypeVar("_T")

    def typechecked(
        target: _T,
    ) -> _T:
        return target if target else typechecked


def uses_great_expectations(f):
    @functools.wraps(f)
    def g(*args, **kwds):
        if not HAS_GREAT_EXPECTATIONS:
            raise ModuleNotFoundError(great_expectations_not_installed_message)
        return f(*args, **kwds)

    return g


def uses_polars(f):
    @functools.wraps(f)
    def g(*args, **kwds):
        if not HAS_POLARS:
            raise ModuleNotFoundError(polars_not_installed_message)
        return f(*args, **kwds)

    return g


def uses_confluent_kafka(f):
    @functools.wraps(f)
    def g(*args, **kwds):
        if not HAS_CONFLUENT_KAFKA:
            raise ModuleNotFoundError(confluent_kafka_not_installed_message)
        return f(*args, **kwds)

    return g
