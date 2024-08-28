#
#   Copyright 2024 Hopsworks AB
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

"""Automatic management of aliases.

The associated scripts are located in `python/aliases.py`.
"""

from __future__ import annotations

import functools
import inspect
import warnings
from typing import Optional, Tuple

from hopsworks.internal.exceptions import InternalError


_aliases = {}


def _aliases_add(from_import: Tuple[str, str], *paths: str):
    global _aliases
    if "." in from_import[1]:
        raise InternalError("Impossible to create alias for not importable symbol.")
    for p in paths:
        _aliases.setdefault(p, []).append(from_import)


def public(*paths: str):
    """Make a function or class publically available.

    If you want to publish a constant, use `publish`.
    Note that it is impossible to create an alias for a variable, i.e., it is impossible to make a change of a variable in one module to propogate to another variable in another module.

    # Arguments
        paths: the import paths under which the entity is publically avilable; effectively results in generation of aliases in all of the paths for the entity.
    """

    global publics

    def decorator(symbol):
        if not hasattr(symbol, "__qualname__"):
            raise InternalError("The symbol should be importable to be public.")
        _aliases_add((symbol.__module__, symbol.__qualname__), *paths)
        return symbol

    return decorator


def publish(name: str, *paths: str):
    """Make a constant publically available.

    Since `public` decorator works only for classes and functions, this function should be used for public constants.
    Note that it is impossible to create an alias for a variable, i.e., it is impossible to make a change of a variable in one module to propogate to another variable in another module.

    # Arguments
        name: name of the constant to be published.
        paths: the import paths under which the names declared in the current module will be publically available; effectively results in generation of aliases in all of the paths for all the names declared in the current module.
    """

    caller = inspect.getmodule(inspect.stack()[1][0])

    _aliases_add((caller.__name__, name), *paths)


class DeprecatedCallWarning(Warning):
    pass


def deprecated(*, available_until: Optional[str] = None):
    """Mark a function or class as deprecated.

    Use of the entity outside hopsworks will print a warning, saying that it is going to be removed from the public API in one of the future releases.

    # Arguments
        available_until: the first hopsworks release in which the entity will become unavailable, defaults to `None`; if the release is known, it is reoprted to the external user in the warning.
    """

    v = f"version {available_until}" if available_until else "a future release"

    def deprecate(symbol):
        if inspect.isclass(symbol):
            methods = inspect.getmembers(symbol, predicate=inspect.isfunction)
            for name, value in methods:
                setattr(symbol, name, deprecate(value))
        elif inspect.isfunction(symbol):

            @functools.wraps(symbol)
            def deprecated_f(*args, **kwargs):
                caller = inspect.getmodule(inspect.stack()[1][0])
                if not caller or not caller.__name__.startswith("hopsworks"):
                    warnings.warn(
                        f"Use of {symbol.__qualname__} is deprecated."
                        f"The function will be removed in {v} of hopsworks.",
                        DeprecatedCallWarning,
                        stacklevel=2,
                    )
                return symbol(*args, **kwargs)

            return deprecated_f
        else:
            raise InternalError("Deprecation of something else than class or function.")

    return deprecate
