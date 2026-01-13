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

"""Automatic management of aliases.

The associated scripts are located in `python/aliases.py`.
"""

from __future__ import annotations

import functools
import inspect
import warnings


class InternalAliasError(Exception):
    """Internal hopsworks exception related to aliases.

    Ideally, this exception should never happen, as it means misconfiguration of aliases, for example, if a public alias is requested for a method of a class.
    """


def public(
    *paths: str,
    as_alias: str | None = None,
    deprecated_by: set[str] | str | None = None,
    available_until: str | None = None,
):
    """Make a function or class publically available.

    If you want to publish a constant, use `publish`.

    Parameters:
        paths: The import paths under which the entity is publically available; effectively results in generation of aliases in all of the paths for the entity.
        as_alias: Make the alias of the specified name.
        deprecated_by:
            Make the alias deprecated and provide a set of recommendations to use instead; use of the entity outside hopsworks will print a warning, saying that it is going to be removed from the public API in one of the future releases.
            See `deprecated` decorator for the implementation of construction of the deprecated objects.
        available_until: The first hopsworks release in which the entity will become unavailable, defaults to `None`; if the release is known, it is reported to the external user in the deprecation warning.
    """
    # The decorator is implemented in hopsworks-aliases setuptools plugin.
    return lambda symbol: symbol


class HopsworksDeprecationWarning(DeprecationWarning):
    pass


def deprecated(
    *deprecated_by: str,
    available_until: str | None = None,
):
    """Create a deprecated version of a function or a class.

    Use of the entity outside hopsworks will print a warning, saying that it is going to be removed from the public API in one of the future releases.
    Therefore, do not use it on classes or functions used internally; it is a utility for creation of deprecated aliases.

    Parameters:
        deprecated_by: A set of recommendations to use instead.
        available_until: The first hopsworks release in which the entity will become unavailable, defaults to `None`; if the release is known, it is reported to the external user in the warning.
    """
    v = f"version {available_until}" if available_until else "a future release"
    if len(deprecated_by) == 1:
        recs = deprecated_by[0]
    elif len(deprecated_by) == 2:
        recs = f"{deprecated_by[0]} or {deprecated_by[1]}"
    else:
        recs = f"{', '.join(deprecated_by[:-1])}, or {deprecated_by[-1]}"

    def deprecate(symbol):
        if inspect.isclass(symbol):
            methods = inspect.getmembers(symbol, predicate=inspect.isfunction)
            for name, value in methods:
                setattr(symbol, name, deprecate(value))
            return symbol
        if inspect.isfunction(symbol):

            @functools.wraps(symbol)
            def deprecated_f(*args, **kwargs):
                caller = inspect.getmodule(inspect.stack()[1][0])
                if not caller or not caller.__name__.startswith("hopsworks"):
                    warnings.warn(
                        f"{symbol.__qualname__} is deprecated."
                        f" The function will be removed in {v} of hopsworks."
                        f" Consider using {recs} instead.",
                        HopsworksDeprecationWarning,
                        stacklevel=2,
                    )
                return symbol(*args, **kwargs)

            return deprecated_f
        raise InternalAliasError(
            "Deprecation of something else than class or function."
        )

    return deprecate
