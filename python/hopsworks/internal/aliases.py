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
from dataclasses import dataclass, field
from typing import Dict, Optional


class InternalAliasError(Exception):
    """Internal hopsworks exception related to aliases.

    Ideally, this exception should never happen, as it means misconfiguration of aliases, for example, if a public alias is requested for a method of a class.
    """


@dataclass
class Alias:
    from_module: str
    import_name: str
    in_modules: Dict[str, InModule] = field(default_factory=dict)

    @dataclass
    class InModule:
        in_module: str
        as_alias: Optional[str] = None
        deprecated: bool = False
        available_until: Optional[str] = None

        def get_id(self):
            res = self.in_module
            if self.as_alias:
                res += f" as {self.as_alias}"
            return res

        def update(self, other: Alias.InModule):
            self.deprecated |= other.deprecated
            if self.available_until:
                if self.available_until != other.available_until:
                    raise InternalAliasError(
                        "Deprecated alias is declared available until different releases."
                    )
            else:
                self.available_until = other.available_until

    def __post_init__(self):
        if "." in self.import_name:
            raise InternalAliasError(
                "Impossible to create alias for not importable symbol."
            )

    def add(self, *in_modules: InModule):
        for im in in_modules:
            self.in_modules.setdefault(im.get_id(), im).update(im)

    def get_id(self):
        return f"{self.from_module}.{self.import_name}"

    def update(self, other: Alias):
        for imid, im in other.in_modules.items():
            self.in_modules.setdefault(imid, im).update(im)


class Registry:
    def __new__(cls):
        return Registry

    aliases: Dict[str, Alias] = {}
    modules = {}

    @staticmethod
    def get_modules():
        for module, exclude, paths in Registry.modules.values():
            for name in set(x for x, _ in inspect.getmembers(module)) - exclude:
                alias = Alias(module.__name__, name)
                alias.add(*(Alias.InModule(p) for p in paths))
                Registry.add(alias)
        res = {}
        for alias in Registry.aliases.values():
            for im in alias.in_modules.values():
                from_import = alias.from_module, alias.import_name, im
                parts = im.in_module.split(".")
                for i in range(1, len(parts)):
                    res.setdefault(".".join(parts[:i]), [])
                res.setdefault(im.in_module, []).append(from_import)
        return res

    @staticmethod
    def add(*aliases):
        for alias in aliases:
            Registry.aliases.setdefault(alias.get_id(), alias).update(alias)

    @staticmethod
    def add_module(module, exclude, paths):
        Registry.modules[module.__name__] = module, exclude, paths


def public(
    *paths: str,
    as_alias: Optional[str] = None,
    deprecated: bool = False,
    available_until: Optional[str] = None,
):
    """Make a function or class publically available.

    If you want to publish a constant, use `publish`.

    # Arguments
        paths: the import paths under which the entity is publically avilable; effectively results in generation of aliases in all of the paths for the entity.
        as_alias: make the alias of the specified name.
        deprecated: make the alias deprected; use of the entity outside hopsworks will print a warning, saying that it is going to be removed from the public API in one of the future releases. See `deprecated` decorator for the implementation of construction of the deprecated objects.
        available_until: the first hopsworks release in which the entity will become unavailable, defaults to `None`; if the release is known, it is reoprted to the external user in the warning and `deprected` becomes set up.
    """

    if available_until:
        deprecated = True

    def decorator(symbol):
        if not hasattr(symbol, "__qualname__"):
            raise InternalAliasError("The symbol should be importable to be public.")
        alias = Alias(symbol.__module__, symbol.__qualname__)
        alias.add(
            *(Alias.InModule(p, as_alias, deprecated, available_until) for p in paths)
        )
        Registry.add(alias)
        return symbol

    return decorator


def publish(*paths: str):
    """Publish all of the names defined in this module after this call.

    Since `public` decorator works only for classes and functions, this context should be used for public constants.
    It is also useful for bulk publishing.
    Note that it is impossible to create an alias for a variable, i.e., it is impossible to make a change of a variable in one module to propogate to another variable in another module.

    In case you need to publish something from the begining of a module, use `Publisher`.

    If you need to deprecate an alias, use `public` instead.

    # Arguments
        paths: the import paths under which the names declared in this context will be publically available; effectively results in generation of aliases in all of the paths for all the names declared in the context.
    """

    caller = inspect.getmodule(inspect.stack()[1][0])
    exclude = set(x for x, _ in inspect.getmembers(caller))
    Registry.add_module(caller, exclude, paths)


class Publisher:
    """Publish all of the names defined inside this context.

    This class is intended for bulk publishing of entitities which are to be declared in the begining of a module, so that publish is not usable; in other cases, use publish instead.
    Note that it is impossible to create an alias for a variable, i.e., it is impossible to make a change of a variable in one module to propogate to another variable in another module.

    If you need to deprecate an alias, use `public` instead.

    # Arguments
        paths: the import paths under which the names declared in this context will be publically available; effectively results in generation of aliases in all of the paths for all the names declared in the context.
    """

    def __init__(self, *paths: str):
        self.paths = set(paths)
        self.caller = inspect.getmodule(inspect.stack()[1][0])

    def __enter__(self):
        self.exclude = set(x for x, _ in inspect.getmembers(self.caller))

    def __exit__(self, _exc_type, _exc_value, _traceback):
        for name in set(x for x, _ in inspect.getmembers(self.caller)) - self.exclude:
            alias = Alias(self.caller.__name__, name)
            alias.add(*(Alias.InModule(p) for p in self.paths))
            Registry.add(alias)


class DeprecatedCallWarning(Warning):
    pass


def deprecated(*, available_until: Optional[str] = None):
    """Create a deprecated version of a function or a class.

    Use of the entity outside hopsworks will print a warning, saying that it is going to be removed from the public API in one of the future releases.
    Therefore, do not use it on classes or functions used internally; it is a utility for creation of deprecated aliases.

    # Arguments
        available_until: the first hopsworks release in which the entity will become unavailable, defaults to `None`; if the release is known, it is reoprted to the external user in the warning.
    """

    v = f"version {available_until}" if available_until else "a future release"

    def deprecate(symbol):
        if inspect.isclass(symbol):
            methods = inspect.getmembers(symbol, predicate=inspect.isfunction)
            for name, value in methods:
                setattr(symbol, name, deprecate(value))
            return symbol
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
            raise InternalAliasError(
                "Deprecation of something else than class or function."
            )

    return deprecate
