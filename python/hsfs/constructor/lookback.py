#
#   Copyright 2026 Logical Clocks AB
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

from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common.util import convert_event_time_to_timestamp
from hsfs.decorators import typechecked


if TYPE_CHECKING:
    from datetime import date, datetime


EVENT_TIME = "event_time"
PARTITION_KEY = "partition_key"
_VALID_LOOKBACK_KEYS = (EVENT_TIME, PARTITION_KEY)

_UNIFORM_LOOKBACK_DICT_KEYS = frozenset({"key", "start", "end"})
_LOOKBACKS_DICT_KEYS = frozenset({"default", "feature_groups"})


@public
@typechecked
class Lookback:
    """A lookback window for PIT joins on a feature view.

    Bounds the rows of the joined feature group(s) that participate in the
    point-in-time join, so flyingduck and Spark can prune partitions before
    reading files.

    `key="partition_key"` emits the predicate against the FG's partition
    column — flyingduck and Spark prune partitions before reading files.
    `key="event_time"` emits against the event_time column — row-level
    correctness only; partition pruning is engine-dependent.

    Use this class for a uniform window across every FG; use
    [`Lookbacks`][hsfs.constructor.lookback.Lookbacks] for per-FG
    overrides.

    Example:
        ```python
        fv.get_batch_data(
            lookback=Lookback(
                key="partition_key",
                start=date(2026, 5, 5),
                end=date(2026, 5, 17),
            ),
        )
        ```
    """

    def __init__(
        self,
        key: str,
        start: date | datetime | int | str,
        end: date | datetime | int | str | None = None,
    ) -> None:
        if key not in _VALID_LOOKBACK_KEYS:
            raise ValueError(
                f"Lookback `key` must be one of {_VALID_LOOKBACK_KEYS!r}; got {key!r}."
            )
        if start is None:
            raise ValueError("Lookback `start` is required.")
        if end is not None:
            start_ms = convert_event_time_to_timestamp(start)
            end_ms = convert_event_time_to_timestamp(end)
            if start_ms >= end_ms:
                raise ValueError(
                    f"Lookback `start` ({start!r}) must be strictly earlier "
                    f"than `end` ({end!r})."
                )
        self._key = key
        self._start = start
        self._end = end

    @property
    def key(self) -> str:
        """Column the lookback predicate targets on each joined feature group."""
        return self._key

    @property
    def start(self) -> date | datetime | int | str:
        """Required lower bound of the lookback window.

        Holds whatever shape `__init__` accepted: a `date`/`datetime` for user-form
        construction, or epoch-millis `int` / ISO `str` after `from_response_json`
        reconstructs from the wire payload. `convert_event_time_to_timestamp` accepts
        all four shapes uniformly.
        """
        return self._start

    @property
    def end(self) -> date | datetime | int | str | None:
        """Optional upper bound; `None` emits a one-sided lower-bound predicate only.

        Same value-shape contract as `start`.
        """
        return self._end

    def to_dict(self) -> dict[str, Any]:
        """Return the wire-format payload for the backend.

        Bounds are emitted as epoch milliseconds; `key` is upper-cased to
        match the backend enum; `end` is omitted when not provided.

        Returns:
            The wire-format dict with keys `lookbackKey`, `start`, and
            optionally `end`.
        """
        payload: dict[str, Any] = {
            "lookbackKey": self._key.upper(),
            "start": convert_event_time_to_timestamp(self._start),
        }
        if self._end is not None:
            payload["end"] = convert_event_time_to_timestamp(self._end)
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | Lookback | None) -> Lookback | None:
        """Construct a `Lookback` from a user-form dict (or pass through an instance).

        Accepts `{"key", "start", "end"}`, an existing `Lookback`, or `None`.

        Args:
            value: A user-form dict, an existing `Lookback`, or `None`.

        Returns:
            A `Lookback` instance, or `None` if `value` was `None`.
        """
        if value is None or isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"Lookback expects a dict or `Lookback` instance; got {type(value)!r}."
            )
        return cls(
            key=value.get("key"),
            start=value.get("start"),
            end=value.get("end"),
        )

    @classmethod
    def from_response_json(cls, value: dict[str, Any] | None) -> Lookback | None:
        """Reconstruct a `Lookback` from the wire-format payload the backend echoes.

        `Query.from_response_json` uses this to restore `_lookbacks` when the
        materialization Spark job (or any cross-process consumer) rebuilds a
        Query from its serialized form.
        The decamelized wire dict uses `lookback_key` instead of `key`, and the
        reconstructed `key` is lower-cased to match the SDK contract.

        Args:
            value: The decamelized wire dict (`lookback_key`, `start`, `end`), or `None`.

        Returns:
            A `Lookback` instance, or `None` when `value` is `None` or its `lookback_key` is missing.
        """
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"Lookback.from_response_json expects a dict; got {type(value)!r}."
            )
        key = value.get("lookback_key") or value.get("key")
        if key is None:
            return None
        return cls(
            key=str(key).lower(),
            start=value.get("start"),
            end=value.get("end"),
        )


@public
@typechecked
class Lookbacks:
    """Lookback configuration with per-feature-group overrides.

    `default` applies to every feature group in the query (root and joined)
    that has no matching `feature_groups` entry; `feature_groups` overrides
    the default for the named feature groups. At least one of the two must
    be provided.

    In per-FG-only mode (no `default`), feature groups not listed receive
    no lookback and behave as they would with `lookback=None`.

    For a uniform lookback across the whole feature view, use
    [`Lookback`][hsfs.constructor.lookback.Lookback] directly.

    `feature_groups` keys identify each FG one of two ways:

    - `"name"` — matches every FG (root or joined) with that name, any
      version. Covers the common case where each FG appears once in the FV.
    - FG instance — matches every FG with the same `(name, version)`. Use
      this to disambiguate when two versions of the same FG appear in the FV.

    When a single key matches multiple join sites (e.g. the same FG joined
    with two prefixes), the same lookback is applied to all matches.

    When both shapes are supplied for the same name (a bare-string and an
    FG-instance key that resolve to the same FG), the exact-version entry
    takes precedence at its specific join site and the any-version entry
    applies elsewhere — letting users express "lookback A for every version
    of `dim_a`, except v1 which gets lookback B."

    FG-instance keys are not JSON-serializable; for config files / serialised
    payloads use string keys.

    All matching, default fallback, and FG-eligibility validation happen on
    the backend in `QueryController.resolveLookbacks`; this class is purely
    the SDK-side container and wire serializer.

    Example:
        ```python
        fv.get_batch_data(
            lookback=Lookbacks(
                default=Lookback(key="partition_key", start=date(2026, 5, 5)),
                feature_groups={
                    "transactions": Lookback(key="event_time",
                                             start=datetime(2026, 5, 1, tzinfo=timezone.utc)),
                },
            ),
        )
        ```
    """

    def __init__(
        self,
        default: Lookback | dict[str, Any] | None = None,
        feature_groups: dict[Any, Lookback | dict[str, Any]] | None = None,
    ) -> None:
        # Import done here to prevent circular dependencies
        from hsfs.feature_group import FeatureGroupBase

        self._default = Lookback.from_dict(default) if default is not None else None

        entries: dict[Any, Lookback] = {}
        for raw_key, raw_value in (feature_groups or {}).items():
            if not isinstance(raw_key, (str, FeatureGroupBase)):
                raise ValueError(
                    f"feature_groups key must be a str or FeatureGroup instance; "
                    f"got {type(raw_key).__name__}"
                )
            if raw_value is None:
                raise ValueError(
                    f"feature_groups[{raw_key!r}] must be a Lookback or dict; got None."
                )
            entries[raw_key] = Lookback.from_dict(raw_value)

        if self._default is None and not entries:
            raise ValueError(
                "Lookbacks requires at least one of `default` or `feature_groups` "
                "to be set."
            )
        self._feature_groups: dict[Any, Lookback] = entries

    @property
    def default(self) -> Lookback | None:
        """Uniform default applied to FGs without a `feature_groups` entry."""
        return self._default

    @property
    def feature_groups(self) -> dict[Any, Lookback]:
        """User-supplied key → `Lookback` override map (keys returned verbatim).

        Returns a copy to prevent external mutation of internal state.
        """
        return dict(self._feature_groups)

    def to_dict(self) -> dict[str, Any]:
        """Return the wire-format payload for the backend.

        Emits `{"defaultLookback": <LookbackDTO>, "featureGroups": [<entries>]}`.
        Each entry carries `{"name", "version", "lookback"}`; bare-string keys
        emit `version=None` (matches every version of the named FG on the
        backend side), while FG-instance keys emit the FG's pinned version.

        Returns:
            The wire-format dict with the keys `defaultLookback` and
            `featureGroups`; either may be omitted when not set.
        """
        payload: dict[str, Any] = {}
        if self._default is not None:
            payload["defaultLookback"] = self._default.to_dict()
        if self._feature_groups:
            payload["featureGroups"] = [
                {
                    "name": raw_key if isinstance(raw_key, str) else raw_key.name,
                    "version": None if isinstance(raw_key, str) else raw_key.version,
                    "lookback": lookback.to_dict(),
                }
                for raw_key, lookback in self._feature_groups.items()
            ]
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | Lookbacks | None) -> Lookbacks | None:
        """Construct a `Lookbacks` from its own user-form dict.

        Accepts `{"default": ..., "feature_groups": {...}}`, an existing
        `Lookbacks`, or `None`. For shapes that might be a uniform `Lookback`
        dict instead, use `Lookbacks.from_user_input`.

        Args:
            value: A `Lookbacks`-shaped user-form dict, an existing
                `Lookbacks`, or `None`.

        Returns:
            A `Lookbacks` instance, or `None` if `value` was `None`.
        """
        if value is None or isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"Lookbacks expects a dict or `Lookbacks` instance; got "
                f"{type(value)!r}."
            )
        return cls(
            default=value.get("default"),
            feature_groups=value.get("feature_groups"),
        )

    @classmethod
    def from_response_json(cls, value: dict[str, Any] | None) -> Lookbacks | None:
        """Reconstruct a `Lookbacks` from the wire-format payload the backend echoes.

        Per-feature-group entries are keyed by bare-string names because
        Feature Group instances are not available at deserialization time;
        version selection relies on the backend's `version=null` any-version
        semantics.

        Args:
            value: The decamelized wire dict (`default_lookback`, `feature_groups`), or `None`.

        Returns:
            A `Lookbacks` instance, or `None` when `value` is `None` or carries no usable entries.
        """
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"Lookbacks.from_response_json expects a dict; got {type(value)!r}."
            )
        default = Lookback.from_response_json(value.get("default_lookback"))
        feature_groups: dict[Any, Lookback] = {}
        for entry in value.get("feature_groups", []) or []:
            if not isinstance(entry, dict):
                continue
            lookback = Lookback.from_response_json(entry.get("lookback"))
            if lookback is None:
                continue
            name = entry.get("name")
            if name is None:
                continue
            feature_groups[name] = lookback
        if default is None and not feature_groups:
            return None
        return cls(default=default, feature_groups=feature_groups or None)

    @classmethod
    def from_user_input(
        cls,
        value: Lookback | Lookbacks | dict[str, Any] | None,
    ) -> Lookbacks | None:
        """Normalize any user-supplied `lookback=` argument to a `Lookbacks`.

        Accepts a `Lookback` (wrapped as `Lookbacks(default=value)`), a
        `Lookbacks` (passthrough), a dict in either uniform shape
        (`{key, start, end}`) or per-FG shape (`{default, feature_groups}`),
        or `None`. Callers past the public API boundary then deal with a
        single internal type.

        Args:
            value: A `Lookback`, `Lookbacks`, a user-form dict, or `None`.

        Returns:
            A `Lookbacks` instance, or `None` if `value` was `None`.
        """
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if isinstance(value, Lookback):
            return cls(default=value)
        if not isinstance(value, dict):
            raise TypeError(
                f"lookback must be Lookback, Lookbacks, dict, or None; "
                f"got {type(value).__name__}"
            )
        if not value:
            raise ValueError(
                "lookback dict cannot be empty; pass a uniform shape with keys "
                "{'key', 'start', 'end'} or a per-FG shape with keys "
                "{'default', 'feature_groups'}, or omit the parameter entirely"
            )
        input_keys = set(value)
        uniform_keys_present = input_keys & _UNIFORM_LOOKBACK_DICT_KEYS
        lookbacks_keys_present = input_keys & _LOOKBACKS_DICT_KEYS
        if uniform_keys_present and lookbacks_keys_present:
            raise ValueError(
                "lookback dict mixes uniform keys (key/start/end) with per-FG "
                "keys (default/feature_groups); pick one shape"
            )
        unknown_keys = input_keys - _UNIFORM_LOOKBACK_DICT_KEYS - _LOOKBACKS_DICT_KEYS
        if unknown_keys:
            raise ValueError(
                f"unknown lookback keys: {sorted(unknown_keys)}. "
                "Allowed: {key, start, end} or {default, feature_groups}"
            )
        if uniform_keys_present:
            return cls(default=Lookback.from_dict(value))
        return cls.from_dict(value)
