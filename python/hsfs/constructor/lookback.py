#
#   Copyright 2026 Hopsworks AB
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


EVENT_TIME = "EVENT_TIME"
PARTITION_KEY = "PARTITION_KEY"
_VALID_LOOKBACK_KEYS = (EVENT_TIME, PARTITION_KEY)

_UNIFORM_LOOKBACK_DICT_KEYS = frozenset({"key", "start", "end"})
_LOOKBACK_DICT_KEYS = frozenset({"default", "feature_group_lookbacks"})


@public
@typechecked
class FeatureGroupLookback:
    """A lookback window for point-in-time joins on a feature view.

    A lookback bounds how far back in time the joined feature group(s) may
    contribute rows to a point-in-time join, so partition scans stay bounded
    as feature groups grow.

    Use `key="PARTITION_KEY"` when the feature group is partitioned by a DATE
    column aligned with the lookback bounds; the storage engine then prunes
    whole partitions before reading any files.
    Use `key="EVENT_TIME"` to bound on the event-time column; the result is
    always correct, but whether the engine can also prune files depends on
    the storage format.

    Use this class for a uniform window across every feature group in the
    view; use [`Lookback`][hsfs.constructor.lookback.Lookback] when
    different feature groups need different windows.

    Example:
        ```python
        fv.get_batch_data(
            lookback=FeatureGroupLookback(
                key="PARTITION_KEY",
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
        normalized_key = key.upper() if isinstance(key, str) else key
        if normalized_key not in _VALID_LOOKBACK_KEYS:
            raise ValueError(
                f"FeatureGroupLookback `key` must be one of {_VALID_LOOKBACK_KEYS!r} "
                f"(case-insensitive); got {key!r}."
            )
        if start is None:
            raise ValueError("FeatureGroupLookback `start` is required.")
        if end is not None:
            start_ms = convert_event_time_to_timestamp(start)
            end_ms = convert_event_time_to_timestamp(end)
            if start_ms >= end_ms:
                raise ValueError(
                    f"FeatureGroupLookback `start` ({start!r}) must be strictly earlier "
                    f"than `end` ({end!r})."
                )
        self._key = normalized_key
        self._start = start
        self._end = end

    @public
    @property
    def key(self) -> str:
        """Column the lookback predicate targets on each joined feature group.

        One of `"PARTITION_KEY"` or `"EVENT_TIME"`.
        """
        return self._key

    @public
    @property
    def start(self) -> date | datetime | int | str:
        """Lower bound of the lookback window (inclusive)."""
        return self._start

    @public
    @property
    def end(self) -> date | datetime | int | str | None:
        """Upper bound of the lookback window (inclusive), or `None` for an open-ended upper side."""
        return self._end

    def to_dict(self) -> dict[str, Any]:
        """Serialize this lookback to a JSON-compatible dict.

        Returns:
            A dict carrying the lookback's key, start, and (when set) end.
        """
        payload: dict[str, Any] = {
            "lookbackKey": self._key,
            "start": convert_event_time_to_timestamp(self._start),
        }
        if self._end is not None:
            payload["end"] = convert_event_time_to_timestamp(self._end)
        return payload

    @classmethod
    def from_dict(
        cls, value: dict[str, Any] | FeatureGroupLookback | None
    ) -> FeatureGroupLookback | None:
        """Build a `FeatureGroupLookback` from a `{"key", "start", "end"}` dict.

        An existing `FeatureGroupLookback` is returned unchanged; `None` is returned as
        `None`.

        Parameters:
            value:
                A `{"key", "start", "end"}` dict, an existing `FeatureGroupLookback`, or `None`.

        Returns:
            A `FeatureGroupLookback` instance, or `None` if `value` was `None`.
        """
        if value is None or isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"FeatureGroupLookback expects a dict or `FeatureGroupLookback` instance; got {type(value)!r}."
            )
        return cls(
            key=value.get("key"),
            start=value.get("start"),
            end=value.get("end"),
        )

    @classmethod
    def from_response_json(
        cls, value: dict[str, Any] | None
    ) -> FeatureGroupLookback | None:
        """Reconstruct a `FeatureGroupLookback` from the payload returned by the Hopsworks backend.

        Parameters:
            value:
                The payload dict, or `None`.

        Returns:
            A `FeatureGroupLookback` instance, or `None` if `value` was `None` or the key field was missing.
        """
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"FeatureGroupLookback.from_response_json expects a dict; got {type(value)!r}."
            )
        key = value.get("lookback_key") or value.get("key")
        if key is None:
            return None
        return cls(
            key=key,
            start=value.get("start"),
            end=value.get("end"),
        )


@public
@typechecked
class Lookback:
    """Per-feature-group lookback windows for a point-in-time-joined feature view.

    `default` applies to every feature group in the view (root and joined)
    that does not have a matching `feature_group_lookbacks` override.
    `feature_group_lookbacks` maps a specific feature group to its own lookback,
    which wins over the default for that feature group.
    At least one of `default` or `feature_group_lookbacks` must be set.

    If `default` is omitted, feature groups not listed in `feature_group_lookbacks`
    are read without any lookback (same as not passing `lookback=...`).

    Use [`FeatureGroupLookback`][hsfs.constructor.lookback.FeatureGroupLookback] directly for a
    uniform window across every feature group in the view.

    `feature_group_lookbacks` keys identify the feature group in one of two ways:

    - `"name"` — matches every feature group with that name, regardless of
      version. Covers the common case where each feature group appears once
      in the view.
    - Feature group instance — matches a specific `(name, version)`. Use
      this when the view joins two versions of the same feature group.

    If you supply both a name string and a feature group instance that
    resolve to the same name, the instance entry wins for its specific
    version and the name entry applies to the other versions.

    Feature group instances are not JSON-serializable; if you need to
    persist this configuration, use name strings as keys.

    Example:
        ```python
        fv.get_batch_data(
            lookback=Lookback(
                default=FeatureGroupLookback(key="PARTITION_KEY", start=date(2026, 5, 5)),
                feature_group_lookbacks={
                    "transactions": FeatureGroupLookback(key="EVENT_TIME",
                                             start=datetime(2026, 5, 1, tzinfo=timezone.utc)),
                },
            ),
        )
        ```
    """

    def __init__(
        self,
        default: FeatureGroupLookback | dict[str, Any] | None = None,
        feature_group_lookbacks: dict[Any, FeatureGroupLookback | dict[str, Any]]
        | None = None,
    ) -> None:
        # Import done here to prevent circular dependencies
        from hsfs.feature_group import FeatureGroupBase

        self._default = (
            FeatureGroupLookback.from_dict(default) if default is not None else None
        )

        entries: dict[Any, FeatureGroupLookback] = {}
        for raw_key, raw_value in (feature_group_lookbacks or {}).items():
            if not isinstance(raw_key, (str, FeatureGroupBase)):
                raise ValueError(
                    f"feature_group_lookbacks key must be a str or FeatureGroup instance; "
                    f"got {type(raw_key).__name__}"
                )
            if raw_value is None:
                raise ValueError(
                    f"feature_group_lookbacks[{raw_key!r}] must be a FeatureGroupLookback or dict; got None."
                )
            entries[raw_key] = FeatureGroupLookback.from_dict(raw_value)

        if self._default is None and not entries:
            raise ValueError(
                "Lookback requires at least one of `default` or `feature_group_lookbacks` "
                "to be set."
            )
        self._feature_groups: dict[Any, FeatureGroupLookback] = entries

    @public
    @property
    def default(self) -> FeatureGroupLookback | None:
        """FeatureGroupLookback applied to every feature group without a `feature_group_lookbacks` override."""
        return self._default

    @public
    @property
    def feature_group_lookbacks(self) -> dict[Any, FeatureGroupLookback]:
        """Per-feature-group overrides keyed by feature group name or instance."""
        return dict(self._feature_groups)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this configuration to a JSON-compatible dict.

        Returns:
            A dict carrying the default lookback (when set) and the
            per-feature-group overrides (when any are set).
        """
        payload: dict[str, Any] = {}
        if self._default is not None:
            payload["defaultLookback"] = self._default.to_dict()
        if self._feature_groups:
            payload["featureGroupLookbacks"] = [
                {
                    "name": raw_key if isinstance(raw_key, str) else raw_key.name,
                    "version": None if isinstance(raw_key, str) else raw_key.version,
                    "lookback": lookback.to_dict(),
                }
                for raw_key, lookback in self._feature_groups.items()
            ]
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | Lookback | None) -> Lookback | None:
        """Build a `Lookback` from a `{"default", "feature_group_lookbacks"}` dict.

        An existing `Lookback` is returned unchanged; `None` is returned as
        `None`.
        If the dict shape might also be a uniform single-window payload
        (`{key, start, end}`), use
        [`from_user_input`][hsfs.constructor.lookback.Lookback.from_user_input]
        instead, which accepts both shapes.

        Parameters:
            value:
                A `{"default", "feature_group_lookbacks"}` dict, an existing
                `Lookback`, or `None`.

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
            default=value.get("default"),
            feature_group_lookbacks=value.get("feature_group_lookbacks"),
        )

    @classmethod
    def from_response_json(cls, value: dict[str, Any] | None) -> Lookback | None:
        """Reconstruct a `Lookback` from the payload returned by the Hopsworks backend.

        Per-feature-group overrides come back keyed by name string only,
        since feature group instances are not available when deserialising.

        Parameters:
            value:
                The payload dict, or `None`.

        Returns:
            A `Lookback` instance, or `None` if `value` was `None` or carried no usable entries.
        """
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise TypeError(
                f"Lookback.from_response_json expects a dict; got {type(value)!r}."
            )
        default = FeatureGroupLookback.from_response_json(value.get("default_lookback"))
        feature_group_lookbacks: dict[Any, FeatureGroupLookback] = {}
        for entry in value.get("feature_group_lookbacks", []) or []:
            if not isinstance(entry, dict):
                continue
            lookback = FeatureGroupLookback.from_response_json(entry.get("lookback"))
            if lookback is None:
                continue
            name = entry.get("name")
            if name is None:
                continue
            feature_group_lookbacks[name] = lookback
        if default is None and not feature_group_lookbacks:
            return None
        return cls(
            default=default, feature_group_lookbacks=feature_group_lookbacks or None
        )

    @classmethod
    def from_user_input(
        cls,
        value: FeatureGroupLookback | Lookback | dict[str, Any] | None,
    ) -> Lookback | None:
        """Normalize any value accepted by the `lookback=` parameter to a `Lookback`.

        A `FeatureGroupLookback` is wrapped as a uniform default; a `Lookback` is
        returned unchanged; a dict is interpreted as either the uniform
        single-window shape (`{"key", "start", "end"}`) or the per-
        feature-group shape (`{"default", "feature_group_lookbacks"}`); `None` is
        returned as `None`.
        Mixing keys from both dict shapes is rejected.

        Parameters:
            value:
                A `FeatureGroupLookback`, `Lookback`, dict, or `None`.

        Returns:
            A `Lookback` instance, or `None` if `value` was `None`.
        """
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if isinstance(value, FeatureGroupLookback):
            return cls(default=value)
        if not isinstance(value, dict):
            raise TypeError(
                f"lookback must be FeatureGroupLookback, Lookback, dict, or None; "
                f"got {type(value).__name__}"
            )
        if not value:
            raise ValueError(
                "lookback dict cannot be empty; pass a uniform shape with keys "
                "{'key', 'start', 'end'} or a per-FG shape with keys "
                "{'default', 'feature_group_lookbacks'}, or omit the parameter entirely"
            )
        input_keys = set(value)
        uniform_keys_present = input_keys & _UNIFORM_LOOKBACK_DICT_KEYS
        lookback_keys_present = input_keys & _LOOKBACK_DICT_KEYS
        if uniform_keys_present and lookback_keys_present:
            raise ValueError(
                "lookback dict mixes uniform keys (key/start/end) with per-FG "
                "keys (default/feature_group_lookbacks); pick one shape"
            )
        unknown_keys = input_keys - _UNIFORM_LOOKBACK_DICT_KEYS - _LOOKBACK_DICT_KEYS
        if unknown_keys:
            raise ValueError(
                f"unknown lookback keys: {sorted(unknown_keys)}. "
                "Allowed: {key, start, end} or {default, feature_group_lookbacks}"
            )
        if uniform_keys_present:
            return cls(default=FeatureGroupLookback.from_dict(value))
        return cls.from_dict(value)
