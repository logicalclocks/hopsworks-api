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

from datetime import date, datetime, timezone

import pytest
from hsfs.constructor.lookback import (
    EVENT_TIME,
    PARTITION_KEY,
    FeatureGroupLookback,
    Lookback,
)
from hsfs.feature_group import FeatureGroup


def _epoch_ms(d: date | datetime) -> int:
    if isinstance(d, datetime):
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return int(d.timestamp() * 1000)
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


# ---------- Fixtures -----------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_client(mocker):
    """Patch HTTP client + engine so `FeatureGroup` and `Query` construct cheaply.

    `FeatureGroupBase.__init__` instantiates `VariableApi`/`AlertsApi`, both of
    which look up the HTTP client. `Query.__init__` calls `engine.get_type()`.
    Mocking these avoids any network or engine-init work in unit tests.
    """
    mocker.patch("hopsworks_common.client.get_instance")
    mocker.patch("hsfs.engine.get_type", return_value="python")


def _fg(name, version=1, featurestore_id=99):
    """Construct a real `FeatureGroup` minimally."""
    return FeatureGroup(
        name, version, featurestore_id, id=hash((name, version)) & 0xFFFFFFFF
    )


# ---------- FeatureGroupLookback construction matrix --------------------------------------


class TestFeatureGroupLookbackConstruction:
    def test_partition_key_both_bounds(self):
        lb = FeatureGroupLookback(
            key=PARTITION_KEY,
            start=date(2026, 5, 10),
            end=date(2026, 5, 17),
        )
        assert lb.key == "PARTITION_KEY"
        assert lb.start == date(2026, 5, 10)
        assert lb.end == date(2026, 5, 17)

    def test_event_time_with_datetime_bounds(self):
        lb = FeatureGroupLookback(
            key=EVENT_TIME,
            start=datetime(2026, 5, 10, 0, 0, tzinfo=timezone.utc),
            end=datetime(2026, 5, 17, 12, 30, tzinfo=timezone.utc),
        )
        assert lb.key == "EVENT_TIME"

    def test_end_optional(self):
        lb = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 10))
        assert lb.end is None

    def test_mixed_date_and_datetime_bounds(self):
        lb = FeatureGroupLookback(
            key=PARTITION_KEY,
            start=date(2026, 5, 10),
            end=datetime(2026, 5, 17, 23, 59, tzinfo=timezone.utc),
        )
        assert lb.start == date(2026, 5, 10)
        assert isinstance(lb.end, datetime)

    def test_invalid_key(self):
        with pytest.raises(ValueError, match="`key` must be one of"):
            FeatureGroupLookback(key="bogus", start=date(2026, 5, 10))

    def test_key_is_case_insensitive(self):
        # The validation check accepts any casing; the stored `.key` is the
        # canonical upper-case form regardless of what the caller passed.
        for raw in ("partition_key", "Partition_Key", "PARTITION_KEY"):
            lb = FeatureGroupLookback(key=raw, start=date(2026, 5, 10))
            assert lb.key == PARTITION_KEY
        for raw in ("event_time", "Event_Time", "EVENT_TIME"):
            lb = FeatureGroupLookback(key=raw, start=date(2026, 5, 10))
            assert lb.key == EVENT_TIME

    def test_inverted_range_rejected(self):
        with pytest.raises(ValueError, match="must be strictly earlier than"):
            FeatureGroupLookback(
                key=PARTITION_KEY,
                start=date(2026, 5, 17),
                end=date(2026, 5, 10),
            )

    def test_equal_bounds_rejected(self):
        with pytest.raises(ValueError, match="must be strictly earlier than"):
            FeatureGroupLookback(
                key=PARTITION_KEY,
                start=date(2026, 5, 10),
                end=date(2026, 5, 10),
            )


# ---------- FeatureGroupLookback serializers ----------------------------------------------


class TestFeatureGroupLookbackToDict:
    def test_both_bounds_wire_format(self):
        lb = FeatureGroupLookback(
            key=PARTITION_KEY,
            start=date(2026, 5, 10),
            end=date(2026, 5, 17),
        )
        payload = lb.to_dict()
        assert payload["lookbackKey"] == "PARTITION_KEY"
        assert payload["start"] == _epoch_ms(date(2026, 5, 10))
        assert payload["end"] == _epoch_ms(date(2026, 5, 17))

    def test_end_omitted_from_payload(self):
        lb = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 10))
        payload = lb.to_dict()
        assert "end" not in payload
        assert payload["start"] == _epoch_ms(date(2026, 5, 10))


class TestFeatureGroupLookbackFromDict:
    def test_user_form_round_trip(self):
        rebuilt = FeatureGroupLookback.from_dict(
            {
                "key": "partition_key",
                "start": date(2026, 5, 10),
                "end": date(2026, 5, 17),
            }
        )
        assert rebuilt.key == "PARTITION_KEY"
        assert rebuilt.start == date(2026, 5, 10)
        assert rebuilt.end == date(2026, 5, 17)

    def test_passthrough_existing_instance(self):
        lb = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 10))
        assert FeatureGroupLookback.from_dict(lb) is lb

    def test_none_input(self):
        assert FeatureGroupLookback.from_dict(None) is None

    def test_rejects_non_dict_input(self):
        with pytest.raises(TypeError):
            FeatureGroupLookback.from_dict("not a dict")


# ---------- Lookback construction -----------------------------------------


class TestLookbackConstruction:
    def _lb(self):
        return FeatureGroupLookback(
            key=PARTITION_KEY, start=date(2026, 5, 5), end=date(2026, 5, 17)
        )

    def test_default_only(self):
        plan = Lookback(default=self._lb())
        assert plan.default is not None
        assert plan.feature_group_lookbacks == {}

    def test_feature_groups_only(self):
        plan = Lookback(feature_group_lookbacks={"transactions": self._lb()})
        assert plan.default is None
        assert "transactions" in plan.feature_group_lookbacks

    def test_both(self):
        plan = Lookback(
            default=self._lb(), feature_group_lookbacks={"transactions": self._lb()}
        )
        assert plan.default is not None
        assert "transactions" in plan.feature_group_lookbacks

    def test_empty_plan_rejected(self):
        with pytest.raises(
            ValueError, match="at least one of `default` or `feature_group_lookbacks`"
        ):
            Lookback()

    def test_empty_feature_groups_rejected(self):
        with pytest.raises(
            ValueError, match="at least one of `default` or `feature_group_lookbacks`"
        ):
            Lookback(feature_group_lookbacks={})

    def test_dict_values_in_feature_groups(self):
        plan = Lookback(
            feature_group_lookbacks={
                "transactions": {"key": "partition_key", "start": date(2026, 5, 5)}
            }
        )
        assert plan.feature_group_lookbacks["transactions"].key == "PARTITION_KEY"

    def test_dict_default(self):
        plan = Lookback(default={"key": "partition_key", "start": date(2026, 5, 5)})
        assert plan.default.key == "PARTITION_KEY"


# ---------- Key shape matrix ---------------------------------------------------


class TestKeyShapes:
    def _lb(self):
        return FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))

    def test_bare_string(self):
        # Bare-string key encodes "any version" on the wire (backend matches every
        # version of the named FG).
        plan = Lookback(feature_group_lookbacks={"dim_a": self._lb()})
        assert "dim_a" in plan.feature_group_lookbacks

    def test_fg_instance(self):
        # FG-instance key encodes the FG's specific (name, version) pair.
        fg = _fg("dim_a", 1)
        plan = Lookback(feature_group_lookbacks={fg: self._lb()})
        assert fg in plan.feature_group_lookbacks


# ---------- Malformed-key rejection -------------------------------------------


class TestMalformedKeys:
    def _lb(self):
        return FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))

    def test_none_key(self):
        with pytest.raises(ValueError, match="must be a str or FeatureGroup"):
            Lookback(feature_group_lookbacks={None: self._lb()})

    def test_int_key(self):
        with pytest.raises(ValueError, match="must be a str or FeatureGroup"):
            Lookback(feature_group_lookbacks={42: self._lb()})

    def test_tuple_key_rejected(self):
        with pytest.raises(ValueError, match="must be a str or FeatureGroup"):
            Lookback(feature_group_lookbacks={("dim_a", 1): self._lb()})

    def test_object_with_name_and_version_attrs_rejected(self):
        # Anything that isn't an actual FeatureGroupBase subclass must be rejected,
        # even if it happens to expose .name + .version.
        class _NotAnFG:
            name = "dim_a"
            version = 1

        with pytest.raises(ValueError, match="must be a str or FeatureGroup"):
            Lookback(feature_group_lookbacks={_NotAnFG(): self._lb()})

    def test_none_value_rejected(self):
        # A None value in feature_group_lookbacks must be rejected up-front rather than
        # silently stored (which would later crash to_dict with AttributeError).
        with pytest.raises(
            ValueError, match="must be a FeatureGroupLookback or dict; got None"
        ):
            Lookback(feature_group_lookbacks={"dim_a": None})


# ---------- Dict disambiguation -----------------------------------------------


class TestDictDisambiguation:
    def test_empty_dict_rejected(self):
        with pytest.raises(ValueError, match="lookback dict cannot be empty"):
            Lookback.from_user_input({})

    def test_mixed_keys_rejected(self):
        with pytest.raises(ValueError, match="mixes uniform keys"):
            Lookback.from_user_input(
                {
                    "key": "partition_key",
                    "feature_group_lookbacks": {"x": {"key": "event_time"}},
                }
            )

    def test_unknown_keys_rejected(self):
        with pytest.raises(ValueError, match="unknown lookback keys"):
            Lookback.from_user_input({"foo": "bar"})

    def test_leaf_dict_returns_lookbacks_with_default(self):
        # A uniform-shape dict is wrapped as Lookback(default=FeatureGroupLookback(...))
        # so internal callers always see a single type. Every field on the
        # inner FeatureGroupLookback must come through from the dict — this is the entry
        # point used by `FeatureView.get_batch_data(lookback={...})`.
        result = Lookback.from_user_input(
            {
                "key": "partition_key",
                "start": date(2026, 5, 5),
                "end": date(2026, 5, 17),
            }
        )
        assert isinstance(result, Lookback)
        assert isinstance(result.default, FeatureGroupLookback)
        assert result.default.key == "PARTITION_KEY"
        assert result.default.start == date(2026, 5, 5)
        assert result.default.end == date(2026, 5, 17)
        assert result.feature_group_lookbacks == {}

    def test_plan_dict_returns_lookbacks(self):
        result = Lookback.from_user_input(
            {
                "feature_group_lookbacks": {
                    "transactions": {"key": "partition_key", "start": date(2026, 5, 5)}
                }
            }
        )
        assert isinstance(result, Lookback)
        assert "transactions" in result.feature_group_lookbacks

    def test_none_returns_none(self):
        assert Lookback.from_user_input(None) is None

    def test_lookback_instance_wrapped_as_lookbacks(self):
        lb = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))
        result = Lookback.from_user_input(lb)
        assert isinstance(result, Lookback)
        assert result.default is lb
        assert result.feature_group_lookbacks == {}

    def test_lookbacks_instance_passthrough(self):
        plan = Lookback(
            default=FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))
        )
        assert Lookback.from_user_input(plan) is plan

    def test_non_dict_non_instance_rejected(self):
        with pytest.raises(TypeError, match="lookback must be"):
            Lookback.from_user_input(42)

    def test_fully_dict_input_constructs_inner_lookbacks(self):
        # Config-file style: a single deeply-nested dict with `default` and
        # `feature_group_lookbacks` as dicts and inner FeatureGroupLookback dicts as their values.
        # Every field must round-trip through to a real `FeatureGroupLookback` instance.
        result = Lookback.from_user_input(
            {
                "default": {
                    "key": "partition_key",
                    "start": date(2026, 5, 5),
                    "end": date(2026, 5, 17),
                },
                "feature_group_lookbacks": {
                    "dim_a": {
                        "key": "event_time",
                        "start": date(2026, 5, 10),
                    },
                },
            }
        )
        assert isinstance(result, Lookback)
        assert isinstance(result.default, FeatureGroupLookback)
        assert result.default.key == "PARTITION_KEY"
        assert result.default.start == date(2026, 5, 5)
        assert result.default.end == date(2026, 5, 17)
        dim_a_lb = result.feature_group_lookbacks["dim_a"]
        assert isinstance(dim_a_lb, FeatureGroupLookback)
        assert dim_a_lb.key == "EVENT_TIME"
        assert dim_a_lb.start == date(2026, 5, 10)
        assert dim_a_lb.end is None


# ---------- Lookback wire serialization --------------------------------------


class TestLookbackToDict:
    """Verifies the SDK→backend wire shape emitted by `Lookback.to_dict()`.

    The backend's `QueryController.resolveLookback` consumes this shape: it reads
    `defaultLookback` and walks `featureGroups`, matching entries to query nodes
    by `(name, version)` with `version=None` encoding "any version".
    """

    def _lb_dict(self, start, end=None):
        # Mirror FeatureGroupLookback.to_dict() so the assertion isn't tied to repr internals.
        return FeatureGroupLookback(key=PARTITION_KEY, start=start, end=end).to_dict()

    def test_default_only_emits_default_no_feature_groups(self):
        lb = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))
        wire = Lookback(default=lb).to_dict()
        assert wire == {"defaultLookback": lb.to_dict()}

    def test_bare_string_key_emits_null_version(self):
        # version=None on the wire tells the backend "any version of this name".
        lb = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))
        wire = Lookback(feature_group_lookbacks={"dim_a": lb}).to_dict()
        assert wire == {
            "featureGroupLookbacks": [
                {"name": "dim_a", "version": None, "lookback": lb.to_dict()},
            ],
        }

    def test_fg_instance_key_emits_specific_version(self):
        lb = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))
        fg = _fg("dim_a", 3)
        wire = Lookback(feature_group_lookbacks={fg: lb}).to_dict()
        assert wire == {
            "featureGroupLookbacks": [
                {"name": "dim_a", "version": 3, "lookback": lb.to_dict()},
            ],
        }

    def test_default_plus_feature_groups_emits_both(self):
        default = FeatureGroupLookback(key=PARTITION_KEY, start=date(2026, 5, 5))
        override = FeatureGroupLookback(key=EVENT_TIME, start=date(2026, 5, 10))
        wire = Lookback(
            default=default,
            feature_group_lookbacks={"dim_a": override},
        ).to_dict()
        assert wire == {
            "defaultLookback": default.to_dict(),
            "featureGroupLookbacks": [
                {"name": "dim_a", "version": None, "lookback": override.to_dict()},
            ],
        }


class TestFeatureGroupLookbackFromResponseJsonRoundTrip:
    """Cross-process reconstruction via the wire form.

    The Spark materialization job and any other cross-process consumer rebuilds a Query
    from its serialized payload via `Query.from_response_json`, which in turn calls
    `Lookback.from_response_json` / `FeatureGroupLookback.from_response_json`. The reconstructed
    `_start`/`_end` carry raw epoch-millis `int`s rather than the user-form
    `date`/`datetime` shapes — `to_dict()` must still round-trip the integer bounds
    losslessly because that's what the SDK re-sends to the backend.
    """

    def test_epoch_ms_int_bounds_survive_round_trip(self):
        start_ms = _epoch_ms(date(2026, 5, 10))
        end_ms = _epoch_ms(date(2026, 5, 17))
        wire = {"lookback_key": "PARTITION_KEY", "start": start_ms, "end": end_ms}
        lb = FeatureGroupLookback.from_response_json(wire)
        assert lb is not None
        # Properties expose the raw int (per widened type hints).
        assert lb.start == start_ms
        assert lb.end == end_ms
        assert lb.key == PARTITION_KEY
        # to_dict must re-emit the same epoch-ms ints (not strings, not None).
        payload = lb.to_dict()
        assert payload == {
            "lookbackKey": "PARTITION_KEY",
            "start": start_ms,
            "end": end_ms,
        }

    def test_lookbacks_round_trip_with_default_and_overrides(self):
        start_ms = _epoch_ms(date(2026, 5, 5))
        end_ms = _epoch_ms(date(2026, 5, 12))
        wire = {
            "default_lookback": {
                "lookback_key": "PARTITION_KEY",
                "start": start_ms,
                "end": end_ms,
            },
            "feature_group_lookbacks": [
                {
                    "name": "dim_a",
                    "version": None,
                    "lookback": {
                        "lookback_key": "EVENT_TIME",
                        "start": start_ms,
                    },
                },
            ],
        }
        looks = Lookback.from_response_json(wire)
        assert looks is not None
        # The reconstructed Lookback goes through to_dict cleanly — guards against
        # the same `_lookbacks is dict` AttributeError class as the TD constructor fix.
        payload = looks.to_dict()
        assert payload["defaultLookback"]["lookbackKey"] == "PARTITION_KEY"
        assert payload["defaultLookback"]["start"] == start_ms
        assert payload["defaultLookback"]["end"] == end_ms
        assert payload["featureGroupLookbacks"][0]["name"] == "dim_a"
        assert (
            payload["featureGroupLookbacks"][0]["lookback"]["lookbackKey"]
            == "EVENT_TIME"
        )
