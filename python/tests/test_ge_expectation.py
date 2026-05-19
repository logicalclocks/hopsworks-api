#
#   Copyright 2022 Hopsworks AB
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
import pytest
from hsfs import ge_expectation
from hsfs.core.constants import GE_MAJOR, HAS_GREAT_EXPECTATIONS


def _ge_expectation_configuration():
    """Return the ExpectationConfiguration class for the installed GE version.

    GE 1.x moved it out of ``great_expectations.core``.
    """
    if GE_MAJOR == 1:
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        return ExpectationConfiguration
    import great_expectations

    return great_expectations.core.ExpectationConfiguration


def _make_ge_expectation_configuration(expectation_type, kwargs, meta):
    """Construct a GE ExpectationConfiguration with version-correct kwargs."""
    cls = _ge_expectation_configuration()
    if GE_MAJOR == 1:
        return cls(type=expectation_type, kwargs=kwargs, meta=meta)
    return cls(expectation_type=expectation_type, kwargs=kwargs, meta=meta)


def _expectation_type_attr(ge_object):
    """Return the expectation type across GE versions and object types.

    GE 1.x exposes ``type`` on ``ExpectationConfiguration`` and
    ``expectation_type`` on ``Expectation`` subclasses; GE 0.x exposes
    ``expectation_type`` everywhere.
    """
    return getattr(ge_object, "expectation_type", None) or getattr(
        ge_object, "type", None
    )


class TestGeExpectation:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get"]["response"]

        # Act
        expect = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        print(expect.meta)
        assert expect.expectation_type == "1"
        assert expect.kwargs == {"kwargs_key": "kwargs_value"}
        assert expect.meta["meta_key"] == "meta_value"
        assert expect.meta["expectationId"] == 32
        assert expect.id == 32

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get_list"]["response"]

        # Act
        ge_list = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        assert len(ge_list) == 1
        expect = ge_list[0]
        print(expect.meta)
        assert expect.expectation_type == "1"
        assert expect.kwargs == {"kwargs_key": "kwargs_value"}
        assert expect.meta["meta_key"] == "meta_value"
        assert expect.meta["expectationId"] == 32
        assert expect.id == 32

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get_list_empty"]["response"]

        # Act
        ge_list = ge_expectation.GeExpectation.from_response_json(json)

        # Assert
        assert len(ge_list) == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_from_ge_object(self):
        # Arrange
        expectationId = 32
        expectation_type = "expect_column_min_to_be_between"
        kwargs = {"kwargs_key": "kwargs_value"}
        meta = {"meta_key": "meta_value", "expectationId": expectationId}
        ge_object = _make_ge_expectation_configuration(expectation_type, kwargs, meta)

        # Act
        expect = ge_expectation.GeExpectation.from_ge_type(ge_object)

        # Assert
        assert expect.id == 32
        assert expect.expectation_type == expectation_type
        assert expect.kwargs == kwargs
        assert expect.meta == meta

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_to_ge_type(self):
        # Arrange
        expect = ge_expectation.GeExpectation(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "feature_a"},
            meta={"meta_key": "meta_value", "expectationId": 7},
            id=7,
        )

        # Act
        ge_object = expect.to_ge_type()

        # Assert
        assert isinstance(ge_object, _ge_expectation_configuration())
        assert _expectation_type_attr(ge_object) == "expect_column_to_exist"
        assert ge_object.kwargs == {"column": "feature_a"}
        assert ge_object.meta == {"meta_key": "meta_value", "expectationId": 7}

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_round_trip_ge_to_hopsworks_to_ge(self):
        # Arrange
        original = _make_ge_expectation_configuration(
            "expect_column_min_to_be_between",
            {"column": "x", "min_value": 0, "max_value": 1},
            {"meta_key": "meta_value", "expectationId": 32},
        )

        # Act
        hops = ge_expectation.GeExpectation.from_ge_type(original)
        rebuilt = hops.to_ge_type()

        # Assert
        assert _expectation_type_attr(rebuilt) == _expectation_type_attr(original)
        assert rebuilt.kwargs == original.kwargs
        assert rebuilt.meta == original.meta

    def test_round_trip_response_json_preserves_fields(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_expectation"]["get"]["response"]

        # Act
        expect = ge_expectation.GeExpectation.from_response_json(json)
        emitted = expect.to_json_dict()

        # Assert
        # to_json_dict emits camelCase that the backend will re-decamelize on read.
        assert emitted == {
            "id": 32,
            "expectationType": "1",
            "kwargs": {"kwargs_key": "kwargs_value"},
            "meta": {"meta_key": "meta_value", "expectationId": 32},
        }

    def test_to_dict_snapshot(self):
        import json as _json

        # Arrange
        expect = ge_expectation.GeExpectation(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "feature_a"},
            meta={"meta_key": "meta_value", "expectationId": 7},
            id=7,
        )

        # Act
        d = expect.to_dict()

        # Assert
        # to_dict serializes kwargs and meta as JSON strings for the wire format.
        assert d == {
            "id": 7,
            "expectationType": "expect_column_to_exist",
            "kwargs": _json.dumps({"column": "feature_a"}),
            "meta": _json.dumps({"meta_key": "meta_value", "expectationId": 7}),
        }

    def test_to_dict_snapshot_without_id(self):
        import json as _json

        # Arrange
        expect = ge_expectation.GeExpectation(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "feature_a"},
            meta={"meta_key": "meta_value"},
        )

        # Act
        d = expect.to_dict()

        # Assert
        assert d == {
            "id": None,
            "expectationType": "expect_column_to_exist",
            "kwargs": _json.dumps({"column": "feature_a"}),
            "meta": _json.dumps({"meta_key": "meta_value"}),
        }

    def test_to_json_dict_snapshot(self):
        # Arrange
        expect = ge_expectation.GeExpectation(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "feature_a"},
            meta={"meta_key": "meta_value", "expectationId": 7},
            id=7,
        )

        # Act
        d = expect.to_json_dict()

        # Assert
        # to_json_dict keeps kwargs and meta as nested dicts (no JSON encoding).
        assert d == {
            "id": 7,
            "expectationType": "expect_column_to_exist",
            "kwargs": {"column": "feature_a"},
            "meta": {"meta_key": "meta_value", "expectationId": 7},
        }

    def test_to_json_dict_decamelized_snapshot(self):
        # Arrange
        expect = ge_expectation.GeExpectation(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "feature_a"},
            meta={"expectationId": 7},
            id=7,
        )

        # Act
        d = expect.to_json_dict(decamelize=True)

        # Assert
        # decamelize=True converts camelCase keys to snake_case.
        assert d == {
            "id": 7,
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "feature_a"},
            "meta": {"expectation_id": 7},
        }
