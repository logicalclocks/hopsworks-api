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
from hsfs import ge_validation_result
from hsfs.core.constants import GE_MAJOR, HAS_GREAT_EXPECTATIONS


def _make_ge_expectation_configuration(expectation_type, kwargs, meta):
    """Construct a GE ExpectationConfiguration with version-correct kwargs."""
    if GE_MAJOR == 1:
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        return ExpectationConfiguration(type=expectation_type, kwargs=kwargs, meta=meta)
    import great_expectations

    return great_expectations.core.ExpectationConfiguration(
        expectation_type=expectation_type, kwargs=kwargs, meta=meta
    )


class TestValidationResult:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get"]["response"]

        # Act
        vr = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert vr.id == 11
        assert vr.success is True
        assert vr._observed_value == "test_observed_value"
        assert vr._expectation_id == 22
        assert vr._validation_report_id == 33
        assert vr.result == {"result_key": "result_value"}
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.exception_info == {"exception_info_key": "exception_info_value"}
        assert vr.expectation_config == {
            "expectation_config_key": "expectation_config_value"
        }

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get_basic_info"]["response"]

        # Act
        vr = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert vr.id is None
        assert vr.success is True
        assert vr._observed_value is None
        assert vr._expectation_id is None
        assert vr._validation_report_id is None
        assert vr.result == {"result_key": "result_value"}
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.exception_info == {"exception_info_key": "exception_info_value"}
        assert vr.expectation_config == {
            "expectation_config_key": "expectation_config_value"
        }

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get_list"]["response"]

        # Act
        vr_list = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert len(vr_list) == 1
        vr = vr_list[0]
        assert vr.id == 11
        assert vr.success is True
        assert vr._observed_value == "test_observed_value"
        assert vr._expectation_id == 22
        assert vr._validation_report_id == 33
        assert vr.result == {"result_key": "result_value"}
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.exception_info == {"exception_info_key": "exception_info_value"}
        assert vr.expectation_config == {
            "expectation_config_key": "expectation_config_value"
        }

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get_list_empty"]["response"]

        # Act
        vr_list = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert len(vr_list) == 0

    def test_timezone_support_validation_time(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["ge_validation_result"]["get"]["response"]
        json["validation_time"] = "2023-02-15T09:20:03.000414-05"

        # Act
        vr = ge_validation_result.ValidationResult.from_response_json(json)

        # Assert
        assert vr.id == 11
        assert vr.validation_time == 1676470803000

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_to_ge_type(self, backend_fixtures):
        import great_expectations

        # Arrange
        json = backend_fixtures["ge_validation_result"]["get"]["response"]
        vr = ge_validation_result.ValidationResult.from_response_json(json)

        # Act
        ge_result = vr.to_ge_type()

        # Assert
        assert isinstance(
            ge_result, great_expectations.core.ExpectationValidationResult
        )
        assert ge_result.success is True
        assert ge_result.result == {"result_key": "result_value"}
        assert ge_result.meta == {"meta_key": "meta_value"}
        assert ge_result.exception_info == {
            "exception_info_key": "exception_info_value"
        }
        assert ge_result.expectation_config == {
            "expectation_config_key": "expectation_config_value"
        }

    def test_to_dict_snapshot(self):
        import json as _json

        # Arrange
        vr = ge_validation_result.ValidationResult(
            success=True,
            result={"observed_value": 1},
            expectation_config={"expectation_type": "expect_column_to_exist"},
            exception_info={"raised_exception": False},
            meta={"k": "v"},
            id=11,
        )

        # Act
        d = vr.to_dict()

        # Assert
        # to_dict serializes exceptionInfo, expectationConfig, result, and meta
        # as JSON strings for the wire format.
        assert d == {
            "id": 11,
            "success": True,
            "exceptionInfo": _json.dumps({"raised_exception": False}),
            "expectationConfig": _json.dumps(
                {"expectation_type": "expect_column_to_exist"}
            ),
            "result": _json.dumps({"observed_value": 1}),
            "meta": _json.dumps({"k": "v"}),
        }

    def test_to_json_dict_snapshot(self):
        # Arrange
        vr = ge_validation_result.ValidationResult(
            success=True,
            result={"observed_value": 1},
            expectation_config={"expectation_type": "expect_column_to_exist"},
            exception_info={"raised_exception": False},
            meta={"k": "v"},
            id=11,
        )

        # Act
        d = vr.to_json_dict()

        # Assert
        # to_json_dict keeps every field as nested dicts.
        assert d == {
            "id": 11,
            "success": True,
            "exceptionInfo": {"raised_exception": False},
            "expectationConfig": {"expectation_type": "expect_column_to_exist"},
            "result": {"observed_value": 1},
            "meta": {"k": "v"},
        }

    @pytest.mark.skipif(
        GE_MAJOR != 1,
        reason="GE 1.x-specific normalization test",
    )
    def test_expectation_config_normalized_to_legacy_shape_from_ge_v1(self):
        # Arrange
        # The GE 1.x ExpectationValidationResult.to_json_dict() emits
        # {type, kwargs, meta, severity}. The frontend reads expectation_type
        # (legacy shape) - the SDK must rewrite the dict on the way in.
        ge_v1_shape = {
            "type": "expect_column_to_exist",
            "kwargs": {"column": "x"},
            "meta": {"k": "v"},
            "severity": "critical",
        }

        # Act
        vr = ge_validation_result.ValidationResult(
            success=True,
            result={"observed_value": 1},
            expectation_config=ge_v1_shape,
            exception_info={},
            meta={},
        )

        # Assert
        assert vr.expectation_config.get("expectation_type") == "expect_column_to_exist"
        assert "type" not in vr.expectation_config
        assert "severity" not in vr.expectation_config
        # to_dict emits the JSON-string form the wire uses; parse and recheck.
        import json as _json

        wire = _json.loads(vr.to_dict()["expectationConfig"])
        assert wire["expectation_type"] == "expect_column_to_exist"
        assert "type" not in wire
        assert "severity" not in wire

    def test_expectation_config_normalization_is_idempotent(self):
        # Already-legacy dicts must pass through unchanged.
        vr = ge_validation_result.ValidationResult(
            success=True,
            result={"observed_value": 1},
            expectation_config={
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "x"},
                "meta": {},
            },
            exception_info={},
            meta={},
        )
        assert vr.expectation_config == {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "x"},
            "meta": {},
        }

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_round_trip_via_ge(self):
        import great_expectations

        # Arrange
        original = great_expectations.core.ExpectationValidationResult(
            success=False,
            expectation_config=_make_ge_expectation_configuration(
                "expect_column_to_exist", {"column": "x"}, {}
            ),
            result={"observed_value": 7, "element_count": 10},
            exception_info={"raised_exception": False},
            meta={"k": "v"},
        )

        # Act
        hops = ge_validation_result.ValidationResult(**original.to_json_dict())
        rebuilt = hops.to_ge_type()

        # Assert
        assert rebuilt.success is False
        assert rebuilt.result == original.result
        assert rebuilt.meta == original.meta
        assert rebuilt.exception_info == original.exception_info
