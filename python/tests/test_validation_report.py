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
from hsfs import ge_validation_result, validation_report
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


class TestValidationReport:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get"]["response"]

        # Act
        vr = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert vr.id == 11
        assert vr.success is True
        assert vr._full_report_path == "test_full_report_path"
        assert vr._validation_time == "test_validation_time"
        assert vr._featurestore_id == "test_featurestore_id"
        assert vr._featuregroup_id == "test_featuregroup_id"
        assert vr.ingestion_result == "INGESTED"
        assert len(vr.results) == 1
        assert isinstance(vr.results[0], ge_validation_result.ValidationResult)
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.statistics == {"statistics_key": "statistics_value"}
        assert vr.evaluation_parameters == {
            "evaluation_parameters_key": "evaluation_parameters_value"
        }

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get_basic_info"]["response"]

        # Act
        vr = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert vr.id is None
        assert vr.success is True
        assert vr._full_report_path is None
        assert vr._validation_time is None
        assert vr._featurestore_id is None
        assert vr._featuregroup_id is None
        assert vr.ingestion_result == "unknown"
        assert len(vr.results) == 1
        assert isinstance(vr.results[0], ge_validation_result.ValidationResult)
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.statistics == {"statistics_key": "statistics_value"}
        assert vr.evaluation_parameters is None

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get_list"]["response"]

        # Act
        vr_list = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert len(vr_list) == 1
        vr = vr_list[0]
        assert vr.id == 11
        assert vr.success is True
        assert vr._full_report_path == "test_full_report_path"
        assert vr._validation_time == "test_validation_time"
        assert vr._featurestore_id == "test_featurestore_id"
        assert vr._featuregroup_id == "test_featuregroup_id"
        assert vr.ingestion_result == "test_ingestion_result"
        assert len(vr.results) == 1
        assert isinstance(vr.results[0], ge_validation_result.ValidationResult)
        assert vr.meta == {"meta_key": "meta_value"}
        assert vr.statistics == {"statistics_key": "statistics_value"}
        assert vr.evaluation_parameters == {
            "evaluation_parameters_key": "evaluation_parameters_value"
        }

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get_list_empty"]["response"]

        # Act
        vr_list = validation_report.ValidationReport.from_response_json(json)

        # Assert
        assert len(vr_list) == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_to_ge_type(self, backend_fixtures):
        import great_expectations

        # Arrange
        json = backend_fixtures["validation_report"]["get"]["response"]
        vr = validation_report.ValidationReport.from_response_json(json)

        # Act
        ge_report = vr.to_ge_type()

        # Assert
        assert isinstance(
            ge_report, great_expectations.core.ExpectationSuiteValidationResult
        )
        assert ge_report.success is True
        assert ge_report.statistics == {"statistics_key": "statistics_value"}
        assert ge_report.meta == {"meta_key": "meta_value"}
        # GE 1.x renamed evaluation_parameters to suite_parameters.
        params_attr = "suite_parameters" if GE_MAJOR == 1 else "evaluation_parameters"
        assert getattr(ge_report, params_attr) == {
            "evaluation_parameters_key": "evaluation_parameters_value"
        }
        assert len(ge_report.results) == 1
        assert isinstance(
            ge_report.results[0], great_expectations.core.ExpectationValidationResult
        )

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_round_trip_response_json_to_ge_preserves_fields(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["validation_report"]["get"]["response"]

        # Act
        vr = validation_report.ValidationReport.from_response_json(json)
        ge_report = vr.to_ge_type()

        # Assert
        # Round-tripping into a GE object must preserve every wire-format field.
        assert ge_report.success is True
        assert ge_report.statistics == {"statistics_key": "statistics_value"}
        assert ge_report.meta == {"meta_key": "meta_value"}
        params_attr = "suite_parameters" if GE_MAJOR == 1 else "evaluation_parameters"
        assert getattr(ge_report, params_attr) == {
            "evaluation_parameters_key": "evaluation_parameters_value"
        }
        assert ge_report.results[0].success is True
        assert ge_report.results[0].result == {"result_key": "result_value"}
        assert ge_report.results[0].meta == {"meta_key": "meta_value"}

    def test_to_dict_snapshot(self):
        import json as _json

        # Arrange
        result = ge_validation_result.ValidationResult(
            success=True,
            result={"observed_value": 1},
            expectation_config={"expectation_type": "expect_column_to_exist"},
            exception_info={"raised_exception": False},
            meta={},
        )
        vr = validation_report.ValidationReport(
            success=True,
            results=[result],
            meta={"k": "v"},
            statistics={"successful_expectations": 1},
            evaluation_parameters={"p": 1},
            id=11,
            ingestion_result="ingested",
        )

        # Act
        d = vr.to_dict()

        # Assert
        # to_dict JSON-stringifies meta/statistics/evaluationParameters and
        # uppercases ingestionResult. The results list is kept raw and
        # serialized downstream by util.Encoder.
        assert d == {
            "id": 11,
            "success": True,
            "evaluationParameters": _json.dumps({"p": 1}),
            "statistics": _json.dumps({"successful_expectations": 1}),
            "results": vr._results,
            "meta": _json.dumps({"k": "v"}),
            "ingestionResult": "INGESTED",
        }
        # The raw results list contains the underlying ValidationResult objects.
        assert len(d["results"]) == 1
        assert isinstance(d["results"][0], ge_validation_result.ValidationResult)

    def test_to_json_dict_snapshot(self):
        # Arrange
        result = ge_validation_result.ValidationResult(
            success=True,
            result={"observed_value": 1},
            expectation_config={"expectation_type": "expect_column_to_exist"},
            exception_info={"raised_exception": False},
            meta={"k": "v"},
            id=22,
        )
        vr = validation_report.ValidationReport(
            success=True,
            results=[result],
            meta={"k": "v"},
            statistics={"successful_expectations": 1},
            evaluation_parameters={"p": 1},
            id=11,
        )

        # Act
        d = vr.to_json_dict()

        # Assert
        # to_json_dict keeps everything as nested dicts and does not include
        # ingestionResult (intentional - that field is only on to_dict).
        assert d == {
            "id": 11,
            "success": True,
            "evaluationParameters": {"p": 1},
            "statistics": {"successful_expectations": 1},
            "results": [
                {
                    "id": 22,
                    "success": True,
                    "exceptionInfo": {"raised_exception": False},
                    "expectationConfig": {"expectation_type": "expect_column_to_exist"},
                    "result": {"observed_value": 1},
                    "meta": {"k": "v"},
                }
            ],
            "meta": {"k": "v"},
        }

    @pytest.mark.skipif(
        GE_MAJOR != 1,
        reason="GE 1.x-specific normalization test",
    )
    def test_validation_report_results_preserve_legacy_shape_from_ge_v1(self):
        import json as _json

        import great_expectations

        # Arrange: GE 1.x emits expectation_config with type/severity keys.
        ge_result = great_expectations.core.ExpectationValidationResult(
            success=True,
            expectation_config=_make_ge_expectation_configuration(
                "expect_column_to_exist", {"column": "x"}, {}
            ),
            result={"observed_value": 1},
            exception_info={"raised_exception": False},
            meta={},
        )

        # Act
        vr = validation_report.ValidationReport(
            success=True,
            results=[ge_result],
            meta={},
            statistics={"successful_expectations": 1},
        )

        # Assert: the wire-format ValidationResult.expectationConfig must contain
        # expectation_type, the field the frontend reads at
        # src/modules/feature-group/.../ResultsTable.tsx:43.
        assert len(vr.results) == 1
        result = vr.results[0]
        assert (
            result.expectation_config.get("expectation_type")
            == "expect_column_to_exist"
        )
        assert "type" not in result.expectation_config
        assert "severity" not in result.expectation_config
        wire = _json.loads(result.to_dict()["expectationConfig"])
        assert wire["expectation_type"] == "expect_column_to_exist"

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_constructor_accepts_ge_results(self):
        import great_expectations

        # Arrange
        ge_result = great_expectations.core.ExpectationValidationResult(
            success=True,
            expectation_config=_make_ge_expectation_configuration(
                "expect_column_to_exist", {"column": "x"}, {}
            ),
            result={"observed_value": 1},
            exception_info={"raised_exception": False},
            meta={},
        )

        # Act
        vr = validation_report.ValidationReport(
            success=True,
            results=[ge_result],
            meta={},
            statistics={"successful_expectations": 1},
        )

        # Assert
        # The constructor must coerce raw GE results into ValidationResult instances.
        assert len(vr.results) == 1
        assert isinstance(vr.results[0], ge_validation_result.ValidationResult)
        assert vr.results[0].success is True
        # Wire-format normalization: expectation_config must contain expectation_type
        # (legacy shape) regardless of GE version, so the frontend keeps rendering.
        assert vr.results[0].expectation_config.get("expectation_type") == (
            "expect_column_to_exist"
        )
