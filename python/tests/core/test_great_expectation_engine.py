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

import hsfs.expectation_suite as es
import pandas as pd
import pytest
from hsfs import feature_group, validation_report
from hsfs.core import great_expectation_engine
from hsfs.core.constants import GE_MAJOR, HAS_GREAT_EXPECTATIONS


if HAS_GREAT_EXPECTATIONS:
    import great_expectations


def _make_ge_expectation_suite(name, expectations, meta):
    """Construct a GE ExpectationSuite with version-correct kwargs."""
    if GE_MAJOR == 1:
        return great_expectations.core.ExpectationSuite(
            name=name, expectations=expectations, meta=meta
        )
    return great_expectations.core.ExpectationSuite(
        expectation_suite_name=name, expectations=expectations, meta=meta
    )


def _make_ge_expectation_configuration(expectation_type, kwargs, meta):
    """Construct a GE ExpectationConfiguration with version-correct kwargs."""
    if GE_MAJOR == 1:
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        return ExpectationConfiguration(type=expectation_type, kwargs=kwargs, meta=meta)
    return great_expectations.core.ExpectationConfiguration(
        expectation_type=expectation_type, kwargs=kwargs, meta=meta
    )


def _make_ge_suite_validation_result():
    """Construct an empty ExpectationSuiteValidationResult across versions.

    GE 1.x added required positional kwargs (success, results, suite_name).
    """
    if GE_MAJOR == 1:
        return great_expectations.core.ExpectationSuiteValidationResult(
            success=True, results=[], suite_name=""
        )
    return great_expectations.core.ExpectationSuiteValidationResult()


class TestGreatExpectationEngine:
    @pytest.fixture(autouse=True)
    def mock_has_deltalake(self, mocker):
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )

    def test_validate(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        mock_fg_get_expectation_suite.return_value = None

        # Act
        ge_engine.validate(
            feature_group=fg, dataframe=None, save_report=None, validation_options={}
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 0
        assert mock_vr.call_count == 0

    def test_validate_suite(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=None, meta={}
        )

        validation_options = {"run_validation": False}

        mock_fg_get_expectation_suite.return_value = suite

        # Act
        ge_engine.validate(
            feature_group=fg,
            dataframe=None,
            save_report=None,
            validation_options=validation_options,
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 0
        assert mock_vr.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_validate_suite_validation_options(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=None, meta={}
        )

        validation_options = {"run_validation": True}

        mock_fg_get_expectation_suite.return_value = suite

        # Act
        ge_engine.validate(
            feature_group=fg,
            dataframe=None,
            validation_options=validation_options,
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 0
        assert mock_vr.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_validate_suite_validation_options_save_report(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )
        mock_fg_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )
        mock_vr = mocker.patch("hsfs.validation_report.ValidationReport")

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], partition_key=[]
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=None, meta={}
        )

        validation_options = {"run_validation": True}

        mock_fg_get_expectation_suite.return_value = suite

        # Act
        ge_engine.validate(
            feature_group=fg,
            dataframe=None,
            save_report=True,
            validation_options=validation_options,
        )

        # Assert
        assert mock_fg_save_validation_report.call_count == 1
        assert mock_vr.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_convert_expectation_suite(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = _make_ge_expectation_suite(name="suite_name", expectations=[], meta={})

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=_make_ge_expectation_suite(
                name="attached_to_feature_group", expectations=[], meta={}
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        converted_suite = ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg, expectation_suite=suite
        )

        # Assert
        assert isinstance(converted_suite, es.ExpectationSuite)
        assert converted_suite.expectation_suite_name == "suite_name"
        assert mock_fg_get_expectation_suite.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_fake_convert_expectation_suite(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name", expectations=[], meta={}
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=_make_ge_expectation_suite(
                name="attached_to_feature_group", expectations=[], meta={}
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        converted_suite = ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg, expectation_suite=suite
        )

        # Assert
        assert isinstance(converted_suite, es.ExpectationSuite)
        assert converted_suite.expectation_suite_name == "suite_name"
        assert mock_fg_get_expectation_suite.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_fetch_expectation_suite(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = None

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=_make_ge_expectation_suite(
                name="attached_to_feature_group", expectations=[], meta={}
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg, expectation_suite=suite
        )

        # Assert
        assert mock_fg_get_expectation_suite.call_count == 1

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_fetch_expectation_suite_false(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        suite = None

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            expectation_suite=_make_ge_expectation_suite(
                name="attached_to_feature_group", expectations=[], meta={}
            ),
        )

        mock_fg_get_expectation_suite = mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_expectation_suite"
        )

        # Act
        result = ge_engine.fetch_or_convert_expectation_suite(
            feature_group=fg,
            expectation_suite=suite,
            validation_options={"fetch_expectation_suite": False},
        )

        # Assert
        assert mock_fg_get_expectation_suite.call_count == 0
        assert result.expectation_suite_name == "attached_to_feature_group"
        assert isinstance(result, es.ExpectationSuite)

    def test_should_run_validation_based_on_suite(self):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=True,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={}
        )

        # Assert
        assert run_validation is True

    def test_should_not_run_validation_based_on_suite(self):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=False,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={}
        )

        # Assert
        assert run_validation is False

    def test_should_run_validation_based_validation_options(self, mocker):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=True,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={"run_validation": False}
        )

        # Assert
        assert run_validation is False

    def test_should_not_run_validation_based_validation_options(self):
        # Arrange
        feature_store_id = 99
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=False,
        )

        # Act
        run_validation = ge_engine.should_run_validation(
            expectation_suite=suite, validation_options={"run_validation": True}
        )

        # Assert
        assert run_validation is True

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_not_save_but_convert_report(self, mocker):
        # Arrange
        feature_store_id = 99
        save_report = False
        ge_type = False
        validation_options = {}

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        report = _make_ge_suite_validation_result()

        mock_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )

        # Act
        converted_report = ge_engine.save_or_convert_report(
            save_report=save_report,
            ge_type=ge_type,
            feature_group=fg,
            report=report,
            validation_options=validation_options,
        )

        # Assert
        assert isinstance(converted_report, validation_report.ValidationReport)
        assert mock_save_validation_report.call_count == 0

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_save_but_not_convert_report(self, mocker):
        # Arrange
        feature_store_id = 99
        save_report = True
        ge_type = True
        validation_options = {}

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        report = _make_ge_suite_validation_result()

        mock_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )

        # Act
        ge_engine.save_or_convert_report(
            save_report=save_report,
            ge_type=ge_type,
            feature_group=fg,
            report=report,
            validation_options=validation_options,
        )

        # Assert
        assert mock_save_validation_report.call_count == 1

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed",
    )
    def test_not_save_and_not_convert_report(self, mocker):
        # Arrange
        feature_store_id = 99
        save_report = True
        ge_type = True
        # This should override save_report
        validation_options = {"save_report": False}

        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )

        report = _make_ge_suite_validation_result()

        mock_save_validation_report = mocker.patch(
            "hsfs.feature_group.FeatureGroup.save_validation_report"
        )

        # Act
        converted_report = ge_engine.save_or_convert_report(
            save_report=save_report,
            ge_type=ge_type,
            feature_group=fg,
            report=report,
            validation_options=validation_options,
        )

        # Assert
        assert isinstance(
            converted_report, great_expectations.core.ExpectationSuiteValidationResult
        )
        assert mock_save_validation_report.call_count == 0

    @pytest.mark.skipif(
        HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is installed, do not check for module not found error",
    )
    def test_raise_module_not_found_error(self, mocker):
        # Arrange
        ge_engine = great_expectation_engine.GreatExpectationEngine(feature_store_id=11)
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=11,
            partition_key=[],
            primary_key=["id"],
        )
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        suite = es.ExpectationSuite(
            expectation_suite_name="suite_name",
            expectations=[],
            meta={},
            run_validation=True,
        )
        mocker.patch("hsfs.util.get_feature_group_url", return_value="https://url")

        # Act
        with pytest.raises(ModuleNotFoundError):
            ge_engine.validate(
                feature_group=fg,
                dataframe=df,
                expectation_suite=suite,
            )


# Spark engine path (hsfs/engine/spark.py:1538) is intentionally not exercised
# here - Spark is not available in the unit test environment. Locking down the
# Spark validate_with_great_expectations behavior is deferred to integration tests.
class TestGreatExpectationEngineEndToEnd:
    """End-to-end tests that exercise the real great_expectations call.

    The existing TestGreatExpectationEngine mocks engine.get_instance() so it
    never reaches the GE library. These tests intentionally do call into GE so
    that an upgrade-time regression in the GE-boundary surfaces here.
    """

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_validate_pandas_dataframe_succeeds(self):
        from hsfs.engine import python as python_engine

        # Arrange
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
        suite = _make_ge_expectation_suite(
            name="suite",
            expectations=[
                _make_ge_expectation_configuration(
                    "expect_column_values_to_not_be_null",
                    {"column": "id"},
                    {},
                ),
            ],
            meta={},
        )

        # Act
        report = python_engine.Engine().validate_with_great_expectations(
            dataframe=df, expectation_suite=suite
        )

        # Assert
        assert report.success is True
        assert len(report.results) == 1
        assert report.results[0].success is True

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_validate_pandas_dataframe_fails(self):
        from hsfs.engine import python as python_engine

        # Arrange
        df = pd.DataFrame({"id": [1, None, 3]})
        suite = _make_ge_expectation_suite(
            name="suite",
            expectations=[
                _make_ge_expectation_configuration(
                    "expect_column_values_to_not_be_null",
                    {"column": "id"},
                    {},
                ),
            ],
            meta={},
        )

        # Act
        report = python_engine.Engine().validate_with_great_expectations(
            dataframe=df, expectation_suite=suite
        )

        # Assert
        assert report.success is False
        assert report.results[0].success is False

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_validate_pandas_dataframe_returns_ge_type(self):
        from hsfs.engine import python as python_engine

        # Arrange
        df = pd.DataFrame({"id": [1, 2]})
        suite = _make_ge_expectation_suite(
            name="suite",
            expectations=[],
            meta={},
        )

        # Act
        report = python_engine.Engine().validate_with_great_expectations(
            dataframe=df, expectation_suite=suite
        )

        # Assert
        # The SDK depends on this exact return type for to_json_dict / serialization.
        assert isinstance(
            report, great_expectations.core.ExpectationSuiteValidationResult
        )

    @pytest.mark.skipif(
        GE_MAJOR != 1,
        reason="GE 1.x batch.validate only accepts result_format and expectation_parameters; the legacy kwarg surface differs.",
    )
    def test_validate_propagates_result_format_to_ge_v1(self):
        from hsfs.engine import python as python_engine

        # Arrange
        df = pd.DataFrame({"id": [1, 2, None, 4, 5]})
        suite = _make_ge_expectation_suite(
            name="suite",
            expectations=[
                _make_ge_expectation_configuration(
                    "expect_column_values_to_not_be_null", {"column": "id"}, {}
                ),
            ],
            meta={},
        )

        # Act: COMPLETE format must surface the per-row indices that SUMMARY omits.
        report = python_engine.Engine().validate_with_great_expectations(
            dataframe=df,
            expectation_suite=suite,
            ge_validate_kwargs={"result_format": "COMPLETE"},
        )

        # Assert: presence of unexpected_index_list is the discriminator between
        # COMPLETE and the default SUMMARY result format.
        assert "unexpected_index_list" in report.results[0].result

    @pytest.mark.skipif(
        GE_MAJOR != 1,
        reason="GE 1.x suite parameters use the $PARAMETER placeholder syntax.",
    )
    def test_validate_propagates_expectation_parameters_to_ge_v1(self):
        from hsfs.engine import python as python_engine

        # Arrange: the suite uses a $PARAMETER placeholder bound at validate time.
        df = pd.DataFrame({"id": [5, 6, 7, 8, 9]})
        suite = _make_ge_expectation_suite(
            name="suite",
            expectations=[
                _make_ge_expectation_configuration(
                    "expect_column_min_to_be_between",
                    {
                        "column": "id",
                        "min_value": {"$PARAMETER": "lo"},
                        "max_value": 100,
                    },
                    {},
                ),
            ],
            meta={},
        )
        engine = python_engine.Engine()

        # Act
        passing = engine.validate_with_great_expectations(
            dataframe=df,
            expectation_suite=suite,
            ge_validate_kwargs={"expectation_parameters": {"lo": 5}},
        )
        failing = engine.validate_with_great_expectations(
            dataframe=df,
            expectation_suite=suite,
            ge_validate_kwargs={"expectation_parameters": {"lo": 100}},
        )

        # Assert: the parameter binding actually reaches the validator.
        assert passing.success is True
        assert failing.success is False

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_validate_with_polars_converts_to_pandas(self):
        polars = pytest.importorskip("polars")
        from hsfs.engine import python as python_engine

        # Arrange
        df = polars.DataFrame({"id": [1, 2, 3]})
        suite = _make_ge_expectation_suite(
            name="suite",
            expectations=[
                _make_ge_expectation_configuration(
                    "expect_column_values_to_not_be_null",
                    {"column": "id"},
                    {},
                ),
            ],
            meta={},
        )

        # Act
        with pytest.warns(util_warning_for_polars()):
            report = python_engine.Engine().validate_with_great_expectations(
                dataframe=df, expectation_suite=suite
            )

        # Assert
        assert report.success is True

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_orchestrated_validate_returns_validation_report(self, mocker):
        from hsfs.engine import python as python_engine

        # Arrange
        feature_store_id = 99
        mocker.patch("hsfs.engine.get_type")
        # Use a real Python engine so the orchestrator reaches the real GE call.
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine.Engine())
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
        )
        df = pd.DataFrame({"id": [1, 2, 3]})
        suite = es.ExpectationSuite(
            expectation_suite_name="suite",
            expectations=[
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                    "meta": {},
                }
            ],
            meta={},
            run_validation=True,
        )
        ge_engine = great_expectation_engine.GreatExpectationEngine(
            feature_store_id=feature_store_id
        )

        # Act
        result = ge_engine.validate(
            feature_group=fg,
            dataframe=df,
            expectation_suite=suite,
            save_report=False,
            ge_type=False,
        )

        # Assert
        # ge_type=False asks for the Hopsworks ValidationReport wrapper.
        assert isinstance(result, validation_report.ValidationReport)
        assert result.success is True
        # The wrapper must round-trip back through to_ge_type without errors.
        ge_report = result.to_ge_type()
        assert isinstance(
            ge_report, great_expectations.core.ExpectationSuiteValidationResult
        )


def util_warning_for_polars():
    """Return the warning class emitted when Polars data is auto-converted."""
    from hsfs import util

    return util.FeatureGroupWarning
