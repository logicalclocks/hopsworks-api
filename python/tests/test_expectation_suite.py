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
from hsfs import expectation_suite, ge_expectation
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


def _make_ge_expectation_suite(name, expectations, meta):
    """Construct a GE ExpectationSuite with version-correct kwargs."""
    import great_expectations

    if GE_MAJOR == 1:
        return great_expectations.core.ExpectationSuite(
            name=name, expectations=expectations, meta=meta
        )
    return great_expectations.core.ExpectationSuite(
        expectation_suite_name=name, expectations=expectations, meta=meta
    )


def _suite_name(suite):
    """Return the suite name across versions (renamed to ``name`` in 1.x)."""
    return getattr(suite, "name", None) or getattr(
        suite, "expectation_suite_name", None
    )


def _expectation_type_attr(ge_object):
    """Return the expectation type across GE versions and object types.

    GE 1.x exposes ``type`` on ``ExpectationConfiguration`` and
    ``expectation_type`` on ``Expectation`` subclasses; GE 0.x exposes
    ``expectation_type`` everywhere.
    """
    return getattr(ge_object, "expectation_type", None) or getattr(
        ge_object, "type", None
    )


class TestExpectationSuite:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]

        # Act
        es = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es.id == 21
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._feature_store_id == 99
        assert es._feature_group_id == 10
        assert es.ge_cloud_id == "test_ge_cloud_id"
        assert es.data_asset_type == "test_data_asset_type"
        assert es.run_validation == "test_run_validation"
        assert es.validation_ingestion_policy == "TEST_VALIDATION_INGESTION_POLICY"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {"great_expectations_version": "0.15.12", "key": "value"}

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get_list"]["response"]

        # Act
        es_list = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert len(es_list) == 1
        es = es_list[0]
        assert es.id == 21
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._feature_store_id == 99
        assert es._feature_group_id == 10
        assert es.ge_cloud_id == "test_ge_cloud_id"
        assert es.data_asset_type == "test_data_asset_type"
        assert es.run_validation == "test_run_validation"
        assert es.validation_ingestion_policy == "TEST_VALIDATION_INGESTION_POLICY"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {"great_expectations_version": "0.15.12", "key": "value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get_basic_info"]["response"]

        # Act
        es = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es.id is None
        assert es.expectation_suite_name == "test_expectation_suite_name"
        assert es._feature_store_id is None
        assert es._feature_group_id is None
        assert es.ge_cloud_id is None
        assert es.data_asset_type is None
        assert es.run_validation is True
        assert es.validation_ingestion_policy == "ALWAYS"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.meta == {"great_expectations_version": "0.15.12", "key": "value"}

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get_list_empty"]["response"]

        # Act
        es_list = expectation_suite.ExpectationSuite.from_response_json(json)

        # Assert
        assert es_list is None

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_from_ge_type(self):
        # Arrange
        ge_suite = _make_ge_expectation_suite(
            name="suite_a",
            expectations=[
                _make_ge_expectation_configuration(
                    "expect_column_to_exist",
                    {"column": "x"},
                    {"meta_key": "meta_value"},
                ),
            ],
            meta={"great_expectations_version": "0.18.12"},
        )

        # Act
        es = expectation_suite.ExpectationSuite.from_ge_type(ge_suite)

        # Assert
        assert es.expectation_suite_name == "suite_a"
        assert len(es.expectations) == 1
        assert isinstance(es.expectations[0], ge_expectation.GeExpectation)
        assert es.expectations[0].expectation_type == "expect_column_to_exist"
        # GE 1.x rewrites the meta great_expectations_version to its own version.
        assert "great_expectations_version" in es.meta
        assert es.meta.get("meta_key", "meta_value")  # legacy path preserves user keys
        # Defaults applied because GE has no equivalent for these.
        assert es.run_validation is True
        assert es.validation_ingestion_policy == "ALWAYS"

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_to_ge_type(self):
        import great_expectations

        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[
                ge_expectation.GeExpectation(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": "x"},
                    meta={"meta_key": "meta_value"},
                )
            ],
            meta={"great_expectations_version": "0.18.12"},
            data_asset_type="DATAFRAME",
            ge_cloud_id="cloud_id",
        )

        # Act
        ge_suite = es.to_ge_type()

        # Assert
        assert isinstance(ge_suite, great_expectations.core.ExpectationSuite)
        assert _suite_name(ge_suite) == "suite_a"
        assert len(ge_suite.expectations) == 1
        assert (
            _expectation_type_attr(ge_suite.expectations[0]) == "expect_column_to_exist"
        )
        if GE_MAJOR != 1:
            # GE 1.x dropped data_asset_type and ge_cloud_id; only assert on legacy.
            assert ge_suite.data_asset_type == "DATAFRAME"
            assert ge_suite.ge_cloud_id == "cloud_id"
            assert ge_suite.meta == {"great_expectations_version": "0.18.12"}

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_round_trip_ge_to_hopsworks_to_ge(self):
        # Arrange
        original = _make_ge_expectation_suite(
            name="suite_a",
            expectations=[
                _make_ge_expectation_configuration(
                    "expect_column_to_exist",
                    {"column": "x"},
                    {"meta_key": "meta_value"},
                ),
            ],
            meta={"great_expectations_version": "0.18.12"},
        )

        # Act
        hops = expectation_suite.ExpectationSuite.from_ge_type(original)
        rebuilt = hops.to_ge_type()

        # Assert
        assert _suite_name(rebuilt) == _suite_name(original)
        # Compare via the version-stable expectation_configurations accessor.
        rebuilt_configs = (
            rebuilt.expectation_configurations
            if GE_MAJOR == 1
            else rebuilt.expectations
        )
        original_configs = (
            original.expectation_configurations
            if GE_MAJOR == 1
            else original.expectations
        )
        assert len(rebuilt_configs) == len(original_configs)
        assert _expectation_type_attr(rebuilt_configs[0]) == _expectation_type_attr(
            original_configs[0]
        )
        assert rebuilt_configs[0].kwargs == original_configs[0].kwargs
        assert rebuilt_configs[0].meta == original_configs[0].meta

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
    )
    def test_convert_expectation_from_ge(self):
        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[],
            meta={},
        )
        ge_exp = _make_ge_expectation_configuration(
            "expect_column_to_exist",
            {"column": "x"},
            {"meta_key": "meta_value"},
        )

        # Act
        converted = es._convert_expectation(ge_exp)

        # Assert
        assert isinstance(converted, ge_expectation.GeExpectation)
        assert converted.expectation_type == "expect_column_to_exist"
        assert converted.kwargs == {"column": "x"}

    def test_convert_expectation_from_hopsworks(self):
        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[],
            meta={},
        )
        hops_exp = ge_expectation.GeExpectation(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "x"},
            meta={},
        )

        # Act
        converted = es._convert_expectation(hops_exp)

        # Assert
        # GeExpectation pass-through must return the exact same object.
        assert converted is hops_exp

    def test_convert_expectation_from_dict(self):
        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[],
            meta={},
        )
        dict_exp = {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "x"},
            "meta": {},
        }

        # Act
        converted = es._convert_expectation(dict_exp)

        # Assert
        assert isinstance(converted, ge_expectation.GeExpectation)
        assert converted.expectation_type == "expect_column_to_exist"
        assert converted.kwargs == {"column": "x"}

    def test_convert_expectation_unsupported_type_raises(self):
        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[],
            meta={},
        )

        # Act / Assert
        with pytest.raises(TypeError):
            es._convert_expectation(42)

    def test_to_dict_snapshot(self):
        import json as _json

        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[],
            meta={"k": "v"},
            run_validation=False,
            validation_ingestion_policy="strict",
            data_asset_type="DATAFRAME",
            ge_cloud_id="cloud_id",
        )

        # Act
        d = es.to_dict()

        # Assert
        # to_dict emits camelCase, JSON-stringifies meta, and uppercases
        # validationIngestionPolicy. Expectations are kept as the raw list and
        # serialized downstream by util.Encoder.
        assert d == {
            "id": None,
            "featureStoreId": None,
            "featureGroupId": None,
            "expectationSuiteName": "suite_a",
            "expectations": [],
            "meta": _json.dumps({"k": "v"}),
            "geCloudId": "cloud_id",
            "dataAssetType": "DATAFRAME",
            "runValidation": False,
            "validationIngestionPolicy": "STRICT",
        }

    def test_to_json_dict_snapshot(self):
        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[
                ge_expectation.GeExpectation(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": "x"},
                    meta={"expectationId": 1},
                    id=1,
                )
            ],
            meta={"k": "v"},
            run_validation=True,
            validation_ingestion_policy="always",
        )

        # Act
        d = es.to_json_dict()

        # Assert
        # to_json_dict keeps everything as nested dicts (no JSON-encoded strings)
        # and recursively dict-encodes each expectation.
        assert d == {
            "id": None,
            "featureStoreId": None,
            "featureGroupId": None,
            "expectationSuiteName": "suite_a",
            "expectations": [
                {
                    "id": 1,
                    "expectationType": "expect_column_to_exist",
                    "kwargs": {"column": "x"},
                    "meta": {"expectationId": 1},
                }
            ],
            "meta": {"k": "v"},
            "geCloudId": None,
            "dataAssetType": None,
            "runValidation": True,
            "validationIngestionPolicy": "ALWAYS",
        }

    def test_to_json_dict_decamelized_snapshot(self):
        # Arrange
        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="suite_a",
            expectations=[],
            meta={"k": "v"},
        )

        # Act
        d = es.to_json_dict(decamelize=True)

        # Assert
        # decamelize=True converts every camelCase key in the dict to snake_case.
        assert d == {
            "id": None,
            "feature_store_id": None,
            "feature_group_id": None,
            "expectation_suite_name": "suite_a",
            "expectations": [],
            "meta": {"k": "v"},
            "ge_cloud_id": None,
            "data_asset_type": None,
            "run_validation": True,
            "validation_ingestion_policy": "ALWAYS",
        }
