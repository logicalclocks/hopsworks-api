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
from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, Literal


if TYPE_CHECKING:
    import great_expectations

import humps
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import util
from hsfs.core import expectation_suite_engine
from hsfs.core.constants import (
    HAS_GREAT_EXPECTATIONS,
    initialise_expectation_suite_for_single_expectation_api_message,
)
from hsfs.core.expectation_engine import ExpectationEngine
from hsfs.core.variable_api import VariableApi

# if great_expectations is not installed, we will default to using native Hopsworks class as return values
from hsfs.decorators import uses_great_expectations
from hsfs.ge_expectation import GeExpectation


if HAS_GREAT_EXPECTATIONS:
    import great_expectations


class ExpectationSuite:
    """Metadata object representing a feature validation expectation in the Feature Store."""

    def __init__(
        self,
        expectation_suite_name: str,
        expectations: list[
            great_expectations.core.ExpectationConfiguration
            | dict[str, Any]
            | GeExpectation
        ],
        meta: dict[str, Any],
        id: int | None = None,
        run_validation: bool = True,
        validation_ingestion_policy: Literal["always", " strict"] = "always",
        feature_store_id: int | None = None,
        feature_group_id: int | None = None,
        href: str | None = None,
        **kwargs,
    ) -> None:
        self._id = id
        self._expectation_suite_name = expectation_suite_name
        self._ge_cloud_id = kwargs.get("ge_cloud_id")
        self._data_asset_type = kwargs.get("data_asset_type")
        self._run_validation = run_validation
        self._validation_ingestion_policy = validation_ingestion_policy.upper()
        self._expectations = []
        self._href = href

        if feature_store_id is not None and feature_group_id is not None:
            self._feature_store_id = feature_store_id
            self._feature_group_id = feature_group_id
        elif href is not None:
            self._init_feature_store_and_feature_group_ids_from_href(href)
        else:
            self._feature_group_id = None
            self._feature_store_id = None

        self._variable_api: VariableApi = VariableApi()

        # use setters because these need to be transformed from stringified json
        self.expectations = expectations
        self.meta = meta

        self._expectation_engine: ExpectationEngine | None = None
        self._expectation_suite_engine: (
            expectation_suite_engine.ExpectationSuiteEngine | None
        ) = None

        if self.id:
            assert self._feature_store_id is not None, (
                "feature_store_id should not be None if expectation suite id is provided"
            )
            assert self._feature_group_id is not None, (
                "feature_group_id should not be None if expectation suite id is provided"
            )
            self._expectation_engine = ExpectationEngine(
                feature_store_id=self._feature_store_id,
                feature_group_id=self._feature_group_id,
                expectation_suite_id=self.id,
            )
            self._expectation_suite_engine = (
                expectation_suite_engine.ExpectationSuiteEngine(
                    feature_store_id=self._feature_store_id,
                    feature_group_id=self._feature_group_id,
                )
            )

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> ExpectationSuite | list[ExpectationSuite] | None:
        json_decamelized = humps.decamelize(json_dict)
        if (
            "count" in json_decamelized
        ):  # todo count is expected also when providing dict
            if json_decamelized["count"] == 0:
                return None
            return [
                cls(**expectation_suite)
                for expectation_suite in json_decamelized["items"]
            ]
        return cls(**json_decamelized)

    @classmethod
    @uses_great_expectations
    def from_ge_type(
        cls,
        ge_expectation_suite: great_expectations.core.ExpectationSuite,
        run_validation: bool = True,
        validation_ingestion_policy: Literal["ALWAYS", "STRICT"] = "ALWAYS",
        id: int | None = None,
        feature_store_id: int | None = None,
        feature_group_id: int | None = None,
    ) -> ExpectationSuite:
        """Used to create a Hopsworks Expectation Suite instance from a great_expectations instance.

        Parameters:
            ge_expectation_suite: The great_expectations ExpectationSuite instance to convert to a Hopsworks ExpectationSuite.
            run_validation: Whether to run validation on inserts when the expectation suite is attached.
            validation_ingestion_policy: The validation ingestion policy to use when the expectation suite is attached.
            id:
                The id of the expectation suite in Hopsworks.
                If not provided, a new expectation suite will be created.
            feature_store_id: The id of the feature store of the feature group to which the expectation suite belongs.
            feature_group_id: The id of the feature group to which the expectation suite belongs.

        Returns:
            Hopsworks Expectation Suite instance.
        """
        suite_dict = ge_expectation_suite.to_json_dict()

        # Handle GE 1.0+ field name changes: 'name' -> 'expectation_suite_name'
        if "name" in suite_dict and "expectation_suite_name" not in suite_dict:
            suite_dict["expectation_suite_name"] = suite_dict.pop("name")

        # In GE 1.0+, 'id' is the cloud ID (was 'ge_cloud_id')
        # We need to handle the id field carefully - GE's id is not our internal Hopsworks id
        if id is None and "id" in suite_dict:
            # GE's id is the cloud ID, store it as ge_cloud_id
            suite_dict["ge_cloud_id"] = suite_dict.pop("id")

        # Handle GE 1.0+ field name changes in expectations: 'type' -> 'expectation_type'
        if "expectations" in suite_dict:
            for exp in suite_dict["expectations"]:
                if isinstance(exp, dict) and "type" in exp and "expectation_type" not in exp:
                    exp["expectation_type"] = exp.pop("type")

        return cls(
            **suite_dict,
            id=id,
            run_validation=run_validation,
            validation_ingestion_policy=validation_ingestion_policy,
            feature_group_id=feature_group_id,
            feature_store_id=feature_store_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "featureGroupId": self._feature_group_id,
            "expectationSuiteName": self._expectation_suite_name,
            "expectations": self._expectations,
            "meta": json.dumps(self._meta),
            "geCloudId": self._ge_cloud_id,
            "dataAssetType": self._data_asset_type,
            "runValidation": self._run_validation,
            "validationIngestionPolicy": self._validation_ingestion_policy.upper(),
        }

    def to_json_dict(self, decamelize: bool = False) -> dict[str, Any]:
        the_dict = {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "featureGroupId": self._feature_group_id,
            "expectationSuiteName": self._expectation_suite_name,
            "expectations": [
                expectation.to_json_dict() for expectation in self._expectations
            ],
            "meta": self._meta,
            "geCloudId": self._ge_cloud_id,
            "dataAssetType": self._data_asset_type,
            "runValidation": self._run_validation,
            "validationIngestionPolicy": self._validation_ingestion_policy.upper(),
        }

        if decamelize:
            return humps.decamelize(the_dict)
        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    @uses_great_expectations
    def to_ge_type(self) -> great_expectations.core.ExpectationSuite:
        """Convert to Great Expectations ExpectationSuite type."""
        # GE 1.0+ uses 'name' instead of 'expectation_suite_name'
        # and 'id' instead of 'ge_cloud_id'. 'data_asset_type' is removed.
        return great_expectations.core.ExpectationSuite(
            name=self._expectation_suite_name,
            expectations=[
                expectation.to_ge_type() for expectation in self._expectations
            ],
            meta=self._meta,
        )

    def _init_feature_store_and_feature_group_ids_from_href(self, href: str) -> None:
        feature_store_id, feature_group_id = re.search(
            r"\/featurestores\/(\d+)\/featuregroups\/(\d+)\/expectationsuite*",
            href,
        ).groups(0)
        self._feature_store_id = int(feature_store_id)
        self._feature_group_id = int(feature_group_id)

    def _init_expectation_engine(
        self, feature_store_id: int, feature_group_id: int
    ) -> None:
        if self.id:
            self._expectation_engine = ExpectationEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                expectation_suite_id=self.id,
            )
        else:
            raise ValueError(
                initialise_expectation_suite_for_single_expectation_api_message
            )

    def _init_expectation_suite_engine(
        self, feature_store_id: int | None, feature_group_id: int | None
    ) -> None:
        if feature_group_id and feature_store_id:
            self._expectation_suite_engine = (
                expectation_suite_engine.ExpectationSuiteEngine(
                    feature_store_id=feature_store_id,
                    feature_group_id=feature_group_id,
                )
            )
        elif self._feature_group_id and self._feature_store_id:
            self._expectation_suite_engine = (
                expectation_suite_engine.ExpectationSuiteEngine(
                    feature_store_id=self._feature_store_id,
                    feature_group_id=self._feature_group_id,
                )
            )
        else:
            raise ValueError(
                "Provide feature_store_id or feature_group_id to use the expectation suite API"
            )

    # Emulate GE single expectation api to edit list of expectations
    def _convert_expectation(
        self,
        expectation: GeExpectation
        | great_expectations.core.ExpectationConfiguration
        | dict[str, Any],
    ) -> GeExpectation:
        """Convert different representation of expectation to Hopsworks GeExpectation type.

        Parameters:
            expectation: An expectation to convert to Hopsworks GeExpectation type.

        Returns:
            An expectation converted to Hopsworks GeExpectation type.

        Raises:
            TypeError: If the expectation type is not supported.
        """
        if HAS_GREAT_EXPECTATIONS and isinstance(
            expectation, great_expectations.core.ExpectationConfiguration
        ):
            exp_dict = expectation.to_json_dict()
            # Handle GE 1.0+ field name changes: 'type' -> 'expectation_type'
            if "type" in exp_dict and "expectation_type" not in exp_dict:
                exp_dict["expectation_type"] = exp_dict.pop("type")
            return GeExpectation(**exp_dict)
        if isinstance(expectation, GeExpectation):
            return expectation
        if isinstance(expectation, dict):
            # Handle GE 1.0+ field name changes in dict: 'type' -> 'expectation_type'
            if "type" in expectation and "expectation_type" not in expectation:
                expectation = expectation.copy()
                expectation["expectation_type"] = expectation.pop("type")
            return GeExpectation(**expectation)
        raise TypeError(f"Expectation of type {type(expectation)} is not supported.")

    def get_expectation(
        self, expectation_id: int, ge_type: bool = HAS_GREAT_EXPECTATIONS
    ) -> GeExpectation | great_expectations.core.ExpectationConfiguration:
        """Fetch expectation with expectation_id from the backend.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            expectation_suite = fg.get_expectation_suite()
            selected_expectation = expectation_suite.get_expectation(expectation_id=123)
            ```

        Parameters:
            expectation_id: ID of the expectation to fetch from the backend.
            ge_type: Whether to return native Great Expectations object or Hopsworks abstraction.

        Returns:
            The expectation with `expectation_id` registered in the backend.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If the expectation suite is not registered yet.
        """
        if self.id and self._expectation_engine:
            if ge_type:
                return self._expectation_engine.get(expectation_id).to_ge_type()
            return self._expectation_engine.get(expectation_id)
        raise FeatureStoreException(
            initialise_expectation_suite_for_single_expectation_api_message
        )

    def add_expectation(
        self,
        expectation: GeExpectation | great_expectations.core.ExpectationConfiguration,
        ge_type: bool = HAS_GREAT_EXPECTATIONS,
    ) -> GeExpectation | great_expectations.core.ExpectationConfiguration:
        """Append an expectation to the local suite or in the backend if attached to a Feature Group.

        Example:
            ```python
            import great_expectations as gx

            # Option 1: Using class-based expectations (recommended for GE 1.0+)
            expectation_suite.add_expectation(
                gx.expectations.ExpectColumnMinToBeBetween(
                    column="foo_id",
                    min_value=0,
                    max_value=1
                )
            )

            # Option 2: Using ExpectationConfiguration
            expectation_suite.add_expectation(
                gx.core.ExpectationConfiguration(
                    type="expect_column_value_lengths_to_be_between",
                    kwargs={
                        "column": "bar_name",
                        "min_value": 3,
                        "max_value": 10
                    }
                )
            )
            ```

        Parameters:
            expectation: The new expectation object.
            ge_type: Whether to return native Great Expectations object or Hopsworks abstraction.

        Returns:
            The new expectation attached to the Feature Group.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If the expectation suite is not registered yet.
        """
        if self.id:
            converted_expectation = self._convert_expectation(expectation=expectation)
            converted_expectation = self._expectation_engine.create(
                expectation=converted_expectation
            )
            self.expectations = self._expectation_engine.get_expectations_by_suite_id()
            if ge_type:
                return converted_expectation.to_ge_type()
            return converted_expectation
        raise FeatureStoreException(
            initialise_expectation_suite_for_single_expectation_api_message
        )

    def replace_expectation(
        self,
        expectation: GeExpectation | great_expectations.core.ExpectationConfiguration,
        ge_type: bool = HAS_GREAT_EXPECTATIONS,
    ) -> GeExpectation | great_expectations.core.ExpectationConfiguration:
        """Update an expectation from the suite locally or from the backend if attached to a Feature Group.

        Example:
            ```python
            updated_expectation = expectation_suite.replace_expectation(new_expectation_object)
            ```

        Parameters:
            expectation:
                The updated expectation object.
                The meta field should contain an expectationId field.
            ge_type: Whether to return native Great Expectations object or Hopsworks abstraction.

        Returns:
            The updated expectation attached to the Feature Group.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If the expectation suite is not registered yet.
        """
        if self.id:
            converted_expectation = self._convert_expectation(expectation=expectation)
            # To update an expectation we need an id either from meta field or from self.id
            self._expectation_engine.check_for_id(converted_expectation)
            converted_expectation = self._expectation_engine.update(
                expectation=converted_expectation
            )
            # Fetch the expectations from backend to avoid sync issues
            self.expectations = self._expectation_engine.get_expectations_by_suite_id()

            if ge_type:
                return converted_expectation.to_ge_type()
            return converted_expectation
        raise FeatureStoreException(
            initialise_expectation_suite_for_single_expectation_api_message
        )

    def remove_expectation(self, expectation_id: int | None = None) -> None:
        """Remove an expectation from the suite locally and from the backend if attached to a Feature Group.

        Example:
            ```python
            expectation_suite.remove_expectation(expectation_id=123)
            ```

        Parameters:
            expectation_id:
                ID of the expectation to remove.
                The expectation will be deleted both locally and from the backend.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If the expectation suite is not registered yet.
        """
        if self.id:
            self._expectation_engine.delete(expectation_id=expectation_id)
            self.expectations = self._expectation_engine.get_expectations_by_suite_id()
        else:
            raise FeatureStoreException(
                initialise_expectation_suite_for_single_expectation_api_message
            )

    # End of single expectation API

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        es = "ExpectationSuite("

        if hasattr(self, "id"):
            es += f"id={self._id}, "

        es += f"expectation_suite_name='{self._expectation_suite_name}', "
        es += f"expectations={self._expectations}, "
        es += f"meta={self._meta}, "
        es += f"data_asset_type='{self._data_asset_type}', "
        es += f"ge_cloud_id={self._ge_cloud_id}, "
        es += f"run_validation={self._run_validation}, "
        es += f"validation_ingestion_policy='{self._validation_ingestion_policy}'"

        if hasattr(self, "_feature_group_id"):
            es += f", feature_group_id={self._feature_group_id}, "
            es += f"feature_store_id={self._feature_store_id}"

        es += ")"
        return es

    @property
    def id(self) -> int | None:
        """Id of the expectation suite, set by backend."""
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    @property
    def expectation_suite_name(self) -> str:
        """Name of the expectation suite."""
        return self._expectation_suite_name

    @expectation_suite_name.setter
    def expectation_suite_name(self, expectation_suite_name: str) -> None:
        self._expectation_suite_name = expectation_suite_name
        if self.id:
            self._expectation_suite_engine.update_metadata_from_fields(
                **humps.decamelize(self.to_dict())
            )

    @property
    def data_asset_type(self) -> str | None:
        """Data asset type of the expectation suite, not used by backend."""
        return self._data_asset_type

    @data_asset_type.setter
    def data_asset_type(self, data_asset_type: str | None) -> None:
        self._data_asset_type = data_asset_type

    @property
    def ge_cloud_id(self) -> int | None:
        """ge_cloud_id of the expectation suite, not used by backend."""
        return self._ge_cloud_id

    @ge_cloud_id.setter
    def ge_coud_id(self, ge_cloud_id: int | None) -> None:
        self._ge_cloud_id = ge_cloud_id

    @property
    def run_validation(self) -> bool:
        """Boolean to determine whether or not the expectation suite shoudl run on ingestion."""
        return self._run_validation

    @run_validation.setter
    def run_validation(self, run_validation: bool) -> None:
        self._run_validation = run_validation
        if self.id:
            self._expectation_suite_engine.update_metadata_from_fields(
                **humps.decamelize(self.to_dict())
            )

    @property
    def validation_ingestion_policy(self) -> Literal["always", "strict"]:
        """Whether to ingest a df based on the validation result.

        - `"strict"`: ingest df only if all expectations succeed, or
        - `"always"`: always ingest df, even if one or more expectations fail.
        """
        return self._validation_ingestion_policy

    @validation_ingestion_policy.setter
    def validation_ingestion_policy(
        self, validation_ingestion_policy: Literal["always", "strict"]
    ) -> None:
        self._validation_ingestion_policy = validation_ingestion_policy.upper()
        if self.id:
            self._expectation_suite_engine.update_metadata_from_fields(
                **humps.decamelize(self.to_dict())
            )

    @property
    def expectations(self) -> list[GeExpectation]:
        """List of expectations to run at validation."""
        return self._expectations

    @expectations.setter
    def expectations(
        self,
        expectations: list[great_expectations.core.ExpectationConfiguration]
        | list[GeExpectation]
        | list[dict]
        | None,
    ) -> None:
        if expectations is None:
            self._expectations = []
        elif isinstance(expectations, list):
            self._expectations = [
                self._convert_expectation(expectation) for expectation in expectations
            ]
        else:
            raise TypeError(
                f"Type {type(expectations)} not supported. Expectations must be None or a list."
            )

    @property
    def meta(self) -> dict[str, Any]:
        """Meta field of the expectation suite to store additional information."""
        return self._meta

    @meta.setter
    def meta(self, meta: str | dict) -> None:
        if isinstance(meta, dict):
            self._meta = meta
        elif isinstance(meta, str):
            self._meta = json.loads(meta)
        else:
            raise ValueError("Meta field must be stringified json or dict.")

        if self._id and hasattr(self, "_expectation_suite_engine"):
            # Adding test on suite_engine allows to not run it on init
            self._expectation_suite_engine.update_metadata_from_fields(
                **humps.decamelize(self.to_dict())
            )
