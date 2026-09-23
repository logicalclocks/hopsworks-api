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
from __future__ import annotations

import base64
import itertools
import logging
from datetime import date, datetime
from typing import Any

from hsfs import training_dataset_feature as td_feature_mod
from hsfs import util
from hsfs.core import online_store_rest_client_api


_logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    """Parse an RDRS date, which the server writes as `YYYY-MM-DD`.

    `date.fromisoformat` is around thirty times faster than `strptime` for that exact shape.
    It is only used for that shape: from Python 3.11 it also accepts forms `strptime("%Y-%m-%d")` rejects, and a wider parser here would silently start accepting wire values this client never accepted.
    Anything else goes to the original parser, which keeps both its acceptance and its error.
    """
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return date.fromisoformat(value)
    return datetime.strptime(value, "%Y-%m-%d").date()


class OnlineStoreRestClientEngine:
    RETURN_TYPE_FEATURE_VALUE_DICT = "feature_value_dict"
    RETURN_TYPE_FEATURE_VALUE_LIST = "feature_value_list"
    RETURN_TYPE_RESPONSE_JSON = "response_json"  # as a python dict
    MISSING_STATUS = "MISSING"
    BINARY_TYPE = "binary"
    DATE_TYPE = "date"
    FEATURE_TYPE_TO_DECODE = [BINARY_TYPE, DATE_TYPE]

    def __init__(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        features: list[td_feature_mod.TrainingDatasetFeature],
    ):
        """Initialize the Online Store Rest Client Engine.

        This class contains the logic to mediate the interaction between the python client and the RonDB Rest Server Feature Store API.

        Parameters:
            feature_store_name: The name of the feature store in which the feature view is registered.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            features: A list of features to be used for the feature vector conversion. Note that the features
                must be ordered according to the feature vector schema.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Initializing Online Store Rest Client Engine for Feature View {feature_view_name}, version: {feature_view_version} in Feature Store {feature_store_name}."
            )
        self._online_store_rest_client_api = (
            online_store_rest_client_api.OnlineStoreRestClientApi()
        )
        self._feature_store_name = feature_store_name
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._features = features
        self._ordered_feature_names = []
        self._inference_helpers_feature_names = []
        self._no_helpers_feature_names = []
        self._is_inference_helpers_list = []
        self._feature_names_per_fg_id: dict[int, list[str]] = {}
        for feat in features:
            if not feat.label:
                self._ordered_feature_names.append(feat.name)
                if feat.feature_group.id not in self._feature_names_per_fg_id:
                    self._feature_names_per_fg_id[feat.feature_group.id] = [feat.name]
                else:
                    self._feature_names_per_fg_id[feat.feature_group.id].append(
                        feat.name
                    )
                if feat.inference_helper_column:
                    self._is_inference_helpers_list.append(True)
                elif feat.training_helper_column:
                    # Neither an inference helper nor a served feature, but it does occupy a position in the response row, so it needs one here too.
                    # Skipping it shifted every flag after it against the names, which returned the wrong features: a view with a training helper served that helper and dropped a real feature.
                    # `None` matches neither selection, so it is excluded from both.
                    self._is_inference_helpers_list.append(None)
                else:
                    self._is_inference_helpers_list.append(False)
        self._feature_to_decode = self._get_feature_to_decode(features)
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Mapping fg_id to feature names: {self._feature_names_per_fg_id}."
            )

    def _get_feature_to_decode(
        self, features: list[td_feature_mod.TrainingDatasetFeature]
    ) -> dict[int, str]:
        """Get a mapping of feature indices to their types for features that need decoding.

        This method identifies features that have types requiring special decoding from the RonDB Rest Server
        response and maps their position in the ordered feature list to their type.

        Parameters:
            features: List of TrainingDatasetFeature objects containing feature metadata

        Returns:
            A dictionary mapping feature indices to their type strings for features that require decoding.
            The indices correspond to the position in _ordered_feature_names.
        """
        # Walked in the same order the response row is, rather than looked up by name.
        # A label holds no position in that row, so looking one up raised `ValueError` and no view with a date or binary label could build its REST engine at all; and a name that appears twice, which a joined view can produce, resolved both occurrences to the first position, leaving the second undecoded.
        feature_to_decode = {}
        position = 0
        for feat in features:
            if feat.label:
                continue
            if feat.type in self.FEATURE_TYPE_TO_DECODE:
                feature_to_decode[position] = feat.type
            position += 1
        return feature_to_decode

    def _build_base_payload(
        self,
        metadata_options: dict[str, bool] | None = None,
        validate_passed_features: bool = False,
        include_detailed_status: bool = False,
    ) -> dict[str, str | dict[str, bool]]:
        """Build the base payload for the RonDB REST Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        Parameters:
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            validate_passed_features: Whether to validate the passed features against
                the feature view schema on the RonDB Server.
            include_detailed_status: Whether to include detailed status information in the response.
                This is necessary to drop missing features from the feature vector.

        Returns:
            A payload dictionary containing metadata information to send to the RonDB REST Server Feature Store API.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Building base payload for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
            )
        base_payload = {
            "featureStoreName": util._strip_feature_store_suffix(
                self._feature_store_name
            ),
            "featureViewName": self._feature_view_name,
            "featureViewVersion": self._feature_view_version,
            "options": {
                "validatePassedFeatures": validate_passed_features,
                "includeDetailedStatus": include_detailed_status,
            },
        }

        if metadata_options is not None:
            base_payload["metadataOptions"] = {
                "featureName": metadata_options.get("featureName", False),
                "featureType": metadata_options.get("featureType", False),
            }
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Base payload: {base_payload}")
        return base_payload

    def _is_projectable_row(self, row_feature_values: list[Any] | None) -> bool:
        """Whether this row can be read by position.

        A row carries one value per non-label feature.
        One that does not is not the row the projection was prepared against, whether it is short, wide or absent for a reason the caller has to be told about, so it is read by name instead and described in full.
        """
        if row_feature_values is None:
            return True
        return len(row_feature_values) == len(self._is_inference_helpers_list)

    def _is_projectable_batch(
        self,
        rows: list[list[Any] | None],
        detailed_statuses: list[Any],
        drop_missing: bool,
    ) -> bool:
        """Whether every row of this batch can be read by position.

        All of them or none.
        A caller reads a batch as one result, so a mix of rows read by position and rows read by name would be a result whose shape varies from entry to entry, and nothing downstream is prepared to read that.
        """
        if not all(self._is_projectable_row(row) for row in rows):
            return False
        if not drop_missing:
            return True
        if len(detailed_statuses) != len(rows):
            # One entry's statuses per row, or there is a row whose reads are not described.
            # Reading such a batch by position would answer for a row without having checked whether anything behind it failed.
            return False
        for statuses in detailed_statuses:
            # One entry's statuses, which a caller has to be told about if any read behind them failed.
            # A shape this cannot read is itself a reason to take the descriptive path: that path reports a missing status as an error rather than answering with the row anyway.
            if not isinstance(statuses, list):
                return False
            for status in statuses:
                if not isinstance(status, dict) or "httpStatus" not in status:
                    return False
                if status["httpStatus"] != 200:
                    return False
        return True

    def _decode_rdrs_feature_values(
        self, feature_values: list[Any] | None
    ) -> list[Any] | None:
        """Decode binary and date values from the RonDB Rest Server response.

        A null row has nothing to decode.
        RonDB answers a failed read with a null feature vector inside an HTTP 200, and indexing it here raised `TypeError` for any view carrying a date or binary feature, before the null-row branches below ever ran.

        Parameters:
            feature_values: List of feature values from the RonDB Rest Server, or `None` for a null row.

        Returns:
            List of decoded feature values with binary values base64 decoded and date strings
            converted to datetime.date objects, or `None` for a null row.
        """
        if feature_values is None:
            return None
        width = len(feature_values)
        for feature_index, data_type in self._feature_to_decode.items():
            if feature_index >= width:
                # A row narrower than the schema it was decoded against.
                # It is reported as such further up; decoding is not the place to raise IndexError about it.
                continue
            if (
                data_type == self.BINARY_TYPE
                and feature_values[feature_index] is not None
            ):
                feature_values[feature_index] = base64.b64decode(
                    feature_values[feature_index]
                )
            elif (
                data_type == self.DATE_TYPE
                and feature_values[feature_index] is not None
            ):
                feature_values[feature_index] = _parse_date(
                    feature_values[feature_index]
                )
        return feature_values

    def _get_single_feature_vector(
        self,
        entry: dict[str, Any],
        passed_features: dict[str, Any] | None = None,
        metadata_options: dict[str, bool] | None = None,
        drop_missing: bool = False,
        inference_helpers_only: bool = False,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
        timeout: float | None = None,
        projection: tuple[int, ...] | None = None,
    ) -> (
        tuple[list[Any] | dict[str, Any], list[dict[str, Any]] | None] | dict[str, Any]
    ):
        """Get a single feature vector from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        Parameters:
            entry: A dictionary with the feature names as keys and the primary key as values.
            passed_features: A dictionary with the feature names as keys and the values to substitute for this specific vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            drop_missing: Whether to drop missing features from the feature vector. Requires including detailed status.
            inference_helpers_only: Whether to return only the inference helper columns.
            return_type: The type of the return value. Either "feature_value_dict", "feature_value_list" or "response_json".
            timeout: Seconds to wait for the response. The configured REST default applies when unset.
            projection: Response positions to read the caller's vector from, in the caller's order.
                Set by a caller that wants the row itself rather than a feature name to value mapping.

        Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": The status pertinent to this single feature vector. One of COMPLETE, MISSING or ERROR.
                - "features": A list of the feature values.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.
                    Null if metadata options are not set or status is ERROR.
                - "detailedStatus": A list of dictionaries with detailed status information for each read operations.
                    Keys include operationId, featureGroupId, httpStatus and message.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the server response status code is not 200.
            ValueError: If the length of the feature values and metadata in the reponse does not match.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Getting single raw feature vector for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
            )
            _logger.debug(f"entry: {entry}, passed features: {passed_features}")
        payload = self._build_base_payload(
            metadata_options=metadata_options,
            # This ensures consistency with the sql client behaviour.
            validate_passed_features=False,
            # Only necessary to get the detailed status if we are not allowing missing features.
            include_detailed_status=drop_missing,
        )
        payload["entries"] = entry
        payload["passedFeatures"] = passed_features

        response = self._online_store_rest_client_api._get_single_raw_feature_vector(
            payload=payload, timeout=timeout
        )
        if return_type != self.RETURN_TYPE_RESPONSE_JSON:
            return self._convert_rdrs_response_to_feature_value_row(
                row_feature_values=response["features"],
                detailed_status=response.get("detailedStatus", None),
                drop_missing=drop_missing,
                inference_helpers_only=inference_helpers_only,
                return_type=return_type,
                projection=projection,
            )
        return response

    def _get_batch_feature_vectors(
        self,
        entries: list[dict[str, Any]],
        passed_features: list[dict[str, Any]] | None = None,
        metadata_options: dict[str, bool] | None = None,
        drop_missing: bool = False,
        inference_helpers_only: bool = False,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
        timeout: float | None = None,
        projection: tuple[int, ...] | None = None,
    ) -> tuple[list[list[Any] | dict[str, Any]], list[dict[str, Any]]] | dict[str, Any]:
        """Get a list of feature vectors from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        Parameters:
            entries: A list of dictionaries with the feature names as keys and the primary key as values.
            passed_features: A list of dictionaries with the feature names as keys and the values to substitute.
                Note that the list should be ordered in the same way as the entries list.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            drop_missing: Whether to drop missing features from the feature vector. Requires including detailed status.
            inference_helpers_only: Whether to return only the inference helper columns.
            return_type: The type of the return value. Either "feature_value_dict", "feature_value_list" or "response_json".
            timeout: Seconds to wait for the response. The configured REST default applies when unset.
            projection: Response positions to read the caller's vector from, in the caller's order.
                Set by a caller that wants the row itself rather than a feature name to value mapping.

        Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": A list of the status for each feature vector retrieval.
                    Possible values are COMPLETE, MISSING or ERROR.
                - "features": A list containing list of the feature values for each feature_vector.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.
                    Null if metadata options are not set or status is ERROR.
                - "detailedStatus": A list of dictionaries with detailed status information for each read operations.
                    Keys include operationId, featureGroupId, httpStatus and message.

        Raises:
            hsfs.client.exceptions.RestAPIError: If the server response status code is not 200.
            ValueError: If the length of the passed features does not match the length of the entries.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Getting batch raw feature vectors for Feature View {self._feature_view_name}, version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
            )
            _logger.debug(f"entries: {entries}\npassed features: {passed_features}")
        payload = self._build_base_payload(
            metadata_options=metadata_options,
            # This ensures consistency with the sql client behaviour.
            validate_passed_features=False,
            # Only necessary to get the detailed status if we are not allowing missing features.
            include_detailed_status=drop_missing,
        )
        payload["entries"] = entries
        if isinstance(passed_features, list) and (
            len(passed_features) == len(entries) or len(passed_features) == 0
        ):
            payload["passedFeatures"] = passed_features
        elif passed_features is None:
            payload["passedFeatures"] = []
        else:
            raise ValueError(
                "Length of passed features does not match the length of the entries. "
                "If some entries do not have passed features, pass an empty dict for those entries."
            )

        response = self._online_store_rest_client_api._get_batch_raw_feature_vectors(
            payload=payload, timeout=timeout
        )

        if return_type != self.RETURN_TYPE_RESPONSE_JSON:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Converting batch response to feature value rows for each."
                )
            if projection is not None and not self._is_projectable_batch(
                response["features"],
                list(response.get("detailedStatus", None) or ()),
                drop_missing,
            ):
                # Checked for the batch before any of it is read, so what comes back is every row by position or every row by name, never a mix of the two.
                projection = None
            return [
                self._convert_rdrs_response_to_feature_value_row(
                    row_feature_values=row,
                    detailed_status=detailed_status,
                    drop_missing=drop_missing,
                    return_type=return_type,
                    inference_helpers_only=inference_helpers_only,
                    projection=projection,
                )
                for row, detailed_status in itertools.zip_longest(
                    response["features"], response.get("detailedStatus", []) or []
                )
            ]
        return response

    def _convert_rdrs_response_to_feature_value_row(
        self,
        row_feature_values: list[Any] | None,
        drop_missing: bool,
        detailed_status: list[dict[str, Any]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_LIST,
        inference_helpers_only: bool = False,
        projection: tuple[int, ...] | None = None,
    ) -> list[Any] | dict[str, Any] | None:
        """Convert the response from the RonDB Rest Server Feature Store API to a feature:value dict.

        When RonDB Server encounter an error it may send a null value for the feature vector. This function
        will handle this case and return a dictionary with None values for all feature names.

        Parameters:
            row_feature_values: A list of the feature values.
            drop_missing: Whether to drop missing features from the feature vector. Relies on detailed status.
            detailed_status: A list of dictionaries with detailed status information for each read operations.
                Keys include operationId, featureGroupId, httpStatus and message.
            return_type: The type of the return value. Either "feature_value_dict" or "feature_value_list".
            inference_helpers_only: Whether to return only the inference helper columns.
            projection: Response positions to read the caller's vector from, in the caller's order.
                Ignored when a read failed, since the row is then not the shape it was prepared against.

        Returns:
            A dictionary with the feature names as keys and the feature values as values. Values types are not guaranteed to
            match the feature type in the metadata. Timestamp SQL types are converted to python datetime.
            A projected row is returned as a list in the projection's order instead, or as `None` for a
            null row the caller asked to have dropped.
        """
        row_feature_values = self._decode_rdrs_feature_values(row_feature_values)
        if drop_missing and (
            detailed_status is None and row_feature_values is not None
        ):
            raise ValueError(
                "Detailed status is required to drop missing features from the feature vector."
            )
        failed_read_feature_names = []
        if detailed_status is not None and drop_missing:
            for operation_status in detailed_status:
                if operation_status["httpStatus"] != 200:
                    failed_read_feature_names.extend(
                        self.feature_names_per_fg_id[operation_status["featureGroupId"]]
                    )
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Feature names which failed on read: {failed_read_feature_names}."
                )

        if (
            projection is not None
            and not failed_read_feature_names
            and self._is_projectable_row(row_feature_values)
        ):
            # Every read answered and the row is the width the projection was prepared against, so the caller's vector can be taken from it by position.
            # Anything else falls through to the mapping, which the caller knows how to complete and to report on.
            if row_feature_values is None:
                return None if drop_missing else [None] * len(projection)
            return [row_feature_values[index] for index in projection]

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_LIST:
            if row_feature_values is None and drop_missing:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug("Convert null feature vector to empty list.")
                return []
            if row_feature_values is None:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(
                        "Feature vector is null, returning None for all features."
                    )
                return [
                    None
                    for is_helper in self.is_inference_helpers_list
                    if is_helper is inference_helpers_only
                ]
            if drop_missing:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(
                        "Dropping missing features from the feature vector and return as list."
                    )
                return [
                    value
                    for (name, value, is_helper) in zip(
                        self.ordered_feature_names,
                        row_feature_values,
                        self.is_inference_helpers_list,
                        strict=False,
                    )
                    if (
                        name not in failed_read_feature_names
                        and is_helper is inference_helpers_only
                    )
                ]
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Returning feature vector as list.")
            return [
                value
                for (value, is_helper) in zip(
                    row_feature_values,
                    self.is_inference_helpers_list,
                    strict=False,
                )
                if is_helper is inference_helpers_only
            ]

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT:
            if row_feature_values is None and drop_missing:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug("Convert null feature vector to empty dict.")
                return {}
            if row_feature_values is None:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(
                        "Feature vector is null, returning None for all features."
                    )
                return {
                    name: None
                    for (name, is_helper) in zip(
                        self.ordered_feature_names,
                        self.is_inference_helpers_list,
                        strict=False,
                    )
                    if is_helper is inference_helpers_only
                }
            if drop_missing:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(
                        "Dropping missing features from the feature vector and return as dict."
                    )
                return {
                    name: value
                    for (name, value, is_helper) in zip(
                        self.ordered_feature_names,
                        row_feature_values,
                        self.is_inference_helpers_list,
                        strict=False,
                    )
                    if (
                        name not in failed_read_feature_names
                        and is_helper is inference_helpers_only
                    )
                }
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Returning feature vector as dict.")
            return {
                name: value
                for (name, value, is_helper) in zip(
                    self.ordered_feature_names,
                    row_feature_values,
                    self.is_inference_helpers_list,
                    strict=False,
                )
                if is_helper is inference_helpers_only
            }
        return None

    @property
    def feature_store_name(self) -> str:
        return self._feature_store_name

    @property
    def feature_view_name(self) -> str:
        return self._feature_view_name

    @property
    def feature_view_version(self) -> int:
        return self._feature_view_version

    @property
    def features(self) -> list[td_feature_mod.TrainingDatasetFeature]:
        return self._features

    @property
    def ordered_feature_names(self) -> list[str]:
        return self._ordered_feature_names

    @property
    def is_inference_helpers_list(self) -> list[bool]:
        return self._is_inference_helpers_list

    @property
    def feature_names_per_fg_id(self) -> dict[int, list[str]]:
        return self._feature_names_per_fg_id

    @property
    def online_store_rest_client_api(
        self,
    ) -> online_store_rest_client_api.OnlineStoreRestClientApi:
        return self._online_store_rest_client_api
