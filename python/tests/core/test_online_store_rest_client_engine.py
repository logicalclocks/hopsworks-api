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

import datetime

import pytest
from hsfs import training_dataset_feature
from hsfs.core import online_store_rest_client_engine


ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS = "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi._get_batch_raw_feature_vectors"
ONLINE_STORE_REST_CLIENT_API_GET_SINGLE_RAW_FEATURE_VECTOR = "hsfs.core.online_store_rest_client_api.OnlineStoreRestClientApi._get_single_raw_feature_vector"


class TestOnlineRestClientEngine:
    @pytest.fixture()
    def training_dataset_features_online(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_fraud_online_training_dataset_features"
        ]["response"]:
            feat["featuregroup"] = feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def training_dataset_features_ticker(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_ticker_training_dataset_features"
        ]["response"]:
            feat["featuregroup"] = feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def training_dataset_complex_features(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_complex_training_dataset_features"
        ]["response"]:
            feat["featuregroup"] = feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def training_dataset_features_mix_rondb_and_opensearch(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get_profile_fraud_tid_fg"][
            "response"
        ]
        embedded_feature_group = backend_fixtures["feature_group"]["get_embedded_fg"][
            "response"
        ]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_mix_rondb_and_opensearch_training_dataset_features"
        ]["response"]:
            if feat["featuregroup"]["name"] == feature_group["name"]:
                feat["featuregroup"] = feature_group
            else:
                feat["featuregroup"] = embedded_feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def training_dataset_features_composite_keys(self, backend_fixtures):
        feature_group = backend_fixtures["feature_group"]["get"]["response"]
        features = []
        for feat in backend_fixtures["training_dataset_feature"][
            "get_composite_keys_training_dataset_features"
        ]["response"]:
            feat["featuregroup"] = feature_group
            features.append(feat)
        return [
            training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
            for feat in features
        ]

    @pytest.fixture()
    def rest_client_engine_base(self):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=[],
        )

    @pytest.fixture()
    def rest_client_engine_ticker(self, training_dataset_features_ticker):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_features_ticker,
        )

    @pytest.fixture()
    def rest_client_engine_composite_keys(
        self, training_dataset_features_composite_keys
    ):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_features_composite_keys,
        )

    @pytest.fixture()
    def rest_client_engine_mix_rondb_and_opensearch(
        self, training_dataset_features_mix_rondb_and_opensearch
    ):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_features_mix_rondb_and_opensearch,
        )

    @pytest.fixture()
    def rest_client_engine_complex_features(self, training_dataset_complex_features):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=training_dataset_complex_features,
        )

    def test_build_base_payload_default_options(
        self, rest_client_engine_base, backend_fixtures
    ):
        # Act
        payload = rest_client_engine_base._build_base_payload()

        # Assert
        for key, value in payload.items():
            if key != "metadataOptions" and key != "options":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )

        assert ("metadataOptions" in payload) is False
        assert ("options" in payload) is True
        assert payload["options"] == {
            "validatePassedFeatures": False,
            "includeDetailedStatus": False,
        }

    def test_build_base_payload_with_metadata_and_options(
        self,
        rest_client_engine_base,
    ):
        # Act
        payload = rest_client_engine_base._build_base_payload(
            metadata_options={"featureName": True, "featureType": False},
            validate_passed_features=True,  # not default
            include_detailed_status=True,  # not default
        )

        # Assert
        assert payload["metadataOptions"]["featureName"] is True
        assert payload["metadataOptions"]["featureType"] is False
        assert payload["options"] == {
            "validatePassedFeatures": True,
            "includeDetailedStatus": True,
        }

    @pytest.mark.parametrize("drop_missing", [True, False])
    def test_convert_rdrs_response_to_feature_vector_if_null(
        self,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
        drop_missing: bool,
    ):
        # Act
        feature_vector_dict = rest_client_engine_ticker._convert_rdrs_response_to_feature_value_row(
            row_feature_values=None,
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=drop_missing,
        )
        feature_vector_list = rest_client_engine_ticker._convert_rdrs_response_to_feature_value_row(
            row_feature_values=None,
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_LIST,
            drop_missing=drop_missing,
        )

        # Assert
        if drop_missing:
            assert feature_vector_dict == {}
            assert feature_vector_list == []
        else:
            assert feature_vector_dict == {
                "ticker": None,
                "when": None,
                "price": None,
                "volume": None,
            }
            assert feature_vector_list == [None, None, None, None]

    @pytest.mark.parametrize(
        "fixture_key",
        [
            "get_single_vector_response_json_complete",
            "get_single_vector_response_json_complete_no_metadata",
        ],
    )
    def test_convert_rdrs_response_to_feature_vector_row_single_complete_response(
        self,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
        fixture_key,
    ):
        # Arrange
        response = backend_fixtures["rondb_server"][fixture_key]
        reference_feature_vector = {
            "ticker": "APPL",
            "when": "2022-01-01 00:00:00",
            "price": 21.3,
            "volume": 10,
        }

        # Act
        feature_vector_dict = rest_client_engine_ticker._convert_rdrs_response_to_feature_value_row(
            row_feature_values=response["features"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        )

        # Assert
        assert feature_vector_dict == reference_feature_vector

    def test_get_batch_feature_vectors_response_json(
        self,
        mocker,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        # No passed features in active call
        payload["passedFeatures"] = []
        # No need for detailed status if drop_missing is False
        payload["options"]["includeDetailedStatus"] = False

        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ],
        )

        # Act
        response_json = rest_client_engine_ticker._get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_RESPONSE_JSON,
            drop_missing=False,
        )

        # Assert
        mock_online_rest_api.assert_called_once_with(payload=payload, timeout=None)
        # Check that the response was not converted to a feature vector if return_type is response json
        assert (
            response_json
            == backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_complete"
            ]
        )

    @pytest.mark.parametrize(
        "fixture_key",
        [
            "get_batch_vector_response_json_complete",
            "get_batch_vector_response_json_complete_no_metadata",
        ],
    )
    def test_get_batch_feature_vectors_as_dict(
        self,
        mocker,
        backend_fixtures,
        fixture_key,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        # No passed features in active call
        payload["passedFeatures"] = []
        # No need for detailed status if drop_missing is False
        payload["options"]["includeDetailedStatus"] = False
        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][fixture_key],
        )

        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": "2022-01-01 00:00:00",
                "price": 21.3,
                "volume": 10,
            },
            {
                "ticker": "GOOG",
                "when": "2022-01-01 00:00:00",
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rest_client_engine_ticker._get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        )

        # Assert
        mock_online_rest_api.assert_called_once_with(payload=payload, timeout=None)
        assert feature_vector_dict == reference_batch_vectors

    def test_get_batch_feature_partial_pk_missing_vectors_as_dict(
        self,
        mocker,
        backend_fixtures,
        rest_client_engine_ticker: online_store_rest_client_engine.OnlineStoreRestClientEngine,
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        # No passed features in active call
        payload["passedFeatures"] = []

        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_partial_pk_value_no_match"
            ],
        )

        reference_batch_vectors = [
            {},
            {
                "ticker": "GOOG",
                "when": "2022-01-01 00:00:00",
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        feature_vector_dict = rest_client_engine_ticker._get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=True,
        )

        # Assert
        mock_online_rest_api.assert_called_once_with(payload=payload, timeout=None)
        assert feature_vector_dict == reference_batch_vectors

    def test_get_batch_feature_partial_error(
        self, mocker, backend_fixtures, rest_client_engine_ticker
    ):
        # Arrange
        payload = backend_fixtures["rondb_server"]["get_batch_vector_payload"].copy()
        # No passed features in active call
        payload["passedFeatures"] = []
        mock_online_rest_api = mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value=backend_fixtures["rondb_server"][
                "get_batch_vector_response_json_partial_error"
            ],
        )
        reference_batch_vectors = [
            {
                "ticker": "APPL",
                "when": "2022-01-01 00:00:00",
                "price": 21.3,
                "volume": 10,
            },
            {},
            {
                "ticker": "GOOG",
                "when": "2022-01-01 00:00:00",
                "price": 12.3,
                "volume": 43,
            },
        ]

        # Act
        batch_vectors = rest_client_engine_ticker._get_batch_feature_vectors(
            entries=payload["entries"],
            return_type=online_store_rest_client_engine.OnlineStoreRestClientEngine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=True,
        )

        # Assert
        mock_online_rest_api.assert_called_once_with(payload=payload, timeout=None)
        assert batch_vectors == reference_batch_vectors


class TestRestEngineRowHandling:
    """Cases the three existing REST modules never reached.

    The suite's null-row test uses a view whose features need no typed
    decoding, so the decoder's null handling was never exercised, and the list
    return mode was never compared against the dictionary mode it is supposed
    to mirror.
    """

    def _feature(self, name, type_, *, inference_helper=False, training_helper=False):
        from hsfs import feature_group as fg_mod

        feature = training_dataset_feature.TrainingDatasetFeature(
            name=name, type=type_, label=False
        )
        feature.inference_helper_column = inference_helper
        feature.training_helper_column = training_helper
        feature._feature_group = fg_mod.FeatureGroup(
            name="fg", version=1, featurestore_id=99, primary_key=[], id=11
        )
        return feature

    def _engine(self, features):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="test_store_featurestore",
            feature_view_name="test_feature_view",
            feature_view_version=2,
            features=features,
        )

    @pytest.mark.parametrize("decoded_type", ["date", "binary"])
    @pytest.mark.parametrize("drop_missing", [True, False])
    def test_a_null_row_on_a_decoded_view_does_not_raise(
        self, decoded_type, drop_missing
    ):
        """RonDB answers a failed read with a null vector inside an HTTP 200.

        Decoding ran before the null-row branches, so indexing the null row
        raised `TypeError: 'NoneType' object is not subscriptable` for any view
        carrying a date or binary feature.
        """
        engine = self._engine(
            [self._feature("id", "bigint"), self._feature("stamp", decoded_type)]
        )

        as_dict = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=None,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=drop_missing,
        )
        as_list = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=None,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_LIST,
            drop_missing=drop_missing,
        )

        if drop_missing:
            assert as_dict == {} and as_list == []
        else:
            assert as_dict == {"id": None, "stamp": None}
            assert as_list == [None, None]

    def test_the_decoder_passes_a_null_row_through(self):
        engine = self._engine(
            [self._feature("id", "bigint"), self._feature("stamp", "date")]
        )

        assert engine._decode_rdrs_feature_values(None) is None

    def test_list_mode_filters_helpers_the_way_dict_mode_does(self):
        """The two return modes must describe the same row.

        The list mode's non-null, keep-missing branch returned the raw row, so
        it carried inference and training helper columns that the dictionary
        mode filtered out.
        """
        engine = self._engine(
            [
                self._feature("id", "bigint"),
                self._feature("amount", "double"),
                self._feature("helper", "double", inference_helper=True),
            ]
        )
        row = [1, 2.5, 9.9]

        as_dict = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        )
        as_list = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_LIST,
            drop_missing=False,
        )

        assert as_dict == {"id": 1, "amount": 2.5}
        assert as_list == list(as_dict.values())

    def test_the_two_modes_agree_when_only_helpers_are_asked_for(self):
        engine = self._engine(
            [
                self._feature("id", "bigint"),
                self._feature("helper", "double", inference_helper=True),
            ]
        )
        row = [1, 9.9]

        as_dict = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
            inference_helpers_only=True,
        )
        as_list = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_LIST,
            drop_missing=False,
            inference_helpers_only=True,
        )

        assert as_dict == {"helper": 9.9}
        assert as_list == [9.9]


class TestDateParsing:
    """The fast parser is used only for the shape the old one accepted identically."""

    def test_the_canonical_shape_parses(self):
        from hsfs.core.online_store_rest_client_engine import _parse_date

        assert _parse_date("2026-09-13") == datetime.date(2026, 9, 13)

    def test_a_non_canonical_but_previously_accepted_shape_still_parses(self):
        """strptime('%Y-%m-%d') accepts a single-digit month; fromisoformat did not."""
        from hsfs.core.online_store_rest_client_engine import _parse_date

        assert _parse_date("2026-9-3") == datetime.date(2026, 9, 3)

    @pytest.mark.parametrize("value", ["20260913", "2026-09-13T00:00:00", "2026-W37-1"])
    def test_a_shape_the_old_parser_rejected_is_still_rejected(self, value):
        """Python 3.11 widened fromisoformat; the wire contract did not."""
        from hsfs.core.online_store_rest_client_engine import _parse_date

        with pytest.raises(ValueError):
            _parse_date(value)

    @pytest.mark.parametrize("value", ["2026-13-01", "not-a-date", ""])
    def test_an_invalid_date_still_raises_value_error(self, value):
        from hsfs.core.online_store_rest_client_engine import _parse_date

        with pytest.raises(ValueError):
            _parse_date(value)


class TestTrainingHelperAlignment:
    """A training helper occupies a response position and belongs to no selection.

    The flag list skipped training helpers while the name list kept them, and
    the two were zipped without a strictness check, so every flag after such a
    column lined up with the wrong name: the view served the helper and dropped
    a real feature.
    """

    def _engine(self, features):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="fs",
            feature_view_name="fv",
            feature_view_version=1,
            features=features,
        )

    def _feature(self, name, *, inference_helper=False, training_helper=False):
        from hsfs import feature_group as fg_mod

        feature = training_dataset_feature.TrainingDatasetFeature(
            name=name, type="bigint", label=False
        )
        feature.inference_helper_column = inference_helper
        feature.training_helper_column = training_helper
        feature._feature_group = fg_mod.FeatureGroup(
            name="fg", version=1, featurestore_id=99, primary_key=[], id=11
        )
        return feature

    def _view(self):
        return self._engine(
            [
                self._feature("a"),
                self._feature("trainer", training_helper=True),
                self._feature("c"),
                self._feature("inferer", inference_helper=True),
            ]
        )

    def test_the_flags_line_up_with_the_names(self):
        engine = self._view()

        assert len(engine.is_inference_helpers_list) == len(
            engine.ordered_feature_names
        )

    def test_the_served_features_are_the_non_helper_ones(self):
        engine = self._view()
        row = [1, 2, 3, 4]

        as_dict = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        )
        as_list = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_LIST,
            drop_missing=False,
        )

        # derived independently of the builder: position 0 is a, position 2 is c
        assert as_dict == {"a": 1, "c": 3}
        assert as_list == [1, 3]

    def test_the_inference_helper_selection_is_the_helper_alone(self):
        engine = self._view()
        row = [1, 2, 3, 4]

        as_dict = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
            inference_helpers_only=True,
        )

        assert as_dict == {"inferer": 4}

    def test_a_training_helper_belongs_to_neither_selection(self):
        engine = self._view()
        row = [1, 2, 3, 4]

        served = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        )
        helpers = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
            inference_helpers_only=True,
        )

        assert "trainer" not in served
        assert "trainer" not in helpers


class TestDecodeMapPositions:
    """The decode map indexes the response row, so it must be built like one.

    It was built by looking each feature's name up in the ordered names, which
    a label is not in and which a duplicated name resolves to twice.
    """

    def _feature(self, name, type_="bigint", *, label=False, training_helper=False):
        from hsfs import feature_group as fg_mod

        feature = training_dataset_feature.TrainingDatasetFeature(
            name=name, type=type_, label=label
        )
        feature.inference_helper_column = False
        feature.training_helper_column = training_helper
        feature._feature_group = fg_mod.FeatureGroup(
            name="fg", version=1, featurestore_id=99, primary_key=[], id=11
        )
        return feature

    def _engine(self, features):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="fs",
            feature_view_name="fv",
            feature_view_version=1,
            features=features,
        )

    @pytest.mark.parametrize("label_type", ["date", "binary"])
    def test_a_view_with_a_decoded_label_can_be_built(self, label_type):
        """A label holds no position in the response row.

        Looking one up raised `ValueError: 'target' is not in list`, so REST
        retrieval was unavailable for the whole view.
        """
        engine = self._engine(
            [self._feature("a"), self._feature("target", label_type, label=True)]
        )

        assert engine.ordered_feature_names == ["a"]
        assert engine._feature_to_decode == {}

    def test_a_repeated_name_decodes_both_of_its_positions(self):
        """A joined view can carry the same feature name twice."""
        engine = self._engine(
            [
                self._feature("when", "date"),
                self._feature("x"),
                self._feature("when", "date"),
            ]
        )

        assert engine._feature_to_decode == {0: "date", 2: "date"}
        assert engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=["2026-01-02", 5, "2026-03-04"],
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_LIST,
            drop_missing=False,
        ) == [datetime.date(2026, 1, 2), 5, datetime.date(2026, 3, 4)]

    def test_a_decoded_training_helper_keeps_its_position(self):
        """It is decoded because it is in the row, and excluded because it is a helper."""
        engine = self._engine(
            [
                self._feature("a"),
                self._feature("when", "date", training_helper=True),
                self._feature("c"),
            ]
        )

        assert engine._feature_to_decode == {1: "date"}
        assert engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=[1, "2026-09-13", 3],
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            drop_missing=False,
        ) == {"a": 1, "c": 3}


class TestRowProjection:
    """A projected row must carry exactly what the mapping of the same row does.

    The projection reads the response by position instead of by name, which is
    a second way through the hottest code in the client. Every case here fixes
    the two against each other rather than against a hand-written expectation.
    """

    def _feature(
        self,
        name,
        type_="bigint",
        *,
        label=False,
        inference_helper=False,
        training_helper=False,
    ):
        from hsfs import feature_group as fg_mod

        feature = training_dataset_feature.TrainingDatasetFeature(
            name=name, type=type_, label=label
        )
        feature.inference_helper_column = inference_helper
        feature.training_helper_column = training_helper
        feature._feature_group = fg_mod.FeatureGroup(
            name="fg", version=1, featurestore_id=99, primary_key=[], id=11
        )
        return feature

    def _engine(self, features):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="fs",
            feature_view_name="fv",
            feature_view_version=1,
            features=features,
        )

    def _projection(self, engine, target_columns):
        """The positions the serving path prepares, from the serving path itself.

        Built by calling it rather than by restating the rule, so a change to
        the rule cannot leave these tests agreeing with a projection nothing
        uses.
        """
        from hsfs.core import vector_server

        server = vector_server.VectorServer(feature_store_id=1)
        server._rest_client_engine = engine
        return server._rest_row_projection(target_columns)

    @pytest.mark.parametrize("drop_missing", [True, False])
    def test_a_projected_row_matches_the_mapping_of_the_same_row(self, drop_missing):
        engine = self._engine(
            [
                self._feature("a"),
                self._feature("when", "date"),
                self._feature("helper", inference_helper=True),
                self._feature("trainer", training_helper=True),
                self._feature("c"),
                self._feature("target", label=True),
            ]
        )
        targets = ["a", "when", "c"]
        row = [1, "2026-03-04", 9, 8, 3]
        status = [{"httpStatus": 200, "featureGroupId": 11}]

        as_dict = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            detailed_status=status,
            drop_missing=drop_missing,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )
        projected = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            detailed_status=status,
            drop_missing=drop_missing,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=self._projection(engine, targets),
        )

        assert projected == [as_dict[name] for name in targets]

    def test_a_repeated_name_projects_the_position_the_mapping_kept(self):
        """A dictionary built from the row keeps the last of a repeated name."""
        engine = self._engine(
            [self._feature("x"), self._feature("y"), self._feature("x")]
        )
        row = [1, 2, 3]

        as_dict = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            drop_missing=False,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
        )
        projected = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            drop_missing=False,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=self._projection(engine, ["x", "y"]),
        )

        assert projected == [as_dict["x"], as_dict["y"]] == [3, 2]

    def test_a_null_row_asked_to_drop_missing_projects_to_nothing(self):
        """The mapping is empty, which the caller turns into no vector at all."""
        engine = self._engine([self._feature("a"), self._feature("b")])

        assert (
            engine._convert_rdrs_response_to_feature_value_row(
                row_feature_values=None,
                detailed_status=[],
                drop_missing=True,
                return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
            == {}
        )
        assert (
            engine._convert_rdrs_response_to_feature_value_row(
                row_feature_values=None,
                detailed_status=[],
                drop_missing=True,
                return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
                projection=self._projection(engine, ["a", "b"]),
            )
            is None
        )

    def test_a_null_row_kept_projects_to_nulls(self):
        engine = self._engine([self._feature("a"), self._feature("b")])

        assert engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=None,
            drop_missing=False,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=self._projection(engine, ["a", "b"]),
        ) == [None, None]

    def test_a_failed_read_falls_back_to_the_mapping(self):
        """A dropped feature shortens the row, so positions no longer hold."""
        engine = self._engine([self._feature("a"), self._feature("b")])

        projected = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=[1, 2],
            detailed_status=[{"httpStatus": 500, "featureGroupId": 11}],
            drop_missing=True,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=self._projection(engine, ["a", "b"]),
        )

        assert projected == {}

    def test_one_failed_row_drops_the_projection_for_the_whole_batch(self, mocker):
        """A batch is read as a whole, so it must not come back as mixed shapes."""
        engine = self._engine([self._feature("a"), self._feature("b")])
        mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value={
                "features": [[1, 2], [3, 4]],
                "detailedStatus": [
                    [{"httpStatus": 200, "featureGroupId": 11}],
                    [{"httpStatus": 500, "featureGroupId": 11}],
                ],
            },
        )

        rows = engine._get_batch_feature_vectors(
            entries=[{"a": 1}, {"a": 3}],
            drop_missing=True,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=self._projection(engine, ["a", "b"]),
        )

        assert all(isinstance(row, dict) for row in rows)


class TestProjectionRejectsRowsItCannotRead:
    """A row is read by position only when it is the row the positions describe.

    The projection indexed whatever came back. A row narrower than the schema
    raised IndexError where reading by name returns the missing values as null,
    and a wider one was silently truncated instead of being reported as a
    schema mismatch.
    """

    def _feature(self, name, type_="bigint"):
        from hsfs import feature_group as fg_mod

        feature = training_dataset_feature.TrainingDatasetFeature(
            name=name, type=type_, label=False
        )
        feature.inference_helper_column = False
        feature.training_helper_column = False
        feature._feature_group = fg_mod.FeatureGroup(
            name="fg", version=1, featurestore_id=99, primary_key=[], id=11
        )
        return feature

    def _engine(self, types=("bigint", "bigint")):
        return online_store_rest_client_engine.OnlineStoreRestClientEngine(
            feature_store_name="fs",
            feature_view_name="fv",
            feature_view_version=1,
            features=[self._feature(f"f{i}", t) for i, t in enumerate(types)],
        )

    def _projection(self, engine, targets):
        from hsfs.core import vector_server

        server = vector_server.VectorServer(feature_store_id=1)
        server._rest_client_engine = engine
        return server._rest_row_projection(targets)

    @pytest.mark.parametrize("row", [[1], [1, 2, 3]])
    def test_a_row_of_the_wrong_width_is_read_by_name(self, row):
        engine = self._engine()
        projection = self._projection(engine, ["f0", "f1"])

        projected = engine._convert_rdrs_response_to_feature_value_row(
            row_feature_values=list(row),
            drop_missing=False,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=projection,
        )

        assert isinstance(projected, dict), (
            "a row that is not the prepared width was still read by position"
        )

    def test_a_short_row_on_a_decoded_view_does_not_raise(self):
        """Decoding indexes the row too, so it cannot assume the width either."""
        engine = self._engine(types=("date", "bigint"))

        assert (
            engine._convert_rdrs_response_to_feature_value_row(
                row_feature_values=[],
                drop_missing=False,
                return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
            == {}
        )

    def test_a_null_status_entry_sends_the_batch_the_general_way(self, mocker):
        """A shape the status check cannot read is a reason to describe, not to guess."""
        engine = self._engine()
        mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value={
                "features": [None, [1, 2]],
                "detailedStatus": [None, [{"httpStatus": 200, "featureGroupId": 11}]],
            },
        )

        rows = engine._get_batch_feature_vectors(
            entries=[{"f0": 1}, {"f0": 2}],
            drop_missing=True,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=self._projection(engine, ["f0", "f1"]),
        )

        assert all(isinstance(row, dict) for row in rows)

    def test_one_row_of_the_wrong_width_sends_the_whole_batch_the_general_way(
        self, mocker
    ):
        engine = self._engine()
        mocker.patch(
            ONLINE_STORE_REST_CLIENT_API_GET_BATCH_RAW_FEATURE_VECTORS,
            return_value={"features": [[1, 2], [1]]},
        )

        rows = engine._get_batch_feature_vectors(
            entries=[{"f0": 1}, {"f0": 2}],
            drop_missing=False,
            return_type=engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            projection=self._projection(engine, ["f0", "f1"]),
        )

        assert all(isinstance(row, dict) for row in rows), (
            "the batch came back as a mix of shapes"
        )
