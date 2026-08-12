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


from hsfs import (
    statistics_config,
    storage_connector,
    training_dataset,
    training_dataset_feature,
    training_dataset_split,
)


class TestTrainingDataset:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["training_dataset"]["get"]["response"]

        # Act
        td_list = training_dataset.TrainingDataset.from_response_json(json)

        # Assert
        assert len(td_list) == 1
        td = td_list[0]
        assert td.id == 11
        assert td.name == "test_name"
        assert td.version == 1
        assert td.description == "test_description"
        assert td.data_format == "hudi"
        assert td._start_time == 1646438400000
        assert td._end_time == 1646697600000
        assert td.validation_size == 0.0
        assert td.test_size == 0.5
        assert td.train_start == 4
        assert td.train_end == 5
        assert td.validation_start == 6
        assert td.validation_end == 7
        assert td.test_start == 8
        assert td.test_end == 9
        assert td.coalesce is True
        assert td.seed == 123
        assert td.location == "test_location"
        assert td._from_query == "test_from_query"
        assert td._querydto == "test_querydto"
        assert td.feature_store_id == 22
        assert td.train_split == "test_train_split"
        assert td.training_dataset_type == "HOPSFS_TRAINING_DATASET"
        assert isinstance(
            td.data_source.storage_connector, storage_connector.HopsFSConnector
        )
        assert len(td._features) == 1
        assert isinstance(
            td._features[0], training_dataset_feature.TrainingDatasetFeature
        )
        assert len(td.splits) == 1
        assert isinstance(td.splits[0], training_dataset_split.TrainingDatasetSplit)
        assert isinstance(td.statistics_config, statistics_config.StatisticsConfig)
        assert td.label == ["test_name"]

    def test_from_response_json_external(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["training_dataset"]["get_external"]["response"]

        # Act
        td_list = training_dataset.TrainingDataset.from_response_json(json)

        # Assert
        assert len(td_list) == 1
        td = td_list[0]
        assert td.id == 11
        assert td.name == "test_name"
        assert td.version == 1
        assert td.description == "test_description"
        assert td.data_format == "hudi"
        assert td._start_time == 1646438400000
        assert td._end_time == 1646697600000
        assert td.validation_size == 0.0
        assert td.test_size == 0.5
        assert td.train_start == 4
        assert td.train_end == 5
        assert td.validation_start == 6
        assert td.validation_end == 7
        assert td.test_start == 8
        assert td.test_end == 9
        assert td.coalesce is True
        assert td.seed == 123
        assert td.location == "test_location"
        assert td._from_query == "test_from_query"
        assert td._querydto == "test_querydto"
        assert td.feature_store_id == 22
        assert td.train_split == "test_train_split"
        assert td.training_dataset_type == "EXTERNAL_TRAINING_DATASET"
        assert isinstance(
            td.data_source.storage_connector, storage_connector.S3Connector
        )
        assert len(td._features) == 1
        assert isinstance(
            td._features[0], training_dataset_feature.TrainingDatasetFeature
        )
        assert len(td.splits) == 1
        assert isinstance(td.splits[0], training_dataset_split.TrainingDatasetSplit)
        assert isinstance(td.statistics_config, statistics_config.StatisticsConfig)
        assert td.label == ["test_name"]

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["training_dataset"]["get_basic_info"]["response"]

        # Act
        td_list = training_dataset.TrainingDataset.from_response_json(json)

        # Assert
        assert len(td_list) == 1
        td = td_list[0]
        assert td.id is None
        assert td.name == "test_name"
        assert td.version == 1
        assert td.description is None
        assert td.data_format == "hudi"
        assert td._start_time is None
        assert td._end_time is None
        assert td.validation_size is None
        assert td.test_size is None
        assert td.train_start is None
        assert td.train_end is None
        assert td.validation_start is None
        assert td.validation_end is None
        assert td.test_start is None
        assert td.test_end is None
        assert td.coalesce is False
        assert td.seed is None
        assert td.location == ""
        assert td._from_query is None
        assert td._querydto is None
        assert td.feature_store_id == 22
        assert td.train_split is None
        assert td.training_dataset_type == "HOPSFS_TRAINING_DATASET"
        assert isinstance(
            td.data_source.storage_connector, storage_connector.HopsFSConnector
        )
        assert len(td._features) == 0
        assert len(td.splits) == 0
        assert isinstance(td.statistics_config, statistics_config.StatisticsConfig)
        assert td.label == []

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["training_dataset"]["get_empty"]["response"]

        # Act
        td_list = training_dataset.TrainingDataset.from_response_json(json)

        # Assert
        assert len(td_list) == 0

    def test_from_response_json_omits_lookback(self, mocker, backend_fixtures):
        # Older training datasets persisted before lookback support have no `lookbacks`
        # field on the response. `from_response_json` must accept that and leave
        # `_lookback` as None.
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["training_dataset"]["get"]["response"]
        td = training_dataset.TrainingDataset.from_response_json(json)
        if isinstance(td, list):
            td = td[0]
        assert td._lookback is None

    def test_training_dataset_to_dict_emits_lookback_when_set(self, mocker):
        # When the engine layer attaches a Lookback during create_training_*,
        # `to_dict` emits the `lookbacks` wire shape so the backend's
        # `QueryController.resolveLookbacks` can pick it up.
        from datetime import date

        from hsfs.constructor.lookback import FeatureGroupLookback, Lookback

        mocker.patch("hopsworks_common.client._get_instance")
        td = training_dataset.TrainingDataset(
            name="td_lb",
            version=1,
            data_format="parquet",
            featurestore_id=1,
        )
        lb = FeatureGroupLookback(
            key="partition_key", start=date(2026, 5, 10), end=date(2026, 5, 17)
        )
        td._lookback = Lookback(default=lb)
        payload = td.to_dict()
        assert payload["lookback"] == {"defaultLookback": lb.to_dict()}

    def test_training_dataset_accepts_lookback_via_constructor(self, mocker):
        # The in-memory training_data / train_test_split / train_validation_test_split
        # methods construct the TD directly with `lookbacks=Lookback.from_user_input(lookback)`
        # rather than threading through an engine kwarg, so the constructor must accept
        # a Lookback instance and emit it on the wire via to_dict().
        from datetime import date

        from hsfs.constructor.lookback import FeatureGroupLookback, Lookback

        mocker.patch("hopsworks_common.client._get_instance")
        lb = FeatureGroupLookback(
            key="partition_key", start=date(2026, 5, 10), end=date(2026, 5, 17)
        )
        td = training_dataset.TrainingDataset(
            name="td_inmem_lb",
            version=None,
            data_format="tsv",
            featurestore_id=1,
            lookback=Lookback(default=lb),
        )
        payload = td.to_dict()
        assert payload["lookback"] == {"defaultLookback": lb.to_dict()}

    def test_training_dataset_normalizes_wire_lookback_dict(self, mocker):
        # `update_from_response_json` runs `self.__init__(**response)` with the decamelized
        # backend payload. If the response carries `lookbacks`, the constructor must
        # normalize that dict into a `Lookback` instance — otherwise `_lookback` ends up
        # a raw dict and the next `to_dict()` blows up with `AttributeError: 'dict' object
        # has no attribute 'to_dict'`.
        from hsfs.constructor.lookback import Lookback

        mocker.patch("hopsworks_common.client._get_instance")
        wire = {
            "default_lookback": {
                "lookback_key": "PARTITION_KEY",
                "start": 1747008000000,
                "end": 1747612800000,
            },
        }
        td = training_dataset.TrainingDataset(
            name="td_wire_lb",
            version=1,
            data_format="parquet",
            featurestore_id=1,
            lookback=wire,
        )
        assert isinstance(td._lookback, Lookback)
        # Round-trips through to_dict() so wire-form lookbacks restored from the backend
        # remain serialisable for subsequent requests.
        payload = td.to_dict()
        assert payload["lookback"]["defaultLookback"]["lookbackKey"] == "PARTITION_KEY"
