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


from hsfs import feature_group as feature_group_mod
from hsfs import feature_store as feature_store_mod


class TestFeatureStore:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]

        # Act
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Assert
        assert fs.id == 11
        assert fs.name == "test_featurestore_name"
        assert fs._created == "test_created"
        assert fs.project_name == "test_project_name"
        assert fs.project_id == 67
        assert fs._online_feature_store_name == "test_online_featurestore_name"
        assert fs._online_feature_store_size == 31
        assert fs._offline_feature_store_name == "test_offline_featurestore_name"
        assert fs.online_enabled is True
        assert fs._num_feature_groups == 2
        assert fs._num_training_datasets == 3
        assert fs._num_storage_connectors == 4
        assert fs._num_feature_views == 5

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get_basic_info"]["response"]

        # Act
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Assert
        assert fs.id == 11
        assert fs.name == "test_featurestore_name"
        assert fs._created == "test_created"
        assert fs.project_name == "test_project_name"
        assert fs.project_id == 67
        assert fs._online_feature_store_name is None
        assert fs._online_feature_store_size is None
        assert fs._offline_feature_store_name == "test_offline_featurestore_name"
        assert fs.online_enabled is True
        assert fs._num_feature_groups is None
        assert fs._num_training_datasets is None
        assert fs._num_storage_connectors is None
        assert fs._num_feature_views is None

    def test_get_feature_group(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        fg = feature_group_mod.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi._get", return_value=fg
        )

        # Act
        fg_res = fs.get_feature_group("test_feature_group_name")

        # Assert
        assert fg_res.feature_store == fs
        assert fg_res._feature_store == fs

    def test_get_feature_group_by_name_not_found(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi._get_feature_group_by_version",
            return_value=None,
        )

        # Act
        fg_res = fs.get_feature_group("test_feature_group_name")

        # Assert
        assert fg_res is None

    def test_get_feature_groups_not_found(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi._get_feature_group_by_name",
            return_value=[],
        )

        # Act
        fg_res = fs.get_feature_groups("test_feature_group_name")

        # Assert
        assert fg_res == []

    def test_get_feature_group_by_name_and_version_not_found(
        self, backend_fixtures, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi._get_feature_group_by_version",
            return_value=None,
        )

        # Act
        fg_res = fs.get_feature_group("test_feature_group_name", version=10)

        # Assert
        assert fg_res is None

    def test_create_feature_group(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act
        fg_res = fs.create_feature_group(
            "test_feature_group_name",
            version=1,
            primary_key=["bob"],
            event_time="when",
            online_enabled=True,
        )

        # Assert
        assert fg_res.name == "test_feature_group_name"
        assert fg_res.version == 1
        assert fg_res.primary_key == ["bob"]
        assert fg_res.event_time == "when"
        assert fg_res.online_enabled is True
        assert fg_res.feature_store == fs
        assert fg_res._feature_store == fs

    def test_create_feature_group_normalizes_tags(self, backend_fixtures, mocker):
        # Arrange
        from hopsworks_common.tag import Tag

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act: a dict tag is accepted and normalized to a Tag object.
        fg_res = fs.create_feature_group(
            "test_feature_group_name",
            version=1,
            tags={"name": "team", "value": "fraud"},
        )

        # Assert
        assert len(fg_res._tags) == 1
        assert isinstance(fg_res._tags[0], Tag)
        assert fg_res._tags[0].name == "team"
        assert fg_res._tags[0].value == "fraud"

    def test_get_or_create_feature_group_forwards_tags_when_missing(
        self, backend_fixtures, mocker
    ):
        # Arrange
        from hopsworks_common.tag import Tag

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        # The feature group does not exist yet, so it is constructed with tags.
        mocker.patch.object(fs._feature_group_api, "_get", return_value=None)

        # Act
        fg_res = fs.get_or_create_feature_group(
            "test_feature_group_name",
            version=1,
            tags=[Tag(name="team", value="fraud"), {"name": "tier", "value": "gold"}],
        )

        # Assert: mixed Tag/dict input is normalized onto the constructed group.
        assert [(t.name, t.value) for t in fg_res._tags] == [
            ("team", "fraud"),
            ("tier", "gold"),
        ]

    def test_get_or_create_feature_group_attaches_existing_to_shared_job(
        self, backend_fixtures, mocker
    ):
        # Arrange
        from hsfs import feature as feature_mod
        from hsfs.core.data_source import DataSource
        from hsfs.storage_connector import RedshiftConnector

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        sc = RedshiftConnector(9, "sc", fs.id)
        ds = DataSource(storage_connector=sc)
        shared_job = ds.new_ingestion_job("crm_ingestion")

        # An already-existing feature group returned by the backend.
        existing = feature_group_mod.FeatureGroup(
            name="accounts",
            version=1,
            featurestore_id=fs.id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            features=[feature_mod.Feature("id", "int")],
            sink_enabled=True,
            data_source=ds,
        )
        existing._id = 55
        mocker.patch.object(fs._feature_group_api, "_get", return_value=existing)

        # Act
        fg_res = fs.get_or_create_feature_group(
            "accounts", version=1, sink_job=shared_job
        )

        # Assert: the existing feature group is registered as a target so a rerun
        # is not left with an empty multi-table job.
        assert fg_res is existing
        assert [t._feature_group_id for t in shared_job.targets] == [55]

    def test_get_or_create_feature_view_forwards_tags_when_missing(
        self, backend_fixtures, mocker
    ):
        # Arrange
        from hopsworks_common.tag import Tag

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        # The feature view does not exist yet, so create_feature_view is invoked.
        mocker.patch.object(fs._feature_view_engine, "_get", return_value=None)
        create_feature_view = mocker.patch.object(
            fs, "create_feature_view", return_value="created_fv"
        )
        tags = [Tag(name="team", value="fraud")]

        # Act
        fv_res = fs.get_or_create_feature_view(
            name="test_feature_view_name",
            query=mocker.Mock(),
            version=1,
            tags=tags,
        )

        # Assert: tags are forwarded verbatim to create_feature_view.
        assert fv_res == "created_fv"
        assert create_feature_view.call_args.kwargs["tags"] == tags

    def test_create_feature_group_time_travel_format_defaults_to_delta(
        self, backend_fixtures, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act
        fg_res = fs.create_feature_group("test_feature_group_name", version=1)

        # Assert: no format and no data source format falls back to DELTA.
        assert fg_res.time_travel_format == "DELTA"

    def test_create_feature_group_time_travel_format_from_data_source(
        self, backend_fixtures, mocker
    ):
        # Arrange
        from hsfs.core.data_source import DataSource

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_pyiceberg", return_value=True
        )
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act
        fg_res = fs.create_feature_group(
            "test_feature_group_name",
            version=1,
            data_source=DataSource(format="ICEBERG"),
        )

        # Assert: the data source format is used when no format is given.
        assert fg_res.time_travel_format == "ICEBERG"

    def test_create_feature_group_explicit_time_travel_format_wins(
        self, backend_fixtures, mocker
    ):
        # Arrange
        from hsfs.core.data_source import DataSource

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act
        fg_res = fs.create_feature_group(
            "test_feature_group_name",
            version=1,
            time_travel_format="DELTA",
            data_source=DataSource(format="ICEBERG"),
        )

        # Assert: an explicitly requested format wins over the data source.
        assert fg_res.time_travel_format == "DELTA"

    def test_create_external_feature_group_data_format_from_data_source(
        self, backend_fixtures, mocker
    ):
        # Arrange
        from hsfs.core.data_source import DataSource

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act
        fg_res = fs.create_external_feature_group(
            "test_external_fg_name",
            data_source=DataSource(format="parquet"),
        )

        # Assert: the data source format is used when no data_format is given.
        assert fg_res.data_format == "PARQUET"

    def test_create_external_feature_group_explicit_data_format_wins(
        self, backend_fixtures, mocker
    ):
        # Arrange
        from hsfs.core.data_source import DataSource

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)

        # Act
        fg_res = fs.create_external_feature_group(
            "test_external_fg_name",
            data_format="csv",
            data_source=DataSource(format="parquet"),
        )

        # Assert: an explicitly requested data_format wins over the data source.
        assert fg_res.data_format == "CSV"

    def test_get_feature_view_by_name_not_found(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        mocker.patch(
            "hsfs.core.feature_view_api.FeatureViewApi._get_by_name_version",
            return_value=None,
        )

        # Act
        fv_res = fs.get_feature_view("test_feature_view_name")

        # Assert
        assert fv_res is None

    def test_get_feature_views_not_found(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        mocker.patch(
            "hsfs.core.feature_view_api.FeatureViewApi._get_by_name", return_value=[]
        )

        # Act
        fv_res = fs.get_feature_views("test_feature_view_name")

        # Assert
        assert fv_res == []

    def test_get_feature_view_by_name_and_version_not_found(
        self, backend_fixtures, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["feature_store"]["get"]["response"]
        fs = feature_store_mod.FeatureStore.from_response_json(json)
        mocker.patch(
            "hsfs.core.feature_view_api.FeatureViewApi._get_by_name_version",
            return_value=None,
        )

        # Act
        fv_res = fs.get_feature_view("test_feature_view_name")

        # Assert
        assert fv_res is None
