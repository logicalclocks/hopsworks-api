#
#   Copyright 2025 Hopsworks AB
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
import os
from unittest import mock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.core.delta_engine import DeltaEngine


def _make_fg(location: str):
    fg = mock.MagicMock()
    fg.location = location
    fg.name = "fg"
    fg.version = 1
    return fg


def _patch_apis(
    mocker,
    lb_domain: str = None,
    username: str = None,
    datanode_lb: str = None,
    namenode_lb: str = None,
):
    # variable_api mock
    var_api = mocker.Mock()
    # Configure per-service responses if provided; else fallback to lb_domain or error
    def _lb_side_effect(service: str):
        if service == "datanode" and datanode_lb is not None:
            return datanode_lb
        if service == "namenode" and namenode_lb is not None:
            return namenode_lb
        if lb_domain is not None:
            return lb_domain
        raise FeatureStoreException("no lb")

    var_api.get_loadbalancer_external_domain.side_effect = _lb_side_effect
    mocker.patch("hsfs.core.variable_api.VariableApi", return_value=var_api)

    # project_api mock
    proj_api = mocker.Mock()
    if username is not None:
        proj_api.get_user_info.return_value = {"username": username}
    else:
        proj_api.get_user_info.return_value = {}
    mocker.patch("hopsworks.core.project_api.ProjectApi", return_value=proj_api)

    return var_api, proj_api


def _patch_client(mocker, is_external: bool, project_name: str = "proj", certs: str = "/pems"):
    client = mocker.Mock()
    client._is_external.return_value = is_external
    client.project_name = project_name
    client.get_certs_folder.return_value = certs
    mocker.patch("hopsworks_common.client.get_instance", return_value=client)
    return client


class TestDeltaEngine:
    def test_setup_delta_rs_internal_noop(self, mocker, monkeypatch):
        # Arrange
        _patch_client(mocker, is_external=False)
        var_api, proj_api = _patch_apis(mocker, lb_domain="dn.example.com", username="u")
        fg = _make_fg("hopsfs://nn:8020/projects/p1")

        # Act
        DeltaEngine(
            feature_store_id=1,
            feature_store_name="fs",
            feature_group=fg,
            spark_session=None,
            spark_context=None,
        )

        # Assert
        # internal -> no LB lookups performed during setup
        var_api.get_loadbalancer_external_domain.assert_not_called()
        proj_api.get_user_info.assert_not_called()

    def test_setup_delta_rs_external_success(self, mocker, monkeypatch):
        # Arrange
        cl = _patch_client(mocker, is_external=True, project_name="prj", certs="/tmp/pems")
        _patch_apis(mocker, lb_domain="dn.example.com", username="user1")
        fg = _make_fg("hopsfs://nn:8020/projects/p1")

        # Act
        DeltaEngine(
            feature_store_id=1,
            feature_store_name="fs",
            feature_group=fg,
            spark_session=None,
            spark_context=None,
        )

        # Assert
        # env should be set
        assert os.environ["PEMS_DIR"] == "/tmp/pems"
        assert (
            os.environ["HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE"] == "dn.example.com"
        )
        assert os.environ["LIBHDFS_DEFAULT_USER"] == f"{cl.project_name}__user1"

    def test_setup_delta_rs_external_no_datanode_lb(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=True)
        _patch_apis(mocker, lb_domain=None, username="user1")
        fg = _make_fg("hopsfs://nn:8020/projects/p1")

        # Act & Assert
        with pytest.raises(FeatureStoreException) as e:
            DeltaEngine(
                feature_store_id=1,
                feature_store_name="fs",
                feature_group=fg,
                spark_session=None,
                spark_context=None,
            )
        assert "datanode load balancer" in str(e.value)

    def test_setup_delta_rs_external_no_username(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=True, project_name="prj")
        _patch_apis(mocker, lb_domain="dn.example.com", username=None)
        fg = _make_fg("hopsfs://nn:8020/projects/p1")

        # Act & Assert
        with pytest.raises(FeatureStoreException) as e:
            DeltaEngine(
                feature_store_id=1,
                feature_store_name="fs",
                feature_group=fg,
                spark_session=None,
                spark_context=None,
            )
        assert "Cannot get user name" in str(e.value)

    def test_get_delta_rs_location_internal(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        _patch_apis(mocker, lb_domain="nn.example.com", username="user1")
        fg = _make_fg("hopsfs://nn:8020/projects/p1")
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act
        loc = engine._get_delta_rs_location()

        # Assert
        assert loc == "hdfs://nn:8020/projects/p1"

    def test_get_delta_rs_location_external_success(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=True)
        _patch_apis(mocker, lb_domain="nn.lb.example.com", username="user1")
        fg = _make_fg("hopsfs://nn:8020/projects/p1")
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act
        loc = engine._get_delta_rs_location()

        # Assert
        assert loc == "hdfs://nn.lb.example.com:8020/projects/p1"

    def test_get_delta_rs_location_external_error(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=True)
        # Allow _setup_delta_rs to succeed (needs datanode), but fail on namenode during location
        _patch_apis(
            mocker,
            username="user1",
            datanode_lb="dn.example.com",
            namenode_lb=None,
        )
        fg = _make_fg("hopsfs://nn:8020/projects/p1")
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act & Assert
        with pytest.raises(FeatureStoreException) as e:
            engine._get_delta_rs_location()
        assert "namenode load balancer" in str(e.value)

    def test_setup_delta_read_opts_snapshot_query(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        alias = mock.Mock()
        alias.left_feature_group_end_timestamp = None
        alias.left_feature_group_start_timestamp = None

        # Act
        result = engine._setup_delta_read_opts(alias, None)

        # Assert
        assert result == {}

    def test_setup_delta_read_opts_time_travel_query(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        alias = mock.Mock()
        alias.left_feature_group_end_timestamp = 1234567890
        alias.left_feature_group_start_timestamp = None
        mocker.patch("hsfs.util.get_delta_datestr_from_timestamp", return_value="t")

        # Act
        result = engine._setup_delta_read_opts(alias, None)

        # Assert
        assert result == {
            engine.DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT: "t"
        }

    def test_setup_delta_read_opts_merges_options(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        alias = mock.Mock()
        alias.left_feature_group_end_timestamp = 1234567890
        alias.left_feature_group_start_timestamp = None
        mocker.patch("hsfs.util.get_delta_datestr_from_timestamp", return_value="t")

        # Act
        opts = engine._setup_delta_read_opts(alias, {"k": "v"})

        # Assert
        assert opts[engine.DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT] == "t"
        assert opts["k"] == "v"

    def test_generate_merge_query_primary_key_only(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.primary_key = ["id"]
        fg.partition_key = []
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act
        q = engine._generate_merge_query("s", "u")

        # Assert
        assert q == "s.id == u.id"

    def test_generate_merge_query_with_event_time(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.primary_key = ["id"]
        fg.partition_key = []
        fg.event_time = "ts"
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act
        q = engine._generate_merge_query("s", "u")

        # Assert
        assert q == "s.id == u.id AND s.ts == u.ts"

    def test_generate_merge_query_with_partition_keys(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.primary_key = ["id"]
        fg.partition_key = ["p1", "p2"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act
        q = engine._generate_merge_query("s", "u")

        # Assert
        assert q == "s.id == u.id AND s.p1 == u.p1 AND s.p2 == u.p2"

    def test_generate_merge_query_all_key_types(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.primary_key = ["id"]
        fg.partition_key = ["p1", "p2"]
        fg.event_time = "ts"
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act
        q = engine._generate_merge_query("s", "u")

        # Assert
        assert q == "s.id == u.id AND s.ts == u.ts AND s.p1 == u.p1 AND s.p2 == u.p2"

    def test_register_temporary_table_calls_spark_read(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        spark.read.format.return_value.options.return_value.load.return_value.createOrReplaceTempView.return_value = None
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.prepare_spark_location.return_value = "loc"
        engine = DeltaEngine(1, "fs", fg, spark, None)
        mocker.patch.object(engine, "_setup_delta_read_opts", return_value={"a": 1})
        alias = mock.Mock()
        alias.alias = "tmp"

        # Act
        engine.register_temporary_table(alias, {"b": 2})

        # Assert
        spark.read.format.assert_called_once_with(engine.DELTA_SPARK_FORMAT)

    def test_save_delta_fg_calls_write_and_commit_spark(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)
        mock_commit = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi.commit")
        mocker.patch.object(engine, "_write_delta_dataset", return_value=mock.Mock())

        # Act
        result = engine.save_delta_fg(dataset=mock.Mock(), write_options={"x": 1}, validation_id="vid")

        # Assert
        assert mock_commit.called
        assert result == mock_commit.return_value

    def test_save_delta_fg_calls_write_and_commit_rs(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        mock_commit = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi.commit")
        mocker.patch.object(engine, "_write_delta_rs_dataset", return_value=mock.Mock())

        # Act
        result = engine.save_delta_fg(dataset=mock.Mock(), write_options=None, validation_id=None)

        # Assert
        assert mock_commit.called
        assert result == mock_commit.return_value

    def test_delete_record_importerror_spark_delta_spark_missing(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine.delete_record(delete_df=mock.Mock())
        assert "delta-spark" in str(e.value)

    def test_delete_record_importerror_rs_deltalake_missing(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine.delete_record(delete_df=mock.Mock())
        assert "hops-deltalake" in str(e.value)

    def test_write_delta_dataset_importerror_missing_delta_spark(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine._write_delta_dataset(dataset=mock.Mock(), write_options=None)
        assert "delta-spark" in str(e.value)

    def test_write_delta_rs_dataset_importerror_missing_deltalake(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine._write_delta_rs_dataset(dataset=mock.Mock())
        assert "hops-deltalake" in str(e.value)

    def test_prepare_df_for_delta_importerror(self, monkeypatch):
        # Arrange
        # Force ImportError by ensuring pandas/pyarrow imports fail
        import builtins
        original_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name in ("pyarrow",):
                raise ImportError("missing")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        # Act & Assert
        with pytest.raises(ImportError):
            DeltaEngine._prepare_df_for_delta(df=mock.Mock())

    def test_vacuum_executes_sql(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)
        fg.prepare_spark_location.return_value = "/loc"

        # Act
        engine.vacuum(24)

        # Assert
        spark.sql.assert_called_once()
        assert "VACUUM '/loc' RETAIN 24 HOURS" in spark.sql.call_args[0][0]

    def test_get_last_commit_metadata_importerror_spark(self):
        # Act & Assert
        with pytest.raises(ImportError) as e:
            DeltaEngine._get_last_commit_metadata(spark_context=mock.Mock(), base_path="/p")
        assert "delta-spark" in str(e.value)

    def test_get_last_commit_metadata_delta_rs_importerror(self):
        # Act & Assert
        with pytest.raises(ImportError) as e:
            DeltaEngine._get_last_commit_metadata_delta_rs(base_path="/p")
        assert "hops-deltalake" in str(e.value)
