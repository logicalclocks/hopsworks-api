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
import sys
import types
from datetime import date, datetime
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.core.delta_engine import DeltaEngine
from hsfs.feature_group_commit import FeatureGroupCommit


def _make_fg(location: str):
    fg = mock.MagicMock()
    fg.location = location
    fg.name = "fg"
    fg.version = 1
    # Non-partitioned by default so grain materialization short-circuits;
    # partitioned_by tests set this explicitly.
    fg.partitioned_by = None
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

    var_api._get_loadbalancer_external_domain.side_effect = _lb_side_effect
    mocker.patch("hsfs.core.variable_api.VariableApi", return_value=var_api)

    # project_api mock
    proj_api = mocker.Mock()
    if username is not None:
        proj_api._get_user_info.return_value = {"username": username}
    else:
        proj_api._get_user_info.return_value = {}
    mocker.patch("hopsworks_common.core.project_api.ProjectApi", return_value=proj_api)

    return var_api, proj_api


def _patch_client(
    mocker, is_external: bool, project_name: str = "proj", certs: str = "/pems"
):
    client = mocker.Mock()
    client._is_external.return_value = is_external
    client.project_name = project_name
    client._get_certs_folder.return_value = certs
    mocker.patch("hopsworks_common.client._get_instance", return_value=client)
    return client


def _force_missing_delta_spark(monkeypatch):
    import builtins

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "delta.tables":
            raise ImportError("missing delta-spark")
        return original_import(name, *args, **kwargs)

    for mod in ("delta", "delta.tables"):
        if mod in sys.modules:
            monkeypatch.delitem(sys.modules, mod, raising=False)
    monkeypatch.setattr(builtins, "__import__", fake_import)


def _force_missing_deltalake(monkeypatch):
    import builtins

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("deltalake"):
            raise ImportError("missing deltalake")
        return original_import(name, *args, **kwargs)

    for mod in ("deltalake", "deltalake.exceptions"):
        if mod in sys.modules:
            monkeypatch.delitem(sys.modules, mod, raising=False)
    monkeypatch.setattr(builtins, "__import__", fake_import)


def _patch_deltalake_modules(mocker, monkeypatch, table_factory):
    table_not_found_error = type("TableNotFoundError", (Exception,), {})
    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.__path__ = []
    fake_deltalake.DeltaTable = table_factory
    fake_deltalake.write_deltalake = mocker.Mock()

    fake_exceptions = types.ModuleType("deltalake.exceptions")
    fake_exceptions.TableNotFoundError = table_not_found_error
    fake_deltalake.exceptions = fake_exceptions

    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)
    monkeypatch.setitem(sys.modules, "deltalake.exceptions", fake_exceptions)

    return fake_deltalake, table_not_found_error


class TestDeltaEngine:
    def test_setup_delta_rs_internal_noop(self, mocker, monkeypatch):
        # Arrange
        _patch_client(mocker, is_external=False)
        var_api, proj_api = _patch_apis(
            mocker, lb_domain="dn.example.com", username="u"
        )
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
        var_api._get_loadbalancer_external_domain.assert_not_called()
        proj_api._get_user_info.assert_not_called()

    def test_setup_delta_rs_external_success(self, mocker, monkeypatch):
        # Arrange
        cl = _patch_client(
            mocker, is_external=True, project_name="prj", certs="/tmp/pems"
        )
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
        assert os.environ["HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE"] == "dn.example.com"
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
        mocker.patch("hsfs.util._get_delta_datestr_from_timestamp", return_value="t")

        # Act
        result = engine._setup_delta_read_opts(alias, None)

        # Assert
        assert result == {engine.DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT: "t"}

    def test_setup_delta_read_opts_merges_options(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        alias = mock.Mock()
        alias.left_feature_group_end_timestamp = 1234567890
        alias.left_feature_group_start_timestamp = None
        mocker.patch("hsfs.util._get_delta_datestr_from_timestamp", return_value="t")

        # Act
        opts = engine._setup_delta_read_opts(alias, read_options={"k": "v"})

        # Assert
        assert opts[engine.DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT] == "t"
        assert opts["k"] == "v"

    def test_setup_delta_read_opts_cdf_always_uses_timestamp(self, mocker):
        # The CDF branch in _setup_delta_read_opts always emits startingTimestamp
        # regardless of how the start compares to the Delta log's earliest commit.
        # The timestamp-comparison guard has been removed because history() returns
        # the in-commit timestamp while Delta's CDF lower-bound uses file mtime —
        # the two can differ by tens of milliseconds on a fresh table, making a
        # pre-flight comparison unreliable. The retry logic is now in
        # _register_temporary_table instead.
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        alias = mock.Mock()
        alias.left_feature_group_start_timestamp = 1000
        alias.left_feature_group_end_timestamp = 5000
        mocker.patch("hsfs.util._get_delta_datestr_from_timestamp", return_value="t")

        # Act
        result = engine._setup_delta_read_opts(alias, "hopsfs://nn:8020/p")

        # Assert — always startingTimestamp; endingTimestamp present because end is set
        assert result == {
            "readChangeFeed": "true",
            "startingTimestamp": "t",
            "endingTimestamp": "t",
        }

    def test_setup_delta_read_opts_cdf_start_after_earliest_uses_timestamp(
        self, mocker
    ):
        # CDF branch always emits startingTimestamp; no end timestamp set here.
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        alias = mock.Mock()
        alias.left_feature_group_start_timestamp = 5000
        alias.left_feature_group_end_timestamp = None
        mocker.patch("hsfs.util._get_delta_datestr_from_timestamp", return_value="t")

        # Act
        result = engine._setup_delta_read_opts(alias, "hopsfs://nn:8020/p")

        # Assert
        assert result == {"readChangeFeed": "true", "startingTimestamp": "t"}

    def test_setup_delta_read_opts_end_before_earliest_uses_version(self, mocker):
        # Snapshot-with-end counterpart of the same skew: an end time before the
        # Delta log's first commit falls back to versionAsOf on the earliest commit.
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        alias = mock.Mock()
        alias.left_feature_group_start_timestamp = None
        alias.left_feature_group_end_timestamp = 1000
        mocker.patch.object(
            engine, "_get_delta_earliest_commit", return_value=(3, 2000)
        )

        # Act
        result = engine._setup_delta_read_opts(alias, "hopsfs://nn:8020/p")

        # Assert
        assert result == {engine.DELTA_QUERY_TIME_TRAVEL_AS_OF_VERSION: 3}

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

    def test_generate_merge_query_with_partition_values_adds_in_clause(self, mocker):
        # Arrange - verify Option A: literal IN filters are appended so DataFusion
        # can prune Parquet files to only the overlapping partitions.
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.primary_key = ["id"]
        fg.partition_key = ["month"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)

        partition_values = {"month": ["2024-01", "2024-02"]}

        # Act
        q = engine._generate_merge_query("src", "upd", partition_values)

        # Assert - join predicates first, then IN filter for partition pruning
        assert "src.id == upd.id" in q
        assert "src.month == upd.month" in q
        assert "src.month IN ('2024-01', '2024-02')" in q

    def test_generate_merge_query_no_partition_values_unchanged(self, mocker):
        # Arrange - when partition_values is None (e.g. no partition key or
        # unpartitioned table), the query must match the baseline without IN clauses.
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.primary_key = ["id"]
        fg.partition_key = ["month"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)

        q_no_pv = engine._generate_merge_query("s", "u", partition_values=None)
        q_baseline = engine._generate_merge_query("s", "u")

        assert q_no_pv == q_baseline
        assert "IN" not in q_no_pv

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
        engine._register_temporary_table(alias, {"b": 2})

        # Assert
        spark.read.format.assert_called_once_with(engine.DELTA_SPARK_FORMAT)

    def _patch_pyspark_col(self, mocker):
        """Patch pyspark.sql.functions.col so tests don't need a live SparkContext.

        The CDF branch of _register_temporary_table does
        ``from pyspark.sql.functions import col`` inside the method body, so the
        name ``col`` is bound at call time from the real ``pyspark.sql.functions``
        module. We replace the ``col`` attribute on that module so the import
        resolves to a lightweight mock without touching sys.modules.
        """
        import pyspark.sql.functions as _psf

        fake_col = mock.Mock(return_value=mock.Mock())
        fake_col.return_value.isin.return_value = mock.Mock()
        mocker.patch.object(_psf, "col", fake_col)
        return fake_col

    def test_register_temporary_table_cdf_retry_on_earliest_version_error(self, mocker):
        # When the CDF .load() raises an exception whose message contains
        # "before the earliest version", _register_temporary_table must retry
        # with startingVersion (from _get_delta_earliest_commit) instead of
        # startingTimestamp. The tempview must be created on the retry.
        # Arrange
        _patch_client(mocker, is_external=False)
        self._patch_pyspark_col(mocker)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.prepare_spark_location.return_value = "loc"
        engine = DeltaEngine(1, "fs", fg, spark, None)

        # First load raises the Delta earliest-version error; second succeeds.
        first_load_result = mock.Mock()
        first_load_result.filter.return_value.createOrReplaceTempView.side_effect = (
            Exception(
                "The provided timestamp (2026-06-16 13:29:06.164) is before the "
                "earliest version available to this table (2026-06-16 13:29:06.38)."
            )
        )
        retry_load_result = mock.Mock()
        retry_load_result.filter.return_value.createOrReplaceTempView.return_value = (
            None
        )

        spark.read.format.return_value.options.return_value.load.side_effect = [
            first_load_result,
            retry_load_result,
        ]

        mocker.patch.object(
            engine,
            "_get_delta_earliest_commit",
            return_value=(0, 1718540946380),
        )
        mocker.patch(
            "hsfs.util._get_delta_datestr_from_timestamp", return_value="ts_str"
        )

        alias = mock.Mock()
        alias.alias = "tmp"
        alias.left_feature_group_start_timestamp = 1718540946164
        alias.left_feature_group_end_timestamp = None

        # Act
        engine._register_temporary_table(alias, read_options=None, is_cdc_query=True)

        # Assert — two load calls made (first failed, second succeeded)
        assert spark.read.format.return_value.options.return_value.load.call_count == 2

        # Second call must use startingVersion, not startingTimestamp
        second_opts_call = spark.read.format.return_value.options.call_args_list[1]
        retry_opts = (
            second_opts_call[1] if second_opts_call[1] else second_opts_call[0][0]
        )
        assert "startingVersion" in retry_opts
        assert "startingTimestamp" not in retry_opts
        assert retry_opts["startingVersion"] == 0

        # Tempview created exactly once on the retry path
        retry_load_result.filter.return_value.createOrReplaceTempView.assert_called_once_with(
            "tmp"
        )

    def test_register_temporary_table_cdf_retry_both_bounds_before_earliest(
        self, mocker
    ):
        # When BOTH startingTimestamp and endingTimestamp fall before the Delta
        # log's earliest commit (the fresh-FG skew scenario), the retry must
        # remove both timestamp keys and set startingVersion=earliest AND
        # endingVersion=earliest so CDF reads exactly the one commit that
        # covers the monitoring window.
        # Arrange
        _patch_client(mocker, is_external=False)
        self._patch_pyspark_col(mocker)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.prepare_spark_location.return_value = "loc"
        engine = DeltaEngine(1, "fs", fg, spark, None)

        earliest_version = 0
        earliest_ts_ms = 1718540946380  # 13:29:06.380

        first_load_result = mock.Mock()
        first_load_result.filter.return_value.createOrReplaceTempView.side_effect = (
            Exception(
                "The provided timestamp (2026-06-16 13:29:06.164) is before the "
                "earliest version available to this table (2026-06-16 13:29:06.38)."
            )
        )
        retry_load_result = mock.Mock()
        retry_load_result.filter.return_value.createOrReplaceTempView.return_value = (
            None
        )
        spark.read.format.return_value.options.return_value.load.side_effect = [
            first_load_result,
            retry_load_result,
        ]

        mocker.patch.object(
            engine,
            "_get_delta_earliest_commit",
            return_value=(earliest_version, earliest_ts_ms),
        )
        mocker.patch(
            "hsfs.util._get_delta_datestr_from_timestamp", return_value="ts_str"
        )

        alias = mock.Mock()
        alias.alias = "tmp"
        alias.left_feature_group_start_timestamp = 1718540946164  # 13:29:06.164
        alias.left_feature_group_end_timestamp = 1718540946164  # same — also before

        # Act
        engine._register_temporary_table(alias, read_options=None, is_cdc_query=True)

        # Assert — two load calls: first failed, second succeeded
        assert spark.read.format.return_value.options.return_value.load.call_count == 2

        second_opts_call = spark.read.format.return_value.options.call_args_list[1]
        retry_opts = (
            second_opts_call[1] if second_opts_call[1] else second_opts_call[0][0]
        )
        assert retry_opts["startingVersion"] == earliest_version
        assert retry_opts["endingVersion"] == earliest_version
        assert "startingTimestamp" not in retry_opts
        assert "endingTimestamp" not in retry_opts

        retry_load_result.filter.return_value.createOrReplaceTempView.assert_called_once_with(
            "tmp"
        )

    def test_register_temporary_table_cdf_retry_start_before_earliest_end_later(
        self, mocker
    ):
        # When startingTimestamp is before the earliest commit but endingTimestamp
        # is a genuine later bound (end > earliest[1]), the retry must use
        # startingVersion for the start and keep the original endingTimestamp
        # for the end (NOT switch end to endingVersion).
        # Arrange
        _patch_client(mocker, is_external=False)
        self._patch_pyspark_col(mocker)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.prepare_spark_location.return_value = "loc"
        engine = DeltaEngine(1, "fs", fg, spark, None)

        earliest_version = 0
        earliest_ts_ms = 1718540946380  # 13:29:06.380

        first_load_result = mock.Mock()
        first_load_result.filter.return_value.createOrReplaceTempView.side_effect = (
            Exception(
                "The provided timestamp (2026-06-16 13:29:06.164) is before the "
                "earliest version available to this table (2026-06-16 13:29:06.38)."
            )
        )
        retry_load_result = mock.Mock()
        retry_load_result.filter.return_value.createOrReplaceTempView.return_value = (
            None
        )
        spark.read.format.return_value.options.return_value.load.side_effect = [
            first_load_result,
            retry_load_result,
        ]

        mocker.patch.object(
            engine,
            "_get_delta_earliest_commit",
            return_value=(earliest_version, earliest_ts_ms),
        )
        # _get_delta_datestr_from_timestamp is called once for the initial opts
        # (startingTimestamp) and then once more in the retry for endingTimestamp.
        mocker.patch(
            "hsfs.util._get_delta_datestr_from_timestamp", return_value="ts_str"
        )

        alias = mock.Mock()
        alias.alias = "tmp"
        alias.left_feature_group_start_timestamp = 1718540946164  # before earliest
        alias.left_feature_group_end_timestamp = 1718540950000  # well after earliest

        # Act
        engine._register_temporary_table(alias, read_options=None, is_cdc_query=True)

        # Assert — two load calls: first failed, second succeeded
        assert spark.read.format.return_value.options.return_value.load.call_count == 2

        second_opts_call = spark.read.format.return_value.options.call_args_list[1]
        retry_opts = (
            second_opts_call[1] if second_opts_call[1] else second_opts_call[0][0]
        )
        assert retry_opts["startingVersion"] == earliest_version
        assert retry_opts["endingTimestamp"] == "ts_str"
        assert "startingTimestamp" not in retry_opts
        assert "endingVersion" not in retry_opts

        retry_load_result.filter.return_value.createOrReplaceTempView.assert_called_once_with(
            "tmp"
        )

    def test_register_temporary_table_cdf_happy_path_no_retry(self, mocker):
        # When the CDF .load() succeeds on the first attempt, no retry occurs and
        # _get_delta_earliest_commit is never called.
        # Arrange
        _patch_client(mocker, is_external=False)
        self._patch_pyspark_col(mocker)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.prepare_spark_location.return_value = "loc"
        engine = DeltaEngine(1, "fs", fg, spark, None)

        load_result = mock.Mock()
        load_result.filter.return_value.createOrReplaceTempView.return_value = None
        spark.read.format.return_value.options.return_value.load.return_value = (
            load_result
        )

        earliest_mock = mocker.patch.object(engine, "_get_delta_earliest_commit")
        mocker.patch(
            "hsfs.util._get_delta_datestr_from_timestamp", return_value="ts_str"
        )

        alias = mock.Mock()
        alias.alias = "tmp"
        alias.left_feature_group_start_timestamp = 5000
        alias.left_feature_group_end_timestamp = None

        # Act
        engine._register_temporary_table(alias, read_options=None, is_cdc_query=True)

        # Assert — only one load call, no retry, no history lookup
        spark.read.format.return_value.options.return_value.load.assert_called_once()
        earliest_mock.assert_not_called()
        load_result.filter.return_value.createOrReplaceTempView.assert_called_once_with(
            "tmp"
        )

    def test_register_temporary_table_cdf_other_exception_propagates(self, mocker):
        # An AnalysisException (or any error) whose message does NOT contain
        # "before the earliest version" must propagate unmodified — no retry.
        # Arrange
        _patch_client(mocker, is_external=False)
        self._patch_pyspark_col(mocker)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.prepare_spark_location.return_value = "loc"
        engine = DeltaEngine(1, "fs", fg, spark, None)

        unrelated_error = RuntimeError("Column 'x' does not exist")
        load_result = mock.Mock()
        load_result.filter.return_value.createOrReplaceTempView.side_effect = (
            unrelated_error
        )
        spark.read.format.return_value.options.return_value.load.return_value = (
            load_result
        )

        earliest_mock = mocker.patch.object(engine, "_get_delta_earliest_commit")
        mocker.patch(
            "hsfs.util._get_delta_datestr_from_timestamp", return_value="ts_str"
        )

        alias = mock.Mock()
        alias.alias = "tmp"
        alias.left_feature_group_start_timestamp = 5000
        alias.left_feature_group_end_timestamp = None

        # Act & Assert — original error propagates, no retry, no history lookup
        with pytest.raises(RuntimeError, match="Column 'x' does not exist"):
            engine._register_temporary_table(
                alias, read_options=None, is_cdc_query=True
            )

        earliest_mock.assert_not_called()
        spark.read.format.return_value.options.return_value.load.assert_called_once()

    def test_save_delta_fg_calls_write_and_commit_spark(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)
        mock_commit = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi._commit"
        )
        mocker.patch.object(engine, "_write_delta_dataset", return_value=mock.Mock())

        # Act
        result = engine._save_delta_fg(
            dataset=mock.Mock(), write_options={"x": 1}, validation_id="vid"
        )

        # Assert
        assert mock_commit.called
        assert result == mock_commit.return_value

    def test_save_delta_fg_calls_write_and_commit_rs(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        mock_commit = mocker.patch(
            "hsfs.core.feature_group_api.FeatureGroupApi._commit"
        )
        mocker.patch.object(engine, "_write_delta_rs_dataset", return_value=mock.Mock())

        # Act
        result = engine._save_delta_fg(
            dataset=mock.Mock(), write_options=None, validation_id=None
        )

        # Assert
        assert mock_commit.called
        assert result == mock_commit.return_value

    def test_delete_record_importerror_spark_delta_spark_missing(
        self, mocker, monkeypatch
    ):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)
        _force_missing_delta_spark(monkeypatch)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine._delete_record(delete_df=mock.Mock())
        assert "delta-spark" in str(e.value)

    def test_save_empty_table_uses_pyspark_path(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, mock.Mock())
        pyspark_mock = mocker.patch.object(engine, "_save_empty_delta_table_pyspark")
        python_mock = mocker.patch.object(engine, "_save_empty_delta_table_python")

        # Act
        engine._save_empty_table()

        # Assert
        pyspark_mock.assert_called_once_with(write_options=None)
        python_mock.assert_not_called()

    def test_save_empty_table_uses_python_path(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        pyspark_mock = mocker.patch.object(engine, "_save_empty_delta_table_pyspark")
        python_mock = mocker.patch.object(engine, "_save_empty_delta_table_python")

        # Act
        engine._save_empty_table()

        # Assert
        python_mock.assert_called_once_with(write_options=None)
        pyspark_mock.assert_not_called()

    def test_delete_record_importerror_rs_deltalake_missing(self, mocker, monkeypatch):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)

        _force_missing_deltalake(monkeypatch)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine._delete_record(delete_df=mock.Mock())
        assert "hops-deltalake" in str(e.value)

    def test_write_delta_dataset_importerror_missing_delta_spark(
        self, mocker, monkeypatch
    ):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)
        _force_missing_delta_spark(monkeypatch)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine._write_delta_dataset(dataset=mock.Mock(), write_options=None)
        assert "delta-spark" in str(e.value)

    def test_write_delta_rs_dataset_importerror_missing_deltalake(
        self, mocker, monkeypatch
    ):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)

        _force_missing_deltalake(monkeypatch)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            engine._write_delta_rs_dataset(dataset=mock.Mock())
        assert "hops-deltalake" in str(e.value)

    def test_materialize_partitioned_by_grains_arrow(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.partitioned_by = ["year", "month"]
        fg.event_time = "event_ts"
        engine = DeltaEngine(1, "fs", fg, None, None)
        table = pa.table(
            {
                "id": [1, 2],
                "event_ts": pa.array(
                    [datetime(2026, 1, 15), datetime(2026, 3, 2)],
                    type=pa.timestamp("us"),
                ),
            }
        )

        # Act
        out = engine._materialize_partitioned_by_grains(table)

        # Assert: grain columns derived from event_time, in partitioned_by order
        assert out.column_names == ["id", "event_ts", "year", "month"]
        assert out.column("year").to_pylist() == [2026, 2026]
        assert out.column("month").to_pylist() == [1, 3]
        # Idempotent: already-present grains are not recomputed or duplicated
        out2 = engine._materialize_partitioned_by_grains(out)
        assert out2.column_names == out.column_names

    def test_materialize_partitioned_by_grains_integer_seconds_event_time(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.partitioned_by = ["year"]
        fg.event_time = "ts"
        engine = DeltaEngine(1, "fs", fg, None, None)
        # 1736899200 = 2025-01-15 00:00:00 UTC (10-digit -> seconds)
        table = pa.table({"id": [1], "ts": pa.array([1736899200], type=pa.int64())})

        # Act
        out = engine._materialize_partitioned_by_grains(table)

        # Assert
        assert out.column("year").to_pylist() == [2025]

    def test_write_delta_rs_dataset_append_mode_skips_merge(self, mocker, monkeypatch):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.partition_key = []
        fg.primary_key = ["id"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)
        delta_table = mocker.MagicMock()
        fake_deltalake, _ = _patch_deltalake_modules(
            mocker, monkeypatch, mocker.MagicMock(return_value=delta_table)
        )
        dataset = mocker.Mock()
        mocker.patch.object(engine, "_prepare_df_for_delta", return_value=dataset)
        mock_commit = mocker.patch.object(
            engine, "_get_last_commit_metadata", return_value="commit"
        )

        # Act
        result = engine._write_delta_rs_dataset(
            dataset=mocker.Mock(), write_options={"mode": "append"}
        )

        # Assert
        assert result == "commit"
        fake_deltalake.write_deltalake.assert_called_once_with(
            "hdfs://nn:8020/p", dataset, mode="append", storage_options=None
        )
        delta_table.merge.assert_not_called()
        mock_commit.assert_called_once_with(
            None, "hdfs://nn:8020/p", storage_options={}
        )

    def test_write_delta_rs_dataset_existing_table_uses_merge_by_default(
        self, mocker, monkeypatch
    ):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.partition_key = []
        fg.primary_key = ["id"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)
        delta_table = mocker.MagicMock()
        merge_builder = delta_table.merge.return_value
        fake_deltalake, _ = _patch_deltalake_modules(
            mocker, monkeypatch, mocker.MagicMock(return_value=delta_table)
        )
        dataset = mocker.Mock()
        mocker.patch.object(engine, "_prepare_df_for_delta", return_value=dataset)
        mock_commit = mocker.patch.object(
            engine, "_get_last_commit_metadata", return_value="commit"
        )

        # Act
        result = engine._write_delta_rs_dataset(
            dataset=mocker.Mock(), write_options={"mode": "overwrite"}
        )

        # Assert
        assert result == "commit"
        delta_table.merge.assert_called_once_with(
            source=dataset,
            predicate="fg_1_source.id == fg_1_updates.id",
            source_alias="fg_1_updates",
            target_alias="fg_1_source",
        )
        merge_builder.when_matched_update_all.assert_called_once()
        insert_builder = merge_builder.when_matched_update_all.return_value.when_not_matched_insert_all
        insert_builder.assert_called_once()
        insert_builder.return_value.execute.assert_called_once()
        fake_deltalake.write_deltalake.assert_not_called()
        mock_commit.assert_called_once_with(
            None, "hdfs://nn:8020/p", storage_options={}
        )

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

    @pytest.mark.parametrize("input_precision", ["s", "ms", "us", "ns"])
    @pytest.mark.parametrize("target_precision", ["s", "ms", "us", "ns"])
    def test_prepare_df_for_delta_all_precisions(
        self, input_precision, target_precision
    ):
        # Arrange
        import pyarrow as pa

        ts_values = pd.to_datetime(
            [
                "2025-01-01 00:00:00.123456789",
                "2025-01-02 00:00:00.987654321",
                "2025-01-03 00:00:00.555555555",
            ]
        )
        ts_values = ts_values.astype(f"datetime64[{input_precision}]")

        df = pd.DataFrame(
            {"ts": ts_values, "val": [1.0, 2.5, 3.5], "name": ["a", "b", "c"]}
        )

        # Act
        table = DeltaEngine._prepare_df_for_delta(
            df, timestamp_precision=target_precision
        )

        # Assert
        assert isinstance(table, pa.Table)
        # Timestamp column should be cast to target precision
        for field in table.schema:
            if pa.types.is_timestamp(field.type):
                assert field.type.unit == target_precision
        # Other columns should remain unchanged
        assert len(table.columns) == df.shape[1]

    def test_prepare_df_for_delta_date64_cast_to_date32(self):
        # Arrange — date64 (milliseconds since epoch) must be cast to date32
        # (days since epoch) because the Delta kernel statistics parser rejects
        # millisecond values when decoding date fields.
        import pyarrow as pa

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "date32_col": pd.array(
                    [date(2022, 1, 1), date(2022, 6, 15), date(2022, 12, 31)],
                    dtype=pd.ArrowDtype(pa.date32()),
                ),
                "date64_col": pd.array(
                    [date(2022, 1, 1), date(2022, 6, 15), date(2022, 12, 31)],
                    dtype=pd.ArrowDtype(pa.date64()),
                ),
            }
        )

        # Act
        table = DeltaEngine._prepare_df_for_delta(df)

        # Assert — both date columns must be date32 in the output
        assert isinstance(table, pa.Table)
        for field in table.schema:
            if pa.types.is_date(field.type):
                assert field.type == pa.date32(), (
                    f"Column '{field.name}' has type {field.type}, expected date32"
                )
        # Calendar values must be preserved after the cast
        assert table.column("date64_col").to_pylist() == [
            date(2022, 1, 1),
            date(2022, 6, 15),
            date(2022, 12, 31),
        ]
        assert len(table.columns) == df.shape[1]

    def test_prepare_df_for_delta_arrow_table_casts_float16(self):
        # PyArrow tables (e.g. produced from polars.DataFrame.to_arrow()) must be
        # accepted directly so the float16->float32 cast still runs; Delta Lake
        # rejects Float16 outright.
        import pyarrow as pa

        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int32()),
                "f16": pa.array([1.5, 2.5], type=pa.float16()),
            }
        )

        result = DeltaEngine._prepare_df_for_delta(table)

        assert isinstance(result, pa.Table)
        assert result.schema.field("f16").type == pa.float32()
        assert result.column("f16").to_pylist() == [1.5, 2.5]

    def test_prepare_df_for_delta_does_not_mutate_shallow_copy_input(self):
        # Arrange
        # Simulate the real call path: convert_to_default_dataframe produces a
        # shallow copy (deep=False), which is then passed to _prepare_df_for_delta.
        # Column assignment inside the function must only update the copy's column
        # reference and must never propagate back to the original via shared arrays.
        df = pd.DataFrame(
            {
                "ts": pd.to_datetime(["2024-01-01", "2024-01-02"]).tz_localize("UTC"),
                "value": [1.0, 2.0],
            }
        )
        shallow = df.copy(deep=False)
        original_tz = df["ts"].dt.tz

        # Act
        DeltaEngine._prepare_df_for_delta(shallow)

        # Assert - the original df's tz-aware column must be untouched
        assert df["ts"].dt.tz == original_tz

    def test_vacuum_executes_sql(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)
        fg.prepare_spark_location.return_value = "/loc"

        # Act
        engine._vacuum(24)

        # Assert
        spark.sql.assert_called_once()
        assert "VACUUM '/loc' RETAIN 24 HOURS" in spark.sql.call_args[0][0]

    def test_get_last_commit_metadata_importerror_spark(self, monkeypatch, mocker):
        # Arrange — classic (non-Connect) Spark session that can't import
        # delta-spark; the engine should raise a clear ImportError.
        _force_missing_delta_spark(monkeypatch)
        mocker.patch(
            "hopsworks_common.spark_connect_utils._is_spark_connect_session",
            return_value=False,
        )

        # Act & Assert
        with pytest.raises(ImportError) as e:
            DeltaEngine._get_last_commit_metadata(
                spark_context=mock.Mock(), base_path="/p"
            )
        assert "delta-spark" in str(e.value)

    def test_get_last_commit_metadata_delta_rs_importerror(self, monkeypatch):
        # Arrange
        _force_missing_deltalake(monkeypatch)

        # Act & Assert
        with pytest.raises(ImportError) as e:
            DeltaEngine._get_last_commit_metadata(None, "s3://some/path")
        assert "hops-deltalake" in str(e.value)

    def test_get_last_commit_metadata_spark(self, mocker):
        # Arrange — classic Spark path uses ``DeltaTable.forPath(...).history()``.
        mocker.patch(
            "hopsworks_common.spark_connect_utils._is_spark_connect_session",
            return_value=False,
        )
        mock_history_data = [
            {"version": 1, "operation": "WRITE", "timestamp": "2024-01-01T00:00:00Z"},
            {"version": 2, "operation": "MERGE", "timestamp": "2024-01-02T00:00:00Z"},
            {
                "version": 3,
                "operation": "OPTIMIZE",
                "timestamp": "2024-01-03T00:00:00Z",
            },
        ]

        # Create fake Rows with asDict()
        mock_rows = [
            mocker.MagicMock(asDict=lambda row=row: row) for row in mock_history_data
        ]

        # Mock Spark DataFrame
        mock_spark_df = mocker.MagicMock()
        mock_spark_df.collect.return_value = mock_rows

        # Mock DeltaTable
        mock_delta_table = mocker.MagicMock()
        mock_delta_table.history.return_value = mock_spark_df

        mocker_get_delta_feature_group_commit = mocker.patch(
            "hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit",
            return_value="result",
        )

        # Patch DeltaTable
        mocker.patch("delta.tables.DeltaTable.forPath", return_value=mock_delta_table)

        # Act
        result = DeltaEngine._get_last_commit_metadata(
            mocker.MagicMock(), "s3://some/path"
        )

        # Assert
        assert result == "result"
        mocker_get_delta_feature_group_commit.assert_called_once()
        mocker_get_delta_feature_group_commit.assert_called_once_with(
            mock_history_data[1], mock_history_data[0]
        )

    def test_get_last_commit_metadata_spark_connect(self, mocker):
        # Arrange — Connect path bypasses Hive by reading ``_delta_log/*.json``.
        mocker.patch(
            "hopsworks_common.spark_connect_utils._is_spark_connect_session",
            return_value=True,
        )
        mock_history_data = [
            {"version": 1, "operation": "WRITE", "timestamp": "2024-01-01T00:00:00Z"},
            {"version": 2, "operation": "MERGE", "timestamp": "2024-01-02T00:00:00Z"},
            {
                "version": 3,
                "operation": "OPTIMIZE",
                "timestamp": "2024-01-03T00:00:00Z",
            },
        ]

        # asDict must accept ``recursive=True`` because the engine recurses to
        # convert nested ``operationMetrics`` structs into dicts.
        mock_rows = [
            mocker.MagicMock(
                asDict=lambda recursive=True, row=row: row  # noqa: ARG005
            )
            for row in mock_history_data
        ]

        mock_projected_df = mocker.MagicMock()
        mock_projected_df.collect.return_value = mock_rows

        mock_log_df = mocker.MagicMock()
        mock_log_df.columns = ["commitInfo", "metaData", "add"]
        chained = mocker.MagicMock()
        mock_log_df.withColumn.return_value = chained
        chained.filter.return_value = chained
        chained.withColumn.return_value = chained
        chained.select.return_value = mock_projected_df

        mocker_get_delta_feature_group_commit = mocker.patch(
            "hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit",
            return_value="result",
        )

        mock_spark = mocker.MagicMock()
        mock_spark.read.json.return_value = mock_log_df

        # Act
        result = DeltaEngine._get_last_commit_metadata(mock_spark, "s3://some/path")

        # Assert: the engine read ``_delta_log/*.json`` instead of routing
        # through ``DESCRIBE HISTORY`` (which would hit the Hive Metastore).
        mock_spark.read.json.assert_called_once_with("s3://some/path/_delta_log/*.json")
        assert result == "result"
        mocker_get_delta_feature_group_commit.assert_called_once_with(
            mock_history_data[1], mock_history_data[0]
        )

    def test_get_last_commit_metadata_deltars(self, mocker):
        # Arrange
        mock_history_data = [
            {"version": 1, "operation": "WRITE", "timestamp": "2024-01-01T00:00:00Z"},
            {"version": 2, "operation": "MERGE", "timestamp": "2024-01-02T00:00:00Z"},
            {
                "version": 3,
                "operation": "OPTIMIZE",
                "timestamp": "2024-01-03T00:00:00Z",
            },
        ]

        # Fake the deltalake module
        fake_deltalake = types.SimpleNamespace(DeltaTable=mocker.MagicMock())
        sys.modules["deltalake"] = fake_deltalake

        mock_delta_rs_table = mocker.MagicMock()
        mock_delta_rs_table.history.return_value = mock_history_data
        fake_deltalake.DeltaTable.return_value = mock_delta_rs_table

        mocker_get_delta_feature_group_commit = mocker.patch(
            "hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit",
            return_value="result",
        )

        # Act
        result = DeltaEngine._get_last_commit_metadata(None, "s3://some/path")

        # Assert
        assert result == "result"
        mocker_get_delta_feature_group_commit.assert_called_once()
        mocker_get_delta_feature_group_commit.assert_called_once_with(
            mock_history_data[1], mock_history_data[0]
        )

    def test_get_last_commit_metadata_empty_history(self, mocker):
        # Arrange
        mock_history_data = []

        # Fake the deltalake module
        fake_deltalake = types.SimpleNamespace(DeltaTable=mocker.MagicMock())
        sys.modules["deltalake"] = fake_deltalake

        mock_delta_rs_table = mocker.MagicMock()
        mock_delta_rs_table.history.return_value = mock_history_data
        fake_deltalake.DeltaTable.return_value = mock_delta_rs_table

        mocker_get_delta_feature_group_commit = mocker.patch(
            "hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit",
            return_value="result",
        )

        # Act
        result = DeltaEngine._get_last_commit_metadata(None, "s3://some/path")

        # Assert
        assert result is None
        mocker_get_delta_feature_group_commit.assert_not_called()

    def test_get_last_commit_metadata_one_history_entry(self, mocker):
        # Arrange
        mock_history_data = [
            {"version": 1, "operation": "WRITE", "timestamp": "2024-01-01T00:00:00Z"},
        ]

        # Fake the deltalake module
        fake_deltalake = types.SimpleNamespace(DeltaTable=mocker.MagicMock())
        sys.modules["deltalake"] = fake_deltalake

        mock_delta_rs_table = mocker.MagicMock()
        mock_delta_rs_table.history.return_value = mock_history_data
        fake_deltalake.DeltaTable.return_value = mock_delta_rs_table

        mocker_get_delta_feature_group_commit = mocker.patch(
            "hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit",
            return_value="result",
        )

        # Act
        result = DeltaEngine._get_last_commit_metadata(None, "s3://some/path")

        # Assert
        assert result == "result"
        mocker_get_delta_feature_group_commit.assert_called_once()
        mocker_get_delta_feature_group_commit.assert_called_once_with(
            mock_history_data[0], mock_history_data[0]
        )

    def test_get_last_commit_metadata_one_history_entry_optimize(self, mocker):
        # Arrange
        mock_history_data = [
            {
                "version": 1,
                "operation": "OPTIMIZE",
                "timestamp": "2024-01-01T00:00:00Z",
            },
        ]

        # Fake the deltalake module
        fake_deltalake = types.SimpleNamespace(DeltaTable=mocker.MagicMock())
        sys.modules["deltalake"] = fake_deltalake

        mock_delta_rs_table = mocker.MagicMock()
        mock_delta_rs_table.history.return_value = mock_history_data
        fake_deltalake.DeltaTable.return_value = mock_delta_rs_table

        mocker_get_delta_feature_group_commit = mocker.patch(
            "hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit",
            return_value="result",
        )

        # Act
        result = DeltaEngine._get_last_commit_metadata(None, "s3://some/path")

        # Assert
        assert result is None
        mocker_get_delta_feature_group_commit.assert_not_called()

    def test_get_delta_feature_group_commit_merge(self, mocker):
        # Arrange
        last_commit = {
            "operation": "MERGE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {
                "numTargetRowsInserted": 10,
                "numTargetRowsUpdated": 5,
                "numTargetRowsDeleted": 2,
            },
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch(
            "hsfs.core.delta_engine.util._convert_event_time_to_timestamp",
            side_effect=lambda ts: ts,
        )
        mocker.patch(
            "hsfs.core.delta_engine.util._get_hudi_datestr_from_timestamp",
            side_effect=lambda ts: f"date-{ts}",
        )

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(
            last_commit, oldest_commit
        )

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 10
        assert fg_commit.rows_updated == 5
        assert fg_commit.rows_deleted == 2
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    def test_get_delta_feature_group_commit_write(self, mocker):
        # Arrange
        last_commit = {
            "operation": "WRITE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {"numOutputRows": 10},
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch(
            "hsfs.core.delta_engine.util._convert_event_time_to_timestamp",
            side_effect=lambda ts: ts,
        )
        mocker.patch(
            "hsfs.core.delta_engine.util._get_hudi_datestr_from_timestamp",
            side_effect=lambda ts: f"date-{ts}",
        )

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(
            last_commit, oldest_commit
        )

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 10
        assert fg_commit.rows_updated == 0
        assert fg_commit.rows_deleted == 0
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    def test_get_delta_feature_group_commit_other(self, mocker):
        # Arrange
        last_commit = {
            "operation": "OPTIMIZE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {},
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch(
            "hsfs.core.delta_engine.util._convert_event_time_to_timestamp",
            side_effect=lambda ts: ts,
        )
        mocker.patch(
            "hsfs.core.delta_engine.util._get_hudi_datestr_from_timestamp",
            side_effect=lambda ts: f"date-{ts}",
        )

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(
            last_commit, oldest_commit
        )

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 0
        assert fg_commit.rows_updated == 0
        assert fg_commit.rows_deleted == 0
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    def test_get_delta_feature_group_commit_merge_delta_rs(self, mocker):
        # Arrange
        last_commit = {
            "operation": "MERGE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {
                "num_target_rows_inserted": 10,
                "num_target_rows_updated": 5,
                "num_target_rows_deleted": 2,
            },
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch(
            "hsfs.core.delta_engine.util._convert_event_time_to_timestamp",
            side_effect=lambda ts: ts,
        )
        mocker.patch(
            "hsfs.core.delta_engine.util._get_hudi_datestr_from_timestamp",
            side_effect=lambda ts: f"date-{ts}",
        )

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(
            last_commit, oldest_commit
        )

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 10
        assert fg_commit.rows_updated == 5
        assert fg_commit.rows_deleted == 2
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    def test_get_delta_feature_group_commit_write_delta_rs(self, mocker):
        # Arrange
        last_commit = {
            "operation": "WRITE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {"num_added_rows": 10},
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch(
            "hsfs.core.delta_engine.util._convert_event_time_to_timestamp",
            side_effect=lambda ts: ts,
        )
        mocker.patch(
            "hsfs.core.delta_engine.util._get_hudi_datestr_from_timestamp",
            side_effect=lambda ts: f"date-{ts}",
        )

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(
            last_commit, oldest_commit
        )

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 10
        assert fg_commit.rows_updated == 0
        assert fg_commit.rows_deleted == 0
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    def test_get_delta_feature_group_commit_other_delta_rs(self, mocker):
        # Arrange
        last_commit = {
            "operation": "OPTIMIZE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {},
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch(
            "hsfs.core.delta_engine.util._convert_event_time_to_timestamp",
            side_effect=lambda ts: ts,
        )
        mocker.patch(
            "hsfs.core.delta_engine.util._get_hudi_datestr_from_timestamp",
            side_effect=lambda ts: f"date-{ts}",
        )

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(
            last_commit, oldest_commit
        )

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 0
        assert fg_commit.rows_updated == 0
        assert fg_commit.rows_deleted == 0
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    # ------------------------------------------------------------------
    # _can_use_append
    # ------------------------------------------------------------------

    def test_can_use_append_no_partition_key_returns_false(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn/p")
        fg.partition_key = []
        engine = DeltaEngine(1, "fs", fg, None, None)
        fg_source_table = mock.Mock()

        # Act
        result = engine._can_use_append(fg_source_table, mock.Mock())

        # Assert
        assert result is False
        fg_source_table.file_uris.assert_not_called()

    def test_can_use_append_no_overlap_returns_true(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn/p")
        fg.partition_key = ["month"]
        engine = DeltaEngine(1, "fs", fg, None, None)

        dataset = pa.table(
            {"month": ["2024-01", "2024-01", "2024-02"], "val": [1, 2, 3]}
        )
        fg_source_table = mock.Mock()
        fg_source_table.file_uris.return_value = []

        # Act
        result = engine._can_use_append(fg_source_table, dataset)

        # Assert
        assert result is True
        fg_source_table.file_uris.assert_called_once_with(
            partition_filters=[("month", "in", ["2024-01", "2024-02"])]
        )

    def test_can_use_append_overlap_returns_false(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn/p")
        fg.partition_key = ["month"]
        engine = DeltaEngine(1, "fs", fg, None, None)

        dataset = pa.table({"month": ["2024-01"], "val": [1]})
        fg_source_table = mock.Mock()
        fg_source_table.file_uris.return_value = [
            "hdfs://nn/p/month=2024-01/part-0.parquet"
        ]

        # Act
        result = engine._can_use_append(fg_source_table, dataset)

        # Assert
        assert result is False

    def test_can_use_append_exception_falls_back_to_false(self, mocker):
        # Arrange - any error in the overlap check must not crash the write path
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn/p")
        fg.partition_key = ["month"]
        engine = DeltaEngine(1, "fs", fg, None, None)

        dataset = pa.table({"month": ["2024-01"], "val": [1]})
        fg_source_table = mock.Mock()
        fg_source_table.file_uris.side_effect = RuntimeError("HDFS unavailable")

        # Act
        result = engine._can_use_append(fg_source_table, dataset)

        # Assert
        assert result is False

    # ------------------------------------------------------------------
    # _write_delta_rs_dataset routing: append vs merge
    # ------------------------------------------------------------------

    def _setup_fake_deltalake(self, mocker):
        """Inject a fake deltalake module and return the key mocks."""
        fake_write = mocker.Mock()
        fake_delta_table = mocker.Mock()
        fake_delta_table.merge.return_value.when_matched_update_all.return_value.when_not_matched_insert_all.return_value.execute = mocker.Mock()

        class _FakeTableNotFoundError(Exception):
            pass

        fake_deltalake = types.SimpleNamespace(
            DeltaTable=mocker.Mock(return_value=fake_delta_table),
            write_deltalake=fake_write,
        )
        fake_exceptions = types.SimpleNamespace(
            TableNotFoundError=_FakeTableNotFoundError,
        )
        mocker.patch.dict(
            sys.modules,
            {"deltalake": fake_deltalake, "deltalake.exceptions": fake_exceptions},
        )
        return fake_write, fake_delta_table

    def test_write_delta_rs_uses_append_when_no_overlap(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/projects/p1")
        fg.partition_key = ["month"]
        engine = DeltaEngine(1, "fs", fg, None, None)

        fake_write, _ = self._setup_fake_deltalake(mocker)
        mocker.patch.object(
            engine, "_get_delta_rs_location", return_value="hdfs://nn/p"
        )
        mocker.patch.object(engine, "_can_use_append", return_value=True)
        mocker.patch.object(
            engine, "_get_last_commit_metadata", return_value=mock.Mock()
        )

        dataset = pa.table({"month": ["2024-01"], "id": [1]})

        # Act
        engine._write_delta_rs_dataset(dataset)

        # Assert - plain append used, merge chain never invoked
        fake_write.assert_called_once()
        assert fake_write.call_args.kwargs.get("mode") == "append"

    def test_write_delta_rs_uses_merge_when_overlap(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/projects/p1")
        fg.partition_key = ["month"]
        fg.primary_key = ["id"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)

        fake_write, fake_delta_table = self._setup_fake_deltalake(mocker)
        mocker.patch.object(
            engine, "_get_delta_rs_location", return_value="hdfs://nn/p"
        )
        mocker.patch.object(engine, "_can_use_append", return_value=False)
        mocker.patch.object(
            engine, "_get_last_commit_metadata", return_value=mock.Mock()
        )

        dataset = pa.table({"month": ["2024-01"], "id": [1]})

        # Act
        engine._write_delta_rs_dataset(dataset)

        # Assert - merge executed, plain append not called
        fake_delta_table.merge.assert_called_once()
        fake_delta_table.merge.return_value.when_matched_update_all.return_value.when_not_matched_insert_all.return_value.execute.assert_called_once()
        fake_write.assert_not_called()

    def _setup_fake_deltalake_insert(self, mocker):
        """Minimal fake deltalake module for operation=insert tests.

        Only sets up write_deltalake and a table that exists — no merge builder chain needed.
        """
        fake_write = mocker.Mock()

        class _FakeTableNotFoundError(Exception):
            pass

        fake_deltalake = types.SimpleNamespace(
            DeltaTable=mocker.Mock(return_value=mocker.Mock()),
            write_deltalake=fake_write,
        )
        fake_exceptions = types.SimpleNamespace(
            TableNotFoundError=_FakeTableNotFoundError,
        )
        mocker.patch.dict(
            sys.modules,
            {"deltalake": fake_deltalake, "deltalake.exceptions": fake_exceptions},
        )
        return fake_write

    def test_write_delta_rs_insert_operation_skips_merge(self, mocker):
        # Arrange: existing table, operation="insert" should append directly, bypassing merge
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/projects/p1")
        fg.primary_key = ["id"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, None, None)

        fake_write = self._setup_fake_deltalake_insert(mocker)
        mocker.patch.object(
            engine, "_get_delta_rs_location", return_value="hdfs://nn/p"
        )
        mocker.patch.object(
            engine, "_get_last_commit_metadata", return_value=mock.Mock()
        )

        dataset = pa.table({"id": [1]})

        # Act
        engine._write_delta_rs_dataset(dataset, operation="insert")

        # Assert - plain append used
        fake_write.assert_called_once()
        assert fake_write.call_args.kwargs.get("mode") == "append"

    def test_write_delta_dataset_insert_operation_skips_merge(
        self, mocker, monkeypatch
    ):
        # Arrange: existing Spark delta table, operation="insert" should use append not merge
        _patch_client(mocker, is_external=False)
        spark = mocker.MagicMock()
        fg = _make_fg("hopsfs://nn:8020/p")
        fg.partition_key = []
        fg.primary_key = ["id"]
        fg.event_time = None
        engine = DeltaEngine(1, "fs", fg, spark, None)

        fake_delta_table_cls = mocker.MagicMock()
        fake_delta_table_cls.isDeltaTable.return_value = True

        fake_delta = types.ModuleType("delta")
        fake_delta_tables = types.ModuleType("delta.tables")
        fake_delta_tables.DeltaTable = fake_delta_table_cls
        monkeypatch.setitem(sys.modules, "delta", fake_delta)
        monkeypatch.setitem(sys.modules, "delta.tables", fake_delta_tables)

        mocker.patch.object(engine, "_get_last_commit_metadata", return_value="commit")

        dataset = mocker.MagicMock()

        # Act
        result = engine._write_delta_dataset(
            dataset, write_options={}, operation="insert"
        )

        # Assert - append write used, merge builder never invoked
        assert result == "commit"
        dataset.write.format.return_value.options.return_value.mode.assert_called_with(
            "append"
        )
        fake_delta_table_cls.forPath.assert_not_called()

    def test_save_delta_fg_passes_operation_spark(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        spark = mock.Mock()
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, spark, None)
        mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi._commit")
        write_mock = mocker.patch.object(
            engine, "_write_delta_dataset", return_value=mock.Mock()
        )

        # Act
        engine._save_delta_fg(
            dataset=mock.Mock(),
            write_options={},
            validation_id=None,
            operation="insert",
        )

        # Assert - operation forwarded to _write_delta_dataset
        write_mock.assert_called_once()
        assert write_mock.call_args.args[2] == "insert"

    def test_save_delta_fg_passes_operation_rs(self, mocker):
        # Arrange
        _patch_client(mocker, is_external=False)
        fg = _make_fg("hopsfs://nn:8020/p")
        engine = DeltaEngine(1, "fs", fg, None, None)
        mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi._commit")
        write_mock = mocker.patch.object(
            engine, "_write_delta_rs_dataset", return_value=mock.Mock()
        )

        # Act
        engine._save_delta_fg(
            dataset=mock.Mock(),
            write_options=None,
            validation_id=None,
            operation="insert",
        )

        # Assert - operation forwarded to _write_delta_rs_dataset
        write_mock.assert_called_once()
        assert write_mock.call_args.kwargs.get("operation") == "insert"


class TestDeltaEngineConnectMode:
    """Tests for DeltaEngine initialization in Spark Connect mode."""

    def test_warns_when_delta_extension_missing(self, mocker, caplog):
        """In Connect mode, warn if DeltaSparkSessionExtension is not configured."""
        _patch_apis(mocker)
        _patch_client(mocker, is_external=False)

        spark_session = mock.MagicMock()
        spark_session.conf.get.return_value = ""

        import logging

        with caplog.at_level(logging.WARNING, logger="hsfs.core.delta_engine"):
            DeltaEngine(
                feature_store_id=1,
                feature_store_name="fs",
                feature_group=_make_fg("hdfs:///path"),
                spark_session=spark_session,
                spark_context=None,
            )

        assert "Delta SQL extension not configured" in caplog.text

    def test_no_warning_when_delta_extension_present(self, mocker, caplog):
        """No warning when the extension is already on the session."""
        _patch_apis(mocker)
        _patch_client(mocker, is_external=False)

        spark_session = mock.MagicMock()
        spark_session.conf.get.return_value = "io.delta.sql.DeltaSparkSessionExtension"

        import logging

        with caplog.at_level(logging.WARNING, logger="hsfs.core.delta_engine"):
            DeltaEngine(
                feature_store_id=1,
                feature_store_name="fs",
                feature_group=_make_fg("hdfs:///path"),
                spark_session=spark_session,
                spark_context=None,
            )

        assert "Delta SQL extension not configured" not in caplog.text

    def test_classic_mode_skips_extension_check(self, mocker, caplog):
        """In classic Spark mode, the extension check is skipped entirely."""
        _patch_apis(mocker)
        _patch_client(mocker, is_external=False)

        spark_session = mock.MagicMock()
        spark_context = mock.MagicMock()

        import logging

        with caplog.at_level(logging.WARNING, logger="hsfs.core.delta_engine"):
            DeltaEngine(
                feature_store_id=1,
                feature_store_name="fs",
                feature_group=_make_fg("hdfs:///path"),
                spark_session=spark_session,
                spark_context=spark_context,
            )

        # conf.get should NOT have been called for extensions
        spark_session.conf.get.assert_not_called()
        assert "Delta SQL extension not configured" not in caplog.text

    def test_classic_mode_sets_delta_catalog(self, mocker):
        """In classic Spark mode, set spark.sql.catalog.spark_catalog at runtime."""
        _patch_apis(mocker)
        _patch_client(mocker, is_external=False)

        spark_session = mock.MagicMock()
        spark_context = mock.MagicMock()

        DeltaEngine(
            feature_store_id=1,
            feature_store_name="fs",
            feature_group=_make_fg("hdfs:///path"),
            spark_session=spark_session,
            spark_context=spark_context,
        )

        spark_session.conf.set.assert_called_once_with(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
