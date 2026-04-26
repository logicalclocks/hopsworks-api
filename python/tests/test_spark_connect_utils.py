#
#   Copyright 2026 Hopsworks AB
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

from unittest.mock import MagicMock, PropertyMock, patch

from hopsworks_common.spark_connect_utils import (
    is_spark_connect_env,
    is_spark_connect_session,
    is_spark_dataframe,
)


class TestIsSparkConnectEnv:
    def test_env_var_set_1(self, monkeypatch):
        monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "1")
        assert is_spark_connect_env() is True

    def test_env_var_set_true(self, monkeypatch):
        monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "true")
        assert is_spark_connect_env() is True

    def test_env_var_set_True(self, monkeypatch):
        monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "True")
        assert is_spark_connect_env() is True

    def test_env_var_set_0(self, monkeypatch):
        monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "0")
        with patch.dict("sys.modules", {"pyspark.sql.utils": None}):
            assert is_spark_connect_env() is False

    def test_env_var_set_false(self, monkeypatch):
        monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "false")
        with patch.dict("sys.modules", {"pyspark.sql.utils": None}):
            assert is_spark_connect_env() is False

    def test_env_var_not_set(self, monkeypatch):
        monkeypatch.delenv("SPARK_CONNECT_MODE_ENABLED", raising=False)
        with patch.dict("sys.modules", {"pyspark.sql.utils": None}):
            assert is_spark_connect_env() is False

    def test_pyspark_is_remote_true(self, monkeypatch):
        monkeypatch.delenv("SPARK_CONNECT_MODE_ENABLED", raising=False)
        mock_utils = MagicMock()
        mock_utils.is_remote.return_value = True
        with patch.dict(
            "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock(),
                "pyspark.sql.utils": mock_utils,
            },
        ):
            # Need to reimport to pick up the mock
            from importlib import reload

            import hopsworks_common.spark_connect_utils as mod

            reload(mod)
            # After reloading, the helper should detect the mocked remote env
            assert mod.is_spark_connect_env() is True


class TestIsSparkConnectSession:
    def test_env_var_means_connect(self, monkeypatch):
        monkeypatch.setenv("SPARK_CONNECT_MODE_ENABLED", "1")
        session = MagicMock()
        assert is_spark_connect_session(session) is True

    def test_spark_context_available_means_classic(self, monkeypatch):
        monkeypatch.delenv("SPARK_CONNECT_MODE_ENABLED", raising=False)
        session = MagicMock()
        session.sparkContext = MagicMock()
        with patch.dict("sys.modules", {"pyspark.sql.utils": None}):
            assert is_spark_connect_session(session) is False

    def test_spark_context_raises_means_connect(self, monkeypatch):
        monkeypatch.delenv("SPARK_CONNECT_MODE_ENABLED", raising=False)
        session = MagicMock()
        type(session).sparkContext = PropertyMock(
            side_effect=NotImplementedError("not supported")
        )
        with patch.dict("sys.modules", {"pyspark.sql.utils": None}):
            assert is_spark_connect_session(session) is True


class TestIsSparkDataframe:
    """``is_spark_dataframe`` accepts both classic and Connect DataFrames.

    The classic class is a real import (``pyspark`` is a dev dep). The Connect
    class only exists as a stub here so the test does not require a Spark
    Connect server in CI.
    """

    def test_classic_dataframe_returns_true(self):
        from pyspark.sql import DataFrame as ClassicDataFrame

        instance = ClassicDataFrame.__new__(ClassicDataFrame)
        assert is_spark_dataframe(instance) is True

    def test_connect_dataframe_returns_true(self):
        # Stand up a fake ``pyspark.sql.connect.dataframe`` module exposing a
        # marker DataFrame class, then verify the predicate accepts an
        # instance of it. This mirrors what a real Spark Connect session
        # would produce without requiring a live Connect server.
        import sys
        import types

        connect_pkg = types.ModuleType("pyspark.sql.connect")
        connect_dataframe_mod = types.ModuleType("pyspark.sql.connect.dataframe")

        class _FakeConnectDataFrame:
            pass

        connect_dataframe_mod.DataFrame = _FakeConnectDataFrame
        connect_pkg.dataframe = connect_dataframe_mod

        with patch.dict(
            sys.modules,
            {
                "pyspark.sql.connect": connect_pkg,
                "pyspark.sql.connect.dataframe": connect_dataframe_mod,
            },
        ):
            assert is_spark_dataframe(_FakeConnectDataFrame()) is True

    def test_pandas_dataframe_returns_false(self):
        import pandas as pd

        assert is_spark_dataframe(pd.DataFrame({"a": [1]})) is False

    def test_list_returns_false(self):
        assert is_spark_dataframe([1, 2, 3]) is False

    def test_none_returns_false(self):
        assert is_spark_dataframe(None) is False
