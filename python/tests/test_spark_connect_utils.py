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
