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

import errno
import os

import pytest
from hsml.engine import model_engine


def _make_model(mocker, project_name="proj", name="mymodel", version=1):
    m = mocker.MagicMock()
    m.project_name = project_name
    m.name = name
    m.version = version
    return m


class TestModelCacheBaseDirs:
    def test_order_and_default_locations(self, mocker):
        # Arrange
        mocker.patch(
            "hsml.engine.model_engine.constants.MODEL_REGISTRY.MODEL_CACHE_DIR_DEFAULT",
            "/tmp/hopsworks/models",
        )
        mocker.patch("os.path.expanduser", return_value="/home/user")
        mocker.patch("os.getcwd", return_value="/work")

        # Act
        bases = model_engine.model_cache_base_dirs()

        # Assert
        assert bases == [
            "/tmp/hopsworks/models",
            "/home/user/.hopsworks/.cache/models",
            "/work/.hopsworks_cache/models",
        ]

    def test_deduplicates_overlapping_locations(self, mocker):
        # Arrange - working dir resolves to the same place as the temp default
        mocker.patch(
            "hsml.engine.model_engine.constants.MODEL_REGISTRY.MODEL_CACHE_DIR_DEFAULT",
            "/tmp/hopsworks/models",
        )
        mocker.patch("os.path.expanduser", return_value="/home/user")
        mocker.patch(
            "os.getcwd", return_value="/tmp/hopsworks/models/.hopsworks_cache/models/.."
        )

        # Act
        bases = model_engine.model_cache_base_dirs()

        # Assert - the duplicate temp path appears only once
        assert bases.count("/tmp/hopsworks/models") == 1


class TestModelEngineDownload:
    def test_returns_existing_valid_cache_without_downloading(self, mocker, tmp_path):
        # Arrange
        base = str(tmp_path / "tmp")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        cache_path = os.path.join(base, "proj", "mymodel", "1")
        os.makedirs(cache_path)
        with open(os.path.join(cache_path, "model.pkl"), "w") as f:
            f.write("x")
        with open(os.path.join(cache_path, ".download_complete"), "w") as f:
            f.write("done")
        download_spy = mocker.patch.object(
            model_engine.ModelEngine, "_download_model_files"
        )

        # Act
        result = eng.download(m)

        # Assert
        assert result == cache_path
        download_spy.assert_not_called()

    def test_falls_back_to_next_location_on_disk_full(self, mocker, tmp_path):
        # Arrange
        base1 = str(tmp_path / "tmp")
        base2 = str(tmp_path / "home")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs",
            return_value=[base1, base2],
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)

        def fake_download(model_instance, local_path):
            if local_path.startswith(base1):
                raise OSError(errno.ENOSPC, "No space left on device")
            with open(os.path.join(local_path, "model.pkl"), "w") as f:
                f.write("x")

        mocker.patch.object(
            model_engine.ModelEngine,
            "_download_model_files",
            side_effect=fake_download,
        )

        # Act
        result = eng.download(m)

        # Assert - second location used, first cleaned up, marker written
        assert result == os.path.join(base2, "proj", "mymodel", "1")
        assert os.path.exists(os.path.join(result, ".download_complete"))
        assert not os.path.exists(os.path.join(base1, "proj", "mymodel", "1"))

    def test_falls_back_on_permission_error(self, mocker, tmp_path):
        # Arrange
        base1 = str(tmp_path / "tmp")
        base2 = str(tmp_path / "home")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs",
            return_value=[base1, base2],
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)

        def fake_download(model_instance, local_path):
            if local_path.startswith(base1):
                raise PermissionError(errno.EACCES, "Permission denied")
            with open(os.path.join(local_path, "model.pkl"), "w") as f:
                f.write("x")

        mocker.patch.object(
            model_engine.ModelEngine,
            "_download_model_files",
            side_effect=fake_download,
        )

        # Act
        result = eng.download(m)

        # Assert
        assert result == os.path.join(base2, "proj", "mymodel", "1")

    def test_raises_when_all_locations_fail(self, mocker, tmp_path):
        # Arrange
        base1 = str(tmp_path / "tmp")
        base2 = str(tmp_path / "home")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs",
            return_value=[base1, base2],
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        mocker.patch.object(
            model_engine.ModelEngine,
            "_download_model_files",
            side_effect=OSError(errno.ENOSPC, "No space left on device"),
        )

        # Act / Assert
        with pytest.raises(OSError) as exc:
            eng.download(m)
        assert exc.value.errno == errno.ENOSPC

    def test_explicit_local_path_does_not_fall_back(self, mocker, tmp_path):
        # Arrange
        local_path = str(tmp_path / "explicit")
        base_dirs_spy = mocker.patch("hsml.engine.model_engine.model_cache_base_dirs")
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        mocker.patch.object(
            model_engine.ModelEngine,
            "_download_model_files",
            side_effect=PermissionError(errno.EACCES, "Permission denied"),
        )

        # Act / Assert - the user's chosen path is honoured, no cache fallback
        with pytest.raises(PermissionError):
            eng.download(m, local_path=local_path)
        base_dirs_spy.assert_not_called()

    def test_explicit_local_path_success(self, mocker, tmp_path):
        # Arrange
        local_path = str(tmp_path / "explicit")
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        download_spy = mocker.patch.object(
            model_engine.ModelEngine, "_download_model_files"
        )

        # Act
        result = eng.download(m, local_path=local_path)

        # Assert - dir created, files downloaded, no completion marker for explicit paths
        assert result == local_path
        assert os.path.isdir(local_path)
        download_spy.assert_called_once_with(m, local_path)
        assert not os.path.exists(os.path.join(local_path, ".download_complete"))
