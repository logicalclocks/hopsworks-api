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
from hsml import model as model_mod
from hsml.engine import model_engine


def _make_model(mocker, project_name="proj", name="mymodel", version=1, model_id=10):
    m = mocker.MagicMock()
    m.project_name = project_name
    m.name = name
    m.version = version
    m.id = model_id
    return m


def _leaf(base, model_id=10):
    """Cache leaf path for the default model: {base}/proj/mymodel/1/{id}."""
    return os.path.join(base, "proj", "mymodel", "1", str(model_id))


class TestModelCacheBaseDirs:
    def test_order_and_default_locations(self, mocker):
        # Arrange - use os.path so expectations hold on POSIX and Windows alike
        default = os.path.abspath(os.path.join("anytmp", "hopsworks", "models"))
        home = os.path.abspath("anyhome")
        work = os.path.abspath("anywork")
        mocker.patch(
            "hsml.engine.model_engine.constants.MODEL_REGISTRY.MODEL_CACHE_DIR_DEFAULT",
            default,
        )
        mocker.patch("os.path.expanduser", return_value=home)
        mocker.patch("os.getcwd", return_value=work)

        # Act
        bases = model_engine.model_cache_base_dirs()

        # Assert
        assert bases == [
            os.path.abspath(default),
            os.path.abspath(os.path.join(home, ".hopsworks", "cache", "models")),
            os.path.abspath(os.path.join(work, ".hopsworks_cache", "models")),
        ]

    def test_deduplicates_overlapping_locations(self, mocker):
        # Arrange - the temp default points at the same place as the CWD base,
        # so they should collapse to a single entry.
        work = os.path.abspath("anywork")
        dup = os.path.join(work, ".hopsworks_cache", "models")
        mocker.patch(
            "hsml.engine.model_engine.constants.MODEL_REGISTRY.MODEL_CACHE_DIR_DEFAULT",
            dup,
        )
        mocker.patch("os.path.expanduser", return_value=os.path.abspath("anyhome"))
        mocker.patch("os.getcwd", return_value=work)

        # Act
        bases = model_engine.model_cache_base_dirs()

        # Assert - the duplicate path appears only once; home base remains
        assert len(bases) == 2
        assert bases.count(os.path.abspath(dup)) == 1


class TestModelEngineDownload:
    def test_returns_existing_valid_cache_without_downloading(self, mocker, tmp_path):
        # Arrange
        base = str(tmp_path / "tmp")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        cache_path = _leaf(base)
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
        assert result == _leaf(base2)
        assert os.path.exists(os.path.join(result, ".download_complete"))
        assert not os.path.exists(_leaf(base1))

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
        assert result == _leaf(base2)

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

    def test_explicit_local_path_keeps_existing_files(self, mocker, tmp_path):
        # Arrange - an explicit path may hold unrelated files; they must survive
        local_path = str(tmp_path / "explicit")
        os.makedirs(local_path)
        with open(os.path.join(local_path, "keep.txt"), "w") as f:
            f.write("keep")
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        mocker.patch.object(model_engine.ModelEngine, "_download_model_files")

        # Act
        eng.download(m, local_path=local_path)

        # Assert - the user's pre-existing file is not wiped
        assert os.path.exists(os.path.join(local_path, "keep.txt"))

    def test_cleans_stale_incomplete_cache_before_download(self, mocker, tmp_path):
        # Arrange - a cache dir with stale files but no completion marker
        base = str(tmp_path / "tmp")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        cache_path = _leaf(base)
        os.makedirs(cache_path)
        with open(os.path.join(cache_path, "stale.txt"), "w") as f:
            f.write("old")

        def fake_download(model_instance, local_path):
            with open(os.path.join(local_path, "model.pkl"), "w") as f:
                f.write("x")

        mocker.patch.object(
            model_engine.ModelEngine,
            "_download_model_files",
            side_effect=fake_download,
        )

        # Act
        result = eng.download(m)

        # Assert - stale file removed, fresh download present and marked complete
        assert result == cache_path
        assert not os.path.exists(os.path.join(cache_path, "stale.txt"))
        assert os.path.exists(os.path.join(cache_path, "model.pkl"))
        assert os.path.exists(os.path.join(cache_path, ".download_complete"))

    def test_cache_not_owned_is_neither_reused_nor_overwritten(self, mocker, tmp_path):
        # Arrange - a complete-looking cache dir that belongs to another user
        base = str(tmp_path / "tmp")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        cache_path = _leaf(base)
        os.makedirs(cache_path)
        with open(os.path.join(cache_path, ".download_complete"), "w") as f:
            f.write("done")
        with open(os.path.join(cache_path, "model.pkl"), "w") as f:
            f.write("planted")
        mocker.patch.object(
            model_engine.ModelEngine,
            "_is_path_owned_by_current_user",
            return_value=False,
        )
        download_spy = mocker.patch.object(
            model_engine.ModelEngine, "_download_model_files"
        )

        # Act / Assert - not reused as valid, and not overwritten (we don't own it)
        with pytest.raises(PermissionError):
            eng.download(m)
        download_spy.assert_not_called()
        assert os.path.exists(os.path.join(cache_path, "model.pkl"))

    def test_cache_dir_created_with_owner_only_permissions(self, mocker, tmp_path):
        # Arrange
        base = str(tmp_path / "tmp")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        eng = model_engine.ModelEngine()
        m = _make_model(mocker)
        mocker.patch.object(model_engine.ModelEngine, "_download_model_files")

        # Act
        result = eng.download(m)

        # Assert - owner-only permissions (POSIX only)
        if hasattr(os, "getuid"):
            assert oct(os.stat(result).st_mode & 0o777) == oct(0o700)

    def test_recreated_model_version_invalidates_cache(self, mocker, tmp_path):
        # Arrange - a complete cache for an older id of the same name+version
        base = str(tmp_path / "tmp")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        eng = model_engine.ModelEngine()
        old_cache = _leaf(base, model_id=10)
        os.makedirs(old_cache)
        with open(os.path.join(old_cache, "model.pkl"), "w") as f:
            f.write("old")
        with open(os.path.join(old_cache, ".download_complete"), "w") as f:
            f.write("done")

        def fake_download(model_instance, local_path):
            with open(os.path.join(local_path, "model.pkl"), "w") as f:
                f.write("new")

        download_spy = mocker.patch.object(
            model_engine.ModelEngine,
            "_download_model_files",
            side_effect=fake_download,
        )

        # The model version was recreated: same name/version, new backend id.
        recreated = _make_model(mocker, model_id=11)

        # Act
        result = eng.download(recreated)

        # Assert - fresh dir for the new id, re-downloaded, stale id pruned
        assert result == _leaf(base, model_id=11)
        download_spy.assert_called_once()
        assert not os.path.exists(old_cache)
        with open(os.path.join(result, "model.pkl")) as f:
            assert f.read() == "new"


def _seed_cache(base, layout):
    """Create {base}/{project}/{model}/{version} dirs from a nested layout."""
    for project, models in layout.items():
        for model_name, versions in models.items():
            for version in versions:
                os.makedirs(os.path.join(base, project, model_name, str(version)))


class TestModelClearCache:
    def test_clear_all(self, mocker, tmp_path):
        base = str(tmp_path / "cache")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        _seed_cache(base, {"p1": {"m1": [1, 2], "m2": [1]}, "p2": {"m3": [1]}})

        removed = model_mod.Model.clear_cache()

        assert removed == 4
        assert not os.path.exists(base)

    def test_clear_project_scope(self, mocker, tmp_path):
        base = str(tmp_path / "cache")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        _seed_cache(base, {"p1": {"m1": [1, 2], "m2": [1]}, "p2": {"m3": [1]}})

        removed = model_mod.Model.clear_cache(project_name="p1")

        assert removed == 3
        assert not os.path.exists(os.path.join(base, "p1"))
        assert os.path.exists(os.path.join(base, "p2"))

    def test_clear_model_scope(self, mocker, tmp_path):
        base = str(tmp_path / "cache")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        _seed_cache(base, {"p1": {"m1": [1, 2], "m2": [1]}})

        removed = model_mod.Model.clear_cache(project_name="p1", model_name="m1")

        assert removed == 2
        assert not os.path.exists(os.path.join(base, "p1", "m1"))
        assert os.path.exists(os.path.join(base, "p1", "m2"))

    def test_clear_version_scope(self, mocker, tmp_path):
        base = str(tmp_path / "cache")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        _seed_cache(base, {"p1": {"m1": [1, 2]}})

        removed = model_mod.Model.clear_cache(
            project_name="p1", model_name="m1", version=1
        )

        assert removed == 1
        assert not os.path.exists(os.path.join(base, "p1", "m1", "1"))
        assert os.path.exists(os.path.join(base, "p1", "m1", "2"))

    def test_clear_nonexistent_returns_zero(self, mocker, tmp_path):
        base = str(tmp_path / "cache")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        _seed_cache(base, {"p1": {"m1": [1]}})

        assert model_mod.Model.clear_cache(project_name="missing") == 0

    def test_clear_sums_across_multiple_bases(self, mocker, tmp_path):
        base1 = str(tmp_path / "tmp")
        base2 = str(tmp_path / "home")
        mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs",
            return_value=[base1, base2],
        )
        _seed_cache(base1, {"p1": {"m1": [1]}})
        _seed_cache(base2, {"p1": {"m1": [1, 2]}})

        removed = model_mod.Model.clear_cache(project_name="p1", model_name="m1")

        assert removed == 3

    def test_model_name_without_project_raises(self):
        with pytest.raises(ValueError, match="model_name requires project_name"):
            model_mod.Model.clear_cache(model_name="m1")

    def test_version_without_model_raises(self):
        with pytest.raises(ValueError, match="version requires"):
            model_mod.Model.clear_cache(project_name="p1", version=1)

    def test_invalid_combination_does_not_delete(self, mocker, tmp_path):
        base = str(tmp_path / "cache")
        base_dirs_spy = mocker.patch(
            "hsml.engine.model_engine.model_cache_base_dirs", return_value=[base]
        )
        _seed_cache(base, {"p1": {"m1": [1]}})

        with pytest.raises(ValueError):
            model_mod.Model.clear_cache(model_name="m1")

        # Validation happens before any filesystem access, nothing is removed.
        base_dirs_spy.assert_not_called()
        assert os.path.exists(os.path.join(base, "p1", "m1", "1"))
