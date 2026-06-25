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

import pytest
from hsml.utils.local_paths import (
    _normalize_hopsfs_mount_path,
    _resolve_serving_file,
)


@pytest.fixture
def project_name(mocker):
    name = "myproject"
    fake_client = mocker.Mock()
    fake_client._project_name = name
    mocker.patch(
        "hsml.utils.local_paths.client._get_instance",
        return_value=fake_client,
    )
    return name


@pytest.fixture
def local_engine(mocker):
    # `_ensure_dataset_dir` consults engine._dataset_api.exists() per
    # intermediate segment; default to "nothing exists" so the resolver
    # creates every parent. `_resolve_serving_file` step 3 also calls
    # `_dataset_api.exists(path)` to detect HopsFS membership; tests that
    # exercise that branch override the return_value.
    engine = mocker.Mock()
    engine._dataset_api.exists.return_value = False
    return engine


class TestNormalizeHopsfsMountPath:
    @pytest.mark.parametrize(
        "path,expected",
        [
            # /hopsfs/<rest> — per-project FUSE mount in Jobs/Jupyter pods.
            ("/hopsfs/Resources/foo.py", "Resources/foo.py"),
            ("/hopsfs/Models/model.pkl", "Models/model.pkl"),
            # /mnt/hopsfs/<project>/<rest> — cluster-wide mount; strip both
            # the mount base and the next segment (the project name).
            ("/mnt/hopsfs/demo/Models/model.pkl", "Models/model.pkl"),
            (
                "/mnt/hopsfs/demo/Resources/workflows/models/tensorflow",
                "Resources/workflows/models/tensorflow",
            ),
            # Only the *leading* prefix is stripped — repeated segments
            # deeper in the path stay intact (regression case).
            (
                "/hopsfs/Models/hopsfs/archive/model.pkl",
                "Models/hopsfs/archive/model.pkl",
            ),
            (
                "/mnt/hopsfs/demo/Models/mnt/hopsfs/archive/model.pkl",
                "Models/mnt/hopsfs/archive/model.pkl",
            ),
        ],
    )
    def test_mount_paths_normalized(self, path, expected):
        assert _normalize_hopsfs_mount_path(path) == expected

    def test_non_mount_returns_none(self):
        assert _normalize_hopsfs_mount_path("/tmp/foo.py") is None
        assert (
            _normalize_hopsfs_mount_path("/Projects/myproject/Resources/foo.py") is None
        )
        assert _normalize_hopsfs_mount_path("./foo.py") is None
        assert _normalize_hopsfs_mount_path("foo.py") is None
        assert _normalize_hopsfs_mount_path("local/model.pkl") is None


class TestResolveServingFile:
    def test_none_returns_none(self, local_engine):
        assert (
            _resolve_serving_file(
                local_engine, "my_dep", None, "script_file", subdir="predictor"
            )
            is None
        )
        local_engine._upload.assert_not_called()

    def test_absolute_local_file_uploaded(self, tmp_path, project_name, local_engine):
        local = tmp_path / "predictor.py"
        local.write_text("# noop\n")

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            str(local),
            "script_file",
            subdir="predictor",
        )

        assert out == (
            f"/Projects/{project_name}/Deployments/"
            f"my_dep/resources/predictor/predictor.py"
        )
        local_engine._upload.assert_called_once_with(
            str(local),
            "Deployments/my_dep/resources/predictor",
            overwrite=True,
        )
        mkdir_calls = [c.args[0] for c in local_engine._mkdir.mock_calls]
        assert mkdir_calls == [
            "Deployments/my_dep",
            "Deployments/my_dep/resources",
            "Deployments/my_dep/resources/predictor",
        ]

    def test_relative_local_file_uploaded(
        self, tmp_path, project_name, local_engine, monkeypatch
    ):
        # `./predictor.py` from a non-HopsFS cwd → joined with cwd, uploaded.
        local = tmp_path / "predictor.py"
        local.write_text("# noop\n")
        monkeypatch.chdir(tmp_path)

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            "./predictor.py",
            "script_file",
            subdir="predictor",
        )

        assert out.endswith("/my_dep/resources/predictor/predictor.py")
        local_engine._upload.assert_called_once()

    def test_relative_path_from_hopsfs_cwd_skips_upload(
        self, project_name, local_engine, mocker
    ):
        # User in a Jobs/Jupyter pod with cwd under /hopsfs/Resources/.
        # `./foo.py` joined with cwd lands on a FUSE mount → no upload.
        mocker.patch("os.getcwd", return_value="/hopsfs/Resources")
        mocker.patch("os.path.exists", return_value=True)

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            "./foo.py",
            "script_file",
            subdir="predictor",
        )

        assert out == f"/Projects/{project_name}/Resources/foo.py"
        local_engine._upload.assert_not_called()

    def test_hopsfs_fuse_mount_path_passthrough(
        self, project_name, local_engine, mocker
    ):
        # Absolute /hopsfs/<rest> on disk → no upload, normalized to
        # /Projects/<p>/<rest>.
        mocker.patch("os.path.exists", return_value=True)

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            "/hopsfs/Resources/foo.py",
            "script_file",
            subdir="predictor",
        )

        assert out == f"/Projects/{project_name}/Resources/foo.py"
        local_engine._upload.assert_not_called()

    def test_mnt_hopsfs_cluster_mount_passthrough(
        self, project_name, local_engine, mocker
    ):
        # Absolute /mnt/hopsfs/<project>/<rest> on disk → no upload.
        mocker.patch("os.path.exists", return_value=True)

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            "/mnt/hopsfs/myproject/Resources/foo.py",
            "script_file",
            subdir="predictor",
        )

        assert out == f"/Projects/{project_name}/Resources/foo.py"
        local_engine._upload.assert_not_called()

    def test_absolute_hopsfs_path_passthrough(self, project_name, local_engine):
        # /Projects/<p>/... that exists in HopsFS → return as-is, no upload.
        # `os.path.exists` is False (no FUSE mount), so step 3 runs.
        local_engine._dataset_api.exists.return_value = True

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            f"/Projects/{project_name}/Resources/foo.py",
            "script_file",
            subdir="predictor",
        )

        assert out == f"/Projects/{project_name}/Resources/foo.py"
        local_engine._upload.assert_not_called()

    def test_project_relative_hopsfs_path_expanded(self, project_name, local_engine):
        # The form returned by `dataset_api.upload("file.py", "Resources")`.
        local_engine._dataset_api.exists.return_value = True

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            "Resources/foo.py",
            "script_file",
            subdir="predictor",
        )

        assert out == f"/Projects/{project_name}/Resources/foo.py"
        local_engine._upload.assert_not_called()

    def test_hdfs_uri_stripped_to_absolute(self, project_name, local_engine):
        local_engine._dataset_api.exists.return_value = True

        out = _resolve_serving_file(
            local_engine,
            "my_dep",
            f"hdfs://namenode:8020/Projects/{project_name}/Resources/foo.py",
            "script_file",
            subdir="predictor",
        )

        assert out == f"/Projects/{project_name}/Resources/foo.py"
        local_engine._upload.assert_not_called()

    def test_missing_path_raises(self, project_name, local_engine):
        # Not on disk; HopsFS says it doesn't exist either. Default fixture
        # has `_dataset_api.exists.return_value = False`.
        with pytest.raises(ValueError, match="Could not find script_file"):
            _resolve_serving_file(
                local_engine,
                "my_dep",
                "./does_not_exist.py",
                "script_file",
                subdir="predictor",
            )
        local_engine._upload.assert_not_called()

    def test_local_uploads_to_role_subdir(self, tmp_path, project_name, local_engine):
        # Predictor and transformer with the same basename land in
        # different subdirs.
        f = tmp_path / "script.py"
        f.write_text("# noop\n")

        predictor_out = _resolve_serving_file(
            local_engine, "dep", str(f), "script_file", subdir="predictor"
        )
        transformer_out = _resolve_serving_file(
            local_engine,
            "dep",
            str(f),
            "transformer.script_file",
            subdir="transformer",
        )

        assert predictor_out.endswith("/Deployments/dep/resources/predictor/script.py")
        assert transformer_out.endswith(
            "/Deployments/dep/resources/transformer/script.py"
        )

    def test_overwrite_false_propagated(self, tmp_path, project_name, local_engine):
        local = tmp_path / "predictor.py"
        local.write_text("# noop\n")

        _resolve_serving_file(
            local_engine,
            "my_dep",
            str(local),
            "script_file",
            subdir="predictor",
            overwrite=False,
        )

        local_engine._upload.assert_called_once_with(
            str(local),
            "Deployments/my_dep/resources/predictor",
            overwrite=False,
        )
