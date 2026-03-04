#
#   Copyright 2023 Hopsworks AB
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
import getpass
import importlib
import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager
from unittest import TestCase, mock, skip

import hopsworks
from hopsworks.client import exceptions
from hopsworks.project import Project
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.constants import HOSTS


@contextmanager
def input(*cmds):
    """Replace input."""
    hidden_cmds = [c.get("hidden") for c in cmds if isinstance(c, dict)]
    with mock.patch("getpass.getpass", side_effect=hidden_cmds):
        yield


class TestLogin(TestCase):
    """Test hopsworks login."""

    def setUp(self):
        if not hasattr(self, "system_tmp"):
            importlib.reload(tempfile)
            self.system_tmp = tempfile.gettempdir()

        self.cwd_path = os.path.join(self.system_tmp, str(uuid.uuid4()))
        self.temp_dir = os.path.join(self.cwd_path, "tmp")
        self.home_dir = os.path.join(self.cwd_path, "home")

        os.environ["TMP"] = self.temp_dir
        os.environ["HOME"] = self.home_dir

        os.mkdir(self.cwd_path)
        os.mkdir(self.temp_dir)
        os.mkdir(self.home_dir)

        importlib.reload(tempfile)

        os.chdir(self.cwd_path)

    def tearDown(self):
        os.environ["TMP"] = self.system_tmp
        shutil.rmtree(self.cwd_path)
        hopsworks.logout()

    def _check_api_key_existence(self):
        path = hopsworks._get_cached_api_key_path()

        api_key_name = ".hw_api_key"
        api_key_folder = f".{getpass.getuser()}_hopsworks_app"

        # Path for current working directory api key
        cwd_api_key_path = os.path.join(os.getcwd(), api_key_name)

        # Path for home api key
        home_dir_path = os.path.expanduser("~")
        home_api_key_path = os.path.join(home_dir_path, api_key_folder, api_key_name)

        # Path for tmp api key
        temp_dir_path = tempfile.gettempdir()
        temp_api_key_path = os.path.join(temp_dir_path, api_key_folder, api_key_name)

        return (
            path,
            path == cwd_api_key_path,
            path == home_api_key_path,
            path == temp_api_key_path,
        )

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_api_key_as_input(self):
        # Should accept api key as input from command line

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False

        with input({"hidden": os.environ["APP_API_KEY"]}):
            project = hopsworks.login()
            self.assertTrue(isinstance(project, Project))

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and os.path.exists(path)
        assert in_tmp is False

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_api_key_as_argument(self):
        # Should accept api key as argument
        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False
        # Should create API key in home by default
        project = hopsworks.login(api_key_value=os.environ["APP_API_KEY"])
        self.assertTrue(isinstance(project, Project))

        self.api_key_path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_cmd_input_incorrect(self):
        # Should fail to login with incorrect API key

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and not os.path.exists(path)
        assert in_tmp is False

        with (
            self.assertRaises(exceptions.RestAPIError),
            input({"hidden": "incorrect_api_key"}),
        ):
            hopsworks.login()

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_fallback_to_tmp(self):
        # Should fall back to storing api key in tmp folder if home is not write and executable for user
        os.chmod(self.home_dir, 0o400)

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is False
        assert in_tmp is True and not os.path.exists(path)

        # Should use API key in tmp folder
        with input({"hidden": os.environ["APP_API_KEY"]}):
            project = hopsworks.login()
            self.assertTrue(isinstance(project, Project))

        assert in_cwd is False
        assert in_home is False
        assert in_tmp is True and os.path.exists(path)

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_use_cwd_api_key(self):
        # Should use API key in cwd if exists

        api_key_path = os.path.join(os.getcwd(), ".hw_api_key")
        with open(api_key_path, "w") as f:
            f.write(os.environ["APP_API_KEY"])

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is True and os.path.exists(path)
        assert in_home is False
        assert in_tmp is False

        project = hopsworks.login()
        self.assertTrue(isinstance(project, Project))

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is True and os.path.exists(path)
        assert in_home is False
        assert in_tmp is False

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_use_home_api_key(self):
        # Should use API key in home if exists

        api_key_folder_path = os.path.join(
            os.path.expanduser("~"), f".{getpass.getuser()}_hopsworks_app"
        )
        os.mkdir(api_key_folder_path)
        with open(os.path.join(api_key_folder_path, ".hw_api_key"), "w") as f:
            f.write(os.environ["APP_API_KEY"])

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and os.path.exists(path)
        assert in_tmp is False

        project = hopsworks.login()
        self.assertTrue(isinstance(project, Project))

        path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

        assert in_cwd is False
        assert in_home is True and os.path.exists(path)
        assert in_tmp is False

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_api_key_as_environ(self):
        # Should accept api key as environmet variable
        try:
            os.environ["HOPSWORKS_API_KEY"] = os.environ["APP_API_KEY"]

            path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

            assert in_cwd is False
            assert in_home is True and not os.path.exists(path)
            assert in_tmp is False

            project = hopsworks.login()
            self.assertTrue(isinstance(project, Project))

            path, in_cwd, in_home, in_tmp = self._check_api_key_existence()

            assert in_cwd is False
            assert in_home is True and not os.path.exists(path)
            assert in_tmp is False
        except Exception as e:
            raise e
        finally:
            del os.environ["HOPSWORKS_API_KEY"]

    @skip(
        "To be removed once https://hopsworks.atlassian.net/browse/FSTORE-1474 is resolved."
    )
    def test_login_newline_in_api_key(self):
        try:
            imaginaryApiKey = "ImaginaryApiKey\n"
            hopsworks.login(api_key_value=imaginaryApiKey)
        except Exception as e:
            self.assertNotIn(imaginaryApiKey.strip(), str(e))


class TestLoginUnit:
    """Unit tests for hopsworks.login() — all network I/O is mocked."""

    def setup_method(self):
        # Arrange — reset module globals so tests are isolated.
        hopsworks._connected_project = None
        hopsworks._hw_connection = hopsworks.Connection.connection

    def teardown_method(self):
        hopsworks._connected_project = None
        hopsworks._hw_connection = hopsworks.Connection.connection

    def _mock_conn_factory(self, mocker, mock_project):
        """Patch Connection.connection to return a mock connection instance."""
        mock_conn_instance = mocker.MagicMock()
        mock_conn_instance.get_project.return_value = mock_project
        mock_conn_factory = mocker.patch.object(
            hopsworks.Connection, "connection", return_value=mock_conn_instance
        )
        return mock_conn_factory, mock_conn_instance

    def _patch_side_effects(self, mocker, mock_project):
        """Suppress helpers that are not under test."""
        # hopsworks.client gets overridden to the hopsworks/client/ subpackage
        # (which lacks stop()) once `from hopsworks.client import exceptions` runs.
        # create=True injects stop() into that subpackage namespace.
        mocker.patch("hopsworks.client.stop", create=True)
        mocker.patch("hopsworks._prompt_project", return_value=mock_project)
        mocker.patch("hopsworks._set_active_project")
        mocker.patch("hopsworks._initialize_module_apis")

    # ── SaaS paths ────────────────────────────────────────────────────────────

    def test_login_saas_with_api_key_value(self, mocker):
        # Arrange
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "https://eu-west.cloud.hopsworks.ai/p/1"
        mock_conn_factory, _ = self._mock_conn_factory(mocker, mock_project)
        self._patch_side_effects(mocker, mock_project)

        # Act
        result = hopsworks.login(api_key_value="test-api-key")

        # Assert
        mock_conn_factory.assert_called_once()
        call_kwargs = mock_conn_factory.call_args.kwargs
        assert call_kwargs["host"] == HOSTS.SAAS_HOST
        assert call_kwargs["api_key_value"] == "test-api-key"
        assert result is mock_project

    def test_login_saas_with_cached_key_file(self, mocker, tmp_path):
        # Arrange — a cached .hw_api_key file already exists on disk.
        api_key_path = tmp_path / ".hw_api_key"
        api_key_path.write_text("cached-key")
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "https://eu-west.cloud.hopsworks.ai/p/1"
        mock_conn_factory, _ = self._mock_conn_factory(mocker, mock_project)
        self._patch_side_effects(mocker, mock_project)
        mocker.patch(
            "hopsworks._get_cached_api_key_path", return_value=str(api_key_path)
        )

        # Act
        result = hopsworks.login()

        # Assert — connection must be opened with the cached key file, not a value.
        mock_conn_factory.assert_called_once()
        call_kwargs = mock_conn_factory.call_args.kwargs
        assert call_kwargs["api_key_file"] == str(api_key_path)
        assert result is mock_project

    def test_login_saas_prompts_and_saves_key_when_no_cached_file(
        self, mocker, tmp_path
    ):
        # Arrange — no cached key; login must prompt the user and persist the result.
        api_key_path = tmp_path / "subdir" / ".hw_api_key"  # parent does not exist yet
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "https://eu-west.cloud.hopsworks.ai/p/1"
        mock_conn_factory, _ = self._mock_conn_factory(mocker, mock_project)
        self._patch_side_effects(mocker, mock_project)
        mocker.patch(
            "hopsworks._get_cached_api_key_path", return_value=str(api_key_path)
        )
        mock_getpass = mocker.patch("getpass.getpass", return_value="prompted-key")

        # Act
        result = hopsworks.login()

        # Assert
        mock_getpass.assert_called_once()
        mock_conn_factory.assert_called_once()
        call_kwargs = mock_conn_factory.call_args.kwargs
        assert call_kwargs["api_key_value"] == "prompted-key"
        assert api_key_path.exists(), "prompted key must be persisted to disk"
        assert api_key_path.read_text() == "prompted-key"
        assert result is mock_project

    def test_login_saas_invalid_cached_key_falls_back_to_prompt(
        self, mocker, tmp_path
    ):
        # Arrange — cached key exists but the server rejects it; login must delete
        # the stale file and prompt for a fresh key.
        api_key_path = tmp_path / ".hw_api_key"
        api_key_path.write_text("expired-key")
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "https://eu-west.cloud.hopsworks.ai/p/1"
        mock_conn_instance = mocker.MagicMock()
        mock_conn_instance.get_project.return_value = mock_project
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = {}
        mock_response.status_code = 401
        mock_response.reason = "Unauthorized"
        mock_response.content = b""
        mock_conn_factory = mocker.patch.object(
            hopsworks.Connection,
            "connection",
            side_effect=[
                RestAPIError("https://eu-west.cloud.hopsworks.ai", mock_response),
                mock_conn_instance,
            ],
        )
        self._patch_side_effects(mocker, mock_project)
        mocker.patch(
            "hopsworks._get_cached_api_key_path", return_value=str(api_key_path)
        )
        mock_getpass = mocker.patch("getpass.getpass", return_value="new-key")

        # Act
        result = hopsworks.login()

        # Assert
        assert mock_conn_factory.call_count == 2
        mock_getpass.assert_called_once()
        # The stale file is deleted then re-created with the fresh key.
        assert api_key_path.exists()
        assert api_key_path.read_text() == "new-key"
        assert result is mock_project

    # ── Non-SaaS paths ────────────────────────────────────────────────────────

    def test_login_non_saas_with_api_key_value(self, mocker):
        # Arrange
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "https://my.hopsworks.server/p/1"
        mock_conn_factory, _ = self._mock_conn_factory(mocker, mock_project)
        self._patch_side_effects(mocker, mock_project)

        # Act
        result = hopsworks.login(
            host="my.hopsworks.server", api_key_value="non-saas-key"
        )

        # Assert
        mock_conn_factory.assert_called_once()
        call_kwargs = mock_conn_factory.call_args.kwargs
        assert call_kwargs["host"] == "my.hopsworks.server"
        assert call_kwargs["api_key_value"] == "non-saas-key"
        assert result is mock_project

    def test_login_non_saas_uses_hopsworks_api_key_env_var(self, mocker, monkeypatch):
        # Arrange
        monkeypatch.setenv("HOPSWORKS_API_KEY", "env-key")
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "https://my.hopsworks.server/p/1"
        mock_conn_factory, _ = self._mock_conn_factory(mocker, mock_project)
        self._patch_side_effects(mocker, mock_project)

        # Act
        result = hopsworks.login(host="my.hopsworks.server")

        # Assert
        mock_conn_factory.assert_called_once()
        call_kwargs = mock_conn_factory.call_args.kwargs
        assert call_kwargs["api_key_value"] == "env-key"
        assert result is mock_project

    def test_login_non_saas_with_api_key_file(self, mocker, tmp_path):
        # Arrange
        key_file = tmp_path / "my_key.txt"
        key_file.write_text("file-key")
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "https://my.hopsworks.server/p/1"
        mock_conn_factory, _ = self._mock_conn_factory(mocker, mock_project)
        self._patch_side_effects(mocker, mock_project)

        # Act
        result = hopsworks.login(
            host="my.hopsworks.server", api_key_file=str(key_file)
        )

        # Assert — the file content is read and passed as api_key_value.
        mock_conn_factory.assert_called_once()
        call_kwargs = mock_conn_factory.call_args.kwargs
        assert call_kwargs["api_key_value"] == "file-key"
        assert result is mock_project

    # ── Inside Hopsworks (REST_ENDPOINT) path ─────────────────────────────────

    def test_login_inside_hopsworks_uses_rest_endpoint(self, mocker, monkeypatch):
        # Arrange — REST_ENDPOINT env var signals an on-cluster client; the
        # connection factory is called without host/api-key and project is
        # retrieved directly from the connection, not via _prompt_project.
        monkeypatch.setenv("REST_ENDPOINT", "http://localhost:8181")
        mock_project = mocker.MagicMock()
        mock_project.get_url.return_value = "http://localhost:8181/p/1"
        mock_conn_instance = mocker.MagicMock()
        mock_conn_instance.get_project.return_value = mock_project
        mock_conn_factory = mocker.patch.object(
            hopsworks.Connection, "connection", return_value=mock_conn_instance
        )
        mocker.patch("hopsworks.client.stop", create=True)
        mocker.patch("hopsworks._initialize_module_apis")
        mock_prompt_project = mocker.patch("hopsworks._prompt_project")

        # Act
        result = hopsworks.login()

        # Assert
        mock_conn_factory.assert_called_once()
        mock_conn_instance.get_project.assert_called_once()
        mock_prompt_project.assert_not_called()
        assert result is mock_project
