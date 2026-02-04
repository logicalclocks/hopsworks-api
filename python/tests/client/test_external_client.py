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

import base64
import os
import stat
import sys
import tempfile

from hopsworks_common.client.base import Client as BaseClient
from hopsworks_common.client.external import Client as ExternalClient


# Windows doesn't support Unix-style file permissions
IS_WINDOWS = sys.platform == "win32"


class TestSecureFileWriting:
    """Tests for secure file writing with restricted permissions."""

    def test_write_secure_file_creates_file_with_0600_permissions(self):
        """Test that _write_secure_file creates files with 0o600 permissions."""
        client = BaseClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test_file.txt")
            test_content = "sensitive data"

            client._write_secure_file(test_content, test_path)

            # Verify file exists and has correct content
            assert os.path.exists(test_path)
            with open(test_path) as f:
                assert f.read() == test_content

            # Verify permissions are 0o600 (owner read/write only)
            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                file_stat = os.stat(test_path)
                file_mode = stat.S_IMODE(file_stat.st_mode)
                assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"

    def test_write_secure_file_with_bytes_creates_file_with_0600_permissions(self):
        """Test that _write_secure_file handles bytes content correctly."""
        client = BaseClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test_file.bin")
            test_content = b"binary sensitive data"

            client._write_secure_file(test_content, test_path)

            # Verify file exists and has correct content
            assert os.path.exists(test_path)
            with open(test_path, "rb") as f:
                assert f.read() == test_content

            # Verify permissions are 0o600
            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                file_stat = os.stat(test_path)
                file_mode = stat.S_IMODE(file_stat.st_mode)
                assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"

    def test_write_pem_file_creates_file_with_0600_permissions(self):
        """Test that _write_pem_file creates files with 0o600 permissions."""
        client = BaseClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.pem")
            test_content = (
                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
            )

            client._write_pem_file(test_content, test_path)

            # Verify file exists and has correct content
            assert os.path.exists(test_path)
            with open(test_path) as f:
                assert f.read() == test_content

            # Verify permissions are 0o600
            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                file_stat = os.stat(test_path)
                file_mode = stat.S_IMODE(file_stat.st_mode)
                assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"


class TestExternalClientCertificates:
    """Tests for external client certificate handling."""

    def test_materialize_certs_creates_folder_with_random_name_for_default_path(
        self, mocker
    ):
        """Test that _materialize_certs creates a folder with random name prefix when using default /tmp."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = None
        # Use the default /tmp path which triggers random subdirectory creation
        client._cert_folder_base = "/tmp"
        client._project_id = "123"

        # Mock _get_credentials to return fake credentials
        fake_credentials = {
            "kStore": base64.b64encode(b"fake keystore").decode(),
            "tStore": base64.b64encode(b"fake truststore").decode(),
            "password": "fake_password",
            "caChain": "fake_ca_chain",
            "clientCert": "fake_client_cert",
            "clientKey": "fake_client_key",
        }
        mocker.patch.object(client, "_get_credentials", return_value=fake_credentials)

        try:
            client._materialize_certs()

            # Verify folder was created with correct prefix
            assert client._cert_folder is not None
            assert os.path.exists(client._cert_folder)
            folder_name = os.path.basename(client._cert_folder)
            assert folder_name.startswith("hopsworks_certs_")

            # Verify folder is in the correct base directory
            assert os.path.dirname(client._cert_folder) == client._cert_folder_base
        finally:
            # Cleanup
            if client._cert_folder and os.path.exists(client._cert_folder):
                for f in os.listdir(client._cert_folder):
                    os.remove(os.path.join(client._cert_folder, f))
                os.rmdir(client._cert_folder)

    def test_materialize_certs_creates_files_with_0600_permissions(self, mocker):
        """Test that certificate files are created with 0o600 permissions."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = None
        # Use the default /tmp path
        client._cert_folder_base = "/tmp"
        client._project_id = "123"

        # Mock _get_credentials to return fake credentials
        fake_credentials = {
            "kStore": base64.b64encode(b"fake keystore").decode(),
            "tStore": base64.b64encode(b"fake truststore").decode(),
            "password": "fake_password",
            "caChain": "fake_ca_chain",
            "clientCert": "fake_client_cert",
            "clientKey": "fake_client_key",
        }
        mocker.patch.object(client, "_get_credentials", return_value=fake_credentials)

        try:
            client._materialize_certs()

            # Verify all certificate files exist and have correct permissions
            cert_files = [
                os.path.join(client._cert_folder, "keyStore.jks"),
                os.path.join(client._cert_folder, "trustStore.jks"),
                os.path.join(client._cert_folder, "material_passwd"),
            ]

            for cert_file in cert_files:
                assert os.path.exists(cert_file), f"File {cert_file} should exist"
                # Skip permission check on Windows as it doesn't support Unix permissions
                if not IS_WINDOWS:
                    file_stat = os.stat(cert_file)
                    file_mode = stat.S_IMODE(file_stat.st_mode)
                    assert file_mode == 0o600, (
                        f"File {cert_file} should have 0o600 permissions, got {oct(file_mode)}"
                    )
        finally:
            # Cleanup
            if client._cert_folder and os.path.exists(client._cert_folder):
                for f in os.listdir(client._cert_folder):
                    os.remove(os.path.join(client._cert_folder, f))
                os.rmdir(client._cert_folder)

    def test_materialize_certs_folder_has_0700_permissions(self, mocker):
        """Test that the certificate folder has 0o700 permissions (from mkdtemp) when using default path."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = None
        # Use the default /tmp path which triggers random subdirectory creation with 0o700 permissions
        client._cert_folder_base = "/tmp"
        client._project_id = "123"

        # Mock _get_credentials to return fake credentials
        fake_credentials = {
            "kStore": base64.b64encode(b"fake keystore").decode(),
            "tStore": base64.b64encode(b"fake truststore").decode(),
            "password": "fake_password",
            "caChain": "fake_ca_chain",
            "clientCert": "fake_client_cert",
            "clientKey": "fake_client_key",
        }
        mocker.patch.object(client, "_get_credentials", return_value=fake_credentials)

        try:
            client._materialize_certs()

            # Verify folder exists
            assert os.path.exists(client._cert_folder)
            assert os.path.isdir(client._cert_folder)

            # Verify folder has 0o700 permissions
            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                folder_stat = os.stat(client._cert_folder)
                folder_mode = stat.S_IMODE(folder_stat.st_mode)
                assert folder_mode == 0o700, (
                    f"Folder should have 0o700 permissions, got {oct(folder_mode)}"
                )
        finally:
            # Cleanup
            if client._cert_folder and os.path.exists(client._cert_folder):
                for f in os.listdir(client._cert_folder):
                    os.remove(os.path.join(client._cert_folder, f))
                os.rmdir(client._cert_folder)

    def test_get_certs_folder_returns_cert_folder(self, mocker):
        """Test that get_certs_folder returns the certificate folder path."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = "/tmp/hopsworks_certs_abc123"

        assert client.get_certs_folder() == "/tmp/hopsworks_certs_abc123"

    def test_write_b64_cert_to_bytes_creates_file_with_0600_permissions(self, mocker):
        """Test that _write_b64_cert_to_bytes creates files with 0o600 permissions."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.jks")
            test_content = b"fake jks content"
            b64_content = base64.b64encode(test_content).decode()

            client._write_b64_cert_to_bytes(b64_content, test_path)

            # Verify file exists and has correct content
            assert os.path.exists(test_path)
            with open(test_path, "rb") as f:
                assert f.read() == test_content

            # Verify permissions are 0o600
            # Skip permission check on Windows as it doesn't support Unix permissions
            if not IS_WINDOWS:
                file_stat = os.stat(test_path)
                file_mode = stat.S_IMODE(file_stat.st_mode)
                assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"

    def test_materialize_certs_uses_custom_folder_directly(self, mocker):
        """Test that _materialize_certs uses custom cert_folder directly without creating subdirectory."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = None
        client._project_id = "123"

        # Mock _get_credentials to return fake credentials
        fake_credentials = {
            "kStore": base64.b64encode(b"fake keystore").decode(),
            "tStore": base64.b64encode(b"fake truststore").decode(),
            "password": "fake_password",
            "caChain": "fake_ca_chain",
            "clientCert": "fake_client_cert",
            "clientKey": "fake_client_key",
        }
        mocker.patch.object(client, "_get_credentials", return_value=fake_credentials)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Use a custom path (not /tmp)
            custom_cert_folder = os.path.join(tmpdir, "my_custom_certs")
            client._cert_folder_base = custom_cert_folder

            client._materialize_certs()

            # Verify folder is used directly (no random subdirectory)
            assert client._cert_folder == custom_cert_folder
            assert os.path.exists(client._cert_folder)

            # Verify certificates are in the custom folder
            assert os.path.exists(os.path.join(client._cert_folder, "keyStore.jks"))
            assert os.path.exists(os.path.join(client._cert_folder, "trustStore.jks"))
            assert os.path.exists(os.path.join(client._cert_folder, "material_passwd"))

    def test_materialize_certs_creates_custom_folder_if_not_exists(self, mocker):
        """Test that _materialize_certs creates the custom cert_folder if it doesn't exist."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = None
        client._project_id = "123"

        # Mock _get_credentials to return fake credentials
        fake_credentials = {
            "kStore": base64.b64encode(b"fake keystore").decode(),
            "tStore": base64.b64encode(b"fake truststore").decode(),
            "password": "fake_password",
            "caChain": "fake_ca_chain",
            "clientCert": "fake_client_cert",
            "clientKey": "fake_client_key",
        }
        mocker.patch.object(client, "_get_credentials", return_value=fake_credentials)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Use a nested custom path that doesn't exist
            custom_cert_folder = os.path.join(tmpdir, "nested", "path", "to", "certs")
            client._cert_folder_base = custom_cert_folder

            # Verify the path doesn't exist yet
            assert not os.path.exists(custom_cert_folder)

            client._materialize_certs()

            # Verify the nested path was created
            assert os.path.exists(custom_cert_folder)
            assert client._cert_folder == custom_cert_folder

            # Verify certificates are in the custom folder
            assert os.path.exists(os.path.join(client._cert_folder, "keyStore.jks"))


class TestCertFolderEnvironmentVariable:
    """Tests for HOPSWORKS_CERT_FOLDER environment variable handling."""

    def test_cert_folder_env_var_is_used_when_set(self, mocker):
        """Test that HOPSWORKS_CERT_FOLDER environment variable is used when set."""
        from hopsworks.connection import Connection

        with tempfile.TemporaryDirectory() as tmpdir:
            custom_cert_folder = os.path.join(tmpdir, "env_var_certs")

            # Set the environment variable
            mocker.patch.dict(os.environ, {"HOPSWORKS_CERT_FOLDER": custom_cert_folder})

            # Mock the Connection class to capture the cert_folder argument
            mock_connection_instance = mocker.MagicMock()
            mock_connection_class = mocker.patch.object(
                Connection,
                "connection",
                return_value=mock_connection_instance,
            )

            import hopsworks

            # Reset the module state
            hopsworks._hw_connection = Connection.connection
            mocker.patch.object(hopsworks, "_prompt_project", return_value=None)
            mocker.patch.object(hopsworks, "_initialize_module_apis")

            # Call login with api_key_value to skip interactive prompts
            hopsworks.login(
                host="test.hopsworks.ai",
                api_key_value="fake_api_key",
            )

            # Verify the connection was called with the cert_folder from env var
            mock_connection_class.assert_called_once()
            call_kwargs = mock_connection_class.call_args[1]
            assert call_kwargs["cert_folder"] == custom_cert_folder

    def test_cert_folder_argument_takes_precedence_over_env_var(self, mocker):
        """Test that cert_folder argument takes precedence over HOPSWORKS_CERT_FOLDER env var."""
        from hopsworks.connection import Connection

        with tempfile.TemporaryDirectory() as tmpdir:
            env_cert_folder = os.path.join(tmpdir, "env_var_certs")
            arg_cert_folder = os.path.join(tmpdir, "arg_certs")

            # Set the environment variable
            mocker.patch.dict(os.environ, {"HOPSWORKS_CERT_FOLDER": env_cert_folder})

            # Mock the Connection class to capture the cert_folder argument
            mock_connection_instance = mocker.MagicMock()
            mock_connection_class = mocker.patch.object(
                Connection,
                "connection",
                return_value=mock_connection_instance,
            )

            import hopsworks

            # Reset the module state
            hopsworks._hw_connection = Connection.connection
            mocker.patch.object(hopsworks, "_prompt_project", return_value=None)
            mocker.patch.object(hopsworks, "_initialize_module_apis")

            # Call login with explicit cert_folder argument
            hopsworks.login(
                host="test.hopsworks.ai",
                api_key_value="fake_api_key",
                cert_folder=arg_cert_folder,
            )

            # Verify the connection was called with the cert_folder from argument (not env var)
            mock_connection_class.assert_called_once()
            call_kwargs = mock_connection_class.call_args[1]
            assert call_kwargs["cert_folder"] == arg_cert_folder

    def test_default_cert_folder_used_when_env_var_not_set(self, mocker):
        """Test that default /tmp is used when HOPSWORKS_CERT_FOLDER is not set."""
        from hopsworks.connection import Connection

        # Ensure the environment variable is not set
        env_without_cert_folder = {
            k: v for k, v in os.environ.items() if k != "HOPSWORKS_CERT_FOLDER"
        }
        mocker.patch.dict(os.environ, env_without_cert_folder, clear=True)

        # Mock the Connection class to capture the cert_folder argument
        mock_connection_instance = mocker.MagicMock()
        mock_connection_class = mocker.patch.object(
            Connection,
            "connection",
            return_value=mock_connection_instance,
        )

        import hopsworks

        # Reset the module state
        hopsworks._hw_connection = Connection.connection
        mocker.patch.object(hopsworks, "_prompt_project", return_value=None)
        mocker.patch.object(hopsworks, "_initialize_module_apis")

        # Call login without cert_folder argument
        hopsworks.login(
            host="test.hopsworks.ai",
            api_key_value="fake_api_key",
        )

        # Verify the connection was called with the default cert_folder
        mock_connection_class.assert_called_once()
        call_kwargs = mock_connection_class.call_args[1]
        assert call_kwargs["cert_folder"] == "/tmp"
