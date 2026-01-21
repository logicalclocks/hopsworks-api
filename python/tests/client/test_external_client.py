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
import tempfile

import pytest
from hopsworks_common.client.base import Client as BaseClient
from hopsworks_common.client.external import Client as ExternalClient


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
            file_stat = os.stat(test_path)
            file_mode = stat.S_IMODE(file_stat.st_mode)
            assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"

    def test_write_pem_file_creates_file_with_0600_permissions(self):
        """Test that _write_pem_file creates files with 0o600 permissions."""
        client = BaseClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.pem")
            test_content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"

            client._write_pem_file(test_content, test_path)

            # Verify permissions are 0o600
            file_stat = os.stat(test_path)
            file_mode = stat.S_IMODE(file_stat.st_mode)
            assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"


class TestExternalClientCertificates:
    """Tests for external client certificate handling."""

    def test_materialize_certs_creates_folder_with_random_name(self, mocker):
        """Test that _materialize_certs creates a folder with random name prefix."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = None
        client._cert_folder_base = tempfile.gettempdir()
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
        client._cert_folder_base = tempfile.gettempdir()
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

            # Verify all certificate files have 0o600 permissions
            cert_files = [
                os.path.join(client._cert_folder, "keyStore.jks"),
                os.path.join(client._cert_folder, "trustStore.jks"),
                os.path.join(client._cert_folder, "material_passwd"),
            ]

            for cert_file in cert_files:
                assert os.path.exists(cert_file), f"File {cert_file} should exist"
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
        """Test that the certificate folder has 0o700 permissions (from mkdtemp)."""
        mocker.patch.object(ExternalClient, "__init__", lambda self: None)
        client = ExternalClient()
        client._cert_folder = None
        client._cert_folder_base = tempfile.gettempdir()
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

            # Verify folder has 0o700 permissions
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
            file_stat = os.stat(test_path)
            file_mode = stat.S_IMODE(file_stat.st_mode)
            assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"
