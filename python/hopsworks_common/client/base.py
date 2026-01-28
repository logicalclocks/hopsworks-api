#
#   Copyright 2020 Logical Clocks AB
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

import base64
import hashlib
import logging
import os
import struct
import time
from pathlib import Path

import furl
import requests
import urllib3
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_der_private_key,
)
from hopsworks_common.client import auth, exceptions
from hopsworks_common.decorators import connected

_logger = logging.getLogger(__name__)


urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Client:
    TOKEN_FILE = "token.jwt"
    TOKEN_EXPIRED_RETRY_INTERVAL = 0.6
    TOKEN_EXPIRED_MAX_RETRIES = 10

    APIKEY_FILE = "api.key"
    REST_ENDPOINT = "REST_ENDPOINT"
    DEFAULT_DATABRICKS_ROOT_VIRTUALENV_ENV = "DEFAULT_DATABRICKS_ROOT_VIRTUALENV_ENV"
    HOPSWORKS_PUBLIC_HOST = "HOPSWORKS_PUBLIC_HOST"

    def _get_verify(self, verify, trust_store_path):
        """Get verification method for sending HTTP requests to Hopsworks.

        Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c

        :param verify: perform hostname verification
        :type verify: bool
        :param trust_store_path: path of the truststore locally if it was uploaded manually to
            the external environment such as AWS Sagemaker
        :type trust_store_path: str
        :return: if verify is true and the truststore is provided, then return the trust store location
                 if verify is true but the truststore wasn't provided, then return true
                 if verify is false, then return false
        :rtype: str or boolean
        """
        if verify:
            if trust_store_path is not None:
                return trust_store_path
            return True

        return False

    def _get_host_port_pair(self) -> tuple[str, str]:
        """Removes "http or https" from the rest endpoint and returns a list [endpoint, port], where endpoint is on the format /path.. without http://.

        Returns:
            a tuple (endpoint, port)
        """
        endpoint = self._base_url
        if "http" in endpoint:
            last_index = endpoint.rfind("/")
            endpoint = endpoint[last_index + 1 :]
        host, port = endpoint.split(":")
        return host, port

    def _read_jwt(self):
        """Retrieve jwt from local container."""
        return self._read_file(self.TOKEN_FILE)

    def _read_apikey(self):
        """Retrieve apikey from local container."""
        return self._read_file(self.APIKEY_FILE)

    def _read_file(self, secret_file):
        """Retrieve secret from local container."""
        with open(os.path.join(self._secrets_dir, secret_file)) as secret:
            return secret.read()

    def _get_credentials(self, project_id):
        """Makes a REST call to hopsworks for getting the project user certificates needed to connect to services such as Hive.

        :param project_id: id of the project
        :type project_id: int
        :return: JSON response with credentials
        :rtype: dict
        """
        return self._send_request("GET", ["project", project_id, "credentials"])

    def _write_secure_file(self, content: bytes | str, path: str) -> None:
        """Write content to a file with restricted permissions (0o600).

        :param content: content to write (bytes or str)
        :param path: path where file is saved
        """
        mode = "wb" if isinstance(content, bytes) else "w"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, mode) as f:
            f.write(content)

    def _write_pem_file(self, content: str, path: str) -> None:
        self._write_secure_file(content, path)

    @connected
    def _send_request(
        self,
        method,
        path_params,
        query_params=None,
        headers=None,
        data=None,
        stream=False,
        files=None,
        with_base_path_params=True,
    ):
        """Send REST request to Hopsworks.

        Uses the client it is executed from. Path parameters are url encoded automatically.

        :param method: 'GET', 'PUT' or 'POST'
        :type method: str
        :param path_params: a list of path params to build the query url from starting after
            the api resource, for example `["project", 119, "featurestores", 67]`.
        :type path_params: list
        :param query_params: A dictionary of key/value pairs to be added as query parameters,
            defaults to None
        :type query_params: dict, optional
        :param headers: Additional header information, defaults to None
        :type headers: dict, optional
        :param data: The payload as a python dictionary to be sent as json, defaults to None
        :type data: dict, optional
        :param stream: Set if response should be a stream, defaults to False
        :type stream: boolean, optional
        :param files: dictionary for multipart encoding upload
        :type files: dict, optional
        :raises RestAPIError: Raised when request wasn't correctly received, understood or accepted
        :return: Response json
        :rtype: dict
        """
        f_url = furl.furl(self._base_url)
        if with_base_path_params:
            base_path_params = ["hopsworks-api", "api"]
            f_url.path.segments = base_path_params + path_params
        else:
            f_url.path.segments = path_params
        url = str(f_url)

        request = requests.Request(
            method,
            url=url,
            headers=headers,
            data=data,
            params=query_params,
            auth=self._auth,
            files=files,
        )

        _logger.debug(f"url:{url} hostname_verification:{self._verify}")

        prepped = self._session.prepare_request(request)
        response = self._session.send(prepped, verify=self._verify, stream=stream)

        if response.status_code == 401 and self.REST_ENDPOINT in os.environ:
            # refresh token and retry request - only on hopsworks
            response = self._retry_token_expired(
                request, stream, self.TOKEN_EXPIRED_RETRY_INTERVAL, 1
            )

        if response.status_code // 100 != 2:
            raise exceptions.RestAPIError(url, response)

        if stream:
            return response
        # handle different success response codes
        if len(response.content) == 0:
            return None
        return response.json()

    def _retry_token_expired(self, request, stream, wait, retries):
        """Refresh the JWT token and retry the request. Only on Hopsworks.

        As the token might take a while to get refreshed. Keep trying.
        """
        # Sleep the waited time before re-issuing the request
        time.sleep(wait)

        self._auth = auth.BearerAuth(self._read_jwt())
        # Update request with the new token
        request.auth = self._auth
        prepped = self._session.prepare_request(request)
        response = self._session.send(prepped, verify=self._verify, stream=stream)

        if response.status_code == 401 and retries < self.TOKEN_EXPIRED_MAX_RETRIES:
            # Try again.
            return self._retry_token_expired(request, stream, wait * 2, retries + 1)
        # If the number of retries have expired, the _send_request method
        # will throw an exception to the user as part of the status_code validation.
        return response

    def _close(self):
        """Closes a client. Can be implemented for clean up purposes, not mandatory."""
        self._connected = False

    @staticmethod
    def _load_jks(path, password):
        """Parse a JKS keystore, returning private keys, cert chains, and trusted certs.

        Compatible with JKS files generated by any JDK version including JDK 21+.

        Returns:
            tuple: (private_keys, trusted_certs) where
                private_keys is a list of (der_key_bytes, cert_chain_der_list)
                trusted_certs is a list of der_cert_bytes
        """
        with open(path, "rb") as f:
            data = f.read()

        _JKS_MAGIC = b"\xfe\xed\xfe\xed"
        if data[:4] != _JKS_MAGIC:
            raise ValueError(
                f"Not a JKS keystore (magic: {data[:4].hex()}, expected: feedfeed)"
            )

        offset = 4
        (version,) = struct.unpack_from(">I", data, offset)
        offset += 4
        (entry_count,) = struct.unpack_from(">I", data, offset)
        offset += 4

        pw_bytes = password.encode("utf-16-be") if isinstance(password, str) else password

        private_keys = []
        trusted_certs = []

        for _ in range(entry_count):
            (tag,) = struct.unpack_from(">I", data, offset)
            offset += 4

            # Read alias (UTF-16 length-prefixed string)
            (alias_len,) = struct.unpack_from(">H", data, offset)
            offset += 2
            offset += alias_len  # skip alias bytes

            # Read timestamp
            offset += 8  # skip 8-byte timestamp

            if tag == 1:  # Private key entry
                (key_len,) = struct.unpack_from(">I", data, offset)
                offset += 4
                encrypted_key = data[offset : offset + key_len]
                offset += key_len

                # Decrypt the Sun JKS proprietary key format
                der_key = Client._decrypt_jks_key(encrypted_key, pw_bytes)

                # Read cert chain
                (chain_len,) = struct.unpack_from(">I", data, offset)
                offset += 4
                cert_chain = []
                for _ in range(chain_len):
                    (cert_type_len,) = struct.unpack_from(">H", data, offset)
                    offset += 2
                    offset += cert_type_len  # skip cert type string
                    (cert_len,) = struct.unpack_from(">I", data, offset)
                    offset += 4
                    cert_der = data[offset : offset + cert_len]
                    offset += cert_len
                    cert_chain.append(cert_der)

                private_keys.append((der_key, cert_chain))

            elif tag == 2:  # Trusted cert entry
                (cert_type_len,) = struct.unpack_from(">H", data, offset)
                offset += 2
                offset += cert_type_len  # skip cert type string
                (cert_len,) = struct.unpack_from(">I", data, offset)
                offset += 4
                cert_der = data[offset : offset + cert_len]
                offset += cert_len
                trusted_certs.append(cert_der)

        return private_keys, trusted_certs

    @staticmethod
    def _parse_asn1_length(data, offset):
        """Parse ASN.1 DER length and return (length, new_offset)."""
        b = data[offset]
        offset += 1
        if b < 0x80:
            return b, offset
        num_bytes = b & 0x7F
        length = int.from_bytes(data[offset : offset + num_bytes], "big")
        return length, offset + num_bytes

    @staticmethod
    def _decrypt_jks_key(encrypted_key, password_bytes):
        """Decrypt a JKS private key using the Sun proprietary algorithm.

        The encrypted_key is an ASN.1 EncryptedPrivateKeyInfo structure:
          SEQUENCE {
            SEQUENCE { OID (Sun JKS key protector) }
            OCTET STRING { salt(20) + encrypted_data + check_hash(20) }
          }
        """
        # Parse outer SEQUENCE
        if encrypted_key[0] != 0x30:
            raise ValueError("Expected ASN.1 SEQUENCE for EncryptedPrivateKeyInfo")
        _, offset = Client._parse_asn1_length(encrypted_key, 1)

        # Skip AlgorithmIdentifier SEQUENCE
        if encrypted_key[offset] != 0x30:
            raise ValueError("Expected ASN.1 SEQUENCE for AlgorithmIdentifier")
        alg_len, alg_offset = Client._parse_asn1_length(encrypted_key, offset + 1)
        offset = alg_offset + alg_len

        # Parse OCTET STRING containing the encrypted payload
        if encrypted_key[offset] != 0x04:
            raise ValueError("Expected ASN.1 OCTET STRING for encrypted data")
        payload_len, offset = Client._parse_asn1_length(encrypted_key, offset + 1)
        payload = encrypted_key[offset : offset + payload_len]

        # Payload format: 20-byte salt + ciphertext + 20-byte check hash
        salt = payload[:20]
        check_hash = payload[-20:]
        cipher_text = payload[20:-20]

        # Generate XOR keystream and decrypt
        plaintext = bytearray(len(cipher_text))
        cur_hash = salt
        for i in range(0, len(cipher_text), 20):
            cur_hash = hashlib.sha1(password_bytes + cur_hash).digest()
            chunk_len = min(20, len(cipher_text) - i)
            for j in range(chunk_len):
                plaintext[i + j] = cipher_text[i + j] ^ cur_hash[j]

        # Verify integrity
        verify = hashlib.sha1(password_bytes + bytes(plaintext)).digest()
        if verify != check_hash:
            raise ValueError("JKS private key integrity check failed (wrong password?)")

        return bytes(plaintext)

    def _write_pem(
        self, keystore_path, keystore_pw, truststore_path, truststore_pw, prefix
    ):
        ks_keys, ks_certs = self._load_jks(keystore_path, keystore_pw)
        _, ts_certs = self._load_jks(truststore_path, truststore_pw)

        ca_chain_path = os.path.join("/tmp", f"{prefix}_ca_chain.pem")
        self._write_ca_chain(ks_certs, ts_certs, ca_chain_path)

        client_cert_path = os.path.join("/tmp", f"{prefix}_client_cert.pem")
        self._write_client_cert(ks_keys, client_cert_path)

        client_key_path = os.path.join("/tmp", f"{prefix}_client_key.pem")
        self._write_client_key(ks_keys, client_key_path)

        return ca_chain_path, client_cert_path, client_key_path

    @staticmethod
    def _der_to_pem(der_bytes, pem_type="CERTIFICATE"):
        """Convert DER-encoded bytes to PEM string."""
        b64 = base64.b64encode(der_bytes).decode("ascii")
        lines = [b64[i : i + 64] for i in range(0, len(b64), 64)]
        return (
            f"-----BEGIN {pem_type}-----\n"
            + "\n".join(lines)
            + f"\n-----END {pem_type}-----\n"
        )

    def _write_ca_chain(self, ks_certs, ts_certs, ca_chain_path):
        """Writes CA chain PEM from keystore and truststore certificates."""
        ca_chain = ""
        for cert_der in ks_certs + ts_certs:
            ca_chain += self._der_to_pem(cert_der)

        with Path(ca_chain_path).open("w") as f:
            f.write(ca_chain)

    def _write_client_cert(self, ks_keys, client_cert_path):
        """Writes client certificate PEM from keystore private key entries."""
        client_cert = ""
        for _, cert_chain in ks_keys:
            for cert_der in cert_chain:
                client_cert += self._der_to_pem(cert_der)

        with Path(client_cert_path).open("w") as f:
            f.write(client_cert)

    def _write_client_key(self, ks_keys, client_key_path):
        """Writes client private key PEM from keystore."""
        client_key = ""
        for key_der, _ in ks_keys:
            key = load_der_private_key(key_der, password=None)
            client_key += key.private_bytes(
                Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
            ).decode()

        with Path(client_key_path).open("w") as f:
            f.write(client_key)
