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

import datetime
import hashlib
import struct

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from hopsworks_common.client.base import Client


# region JKS builders (mirror of the Sun JKS format that Client._load_jks parses)
def _make_key_and_cert():
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2020, 1, 1))
        .not_valid_after(datetime.datetime(2040, 1, 1))
        .sign(key, hashes.SHA256())
    )
    key_der = key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    cert_der = cert.public_bytes(serialization.Encoding.DER)
    return key_der, cert_der


def _der_len(n):
    if n < 0x80:
        return bytes([n])
    body = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(body)]) + body


def _encrypt_jks_key(plaintext, pw_bytes, salt=b"\x01" * 20):
    # Inverse of Client._decrypt_jks_key: the same SHA-1 XOR keystream, and the
    # trailing check hash the parser verifies against.
    cipher = bytearray(len(plaintext))
    cur_hash = salt
    for i in range(0, len(plaintext), 20):
        cur_hash = hashlib.sha1(pw_bytes + cur_hash).digest()
        for j in range(min(20, len(plaintext) - i)):
            cipher[i + j] = plaintext[i + j] ^ cur_hash[j]
    check_hash = hashlib.sha1(pw_bytes + plaintext).digest()
    return salt + bytes(cipher) + check_hash


def _wrap_encrypted_key(payload):
    # EncryptedPrivateKeyInfo: SEQUENCE { AlgorithmIdentifier, OCTET STRING }.
    alg_id = bytes.fromhex("300c060a2b060104012a02110101")  # Sun JKS key protector
    octet = b"\x04" + _der_len(len(payload)) + payload
    inner = alg_id + octet
    return b"\x30" + _der_len(len(inner)) + inner


def _build_jks(password, private_entries=(), trusted_entries=(), version=2):
    pw_bytes = password.encode("utf-16-be")
    out = b"\xfe\xed\xfe\xed"
    out += struct.pack(">I", version)
    out += struct.pack(">I", len(private_entries) + len(trusted_entries))
    for alias, key_der, cert_chain in private_entries:
        out += struct.pack(">I", 1)
        out += struct.pack(">H", len(alias)) + alias.encode("utf-8")
        out += b"\x00" * 8  # timestamp
        enc = _wrap_encrypted_key(_encrypt_jks_key(key_der, pw_bytes))
        out += struct.pack(">I", len(enc)) + enc
        out += struct.pack(">I", len(cert_chain))
        for cert_der in cert_chain:
            out += struct.pack(">H", len(b"X.509")) + b"X.509"
            out += struct.pack(">I", len(cert_der)) + cert_der
    for alias, cert_der in trusted_entries:
        out += struct.pack(">I", 2)
        out += struct.pack(">H", len(alias)) + alias.encode("utf-8")
        out += b"\x00" * 8  # timestamp
        out += struct.pack(">H", len(b"X.509")) + b"X.509"
        out += struct.pack(">I", len(cert_der)) + cert_der
    out += hashlib.sha1(pw_bytes + b"Mighty Aphrodite" + out).digest()
    return out


# endregion


class TestLoadJks:
    def test_keystore_roundtrip(self, tmp_path):
        key_der, cert_der = _make_key_and_cert()
        path = tmp_path / "ks.jks"
        path.write_bytes(
            _build_jks("pw", private_entries=[("mykey", key_der, [cert_der])])
        )

        keys, certs = Client._load_jks(str(path), "pw")

        assert certs == []
        assert len(keys) == 1
        parsed_key, chain = keys[0]
        assert parsed_key == key_der
        assert chain == [cert_der]

    def test_truststore_only(self, tmp_path):
        _, cert_der = _make_key_and_cert()
        path = tmp_path / "ts.jks"
        path.write_bytes(_build_jks("pw", trusted_entries=[("ca", cert_der)]))

        keys, certs = Client._load_jks(str(path), "pw")

        assert keys == []
        assert certs == [cert_der]

    def test_wrong_password_keystore(self, tmp_path):
        key_der, cert_der = _make_key_and_cert()
        path = tmp_path / "ks.jks"
        path.write_bytes(
            _build_jks("right", private_entries=[("mykey", key_der, [cert_der])])
        )

        with pytest.raises(ValueError, match="integrity check failed"):
            Client._load_jks(str(path), "wrong")

    def test_wrong_password_truststore(self, tmp_path):
        # The keystore-level digest is what catches a wrong password on a
        # truststore that has no private key to decrypt.
        _, cert_der = _make_key_and_cert()
        path = tmp_path / "ts.jks"
        path.write_bytes(_build_jks("right", trusted_entries=[("ca", cert_der)]))

        with pytest.raises(ValueError, match="integrity check failed"):
            Client._load_jks(str(path), "wrong")

    def test_bad_magic(self, tmp_path):
        path = tmp_path / "bad.jks"
        path.write_bytes(b"\x00\x00\x00\x00rest-of-file")

        with pytest.raises(ValueError, match="Not a JKS keystore"):
            Client._load_jks(str(path), "pw")

    def test_unsupported_version(self, tmp_path):
        pw_bytes = "pw".encode("utf-16-be")
        body = b"\xfe\xed\xfe\xed" + struct.pack(">I", 99) + struct.pack(">I", 0)
        body += hashlib.sha1(pw_bytes + b"Mighty Aphrodite" + body).digest()
        path = tmp_path / "v99.jks"
        path.write_bytes(body)

        with pytest.raises(ValueError, match="Unsupported JKS version"):
            Client._load_jks(str(path), "pw")

    def test_unsupported_entry_tag(self, tmp_path):
        # Tag 3 is a JCEKS secret-key entry, which this parser does not handle.
        pw_bytes = "pw".encode("utf-16-be")
        body = b"\xfe\xed\xfe\xed" + struct.pack(">I", 2) + struct.pack(">I", 1)
        body += struct.pack(">I", 3)  # unsupported tag
        body += struct.pack(">H", 2) + b"id"
        body += b"\x00" * 8  # timestamp
        body += hashlib.sha1(pw_bytes + b"Mighty Aphrodite" + body).digest()
        path = tmp_path / "jceks.jks"
        path.write_bytes(body)

        with pytest.raises(ValueError, match="Unsupported JKS entry tag"):
            Client._load_jks(str(path), "pw")
