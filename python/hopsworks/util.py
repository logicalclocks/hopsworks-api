#
#   Copyright 2022 Logical Clocks AB
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

from json import JSONEncoder
from hopsworks.client.exceptions import JobException
from hopsworks.git_file_status import GitFileStatus
from hopsworks import constants

import jks
import base64
import textwrap
from pathlib import Path


class Encoder(JSONEncoder):
    def default(self, obj):
        try:
            return obj.to_dict()
        except AttributeError:
            return super().default(obj)


def convert_to_abs(path, current_proj_name):
    abs_project_prefix = "/Projects/{}".format(current_proj_name)
    if not path.startswith(abs_project_prefix):
        return abs_project_prefix + "/" + path
    else:
        return path


def validate_job_conf(config, project_name):
    # User is required to set the appPath programmatically after getting the configuration
    if config["type"] != "dockerJobConfiguration" and "appPath" not in config:
        raise JobException("'appPath' not set in job configuration")
    elif "appPath" in config and not config["appPath"].startswith("hdfs://"):
        config["appPath"] = "hdfs://" + convert_to_abs(config["appPath"], project_name)

    # If PYSPARK application set the mainClass, if SPARK validate there is a mainClass set
    if config["type"] == "sparkJobConfiguration":
        if config["appPath"].endswith(".py"):
            config["mainClass"] = "org.apache.spark.deploy.PythonRunner"
        elif "mainClass" not in config:
            raise JobException("'mainClass' not set in job configuration")

    return config


def convert_git_status_to_files(files):
    # Convert GitFileStatus to list of file paths
    if isinstance(files[0], GitFileStatus):
        tmp_files = []
        for file_status in files:
            tmp_files.append(file_status.file)
        files = tmp_files

    return files


def _get_ca_chain_location():
    """
    Get location of chain of CA certificates (PEM format) that are required to validate the
    private key certificate of the client
    used for 2-way TLS authentication, for example with Kafka cluster
    Returns:
         string path to ca chain of certificate
    """
    ca_chain_path = Path(constants.SSL_CONFIG.PEM_CA_CHAIN_CERTIFICATE_CONFIG)
    if not ca_chain_path.exists():
        _write_pems()
    return str(ca_chain_path)

def _get_client_certificate_location():
    """
    Get location of client certificate (PEM format) for the private key signed by trusted CA
    used for 2-way TLS authentication, for example with Kafka cluster
    Returns:
        string path to client certificate in PEM format
    """
    certificate_path = Path(constants.SSL_CONFIG.PEM_CLIENT_CERTIFICATE_CONFIG)
    if not certificate_path.exists():
        _write_pems()
    return str(certificate_path)

def _get_client_key_location():
    """
    Get location of client private key (PEM format)
    used for for 2-way TLS authentication, for example with Kafka cluster
    Returns:
        string path to client private key in PEM format
    """
    # Convert JKS to PEMs if they don't exists already
    key_path = Path(constants.SSL_CONFIG.PEM_CLIENT_KEY_CONFIG)
    if not key_path.exists():
        _write_pems()
    return str(key_path)

def _write_pems():
    """
    Converts JKS keystore file into PEM to be compatible with Python libraries
    """
    t_jks_path = _get_trust_store()
    k_jks_path = _get_key_store()

    client_certificate_path = Path(constants.SSL_CONFIG.PEM_CLIENT_CERTIFICATE_CONFIG)
    client_key_path = Path(constants.SSL_CONFIG.PEM_CLIENT_KEY_CONFIG)
    ca_chain_path = Path(constants.SSL_CONFIG.PEM_CA_CHAIN_CERTIFICATE_CONFIG)

    _write_pem(
        k_jks_path,
        t_jks_path,
        _get_key_store_pwd(),
        client_certificate_path,
        client_key_path,
        ca_chain_path,
    )

def _write_pem(
        jks_key_store_path,
        jks_trust_store_path,
        keystore_pw,
        client_key_cert_path,
        client_key_path,
        ca_cert_path,
):
    """
    Converts the JKS keystore, JKS truststore, and the root ca.pem
    client certificate, client key, and ca certificate
    Args:
    :jks_key_store_path: path to the JKS keystore
    :jks_trust_store_path: path to the JKS truststore
    :keystore_pw: path to file with passphrase for the keystores
    :client_key_cert_path: path to write the client's certificate for its private key in PEM format
    :client_key_path: path to write the client's private key in PEM format
    :ca_cert_path: path to write the chain of CA certificates required to validate certificates
    """
    keystore_key_cert, keystore_key, keystore_ca_cert = _convert_jks_to_pem(
        jks_key_store_path, keystore_pw
    )
    (
        truststore_key_cert,
        truststore_key,
        truststore_ca_cert,
    ) = _convert_jks_to_pem(jks_trust_store_path, keystore_pw)
    with client_key_cert_path.open("w") as f:
        f.write(keystore_key_cert)
    with client_key_path.open("w") as f:
        f.write(keystore_key)
    with ca_cert_path.open("w") as f:
        f.write(keystore_ca_cert + truststore_ca_cert)

def _convert_jks_to_pem(jks_path, keystore_pw):
    """
    Converts a keystore JKS that contains client private key,
     client certificate and CA certificate that was used to
     sign the certificate, to three PEM-format strings.
    Args:
    :jks_path: path to the JKS file
    :pw: password for decrypting the JKS file
    Returns:
         strings: (client_cert, client_key, ca_cert)
    """
    # load the keystore and decrypt it with password
    ks = jks.KeyStore.load(jks_path, keystore_pw, try_decrypt_keys=True)
    private_keys_certs = ""
    private_keys = ""
    ca_certs = ""

    # Convert private keys and their certificates into PEM format and append to string
    for alias, pk in ks.private_keys.items():
        if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
            private_keys = private_keys + _bytes_to_pem_str(
                pk.pkey, "RSA PRIVATE KEY"
            )
        else:
            private_keys = private_keys + _bytes_to_pem_str(
                pk.pkey_pkcs8, "PRIVATE KEY"
            )
        for c in pk.cert_chain:
            # c[0] contains type of cert, i.e X.509
            private_keys_certs = private_keys_certs + _bytes_to_pem_str(
                c[1], "CERTIFICATE"
            )

    # Convert CA Certificates into PEM format and append to string
    for alias, c in ks.certs.items():
        ca_certs = ca_certs + _bytes_to_pem_str(c.cert, "CERTIFICATE")
    return private_keys_certs, private_keys, ca_certs

def _bytes_to_pem_str(der_bytes, pem_type):
    """
    Utility function for creating PEM files
    Args:
        der_bytes: DER encoded bytes
        pem_type: type of PEM, e.g Certificate, Private key, or RSA private key
    Returns:
        PEM String for a DER-encoded certificate or private key
    """
    pem_str = ""
    pem_str = pem_str + "-----BEGIN {}-----".format(pem_type) + "\n"
    pem_str = (
            pem_str
            + "\r\n".join(
        textwrap.wrap(base64.b64encode(der_bytes).decode("ascii"), 64)
    )
            + "\n"
    )
    pem_str = pem_str + "-----END {}-----".format(pem_type) + "\n"
    return pem_str

def _get_key_store_path():
    """
    Get keystore path
    Returns:
        keystore path
    """
    k_certificate = Path(constants.SSL_CONFIG.K_CERTIFICATE_CONFIG)
    if k_certificate.exists():
        return k_certificate
    else:
        username = os.environ["HADOOP_USER_NAME"]
        material_directory = Path(os.environ["MATERIAL_DIRECTORY"])
        return material_directory.joinpath(username + constants.SSL_CONFIG.KEYSTORE_SUFFIX)

def _get_key_store():
    return str(_get_key_store_path())

def _get_trust_store_path():
    """
    Get truststore location
    Returns:
         truststore location
    """
    t_certificate = Path(constants.SSL_CONFIG.T_CERTIFICATE_CONFIG)
    if t_certificate.exists():
        return str(t_certificate)
    else:
        username = os.environ["HADOOP_USER_NAME"]
        material_directory = Path(os.environ["MATERIAL_DIRECTORY"])
        return str(material_directory.joinpath(username + constants.SSL_CONFIG.TRUSTSTORE_SUFFIX))

def _get_trust_store():
    return str(_get_trust_store_path())

def _get_trust_store_pwd():
    """
    Get truststore password
    Returns:
         truststore password
    """
    return _get_cert_pw()

def _get_cert_pw():
    """
    Get keystore password from local container
    Returns:
        Certificate password
    """
    pwd_path = Path(constants.SSL_CONFIG.CRYPTO_MATERIAL_PASSWORD)
    if not pwd_path.exists():
        username = os.environ["HADOOP_USER_NAME"]
        material_directory = Path(os.environ["MATERIAL_DIRECTORY"])
        pwd_path = material_directory.joinpath(username + constants.SSL_CONFIG.PASSWORD_SUFFIX)

    with pwd_path.open() as f:
        return f.read()

def _get_key_store_pwd():
    """
    Get keystore password
    Returns:
         keystore password
    """
    return _get_cert_pw()

def parse_avro_msg(msg: bytes, avro_schema: avro.schema.RecordSchema):
    """
    Parses an avro record using a specified avro schema

    # Arguments
        msg: the avro message to parse
        avro_schema: the avro schema

    # Returns:
         The parsed/decoded message
    """

    reader = DatumReader(avro_schema)
    message_bytes = BytesIO(msg)
    decoder = BinaryDecoder(message_bytes)
    return reader.read(decoder)

def convert_json_schema_to_avro(json_schema):
    """Parses a JSON kafka topic schema into an avro schema

    # Arguments
        json_schema: the json schema to convert
    # Returns
        `avro.schema.RecordSchema`: The Avro record schema
    """
    return avro.schema.parse(json_schema)

