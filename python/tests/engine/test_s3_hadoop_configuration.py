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
from unittest.mock import MagicMock

import pytest
from hsfs.engine.spark import Engine
from hsfs.storage_connector import GlueConnector, S3Connector


@pytest.mark.parametrize("connect", [False, True])
@pytest.mark.parametrize("connector_class", [S3Connector, GlueConnector])
@pytest.mark.parametrize("existing_impl", [None, "example.CustomS3FileSystem"])
def test_s3_metadata_locations_resolve_without_replacing_custom_filesystem(
    connect, connector_class, existing_impl
):
    engine = Engine.__new__(Engine)
    engine._is_connect = connect
    engine._spark_session = MagicMock()
    engine._spark_context = MagicMock()
    values = {}
    prefix = "spark.hadoop." if connect else ""
    if existing_impl:
        values[prefix + "fs.s3.impl"] = existing_impl
    spark_conf = engine._spark_session.conf
    spark_conf.get.side_effect = values.get
    spark_conf.set.side_effect = values.__setitem__
    hadoop_conf = engine._spark_context._jsc.hadoopConfiguration.return_value
    hadoop_conf.set.side_effect = values.__setitem__
    hadoop_conf.setIfUnset.side_effect = values.setdefault
    connector = connector_class(
        id=1,
        name="s3_metadata",
        featurestore_id=1,
        bucket="existing-tables",
        access_key="test-key",
        secret_key="test-secret",
        arguments=[
            {
                "name": "fs.s3a.global-conf",
                "value": "False" if connector_class is S3Connector else "True",
            }
        ],
    )

    path = engine._setup_storage_connector(connector, "s3://existing-tables/table")

    assert path == "s3a://existing-tables/table"
    assert values[prefix + "fs.s3.impl"] == (
        existing_impl or "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    if connector_class is S3Connector:
        assert values[prefix + "fs.s3a.bucket.existing-tables.access.key"] == "test-key"
        assert prefix + "fs.s3a.access.key" not in values
    else:
        # Glue obtains the bucket from each table rather than from the connector.
        assert values[prefix + "fs.s3a.access.key"] == "test-key"


def test_s3_preparation_without_a_location_also_configures_metadata_reads():
    engine = Engine.__new__(Engine)
    engine._is_connect = False
    engine._spark_context = MagicMock()
    connector = S3Connector(id=1, name="s3_metadata", featurestore_id=1)

    assert engine._setup_storage_connector(connector) is None
    engine._spark_context._jsc.hadoopConfiguration.return_value.setIfUnset.assert_called_once_with(
        "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
