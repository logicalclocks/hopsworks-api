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

import json
from io import BytesIO

import fastavro
import pandas as pd
import pytest
from hsfs import feature_group
from hsfs.core import kafka_engine
from hsfs.core.feature_group_engine import FeatureGroupEngine
from hsfs.engine import python


AVRO_SCHEMA = (
    '{"type":"record","name":"test_fg","namespace":"test_featurestore.db","fields":'
    '[{"name":"id","type":["null","long"]},'
    '{"name":"state","type":["null","string"]},'
    '{"name":"measurement","type":["null","double"]}]}'
)


class TestOnlineDeleteFillValues:
    def test_fills_non_primary_key_fields_with_null(self, mocker):
        fg = mocker.Mock()
        fg.primary_key = ["id"]
        fg.avro_schema = AVRO_SCHEMA

        assert kafka_engine._online_delete_fill_values(fg) == {
            "state": None,
            "measurement": None,
        }

    def test_composite_primary_key_excluded(self, mocker):
        fg = mocker.Mock()
        fg.primary_key = ["id", "state"]
        fg.avro_schema = AVRO_SCHEMA

        assert kafka_engine._online_delete_fill_values(fg) == {"measurement": None}


class TestCommitDeleteRecordStreamGate:
    def test_online_delete_skipped_for_stream_fg(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            id=10,
            stream=True,
            online_enabled=True,
            time_travel_format="DELTA",
        )
        fg.primary_key = ["id"]

        with pytest.warns(UserWarning, match="stream feature groups"):
            fg.commit_delete_record(pd.DataFrame({"id": [2]}), delete_online=True)

        online.assert_not_called()


class TestDeleteDataframeKafka:
    def _make_feature_group(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.core.kafka_engine._get_kafka_config", return_value={})
        mocker.patch("hsfs.core.kafka_engine.Producer")
        # bypass the online-ingestion round trip in header setup; the delete leg
        # adds the operation/storage headers itself.
        mocker.patch("hsfs.core.kafka_engine._get_headers", return_value={})
        mocker.patch(
            "hsfs.feature_group.FeatureGroup.get_complex_features", return_value=[]
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            id=10,
            stream=False,
        )
        # id=10 makes the constructor derive the key from (absent) features, so
        # set it explicitly to model a backend-initialized online feature group.
        fg.primary_key = ["id"]
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234
        fg._subject = {"id": 1, "schema": AVRO_SCHEMA}
        fg._online_topic_name = "test_topic"
        return fg

    def test_primary_key_only_dataframe_serializes_with_null_fields(self, mocker):
        produced = {}

        def fake_produce(**kwargs):
            produced.update(kwargs)

        mocker.patch("hsfs.core.kafka_engine._kafka_produce", side_effect=fake_produce)
        fg = self._make_feature_group(mocker)

        python.Engine()._delete_dataframe_kafka(fg, pd.DataFrame({"id": [7]}), {})

        assert produced["key"] == "7"
        assert produced["headers"]["operation"] == b"delete"
        assert produced["headers"]["storage"] == b"1"

        with BytesIO(produced["encoded_row"]) as outf:
            record = fastavro.schemaless_reader(
                outf, fastavro.parse_schema(json.loads(AVRO_SCHEMA))
            )
        assert record == {"id": 7, "state": None, "measurement": None}

    def test_extra_columns_override_null_fill(self, mocker):
        produced = {}
        mocker.patch(
            "hsfs.core.kafka_engine._kafka_produce",
            side_effect=lambda **kwargs: produced.update(kwargs),
        )
        fg = self._make_feature_group(mocker)

        python.Engine()._delete_dataframe_kafka(
            fg, pd.DataFrame({"id": [7], "state": ["nevada"]}), {}
        )

        with BytesIO(produced["encoded_row"]) as outf:
            record = fastavro.schemaless_reader(
                outf, fastavro.parse_schema(json.loads(AVRO_SCHEMA))
            )
        assert record == {"id": 7, "state": "nevada", "measurement": None}
