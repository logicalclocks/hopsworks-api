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
import warnings
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


def _stream_online_fg(mocker):
    mocker.patch("hopsworks_common.client._get_instance")
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
    return fg


class TestRemoveRowsStreamFeatureGroup:
    def test_online_delete_by_default(self, mocker):
        # remove_rows deletes from both stores by default, mirroring insert.
        mocker.patch("hsfs.engine._get_type", return_value="python")
        offline = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        fg = _stream_online_fg(mocker)

        fg.remove_rows(pd.DataFrame({"id": [2]}))

        offline.assert_called_once()
        online.assert_called_once()

    def test_offline_delete_only_when_storage_is_offline(self, mocker):
        offline = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        fg = _stream_online_fg(mocker)

        fg.remove_rows(pd.DataFrame({"id": [2]}), storage="offline")

        offline.assert_called_once()
        online.assert_not_called()

    def test_deprecated_commit_delete_record_stays_offline_only(self, mocker):
        # The deprecated alias keeps the offline-only behaviour it had before the online
        # delete existed, so an existing caller does not start deleting online rows.
        offline = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        fg = _stream_online_fg(mocker)

        fg.commit_delete_record(pd.DataFrame({"id": [2]}))

        offline.assert_called_once()
        online.assert_not_called()


def _offline_only_fg(mocker):
    mocker.patch("hopsworks_common.client._get_instance")
    fg = feature_group.FeatureGroup(
        name="test",
        version=1,
        featurestore_id=99,
        primary_key=["id"],
        partition_key=[],
        id=10,
        stream=False,
        online_enabled=False,
        time_travel_format="DELTA",
    )
    fg.primary_key = ["id"]
    return fg


class TestRemoveRowsValidation:
    def test_online_only_storage_skips_the_offline_commit(self, mocker):
        # storage="online" keeps insert's meaning of online alone, so the offline table is
        # left holding the rows and only the tombstone is produced.
        mocker.patch("hsfs.engine._get_type", return_value="python")
        offline = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        fg = _stream_online_fg(mocker)

        fg.remove_rows(pd.DataFrame({"id": [2]}), storage="online")

        offline.assert_not_called()
        online.assert_called_once()

    def test_online_only_storage_on_non_online_fg_is_rejected(self, mocker):
        # Online-only on a feature group with no online store would delete nothing at all,
        # so it raises rather than silently doing nothing.
        from hopsworks_common.client.exceptions import FeatureStoreException

        commit = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        fg = _offline_only_fg(mocker)

        with pytest.raises(FeatureStoreException, match="not online-enabled"):
            fg.remove_rows(pd.DataFrame({"id": [2]}), storage="online")

        commit.assert_not_called()

    def test_online_only_storage_on_embedding_fg_is_rejected(self, mocker):
        # Same reasoning: the vector database is not deletable here, and with the offline
        # leg skipped the call would be a complete no-op.
        from hopsworks_common.client.exceptions import FeatureStoreException

        commit = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        fg = _stream_online_fg(mocker)
        fg._embedding_index = mocker.Mock()

        with pytest.raises(FeatureStoreException, match="embedding index"):
            fg.remove_rows(pd.DataFrame({"id": [2]}), storage="online")

        commit.assert_not_called()

    def test_online_only_storage_skips_the_offline_engine_guard(self, mocker):
        # The HUDI guard is a property of the offline delete, which an online-only delete
        # does not run, so it must not block one on the Python engine.
        mocker.patch("hsfs.engine._get_type", return_value="python")
        offline = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        fg = _stream_online_fg(mocker)
        fg.time_travel_format = "HUDI"

        fg.remove_rows(pd.DataFrame({"id": [2]}), storage="online")

        offline.assert_not_called()
        online.assert_called_once()

    def test_unknown_storage_is_rejected_before_offline_commit(self, mocker):
        from hopsworks_common.client.exceptions import FeatureStoreException

        commit = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        fg = _stream_online_fg(mocker)

        with pytest.raises(FeatureStoreException, match="Invalid storage"):
            fg.remove_rows(pd.DataFrame({"id": [2]}), storage="both")

        commit.assert_not_called()

    def test_unset_storage_deletes_offline_only_on_non_online_fg(self, mocker):
        # Unset storage follows the feature group, so a plain remove_rows must work on a
        # feature group that has no online store rather than raising.
        offline = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        fg = _offline_only_fg(mocker)

        fg.remove_rows(pd.DataFrame({"id": [2]}))

        offline.assert_called_once()
        online.assert_not_called()

    def test_embedding_fg_skips_the_online_leg_and_warns(self, mocker):
        # The embedding index puts the online data in the vector database, which this release
        # does not delete from. The online leg is skipped rather than failing the whole call,
        # and the caller is warned that the two stores are left diverged.
        offline = mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        fg = _stream_online_fg(mocker)
        fg._embedding_index = mocker.Mock()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            fg.remove_rows(pd.DataFrame({"id": [2]}))

        assert any("vector database" in str(w.message) for w in caught)
        offline.assert_called_once()
        online.assert_not_called()


class TestCommitDeleteRecordDeprecated:
    def test_commit_delete_record_warns_and_delegates(self, mocker):
        mocker.patch.object(FeatureGroupEngine, "_commit_delete")
        online = mocker.patch.object(FeatureGroupEngine, "_delete_online_records")
        remove_rows = mocker.spy(feature_group.FeatureGroup, "remove_rows")
        fg = _stream_online_fg(mocker)
        delete_df = pd.DataFrame({"id": [2]})

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            fg.commit_delete_record(delete_df)

        # the deprecation warning fired
        assert any(
            "commit_delete_record is deprecated" in str(w.message) for w in caught
        )
        # and it delegated to remove_rows, which deleted offline only: this alias exists for
        # callers written before the online delete and must never reach the online store
        remove_rows.assert_called_once()
        online.assert_not_called()


class TestDeleteDataframeKafka:
    def _make_feature_group(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.core.kafka_engine._get_kafka_config", return_value={})
        mocker.patch("hsfs.core.kafka_engine.Producer")
        # bypass the online-ingestion round trip in header setup; the delete leg
        # adds the operation header itself.
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
        # No storage header. It gates the online leg alone ("0" makes OnlineFS skip the
        # row), so it cannot mark a record online-only, and absent already ingests.
        assert "storage" not in produced["headers"]

        with BytesIO(produced["encoded_row"]) as outf:
            record = fastavro.schemaless_reader(
                outf, fastavro.parse_schema(json.loads(AVRO_SCHEMA))
            )
        assert record == {"id": 7, "state": None, "measurement": None}

    def test_extra_non_key_columns_are_ignored(self, mocker):
        produced = {}
        mocker.patch(
            "hsfs.core.kafka_engine._kafka_produce",
            side_effect=lambda **kwargs: produced.update(kwargs),
        )
        fg = self._make_feature_group(mocker)

        python.Engine()._delete_dataframe_kafka(
            fg, pd.DataFrame({"id": [7], "state": ["nevada"]}), {}
        )

        # The online delete is by primary key, so a non-key column the caller passes
        # is ignored and serialized as null rather than overriding the tombstone fill.
        with BytesIO(produced["encoded_row"]) as outf:
            record = fastavro.schemaless_reader(
                outf, fastavro.parse_schema(json.loads(AVRO_SCHEMA))
            )
        assert record == {"id": 7, "state": None, "measurement": None}

    def test_entry_count_is_sent_by_default(self, mocker):
        fg = self._make_feature_group(mocker)
        mocker.patch("hsfs.core.kafka_engine._kafka_produce")
        get_headers = mocker.patch(
            "hsfs.core.kafka_engine._get_headers", return_value={}
        )

        python.Engine()._delete_dataframe_kafka(fg, pd.DataFrame({"id": [7, 8]}), {})

        assert get_headers.call_args[0][1] == 2

    def test_disable_online_ingestion_count_skips_the_entry_count(self, mocker):
        fg = self._make_feature_group(mocker)
        mocker.patch("hsfs.core.kafka_engine._kafka_produce")
        get_headers = mocker.patch(
            "hsfs.core.kafka_engine._get_headers", return_value={}
        )

        python.Engine()._delete_dataframe_kafka(
            fg,
            pd.DataFrame({"id": [7, 8]}),
            {"online_ingestion_options": {"disable_online_ingestion_count": True}},
        )

        assert get_headers.call_args[0][1] is None
