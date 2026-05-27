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
from hsfs.online_config import OnlineConfig


class TestOnlineConfig:
    def test_defaults(self):
        config = OnlineConfig()

        assert config.online_comments is None
        assert config.table_space is None
        assert config.primary_key_index_type is None

    def test_primary_key_index_type_hash(self):
        config = OnlineConfig(primary_key_index_type="HASH")

        assert config.primary_key_index_type == "HASH"

    def test_primary_key_index_type_ordered(self):
        config = OnlineConfig(primary_key_index_type="ORDERED")

        assert config.primary_key_index_type == "ORDERED"

    def test_primary_key_index_type_case_normalization(self):
        assert (
            OnlineConfig(primary_key_index_type="hash").primary_key_index_type == "HASH"
        )
        assert (
            OnlineConfig(primary_key_index_type="Ordered").primary_key_index_type
            == "ORDERED"
        )

    def test_primary_key_index_type_invalid_rejected(self):
        with pytest.raises(ValueError, match="Invalid primary_key_index_type"):
            OnlineConfig(primary_key_index_type="BTREE")

    def test_primary_key_index_type_non_string_rejected(self):
        with pytest.raises(TypeError, match="must be a string or None"):
            OnlineConfig(primary_key_index_type=42)

    def test_primary_key_index_type_setter_validates(self):
        config = OnlineConfig()

        config.primary_key_index_type = "HASH"
        assert config.primary_key_index_type == "HASH"

        with pytest.raises(ValueError):
            config.primary_key_index_type = "bogus"

    def test_to_dict_includes_primary_key_index_type(self):
        config = OnlineConfig(
            online_comments=["c1"], table_space="ts", primary_key_index_type="HASH"
        )

        assert config.to_dict() == {
            "onlineComments": ["c1"],
            "tableSpace": "ts",
            "primaryKeyIndexType": "HASH",
        }

    def test_to_dict_primary_key_index_type_unset(self):
        config = OnlineConfig(online_comments=["c1"], table_space="ts")

        assert config.to_dict() == {
            "onlineComments": ["c1"],
            "tableSpace": "ts",
            "primaryKeyIndexType": None,
        }

    def test_from_response_json_round_trip(self):
        payload = {
            "onlineComments": ["NDB_TABLE=READ_BACKUP=1"],
            "tableSpace": "ts_1",
            "primaryKeyIndexType": "ORDERED",
        }

        config = OnlineConfig.from_response_json(payload)

        assert config.online_comments == ["NDB_TABLE=READ_BACKUP=1"]
        assert config.table_space == "ts_1"
        assert config.primary_key_index_type == "ORDERED"

    def test_from_response_json_missing_primary_key_index_type(self):
        # Server may omit primaryKeyIndexType for legacy / shared feature groups.
        payload = {"onlineComments": ["x"], "tableSpace": "ts"}

        config = OnlineConfig.from_response_json(payload)

        assert config.primary_key_index_type is None

    def test_from_response_json_none(self):
        assert OnlineConfig.from_response_json(None) is None

    # secondary_indexes

    def test_secondary_indexes_default(self):
        assert OnlineConfig().secondary_indexes is None

    def test_secondary_indexes_single(self):
        config = OnlineConfig(secondary_indexes=[["user_id"]])

        assert config.secondary_indexes == [["user_id"]]

    def test_secondary_indexes_composite(self):
        config = OnlineConfig(secondary_indexes=[["country", "city"]])

        assert config.secondary_indexes == [["country", "city"]]

    def test_secondary_indexes_multiple(self):
        config = OnlineConfig(secondary_indexes=[["user_id"], ["country", "city"]])

        assert config.secondary_indexes == [["user_id"], ["country", "city"]]

    def test_secondary_indexes_not_list_rejected(self):
        with pytest.raises(TypeError, match="secondary_indexes must be a list"):
            OnlineConfig(secondary_indexes="user_id")

    def test_secondary_indexes_empty_inner_list_rejected(self):
        with pytest.raises(
            ValueError, match=r"secondary_indexes\[0\] must be a non-empty list"
        ):
            OnlineConfig(secondary_indexes=[[]])

    def test_secondary_indexes_empty_column_name_rejected(self):
        with pytest.raises(ValueError, match="invalid column name"):
            OnlineConfig(secondary_indexes=[["user_id", ""]])

    def test_secondary_indexes_non_string_column_rejected(self):
        with pytest.raises(ValueError, match="invalid column name"):
            OnlineConfig(secondary_indexes=[[42]])

    def test_secondary_indexes_setter_validates(self):
        config = OnlineConfig()

        config.secondary_indexes = [["user_id"]]
        assert config.secondary_indexes == [["user_id"]]

        with pytest.raises(TypeError):
            config.secondary_indexes = "user_id"

    def test_to_dict_includes_secondary_indexes(self):
        config = OnlineConfig(secondary_indexes=[["user_id"], ["country", "city"]])

        d = config.to_dict()

        assert d["secondaryIndexes"] == [["user_id"], ["country", "city"]]

    def test_to_dict_secondary_indexes_unset(self):
        assert "secondaryIndexes" not in OnlineConfig().to_dict()

    def test_from_response_json_secondary_indexes_round_trip(self):
        payload = {
            "onlineComments": ["NDB_TABLE=READ_BACKUP=1"],
            "tableSpace": "ts_1",
            "primaryKeyIndexType": "ORDERED",
            "secondaryIndexes": [["user_id"], ["country", "city"]],
        }

        config = OnlineConfig.from_response_json(payload)

        assert config.secondary_indexes == [["user_id"], ["country", "city"]]

    def test_from_response_json_missing_secondary_indexes(self):
        config = OnlineConfig.from_response_json({"onlineComments": ["x"]})

        assert config.secondary_indexes is None
