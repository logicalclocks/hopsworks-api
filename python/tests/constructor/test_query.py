#
#   Copyright 2022 Hopsworks AB
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
import logging
import warnings
from datetime import timedelta

import pytest
from hsfs import feature, feature_group
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor import filter, join, query
from hsfs.constructor.fs_query import FsQuery
from hsfs.engine import spark


class TestQuery:
    fg1 = feature_group.FeatureGroup(
        name="test1",
        version=1,
        featurestore_id=99,
        primary_key=[],
        partition_key=[],
        features=[
            feature.Feature("id", feature_group_id=11),
            feature.Feature("label", feature_group_id=11),
            feature.Feature("tf_name", feature_group_id=11),
        ],
        id=11,
        stream=False,
    )

    fg2 = feature_group.FeatureGroup(
        name="test2",
        version=1,
        featurestore_id=99,
        primary_key=[],
        partition_key=[],
        features=[
            feature.Feature("id", feature_group_id=12),
            feature.Feature("tf1_name", feature_group_id=12),
        ],
        id=12,
        stream=False,
    )

    fg3 = feature_group.FeatureGroup(
        name="test3",
        version=1,
        featurestore_id=99,
        primary_key=[],
        partition_key=[],
        features=[
            feature.Feature("id", feature_group_id=13),
            feature.Feature("tf_name", feature_group_id=13),
            feature.Feature("tf1_name", feature_group_id=13),
            feature.Feature("tf3_name", feature_group_id=13),
        ],
        id=13,
        stream=False,
    )

    fg_spine = feature_group.SpineGroup(
        name="spine",
        version=1,
        featurestore_id=99,
        primary_key=[],
        partition_key=[],
        features=[
            feature.Feature("id", feature_group_id=14),
            feature.Feature("label", feature_group_id=14),
        ],
        id=14,
    )

    def test_from_response_json_python(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["query"]["get"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == 67
        assert isinstance(q._left_feature_group, feature_group.FeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is True

    def test_from_response_json_external_fg_python(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["query"]["get_external_fg"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == 67
        assert isinstance(q._left_feature_group, feature_group.ExternalFeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is True

    def test_from_response_json_spark(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        json = backend_fixtures["query"]["get"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == 67
        assert isinstance(q._left_feature_group, feature_group.FeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is False

    def test_from_response_json_external_fg_spark(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        json = backend_fixtures["query"]["get_external_fg"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name == "test_feature_store_name"
        assert q._feature_store_id == 67
        assert isinstance(q._left_feature_group, feature_group.ExternalFeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time == "test_start_time"
        assert q._left_feature_group_end_time == "test_end_time"
        assert len(q._joins) == 1
        assert isinstance(q._joins[0], join.Join)
        assert isinstance(q._filter, filter.Logic)
        assert q._python_engine is False

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["query"]["get_basic_info"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name is None
        assert q._feature_store_id is None
        assert isinstance(q._left_feature_group, feature_group.FeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time is None
        assert q._left_feature_group_end_time is None
        assert len(q._joins) == 0
        assert q._filter is None
        assert q._python_engine is True
        assert q._left_feature_group.deprecated is False

    def test_from_response_json_basic_info_deprecated(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["query"]["get_basic_info_deprecated"]["response"]

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            q = query.Query.from_response_json(json)

        # Assert
        assert q._feature_store_name is None
        assert q._feature_store_id is None
        assert isinstance(q._left_feature_group, feature_group.FeatureGroup)
        assert len(q._left_features) == 1
        assert isinstance(q._left_features[0], feature.Feature)
        assert q._left_feature_group_start_time is None
        assert q._left_feature_group_end_time is None
        assert len(q._joins) == 0
        assert q._filter is None
        assert q._python_engine is True
        assert q._left_feature_group.deprecated is True
        assert len(warning_record) == 1
        assert str(warning_record[0].message) == (
            f"Feature Group `{q._left_feature_group.name}`, version `{q._left_feature_group.version}` is deprecated"
        )

    def test_as_of(self, mocker, backend_fixtures):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        q = query.Query.from_response_json(backend_fixtures["query"]["get"]["response"])
        q.as_of("2022-01-01 00:00:00")

        assert q.left_feature_group_end_time == 1640995200000
        assert q._joins[0].query.left_feature_group_end_time == 1640995200000

        q = query.Query.from_response_json(backend_fixtures["query"]["get"]["response"])
        q.as_of(None, "2022-01-01 00:00:00")

        assert q.left_feature_group_start_time == 1640995200000
        assert q._joins[0].query.left_feature_group_start_time == 1640995200000

        q = query.Query.from_response_json(backend_fixtures["query"]["get"]["response"])
        q.as_of("2022-01-02 00:00:00", exclude_until="2022-01-01 00:00:00")

        assert q.left_feature_group_end_time == 1641081600000
        assert q.left_feature_group_start_time == 1640995200000
        assert q._joins[0].query.left_feature_group_end_time == 1641081600000
        assert q._joins[0].query.left_feature_group_start_time == 1640995200000

        q.as_of()

        assert q.left_feature_group_end_time is None
        assert q.left_feature_group_start_time is None
        assert q._joins[0].query.left_feature_group_end_time is None
        assert q._joins[0].query.left_feature_group_start_time is None

    def test_collect_feature(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select(["label"]).join(TestQuery.fg2.select(["tf1_name"]))

        features = q.features
        feature_names = [feature.name for feature in features]

        expected_features = [TestQuery.fg1["label"], TestQuery.fg2["tf1_name"]]
        expected_feature_names = ["label", "tf1_name"]

        # Assert
        assert len(feature_names) == len(expected_features)
        for i, feat in enumerate(expected_features):
            assert feat.name == expected_feature_names[i]

    def test_collect_featuregroups(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select(["label"])
            .join(TestQuery.fg2.select(["tf1_name"]))
            .join(TestQuery.fg2.select(["tf1_name"]))
        )
        expected_featuregroups = [TestQuery.fg1, TestQuery.fg2]

        # Assert
        assert len(q.featuregroups) == len(expected_featuregroups)
        assert set(q.featuregroups) == set(expected_featuregroups)

    def test_append_feature(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select([TestQuery.fg1["label"]]).append_feature("id")
        expected_features = [TestQuery.fg1["label"], feature.Feature("id")]

        # Assert
        assert len(q.features) == len(expected_features)
        for i, feat in enumerate(expected_features):
            assert feat.name == expected_features[i].name

    def test_get_feature(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select(TestQuery.fg1["label"]).join(
            TestQuery.fg2.select(TestQuery.fg2["tf1_name"])
        )

        # Assert
        assert id(q.get_feature("tf1_name")) == id(
            TestQuery.fg2.get_feature("tf1_name")
        )

    def test_get_index(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select(TestQuery.fg1["label"]).join(
            TestQuery.fg2.select(TestQuery.fg2["tf1_name"])
        )

        # Assert
        assert id(q.get_feature("tf1_name")) == id(
            TestQuery.fg2.get_feature("tf1_name")
        )

    def test_get_attr(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select(TestQuery.fg1["label"]).join(
            TestQuery.fg2.select(TestQuery.fg2["tf1_name"])
        )

        # Assert
        assert id(q.get_feature("tf1_name")) == id(
            TestQuery.fg2.get_feature("tf1_name")
        )

    def test_get_feature_by_name(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all())
        )

        # Assert
        assert (
            q._get_feature_by_name("tf3_name")[0].name == TestQuery.fg3["tf3_name"].name
        )
        assert (
            q._get_feature_by_name("tf3_name")[0].feature_group_id
            == TestQuery.fg3["tf3_name"].feature_group_id
        )

    def test_get_feature_by_name_prefix(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        assert (
            q._get_feature_by_name("tf_name")[0].name == TestQuery.fg1["tf_name"].name
        )
        assert (
            q._get_feature_by_name("tf_name")[0].feature_group_id
            == TestQuery.fg1["tf_name"].feature_group_id
        )

    def test_get_feature_by_name_ambiguous(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        with pytest.raises(FeatureStoreException) as e_info:
            q._get_feature_by_name("id")[0]

        assert str(e_info.value) == query.Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS.format(
            "id"
        )

    def test_get_feature_by_feature_ambiguous(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        with pytest.raises(FeatureStoreException) as e_info:
            q._get_featuregroup_by_feature(feature.Feature("id"))[0]

        assert str(
            e_info.value
        ) == query.Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS_FG.format("id")

    def test_get_feature_by_feature_non_ambiguous(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Assert
        assert q._get_featuregroup_by_feature(TestQuery.fg3["id"]) == TestQuery.fg3

    def test_get_ambiguous_features_star_schema(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all())
        )

        ambiguous_features = q.get_ambiguous_features()

        expected_ambiguous_features = {
            "id": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg2, TestQuery.fg3]
            ],
            "tf_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg3]
            ],
            "tf1_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg2, TestQuery.fg3]
            ],
        }

        assert sorted(ambiguous_features.keys()) == sorted(
            expected_ambiguous_features.keys()
        )

        for fg_name in ambiguous_features:
            assert sorted(ambiguous_features[fg_name]) == sorted(
                expected_ambiguous_features[fg_name]
            )

    def test_get_ambiguous_features_snowflake_schema(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all().join(
            TestQuery.fg2.select_all().join(TestQuery.fg3.select_all())
        )

        ambiguous_features = q.get_ambiguous_features()

        expected_ambiguous_features = {
            "id": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg2, TestQuery.fg3]
            ],
            "tf_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg3]
            ],
            "tf1_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg2, TestQuery.fg3]
            ],
        }

        assert sorted(ambiguous_features.keys()) == sorted(
            expected_ambiguous_features.keys()
        )

        for fg_name in ambiguous_features:
            assert sorted(ambiguous_features[fg_name]) == sorted(
                expected_ambiguous_features[fg_name]
            )

    def test_get_ambiguous_features_no_ambiguous_features(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all(), prefix="fg2_")
            .join(TestQuery.fg3.select_all(), prefix="fg3_")
        )

        ambiguous_features = q.get_ambiguous_features()

        assert ambiguous_features == {}

    def test_extract_feature_to_feature_group_mapping_joins_star_schema(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all())
        )
        feature_to_feature_group_mapping_root_fg = {
            "id": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "label": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "tf_name": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
        }
        feature_to_feature_group_mapping = (
            q._extract_feature_to_feature_group_mapping_joins(
                q._joins, feature_to_feature_group_mapping_root_fg
            )
        )

        expected_feature_to_feature_group_mapping = {
            "id": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg2, TestQuery.fg3]
            ],
            "label": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "tf_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg3]
            ],
            "tf1_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg2, TestQuery.fg3]
            ],
            "tf3_name": {f"{TestQuery.fg3.name} version {TestQuery.fg3.version}"},
        }

        assert sorted(feature_to_feature_group_mapping.keys()) == sorted(
            expected_feature_to_feature_group_mapping.keys()
        )

        for fg_name in feature_to_feature_group_mapping:
            assert sorted(feature_to_feature_group_mapping[fg_name]) == sorted(
                expected_feature_to_feature_group_mapping[fg_name]
            )

    def test_extract_feature_to_feature_group_mapping_joins_snowflake_schema(
        self, mocker
    ):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all())
        )

        feature_to_feature_group_mapping_root_fg = {
            "id": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "label": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "tf_name": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
        }
        feature_to_feature_group_mapping = (
            q._extract_feature_to_feature_group_mapping_joins(
                q._joins, feature_to_feature_group_mapping_root_fg
            )
        )

        expected_feature_to_feature_group_mapping = {
            "id": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg2, TestQuery.fg3]
            ],
            "label": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "tf_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg1, TestQuery.fg3]
            ],
            "tf1_name": [
                f"{fg.name} version {fg.version}"
                for fg in [TestQuery.fg2, TestQuery.fg3]
            ],
            "tf3_name": {f"{TestQuery.fg3.name} version {TestQuery.fg3.version}"},
        }

        assert sorted(feature_to_feature_group_mapping.keys()) == sorted(
            expected_feature_to_feature_group_mapping.keys()
        )

        for fg_name in feature_to_feature_group_mapping:
            assert sorted(feature_to_feature_group_mapping[fg_name]) == sorted(
                expected_feature_to_feature_group_mapping[fg_name]
            )

    def test_extract_feature_to_feature_group_mapping_joins_no_ambiguity(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all(), prefix="fg2_")
            .join(TestQuery.fg3.select_all(), prefix="fg3_")
        )
        feature_to_feature_group_mapping_root_fg = {
            "id": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "label": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "tf_name": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
        }
        feature_to_feature_group_mapping = (
            q._extract_feature_to_feature_group_mapping_joins(
                q._joins, feature_to_feature_group_mapping_root_fg
            )
        )

        expected_feature_to_feature_group_mapping = {
            "id": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "label": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "tf_name": {f"{TestQuery.fg1.name} version {TestQuery.fg1.version}"},
            "fg2_id": {f"{TestQuery.fg2.name} version {TestQuery.fg2.version}"},
            "fg2_tf1_name": {f"{TestQuery.fg2.name} version {TestQuery.fg2.version}"},
            "fg3_id": {f"{TestQuery.fg3.name} version {TestQuery.fg3.version}"},
            "fg3_tf_name": {f"{TestQuery.fg3.name} version {TestQuery.fg3.version}"},
            "fg3_tf1_name": {f"{TestQuery.fg3.name} version {TestQuery.fg3.version}"},
            "fg3_tf3_name": {f"{TestQuery.fg3.name} version {TestQuery.fg3.version}"},
        }

        assert sorted(feature_to_feature_group_mapping.keys()) == sorted(
            expected_feature_to_feature_group_mapping.keys()
        )

        for fg_name in feature_to_feature_group_mapping:
            assert sorted(feature_to_feature_group_mapping[fg_name]) == sorted(
                expected_feature_to_feature_group_mapping[fg_name]
            )

    def test_check_and_warn_ambiguous_features_snowflake_schema(self, mocker, caplog):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all().join(
            TestQuery.fg2.select_all().join(TestQuery.fg3.select_all())
        )

        with caplog.at_level(logging.WARNING):
            q.check_and_warn_ambiguous_features()

        assert (
            "Ambiguous features detected during query construction.The feature `id` is present in feature groups ['test1 version 1', 'test2 version 1', 'test3 version 1']. The feature `tf_name` is present in feature groups ['test1 version 1', 'test3 version 1']. The feature `tf1_name` is present in feature groups ['test2 version 1', 'test3 version 1']. Automatically prefixing features selected using these feature groups with the feature group name."
            in caplog.text
        )

    def test_check_and_warn_ambiguous_features_star_schema(self, mocker, caplog):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all())
        )

        with caplog.at_level(logging.WARNING):
            q.check_and_warn_ambiguous_features()

        assert (
            "Ambiguous features detected during query construction.The feature `id` is present in feature groups ['test1 version 1', 'test2 version 1', 'test3 version 1']. The feature `tf_name` is present in feature groups ['test1 version 1', 'test3 version 1']. The feature `tf1_name` is present in feature groups ['test2 version 1', 'test3 version 1']. Automatically prefixing features selected using these feature groups with the feature group name."
            in caplog.text
        )

    def test_check_and_warn_ambiguous_features_no_ambiguity(self, mocker, caplog):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all(), prefix="fg2_")
            .join(TestQuery.fg3.select_all(), prefix="fg3_")
        )

        with caplog.at_level(logging.WARNING):
            q.check_and_warn_ambiguous_features()

        assert (
            "Ambiguous features detected while constructing the query. "
            not in caplog.text
        )

    def test_prep_read_spine(self, mocker):
        engine = spark.Engine()
        mocker.patch("hsfs.engine._get_instance", return_value=engine)
        mocker.patch("hsfs.engine._get_type", return_value="spark")

        mock_fs_query = mocker.MagicMock(spec=FsQuery)
        mock_fs_query.query = "SELECT * FROM test"
        mock_fs_query.on_demand_feature_groups = []
        mock_fs_query.hudi_cached_feature_groups = []

        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi._construct_query",
            return_value=mock_fs_query,
        )

        q = query.Query(
            left_feature_group=TestQuery.fg_spine,
            left_features=TestQuery.fg_spine.columns,
        )

        q._prep_read(online=False, read_options={})

        mock_fs_query._register_external.assert_called()
        mock_fs_query._register_delta_tables.assert_called()
        mock_fs_query._register_hudi_tables.assert_called()

    def test_prep_hudi_delta_fg_join(self, mocker):
        engine = spark.Engine()
        mocker.patch("hsfs.engine._get_instance", return_value=engine)
        mocker.patch("hsfs.engine._get_type", return_value="spark")

        mock_fs_query = mocker.MagicMock(spec=FsQuery)
        mock_fs_query.query = "SELECT * FROM test"
        mock_fs_query.on_demand_feature_groups = []
        mock_fs_query.hudi_cached_feature_groups = []

        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi._construct_query",
            return_value=mock_fs_query,
        )

        q = query.Query(
            left_feature_group=TestQuery.fg1,
            left_features=TestQuery.fg1.columns,
        )

        q._prep_read(online=False, read_options={})

        mock_fs_query._register_external.assert_called()
        mock_fs_query._register_delta_tables.assert_called()
        mock_fs_query._register_hudi_tables.assert_called()

    def test_limit_sets_limit(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all().limit(10)

        # Assert
        assert q._limit == 10

    def test_limit_returns_self(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all()
        result = q.limit(5)

        # Assert
        assert result is q

    def test_limit_overrides_previous(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all().limit(10).limit(20)

        # Assert
        assert q._limit == 20

    def test_show_does_not_mutate_limit(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        mock_engine = mocker.MagicMock()
        mocker.patch("hsfs.engine._get_instance", return_value=mock_engine)

        mock_fs_query = mocker.MagicMock(spec=FsQuery)
        mock_fs_query.query = "SELECT * FROM test"
        mock_fs_query.on_demand_feature_groups = []
        mock_fs_query.hudi_cached_feature_groups = []
        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi._construct_query",
            return_value=mock_fs_query,
        )

        q = TestQuery.fg1.select_all()
        q._limit = 50

        q.show(5)

        # _limit should be restored to the original value after show
        assert q._limit == 50

    def test_show_uses_n_as_limit(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        mock_engine = mocker.MagicMock()
        mocker.patch("hsfs.engine._get_instance", return_value=mock_engine)

        mock_fs_query = mocker.MagicMock(spec=FsQuery)
        mock_fs_query.query = "SELECT * FROM test"
        mock_fs_query.on_demand_feature_groups = []
        mock_fs_query.hudi_cached_feature_groups = []

        captured_limit = []

        def capture_limit(query, *args, **kwargs):
            captured_limit.append(query._limit)
            return mock_fs_query

        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi._construct_query",
            side_effect=capture_limit,
        )

        q = TestQuery.fg1.select_all()
        q.show(7)

        # The query sent to the backend should have _limit == 7 during _prep_read
        assert captured_limit[0] == 7

    def test_collect_sets_state(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all().collect(100)

        # Assert
        assert q._collect == 100
        assert q._collect_order_by is None
        assert q._collect_ascending is False

    def test_collect_returns_self(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all()
        result = q.collect(5)

        # Assert
        assert result is q

    def test_collect_with_order_by_and_ascending(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        q = TestQuery.fg1.select_all().collect(50, order_by="ts", ascending=True)

        # Assert
        assert q._collect == 50
        assert q._collect_order_by == "ts"
        assert q._collect_ascending is True

    def test_collect_rejects_non_positive(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        with pytest.raises(ValueError):
            TestQuery.fg1.select_all().collect(0)

    def test_collect_to_dict(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        d = TestQuery.fg1.select_all().collect(100).to_dict()

        # Assert
        assert d["collect"] == 100
        assert d["collectOrderBy"] is None
        assert d["collectAscending"] is False

    def test_collect_to_dict_order_by_feature_serialized_to_name(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act: a Feature order_by must serialize to its column name for the backend
        d = (
            TestQuery.fg1.select_all()
            .collect(100, order_by=TestQuery.fg1["label"])
            .to_dict()
        )

        # Assert
        assert d["collectOrderBy"] == TestQuery.fg1["label"].name

    def test_collect_round_trip(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act: serialize to wire JSON and back preserves collect state
        original = TestQuery.fg1.select_all().collect(25, order_by="ts", ascending=True)
        restored = query.Query.from_response_json(json.loads(original.json()))

        # Assert
        assert restored._collect == 25
        assert restored._collect_order_by == "ts"
        assert restored._collect_ascending is True

    def test_aggregate_sets_state(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        q = TestQuery.fg1.select_all().aggregate(
            {"label": ["count", "SUM"]}, window=timedelta(days=30)
        )

        # functions normalized to lowercase, window normalized to seconds
        assert q._aggregate == {"label": ["count", "sum"]}
        assert q._aggregate_window == 30 * 24 * 3600
        assert q.aggregate({"label": ["min"]}) is q  # non-terminal

    def test_aggregate_rejects_unknown_function(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        with pytest.raises(ValueError, match="unsupported function"):
            TestQuery.fg1.select_all().aggregate({"label": ["median"]})

    def test_aggregate_rejects_empty(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        with pytest.raises(ValueError):
            TestQuery.fg1.select_all().aggregate({})
        with pytest.raises(ValueError):
            TestQuery.fg1.select_all().aggregate({"label": []})

    def test_aggregate_rejects_group_by(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        with pytest.raises(ValueError, match="group_by"):
            TestQuery.fg1.select_all().aggregate({"label": ["count"]}, group_by=["id"])

    def test_aggregate_collect_mutually_exclusive(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        with pytest.raises(ValueError, match="mutually exclusive"):
            TestQuery.fg1.select_all().collect(10).aggregate({"label": ["count"]})
        with pytest.raises(ValueError, match="mutually exclusive"):
            TestQuery.fg1.select_all().aggregate({"label": ["count"]}).collect(10)

    def test_aggregate_count_star(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        q = TestQuery.fg1.select_all().aggregate({"*": ["count"]})
        assert q._aggregate == {"*": ["count"]}

        with pytest.raises(ValueError, match="COUNT"):
            TestQuery.fg1.select_all().aggregate({"*": ["sum"]})

    def test_aggregate_greatest_least_multi_feature(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # spaces around the comma are normalized away
        q = TestQuery.fg1.select_all().aggregate(
            {"label, tf_name": ["greatest", "LEAST"]}
        )
        assert q._aggregate == {"label,tf_name": ["greatest", "least"]}

        # greatest/least require a multi-feature key
        with pytest.raises(ValueError, match="unsupported function"):
            TestQuery.fg1.select_all().aggregate({"label": ["greatest"]})
        # multi-feature keys support only greatest/least
        with pytest.raises(ValueError, match="multi-feature"):
            TestQuery.fg1.select_all().aggregate({"label,tf_name": ["sum"]})
        # a trailing comma is not a valid multi-feature key
        with pytest.raises(ValueError, match="two or more"):
            TestQuery.fg1.select_all().aggregate({"label,": ["greatest"]})

    def test_aggregate_round_trip(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        original = TestQuery.fg1.select_all().aggregate(
            {"label": ["count", "avg"]}, window=3600
        )
        restored = query.Query.from_response_json(json.loads(original.json()))

        assert restored._aggregate == {"label": ["count", "avg"]}
        assert restored._aggregate_window == 3600


class TestQueryRead:
    def test_read_with_start_time_no_event_time_raises(self):
        # Arrange
        from unittest import mock

        fg_without_event_time = feature_group.FeatureGroup(
            name="test_fg_no_event_time",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id", feature_group_id=12),
                feature.Feature("value", feature_group_id=12),
            ],
            id=12,
            stream=False,
            event_time=None,
        )

        with mock.patch("hsfs.engine._get_type", return_value="python"):
            q = query.Query(
                left_feature_group=fg_without_event_time,
                left_features=fg_without_event_time.columns,
            )

            # Act & Assert
            with pytest.raises(FeatureStoreException, match="no event_time column"):
                q.read(start_time="2024-01-01")

    def test_read_with_end_time_no_event_time_raises(self):
        # Arrange
        from unittest import mock

        fg_without_event_time = feature_group.FeatureGroup(
            name="test_fg_no_event_time",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id", feature_group_id=12),
                feature.Feature("value", feature_group_id=12),
            ],
            id=12,
            stream=False,
            event_time=None,
        )

        with mock.patch("hsfs.engine._get_type", return_value="python"):
            q = query.Query(
                left_feature_group=fg_without_event_time,
                left_features=fg_without_event_time.columns,
            )

            # Act & Assert
            with pytest.raises(FeatureStoreException, match="no event_time column"):
                q.read(end_time="2024-01-31")

    def test_filter_then_read_with_start_time_no_event_time_raises(self):
        # Arrange
        from unittest import mock

        fg_without_event_time = feature_group.FeatureGroup(
            name="test_fg_no_event_time",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id", feature_group_id=12),
                feature.Feature("value", feature_group_id=12),
            ],
            id=12,
            stream=False,
            event_time=None,
        )

        with mock.patch("hsfs.engine._get_type", return_value="python"):
            q = query.Query(
                left_feature_group=fg_without_event_time,
                left_features=fg_without_event_time.columns,
            )
            filtered_q = q.filter(fg_without_event_time.get_feature("value") > 10)

            # Act & Assert
            with pytest.raises(FeatureStoreException, match="no event_time column"):
                filtered_q.read(start_time="2024-01-01")

    def test_build_feature_lookup_left_features_only(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        q = TestQuery.fg1.select_all()
        lookup = q._build_feature_lookup()

        assert set(lookup.keys()) == {"id", "label", "tf_name"}
        for name in ("id", "label", "tf_name"):
            entries = lookup[name]
            assert len(entries) == 1
            feat, prefix, fg = entries[0]
            assert feat.name == name
            assert prefix is None
            assert fg == TestQuery.fg1

    def test_build_feature_lookup_with_joins(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        q = TestQuery.fg1.select_all().join(TestQuery.fg2.select_all())
        lookup = q._build_feature_lookup()

        # fg1 has id, label, tf_name; fg2 has id, tf1_name
        assert "label" in lookup
        assert "tf1_name" in lookup
        # "id" appears in both fg1 and fg2
        assert len(lookup["id"]) == 2

    def test_build_feature_lookup_with_prefix(self, mocker, backend_fixtures):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        q = TestQuery.fg1.select_all().join(TestQuery.fg3.select_all(), prefix="fg3_")
        lookup = q._build_feature_lookup()

        # fg3 features should appear both with and without prefix
        assert "tf3_name" in lookup
        assert "fg3_tf3_name" in lookup
        # The prefixed entry should reference fg3
        feat, prefix, fg = lookup["fg3_tf3_name"][0]
        assert feat.name == "tf3_name"
        assert prefix == "fg3_"
        assert fg == TestQuery.fg3

    def test_resolve_feature_from_lookup_single_match(self):
        feat_obj = feature.Feature("col_a")
        fg_obj = TestQuery.fg1
        lookup = {"col_a": [(feat_obj, None, fg_obj)]}

        result_feat, result_prefix, result_fg = (
            query.Query._resolve_feature_from_lookup("col_a", lookup)
        )

        assert result_feat is feat_obj
        assert result_prefix is None
        assert result_fg is fg_obj

    def test_resolve_feature_from_lookup_prefers_no_prefix(self):
        feat_no_prefix = feature.Feature("col_a")
        feat_with_prefix = feature.Feature("col_a")
        fg1 = TestQuery.fg1
        fg2 = TestQuery.fg2
        lookup = {
            "col_a": [
                (feat_with_prefix, "pfx_", fg2),
                (feat_no_prefix, None, fg1),
            ]
        }

        result_feat, result_prefix, result_fg = (
            query.Query._resolve_feature_from_lookup("col_a", lookup)
        )

        assert result_feat is feat_no_prefix
        assert result_prefix is None
        assert result_fg is fg1

    def test_resolve_feature_from_lookup_not_found(self):
        lookup = {"col_a": [(feature.Feature("col_a"), None, TestQuery.fg1)]}

        with pytest.raises(FeatureStoreException, match="could not found be found"):
            query.Query._resolve_feature_from_lookup("missing", lookup)

    def test_resolve_feature_from_lookup_ambiguous(self):
        lookup = {
            "col_a": [
                (feature.Feature("col_a"), None, TestQuery.fg1),
                (feature.Feature("col_a"), None, TestQuery.fg2),
            ]
        }

        with pytest.raises(FeatureStoreException, match="ambiguous"):
            query.Query._resolve_feature_from_lookup("col_a", lookup)

    def test_get_feature_by_name_uses_build_and_resolve(self, mocker, backend_fixtures):
        """Verify _get_feature_by_name delegates to _build_feature_lookup and _resolve_feature_from_lookup."""
        mocker.patch("hsfs.engine._get_type", return_value="python")

        q = (
            TestQuery.fg1.select_all()
            .join(TestQuery.fg2.select_all())
            .join(TestQuery.fg3.select_all(), prefix="fg3")
        )

        # Should resolve unambiguous feature from fg3
        feat, prefix, fg = q._get_feature_by_name("tf3_name")
        assert feat.name == "tf3_name"
        assert fg == TestQuery.fg3


class TestQueryOnlineReadPushdownGate:
    """`read(online=True)` must refuse collect/aggregate queries.

    The backend's online SQL string is the flat join over raw event rows (the fold and
    aggregation are served by the feature-view serving statements, not by that string),
    so executing it would silently return a different shape than offline reads.
    """

    @staticmethod
    def _online_fg(fg_id, name, with_event_time=True):
        return feature_group.FeatureGroup(
            name=name,
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id", feature_group_id=fg_id),
                feature.Feature("ts", feature_group_id=fg_id),
                feature.Feature("amount", feature_group_id=fg_id),
            ],
            id=fg_id,
            stream=False,
            online_enabled=True,
            event_time="ts" if with_event_time else None,
        )

    def test_online_read_collect_query_raises(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        q = self._online_fg(21, "events").select_all().collect(10)

        with pytest.raises(FeatureStoreException, match="collect or aggregate"):
            q._check_read_supported(online=True)

    def test_online_read_aggregate_query_raises(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        q = self._online_fg(22, "events").select_all().aggregate({"amount": ["sum"]})

        with pytest.raises(FeatureStoreException, match="collect or aggregate"):
            q._check_read_supported(online=True)

    def test_online_read_joined_collect_query_raises(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        labels = self._online_fg(23, "labels")
        events = self._online_fg(24, "events")
        q = labels.select_all().join(events.select_all().collect(5), on=["id"])

        with pytest.raises(FeatureStoreException, match="events"):
            q._check_read_supported(online=True)

    def test_online_read_plain_query_passes_gate(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        q = self._online_fg(25, "events").select_all()

        q._check_read_supported(online=True)

    def test_offline_read_collect_query_passes_gate(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        q = self._online_fg(26, "events").select_all().collect(10)

        q._check_read_supported(online=False)


class TestQueryPushdownNameNormalization:
    """Feature names are lower-cased in the feature store.

    collect(order_by=...) and aggregate() keys must normalize like Feature does,
    or the backend lookup misses and the round trip mangles mixed-case keys.
    """

    def test_collect_order_by_string_is_normalized(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        with pytest.warns(UserWarning, match="upper case"):
            q = TestQuery.fg1.select_all().collect(5, order_by="TS")

        assert q._collect_order_by == "ts"

    def test_aggregate_keys_are_normalized(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        with pytest.warns(UserWarning, match="upper case"):
            q = TestQuery.fg1.select_all().aggregate(
                {
                    "Amount": ["sum"],
                    "Amount_In, Amount_Out": ["greatest"],
                    "*": ["count"],
                }
            )

        assert q._aggregate == {
            "amount": ["sum"],
            "amount_in,amount_out": ["greatest"],
            "*": ["count"],
        }

    def test_collect_ascending_null_on_wire_deserializes_false(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")

        original = TestQuery.fg1.select_all().collect(5)
        payload = json.loads(original.json())
        payload["collectAscending"] = None
        restored = query.Query.from_response_json(payload)

        assert restored._collect == 5
        assert restored._collect_ascending is False
