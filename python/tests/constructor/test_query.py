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
import logging
import warnings

import pytest
from hsfs import feature, feature_group
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor import filter, join, lookback, query
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
        mock_fs_query.pushdown_query = None
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

    def test_prep_read_source_pushdown(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        mock_engine = mocker.MagicMock()
        mock_engine._is_flyingduck_query_supported.return_value = False
        mock_engine._is_source_pushdown_supported.return_value = True
        mock_engine._register_pushdown_query.return_value = "SELECT * FROM pushed_down"
        mocker.patch("hsfs.engine._get_instance", return_value=mock_engine)

        mock_fs_query = mocker.MagicMock(spec=FsQuery)
        mock_fs_query.pushdown_query = "SELECT * FROM SALES_DB.PUBLIC.FG0 AS fg0"
        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi._construct_query",
            return_value=mock_fs_query,
        )

        sql_query, _ = TestQuery.fg1.select_all()._prep_read(
            online=False, read_options={}
        )

        assert sql_query == "SELECT * FROM pushed_down"
        mock_engine._register_pushdown_query.assert_called_once_with(mock_fs_query)
        mock_fs_query._register_external.assert_not_called()
        mock_fs_query._register_hudi_tables.assert_not_called()

    def test_prep_read_source_pushdown_unsupported_engine(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mock_engine = mocker.MagicMock()
        mock_engine._is_flyingduck_query_supported.return_value = False
        mock_engine._is_source_pushdown_supported.return_value = False
        mocker.patch("hsfs.engine._get_instance", return_value=mock_engine)

        mock_fs_query = mocker.MagicMock(spec=FsQuery)
        mock_fs_query.pushdown_query = "SELECT * FROM SALES_DB.PUBLIC.FG0 AS fg0"
        mock_fs_query.query = "SELECT * FROM test"
        mock_fs_query.pit_query = None
        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi._construct_query",
            return_value=mock_fs_query,
        )

        sql_query, _ = TestQuery.fg1.select_all()._prep_read(
            online=False, read_options={}
        )

        assert sql_query == "SELECT * FROM test"
        mock_engine._register_pushdown_query.assert_not_called()
        mock_fs_query._register_external.assert_called()

    def test_prep_hudi_delta_fg_join(self, mocker):
        engine = spark.Engine()
        mocker.patch("hsfs.engine._get_instance", return_value=engine)
        mocker.patch("hsfs.engine._get_type", return_value="spark")

        mock_fs_query = mocker.MagicMock(spec=FsQuery)
        mock_fs_query.pushdown_query = None
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
        mock_fs_query.pushdown_query = None
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
        mock_fs_query.pushdown_query = None
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


class TestOnlinePreparedRead:
    """A repeated online read of an unchanged query must not ask the backend again.

    Constructing the query and fetching the online connector are both HTTP
    calls, made before any SQL runs, and neither answer changes while the query
    does not.
    """

    def _query(self, mocker, calls):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        q = TestQuery.fg1.select_all()
        mocker.patch.object(q, "_check_read_supported")
        mocker.patch.object(q, "_to_string", return_value="SELECT 1")
        mocker.patch.object(
            q._query_constructor_api,
            "_construct_query",
            side_effect=lambda *a, **k: calls.append("construct"),
        )
        mocker.patch.object(
            q._storage_connector_api,
            "_get_online_connector",
            side_effect=lambda *a, **k: calls.append("connector") or "connector",
        )
        return q

    def test_repeated_reads_prepare_once(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        calls = []
        q = self._query(mocker, calls)

        first = q._prep_read(True, {})
        for _ in range(4):
            assert q._prep_read(True, {}) == first

        assert calls == ["construct", "connector"]

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda q: q.filter(feature.Feature("id") > 1),
            lambda q: q.limit(10),
            lambda q: q.append_feature(feature.Feature("extra", feature_group_id=11)),
            lambda q: setattr(q, "left_feature_group_start_time", 1),
            lambda q: setattr(q, "left_feature_group_end_time", 2),
            lambda q: setattr(
                q,
                "lookback",
                lookback.Lookback(
                    default=lookback.FeatureGroupLookback(key="EVENT_TIME", start=1)
                ),
            ),
        ],
    )
    def test_a_changed_query_prepares_again(self, mocker, mutate):
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        calls = []
        q = self._query(mocker, calls)

        q._prep_read(True, {})
        mutate(q)
        q._prep_read(True, {})

        assert calls == ["construct", "connector", "construct", "connector"]

    def test_show_does_not_leave_its_row_count_behind(self, mocker):
        """`show(n)` sets the limit for its own read only.

        It prepared under the temporary limit and left that preparation in
        place, so the next ordinary read of the same query ran the preview's
        LIMIT even though the query carries none.
        """
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        calls = []
        q = self._query(mocker, calls)
        rendered = []
        q._to_string.side_effect = lambda fs_query, online: (
            rendered.append(q._limit) or f"SELECT 1 LIMIT {q._limit}"
        )
        mocker.patch("hsfs.engine._get_instance")

        q.show(2, online=True)
        q._prep_read(True, {})

        assert q._limit is None
        assert rendered == [2, None], "the ordinary read reused the preview's limit"

    def test_show_does_not_read_a_prepared_query(self, mocker):
        """A prepared query must not make `show(n)` render someone else's limit."""
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        calls = []
        q = self._query(mocker, calls)
        q.limit(50)
        rendered = []
        q._to_string.side_effect = lambda fs_query, online: (
            rendered.append(q._limit) or "SELECT 1"
        )
        mocker.patch("hsfs.engine._get_instance")

        q._prep_read(True, {})
        q.show(2, online=True)

        assert rendered == [50, 2], "show reused the query's own limit"
        assert q._limit == 50

    def test_show_does_not_evict_what_the_query_prepared(self, mocker):
        """The preview is not this query's read, so it must not take its place.

        Keying on the request already stops the preview's LIMIT from being
        reused, because it is a different request. What it does not stop on its
        own is the preview replacing the entry the query's own reads use, which
        would send the next one back to the backend.
        """
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        calls = []
        q = self._query(mocker, calls)
        mocker.patch("hsfs.engine._get_instance")

        q._prep_read(True, {})
        q.show(2, online=True)
        q._prep_read(True, {})

        assert calls == ["construct", "connector", "construct", "connector"], (
            "the preview replaced the prepared read, so the next one prepared again"
        )

    def test_a_change_inside_a_joined_query_prepares_again(self, mocker):
        """A join's sub-query is changed through its own object, not through this one."""
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        calls = []
        q = self._query(mocker, calls)
        sub = TestQuery.fg2.select_all()
        q.join(sub)

        q._prep_read(True, {})
        sub.filter(feature.Feature("id") > 1)
        q._prep_read(True, {})

        assert calls == ["construct", "connector", "construct", "connector"]

    def test_a_joined_query_prepares_again(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        calls = []
        q = self._query(mocker, calls)

        q._prep_read(True, {})
        q.join(TestQuery.fg2.select_all())
        q._prep_read(True, {})

        assert calls == ["construct", "connector", "construct", "connector"]

    def test_a_new_connection_prepares_again(self, mocker):
        """Credentials belong to a session; a re-login must not read through the old ones."""
        calls = []
        connection = mocker.patch(
            "hopsworks_common.client._get_instance", return_value=object()
        )
        q = self._query(mocker, calls)

        q._prep_read(True, {})
        connection.return_value = object()
        q._prep_read(True, {})

        assert calls == ["construct", "connector", "construct", "connector"]


class TestReadBatches:
    """Batches arrive as they are produced, and the reader is released either way."""

    def _query(self, mocker, engine_instance):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.engine._get_instance", return_value=engine_instance)
        mocker.patch("hopsworks_common.client._get_instance", return_value=object())
        q = TestQuery.fg1.select_all()
        mocker.patch.object(q, "_check_read_supported")
        mocker.patch.object(q, "_to_string", return_value="SELECT 1")
        mocker.patch.object(q._query_constructor_api, "_construct_query")
        mocker.patch.object(
            q._storage_connector_api, "_get_online_connector", return_value="connector"
        )
        return q

    def test_batches_are_yielded_and_the_reader_is_closed(self, mocker):
        closed = []

        def stream(*_args, **_kwargs):
            try:
                yield "batch-1"
                yield "batch-2"
            finally:
                closed.append(True)

        engine_instance = mocker.Mock()
        engine_instance._stream_batches.side_effect = stream
        q = self._query(mocker, engine_instance)

        with q.read_batches(online=True) as batches:
            assert list(batches) == ["batch-1", "batch-2"]

        assert closed == [True]

    def test_leaving_early_still_closes_the_reader(self, mocker):
        """A caller that takes one batch must not leave the server producing the rest."""
        closed = []

        def stream(*_args, **_kwargs):
            try:
                yield "batch-1"
                yield "batch-2"
            finally:
                closed.append(True)

        engine_instance = mocker.Mock()
        engine_instance._stream_batches.side_effect = stream
        q = self._query(mocker, engine_instance)

        with q.read_batches(online=True) as batches:
            assert next(iter(batches)) == "batch-1"

        assert closed == [True]

    def test_an_exception_while_reading_still_closes_the_reader(self, mocker):
        closed = []

        def stream(*_args, **_kwargs):
            try:
                yield "batch-1"
                yield "batch-2"
            finally:
                closed.append(True)

        engine_instance = mocker.Mock()
        engine_instance._stream_batches.side_effect = stream
        q = self._query(mocker, engine_instance)

        with pytest.raises(ValueError, match="caller"):  # noqa: SIM117
            with q.read_batches(online=True) as batches:
                next(iter(batches))
                raise ValueError("caller blew up")

        assert closed == [True]

    def test_the_batch_size_reaches_the_engine(self, mocker):
        engine_instance = mocker.Mock()
        engine_instance._stream_batches.side_effect = lambda *a, **k: iter(())
        q = self._query(mocker, engine_instance)

        with q.read_batches(online=True, batch_size=7):
            pass

        assert engine_instance._stream_batches.call_args.args[4] == 7

    def test_joined_features_are_declared_under_their_prefixed_names(self, mocker):
        """The SQL output names a prefixed join's columns with the prefix."""
        mocker.patch("hsfs.engine._get_type", return_value="python")
        q = TestQuery.fg1.select_all().join(
            TestQuery.fg2.select_all(), on=["id"], prefix="r_"
        )

        names = [f.name for f in q._output_features()]

        assert names == ["id", "label", "tf_name", "r_id", "r_tf1_name"]
        # The query's own features are not renamed.
        assert [f.name for f in TestQuery.fg2.features] == ["id", "tf1_name"]

    @pytest.mark.parametrize("batch_size", [0, -1])
    def test_a_batch_size_below_one_is_refused(self, mocker, batch_size):
        q = self._query(mocker, mocker.Mock())

        with pytest.raises(ValueError, match="at least 1"):  # noqa: SIM117
            with q.read_batches(batch_size=batch_size):
                pass

    def test_an_engine_that_cannot_stream_says_so(self, mocker):
        engine_instance = mocker.Mock(spec=[])
        q = self._query(mocker, engine_instance)

        with pytest.raises(FeatureStoreException, match="Python engine"):  # noqa: SIM117
            with q.read_batches():
                pass
