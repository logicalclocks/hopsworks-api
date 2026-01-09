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
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor.filter import Filter
from hsfs.core import vector_db_client
from hsfs.embedding import EmbeddingIndex
from hsfs.feature import Feature
from hsfs.feature_group import FeatureGroup
from hopsworks_common.util import convert_event_time_to_timestamp


class TestVectorDbClient:
    embedding_index = EmbeddingIndex("2249__embedding_default_embedding")
    embedding_index.add_embedding("f2", 3)
    embedding_index._col_prefix = ""
    with mock.patch("hopsworks_common.client.get_instance"):
        fg = FeatureGroup("test_fg", 1, 99, id=1, embedding_index=embedding_index)
        f1 = Feature("f1", feature_group=fg, primary=True, type="int")
        f2 = Feature("f2", feature_group=fg, primary=True, type="int")
        f3 = Feature("f3", feature_group=fg, type="int")
        f_bool = Feature("f_bool", feature_group=fg, type="boolean")
        f_ts = Feature("f_ts", feature_group=fg, type="timestamp")
        fg.features = [f1, f2, f3, f_bool, f_ts]
        fg2 = FeatureGroup("test_fg", 1, 99, id=2)
        fg2.features = [f1, f2]

    @pytest.fixture(autouse=True)
    def setup_mocks(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")
        # Mock the OpenSearchClientSingleton to return a MagicMock instead of creating a real client
        self.mock_os_wrapper = MagicMock()
        self.mock_os_wrapper.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_index": "2249__embedding_default_embedding",
                        "_type": "_doc",
                        "_id": "2389|4",
                        "_score": 1.0,
                        "_source": {"f1": 4, "f2": [9, 4, 4]},
                    }
                ]
            }
        }
        mocker.patch(
            "hsfs.core.vector_db_client.OpenSearchClientSingleton",
            return_value=self.mock_os_wrapper,
        )

        self.query = self.fg.select_all()
        self.target = vector_db_client.VectorDbClient(self.query)
        # _opensearch_client is no longer injected; use wrapper search result

    @pytest.mark.parametrize(
        "feature_attr, filter_expression, expected_result",
        [
            ("f1", lambda f: None, []),
            ("f1", lambda f: f > 10, [{"range": {"f1": {"gt": 10}}}]),
            ("f1", lambda f: f < 10, [{"range": {"f1": {"lt": 10}}}]),
            ("f1", lambda f: f >= 10, [{"range": {"f1": {"gte": 10}}}]),
            ("f1", lambda f: f <= 10, [{"range": {"f1": {"lte": 10}}}]),
            ("f1", lambda f: f == 10, [{"term": {"f1": 10}}]),
            ("f1", lambda f: f != 10, [{"bool": {"must_not": [{"term": {"f1": 10}}]}}]),
            # IN operator - value should be converted from JSON string to list
            ("f1", lambda f: f.isin([10, 20, 30]), [{"terms": {"f1": [10, 20, 30]}}]),
            ("f1", lambda f: f.like("abc"), [{"wildcard": {"f1": {"value": "*abc*"}}}]),
            # Timestamp type - value should be converted to epoch milliseconds.
            (
                "f_ts",
                lambda f: f > "2024-04-18 12:00:25",
                [{"range": {"f_ts": {"gt": convert_event_time_to_timestamp("2024-04-18 12:00:25")}}}],
            ),
            # Boolean type - string value "true" should be converted to True
            (
                "f_bool",
                lambda f: f == True,
                [{"term": {"f_bool": True}}],
            ),
            # Boolean type with IN operator - JSON array of strings should be converted to list of booleans
            (
                "f_bool",
                lambda f: f.isin([True, False]),
                [{"terms": {"f_bool": [True, False]}}],
            ),
        ],
    )
    def test_get_query_filter(self, feature_attr, filter_expression, expected_result):
        feature = getattr(self, feature_attr)
        filter = filter_expression(feature)
        assert self.target._get_query_filter(filter) == expected_result

    @pytest.mark.parametrize(
        "filter_expression_nested, expected_result",
        [
            (
                lambda f1, f2: (f1 > 10) & (f2 < 20),
                [
                    {
                        "bool": {
                            "must": [
                                {"range": {"f1": {"gt": 10}}},
                                {"range": {"f2": {"lt": 20}}},
                            ]
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: (f1 < 10) | (f2 > 20),
                [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                                {"range": {"f1": {"lt": 10}}},
                                {"range": {"f2": {"gt": 20}}},
                            ],
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: ((f1 < 10) | (f1 > 30)) & ((f2 > 20) | (f2 < 10)),
                [
                    {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"f1": {"lt": 10}}},
                                            {"range": {"f1": {"gt": 30}}},
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                },
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"f2": {"gt": 20}}},
                                            {"range": {"f2": {"lt": 10}}},
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                },
                            ]
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: ((f1 > 10) & (f2 < 20)) | ((f1 > 10) & (f2 < 20)),
                [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                                {
                                    "bool": {
                                        "must": [
                                            {"range": {"f1": {"gt": 10}}},
                                            {"range": {"f2": {"lt": 20}}},
                                        ]
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {"range": {"f1": {"gt": 10}}},
                                            {"range": {"f2": {"lt": 20}}},
                                        ]
                                    }
                                },
                            ],
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: ((f1 > 10) & ((f2 < 20) | ((f1 > 30) & (f2 < 40)))),
                [
                    {
                        "bool": {
                            "must": [
                                {"range": {"f1": {"gt": 10}}},
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"f2": {"lt": 20}}},
                                            {
                                                "bool": {
                                                    "must": [
                                                        {"range": {"f1": {"gt": 30}}},
                                                        {"range": {"f2": {"lt": 40}}},
                                                    ]
                                                }
                                            },
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                },
                            ]
                        }
                    }
                ],
            ),
        ],
    )
    def test_get_query_filter_logic(self, filter_expression_nested, expected_result):
        filter = filter_expression_nested(self.f1, self.f2)
        assert self.target._get_query_filter(filter) == expected_result

    def test_check_filter_when_filter_is_None(self):
        self.target._check_filter(None, self.fg2)

    def test_check_filter_when_filter_is_logic_with_wrong_feature_group(self):
        with pytest.raises(FeatureStoreException):
            self.target._check_filter((self.fg.f1 > 10) & (self.fg.f1 < 30), self.fg2)

    def test_check_filter_when_filter_is_logic_with_correct_feature_group(self):
        self.target._check_filter((self.fg.f1 > 10) & (self.fg.f1 < 30), self.fg)

    def test_check_filter_when_filter_is_filter_with_wrong_feature_group(self):
        with pytest.raises(FeatureStoreException):
            self.target._check_filter((self.fg.f1 < 30), self.fg2)

    def test_check_filter_when_filter_is_filter_with_correct_feature_group(self):
        self.target._check_filter((self.fg.f1 < 30), self.fg)

    def test_check_filter_when_filter_is_not_logic_or_filter(self):
        with pytest.raises(FeatureStoreException):
            self.target._check_filter("f1 > 20", self.fg2)

    def test_read_with_keys(self):
        actual = self.target.read(
            self.fg.id, self.fg.features, keys={"f1": 10, "f2": 20}
        )

        expected_query = {
            "query": {"bool": {"must": [{"match": {"f1": 10}}, {"match": {"f2": 20}}]}},
            "_source": ["f1", "f2", "f3", "f_bool", "f_ts"],
        }
        self.mock_os_wrapper.search.assert_called_once_with(
            body=expected_query, index="2249__embedding_default_embedding"
        )
        expected = [{"f1": 4, "f2": [9, 4, 4]}]
        assert actual == expected

    def test_read_with_pk(self):
        actual = self.target.read(self.fg.id, self.fg.features, pk="f1")

        expected_query = {
            "query": {"bool": {"must": {"exists": {"field": "f1"}}}},
            "size": 10,
            "_source": ["f1", "f2", "f3", "f_bool", "f_ts"],
        }
        self.mock_os_wrapper.search.assert_called_once_with(
            body=expected_query, index="2249__embedding_default_embedding"
        )
        expected = [{"f1": 4, "f2": [9, 4, 4]}]
        assert actual == expected

    def test_read_without_pk_or_keys(self):
        with pytest.raises(FeatureStoreException):
            self.target.read(self.fg.id, self.fg.features)
