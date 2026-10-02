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
import copy
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest
from hopsworks_common.util import _convert_event_time_to_timestamp
from hsfs.client.exceptions import FeatureStoreException, VectorDatabaseException
from hsfs.core import vector_db_client
from hsfs.embedding import EmbeddingIndex
from hsfs.feature import Feature
from hsfs.feature_group import FeatureGroup
from opensearchpy.exceptions import (
    AuthorizationException,
    ConnectionError,
    NotFoundError,
)


class TestVectorDbClient:
    embedding_index = EmbeddingIndex("2249__embedding_default_embedding")
    embedding_index.add_embedding("f2", 3)
    embedding_index._col_prefix = ""
    with mock.patch("hopsworks_common.client._get_instance"):
        fg = FeatureGroup("test_fg", 1, 99, id=1, embedding_index=embedding_index)
        f1 = Feature("f1", feature_group=fg, primary=True, type="int")
        f2 = Feature("f2", feature_group=fg, primary=True, type="int")
        f3 = Feature("f3", feature_group=fg, type="int")
        f_bool = Feature("f_bool", feature_group=fg, type="boolean")
        f_ts = Feature("f_ts", feature_group=fg, type="timestamp")
        fg.columns = [f1, f2, f3, f_bool, f_ts]
        fg2 = FeatureGroup("test_fg", 1, 99, id=2)
        fg2.columns = [f1, f2]

    @pytest.fixture(autouse=True)
    def setup_mocks(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        # Mock the OpenSearchClientSingleton to return a MagicMock instead of creating a real client
        self.mock_os_wrapper = MagicMock()
        self.mock_os_wrapper._search.return_value = {
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
        self.mock_os_wrapper._get_field_mapping.return_value = self._field_mapping(
            "f2", "faiss"
        )
        mocker.patch(
            "hsfs.core.vector_db_client.OpenSearchClientSingleton",
            return_value=self.mock_os_wrapper,
        )
        mocker.patch.object(vector_db_client.VectorDbClient, "_field_knn_filter", {})

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
                [
                    {
                        "range": {
                            "f_ts": {
                                "gt": _convert_event_time_to_timestamp(
                                    "2024-04-18 12:00:25"
                                )
                            }
                        }
                    }
                ],
            ),
            # Boolean type - string value "true" should be converted to True
            (
                "f_bool",
                lambda f: f == True,  # noqa: E712
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
        "feature_attr, filter_expression, col_prefix, expected_result",
        [
            ("f1", lambda f: f > 10, "46_", [{"range": {"46_f1": {"gt": 10}}}]),
            ("f1", lambda f: f < 10, "46_", [{"range": {"46_f1": {"lt": 10}}}]),
            ("f1", lambda f: f >= 10, "46_", [{"range": {"46_f1": {"gte": 10}}}]),
            ("f1", lambda f: f <= 10, "46_", [{"range": {"46_f1": {"lte": 10}}}]),
            ("f1", lambda f: f == 10, "46_", [{"term": {"46_f1": 10}}]),
            (
                "f1",
                lambda f: f != 10,
                "46_",
                [{"bool": {"must_not": [{"term": {"46_f1": 10}}]}}],
            ),
            (
                "f1",
                lambda f: f.isin([10, 20, 30]),
                "46_",
                [{"terms": {"46_f1": [10, 20, 30]}}],
            ),
            (
                "f1",
                lambda f: f.like("abc"),
                "46_",
                [{"wildcard": {"46_f1": {"value": "*abc*"}}}],
            ),
            (
                "f_ts",
                lambda f: f > "2024-04-18 12:00:25",
                "46_",
                [
                    {
                        "range": {
                            "46_f_ts": {
                                "gt": _convert_event_time_to_timestamp(
                                    "2024-04-18 12:00:25"
                                )
                            }
                        }
                    }
                ],
            ),
            (
                "f_bool",
                lambda f: f == True,  # noqa: E712
                "46_",
                [{"term": {"46_f_bool": True}}],
            ),
            (
                "f_bool",
                lambda f: f.isin([True, False]),
                "46_",
                [{"terms": {"46_f_bool": [True, False]}}],
            ),
        ],
    )
    def test_get_query_filter_with_col_prefix(
        self, feature_attr, filter_expression, col_prefix, expected_result
    ):
        feature = getattr(self, feature_attr)
        filter = filter_expression(feature)
        assert self.target._get_query_filter(filter, col_prefix) == expected_result

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
                lambda f1, f2: (f1 > 10) & ((f2 < 20) | ((f1 > 30) & (f2 < 40))),
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

    @pytest.mark.parametrize(
        "filter_expression_nested, col_prefix, expected_result",
        [
            (
                lambda f1, f2: (f1 > 10) & (f2 < 20),
                "46_",
                [
                    {
                        "bool": {
                            "must": [
                                {"range": {"46_f1": {"gt": 10}}},
                                {"range": {"46_f2": {"lt": 20}}},
                            ]
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: (f1 < 10) | (f2 > 20),
                "46_",
                [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                                {"range": {"46_f1": {"lt": 10}}},
                                {"range": {"46_f2": {"gt": 20}}},
                            ],
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: ((f1 < 10) | (f1 > 30)) & ((f2 > 20) | (f2 < 10)),
                "46_",
                [
                    {
                        "bool": {
                            "must": [
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"46_f1": {"lt": 10}}},
                                            {"range": {"46_f1": {"gt": 30}}},
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                },
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"46_f2": {"gt": 20}}},
                                            {"range": {"46_f2": {"lt": 10}}},
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
                "46_",
                [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                                {
                                    "bool": {
                                        "must": [
                                            {"range": {"46_f1": {"gt": 10}}},
                                            {"range": {"46_f2": {"lt": 20}}},
                                        ]
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {"range": {"46_f1": {"gt": 10}}},
                                            {"range": {"46_f2": {"lt": 20}}},
                                        ]
                                    }
                                },
                            ],
                        }
                    }
                ],
            ),
            (
                lambda f1, f2: (f1 > 10) & ((f2 < 20) | ((f1 > 30) & (f2 < 40))),
                "46_",
                [
                    {
                        "bool": {
                            "must": [
                                {"range": {"46_f1": {"gt": 10}}},
                                {
                                    "bool": {
                                        "should": [
                                            {"range": {"46_f2": {"lt": 20}}},
                                            {
                                                "bool": {
                                                    "must": [
                                                        {
                                                            "range": {
                                                                "46_f1": {"gt": 30}
                                                            }
                                                        },
                                                        {
                                                            "range": {
                                                                "46_f2": {"lt": 40}
                                                            }
                                                        },
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
    def test_get_query_filter_logic_with_col_prefix(
        self, filter_expression_nested, col_prefix, expected_result
    ):
        filter = filter_expression_nested(self.f1, self.f2)
        assert self.target._get_query_filter(filter, col_prefix) == expected_result

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
        actual = self.target._read(
            self.fg.id, self.fg.columns, keys={"f1": 10, "f2": 20}
        )

        expected_query = {
            "query": {"bool": {"must": [{"match": {"f1": 10}}, {"match": {"f2": 20}}]}},
            "_source": ["f1", "f2", "f3", "f_bool", "f_ts"],
        }
        self.mock_os_wrapper._search.assert_called_once_with(
            body=expected_query, index="2249__embedding_default_embedding"
        )
        expected = [{"f1": 4, "f2": [9, 4, 4]}]
        assert actual == expected

    def test_read_with_pk(self):
        actual = self.target._read(self.fg.id, self.fg.columns, pk="f1")

        expected_query = {
            "query": {"bool": {"must": [{"exists": {"field": "f1"}}]}},
            "size": 10,
            "_source": ["f1", "f2", "f3", "f_bool", "f_ts"],
        }
        self.mock_os_wrapper._search.assert_called_once_with(
            body=expected_query, index="2249__embedding_default_embedding"
        )
        expected = [{"f1": 4, "f2": [9, 4, 4]}]
        assert actual == expected

    def test_read_without_pk_or_keys(self):
        with pytest.raises(FeatureStoreException):
            self.target._read(self.fg.id, self.fg.columns)

    def test_find_neighbors_builds_knn_query_without_filter(self):
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=5)

        body = self.mock_os_wrapper._search.call_args.kwargs["body"]
        assert body["size"] == 5
        knn = body["query"]["knn"]["f2"]
        assert knn["vector"] == [1.0, 2.0, 3.0]
        assert knn["k"] == 5
        # The filter lives inside the knn clause (OpenSearch 2.x KNN syntax),
        # not as a sibling bool/post_filter.
        assert knn["filter"] == {"bool": {"must": [{"exists": {"field": "f2"}}]}}

    def test_find_neighbors_builds_knn_query_with_filter(self):
        self.target._find_neighbors(
            [1.0, 2.0, 3.0], feature=self.f2, k=5, filter=self.f3 > 10
        )

        body = self.mock_os_wrapper._search.call_args.kwargs["body"]
        knn = body["query"]["knn"]["f2"]
        assert knn["filter"] == {
            "bool": {
                "must": [
                    {"exists": {"field": "f2"}},
                    {"range": {"f3": {"gt": 10}}},
                ]
            }
        }

    @staticmethod
    def _field_mapping(col_name, engine, index="2249__embedding_default_embedding"):
        # The response of GET <index>/_mapping/field/<col_name>
        mapping = {"type": "knn_vector", "dimension": 3}
        if engine:
            mapping["method"] = {"engine": engine, "space_type": "l2", "name": "hnsw"}
        return {
            index: {
                "mappings": {
                    col_name: {"full_name": col_name, "mapping": {col_name: mapping}}
                }
            }
        }

    @pytest.mark.parametrize("engine", ["nmslib", None])
    def test_find_neighbors_puts_filters_next_to_knn_on_nmslib(self, engine):
        # Indices created before 5.1 are on nmslib, or have no method if created by 3.7.
        self.mock_os_wrapper._get_field_mapping.return_value = self._field_mapping(
            "f2", engine
        )

        self.target._find_neighbors(
            [1.0, 2.0, 3.0], feature=self.f2, k=5, filter=self.f3 > 10
        )

        body = self.mock_os_wrapper._search.call_args.kwargs["body"]
        assert body["query"] == {
            "bool": {
                "must": [{"knn": {"f2": {"vector": [1.0, 2.0, 3.0], "k": 5}}}],
                "filter": [
                    {"exists": {"field": "f2"}},
                    {"range": {"f3": {"gt": 10}}},
                ],
            }
        }

    @pytest.mark.parametrize(
        "lookup, mapping_reads",
        [
            # Refused or missing: remembered, so the mapping is read once.
            ({"side_effect": AuthorizationException(403, "security_exception", {})}, 1),
            ({"side_effect": NotFoundError(404, "index_not_found_exception", {})}, 1),
            # Anything else may be transient, so the next search reads the mapping again.
            ({"side_effect": ConnectionError("N/A", "connection refused", None)}, 2),
            (
                {
                    "return_value": {
                        "2249__embedding_default_embedding": {"mappings": {}}
                    }
                },
                2,
            ),
        ],
        ids=["refused", "not_found", "connection_error", "field_not_in_mapping"],
    )
    def test_find_neighbors_keeps_filter_in_knn_when_engine_unknown(
        self, lookup, mapping_reads
    ):
        self.mock_os_wrapper._get_field_mapping.configure_mock(**lookup)

        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=5)
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=5)

        body = self.mock_os_wrapper._search.call_args.kwargs["body"]
        assert body["query"]["knn"]["f2"]["filter"] == {
            "bool": {"must": [{"exists": {"field": "f2"}}]}
        }
        assert self.mock_os_wrapper._get_field_mapping.call_count == mapping_reads

    def test_find_neighbors_reads_engine_once_per_field(self):
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=5)
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=5)

        self.mock_os_wrapper._get_field_mapping.assert_called_once_with(
            index="2249__embedding_default_embedding", field="f2"
        )

    def test_supports_knn_filter_per_field_in_same_index(self):
        # A default project index created before 5.1 gets faiss fields next to its nmslib ones.
        index = "1__embedding_default_project_embedding_0"
        self.mock_os_wrapper._get_field_mapping.side_effect = lambda index, field: (
            self._field_mapping(
                field, {"10_emb": "nmslib", "11_emb": "faiss"}[field], index
            )
        )

        assert not self.target._supports_knn_filter(
            self.mock_os_wrapper, 99, index, "10_emb"
        )
        assert self.target._supports_knn_filter(
            self.mock_os_wrapper, 99, index, "11_emb"
        )

    def test_find_neighbors_retries_with_larger_k_on_nmslib(self, mocker):
        # On nmslib the filters can drop most of the k nearest in a default project index, so the search is repeated with a larger k.
        mocker.patch.object(self.embedding_index, "_col_prefix", "1_")
        mocker.patch.object(
            vector_db_client.VectorDbClient, "_index_result_limit_k", {}
        )
        self.mock_os_wrapper._get_field_mapping.return_value = self._field_mapping(
            "1_f2", "nmslib"
        )
        target = vector_db_client.VectorDbClient(self.fg.select_all())
        hits = {"hits": {"hits": [{"_score": 1.0, "_source": {"1_f1": 4}}]}}
        bodies = []

        def search(body, index, options):
            bodies.append(copy.deepcopy(body))
            if len(bodies) == 2:
                raise VectorDatabaseException(
                    VectorDatabaseException.REQUESTED_K_TOO_LARGE,
                    "",
                    {VectorDatabaseException.REQUESTED_K_TOO_LARGE_INFO_K: 10000},
                )
            return hits

        self.mock_os_wrapper._search.side_effect = search

        target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=5)

        assert [b["query"]["bool"]["must"][0]["knn"]["1_f2"]["k"] for b in bodies] == [
            5,
            2**31 - 1,
            15,
        ]

    def test_convert_to_pandas_type_timestamp_keeps_milliseconds(self):
        # OpenSearch stores timestamps as epoch ms; sub-second precision must
        # survive the conversion, e.g. for timestamp(3) online types (FSTORE-2061)
        epoch_ms = _convert_event_time_to_timestamp("2024-04-18 12:00:25.789")

        result = self.target._convert_to_pandas_type(
            self.fg.columns, {"f1": 4, "f_ts": epoch_ms}
        )

        assert result["f_ts"] == datetime(2024, 4, 18, 12, 0, 25, 789000)

    def test_convert_to_pandas_type_epoch_zero_is_not_null(self):
        # 0 epoch ms is a valid timestamp and must not be skipped as null
        result = self.target._convert_to_pandas_type(
            self.fg.columns, {"f1": 4, "f_ts": 0}
        )

        assert result["f_ts"] == datetime(1970, 1, 1)
