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
import gc
import time
import weakref
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest
from hopsworks_common.core.opensearch import (
    OpenSearchClientSingleton,
    ProjectOpenSearchClient,
)
from hopsworks_common.util import _convert_event_time_to_timestamp
from hsfs.client.exceptions import FeatureStoreException, VectorDatabaseException
from hsfs.core import vector_db_client
from hsfs.embedding import EmbeddingIndex
from hsfs.feature import Feature
from hsfs.feature_group import FeatureGroup
from opensearchpy.exceptions import (
    AuthenticationException,
    AuthorizationException,
    ConnectionError,
    NotFoundError,
    RequestError,
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
        vector_db_client.VectorDbClient._field_knn_filter.clear()
        vector_db_client.VectorDbClient._knn_filter_refused_until.clear()

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
            # Refused or missing, which the wrapper answers with None: remembered, so the mapping is read once.
            ({"return_value": None}, 1),
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
        ids=["refused_or_missing", "connection_error", "field_not_in_mapping"],
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

    @pytest.mark.parametrize("engines", [("nmslib", "faiss"), ("faiss", "nmslib")])
    def test_find_neighbors_mapping_cache_is_scoped_to_client(self, mocker, engines):
        wrappers = []
        for engine in engines:
            client = MagicMock()
            client.indices.get_field_mapping.return_value = self._field_mapping(
                "f2", engine
            )
            client.search.return_value = self.mock_os_wrapper._search.return_value
            wrapper = ProjectOpenSearchClient(
                client, feature_store_id=99, is_cluster_client=False
            )
            mocker.patch(
                "hsfs.core.vector_db_client.OpenSearchClientSingleton",
                return_value=wrapper,
            )

            self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

            wrappers.append(wrapper)
            body = client.search.call_args.kwargs["body"]
            expected_shape = "outside" if engine == "nmslib" else "inside"
            assert self._knn_shape(body) == (expected_shape, 2)
            client.indices.get_field_mapping.assert_called_once()

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

    @staticmethod
    def _opensearch_error(error_type, reason, caused_by=None):
        # The body of an OpenSearch error response, as opensearch-py hands it to RequestError.
        error = {
            "root_cause": [{"type": error_type, "reason": reason}],
            "type": error_type,
            "reason": reason,
        }
        if caused_by:
            error["caused_by"] = caused_by
        return RequestError(400, error_type, {"error": error, "status": 400})

    def _opensearch_1_3(
        self, mocker, col_name="f2", hits=None, search=None, first_error=None
    ):
        """The real client wrapper over the low-level client of an OpenSearch 1.3 with a faiss field."""
        client = MagicMock()
        client.indices.get_field_mapping.return_value = self._field_mapping(
            col_name, "faiss"
        )
        bodies = []

        def opensearch_search(body, index, params):
            bodies.append(copy.deepcopy(body))
            if first_error and len(bodies) == 1:
                raise first_error
            if "filter" in body["query"].get("knn", {}).get(col_name, {}):
                raise self._opensearch_error(
                    "parsing_exception",
                    "[knn] unknown token [START_OBJECT] after [filter]",
                )
            if search:
                return search(body)
            return hits or self.mock_os_wrapper._search.return_value

        client.search.side_effect = opensearch_search
        mocker.patch(
            "hsfs.core.vector_db_client.OpenSearchClientSingleton",
            return_value=ProjectOpenSearchClient(
                client, feature_store_id=99, is_cluster_client=False
            ),
        )
        return bodies

    @staticmethod
    def _knn_shape(body, col_name="f2"):
        if "knn" in body["query"]:
            return "inside", body["query"]["knn"][col_name]["k"]
        return "outside", body["query"]["bool"]["must"][0]["knn"][col_name]["k"]

    def test_find_neighbors_moves_filter_out_of_knn_when_opensearch_cannot_parse_it(
        self, mocker
    ):
        # A 5.1 central cluster creates faiss fields on the OpenSearch 1.3 of a 5.0 satellite.
        bodies = self._opensearch_1_3(mocker)

        results = self.target._find_neighbors(
            [1.0, 2.0, 3.0], feature=self.f2, k=2, filter=self.f3 > 10
        )

        assert len(results) == 1
        assert [self._knn_shape(body) for body in bodies] == [
            ("inside", 2),
            ("outside", 2),
        ]
        assert bodies[1]["query"] == {
            "bool": {
                "must": [{"knn": {"f2": {"vector": [1.0, 2.0, 3.0], "k": 2}}}],
                "filter": [
                    {"exists": {"field": "f2"}},
                    {"range": {"f3": {"gt": 10}}},
                ],
            }
        }
        assert bodies[1]["size"] == 2
        assert bodies[1]["_source"] == bodies[0]["_source"]

        # The next search of the field goes straight to the shape OpenSearch can parse.
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        assert [self._knn_shape(body) for body in bodies[2:]] == [("outside", 2)]

    @pytest.mark.parametrize(
        "mapping_error",
        [
            AuthorizationException(403, "security_exception", {}),
            NotFoundError(404, "index_not_found_exception", {}),
        ],
        ids=["refused", "missing"],
    )
    def test_find_neighbors_keeps_federated_client_when_mapping_is_unreadable(
        self, mocker, mapping_error
    ):
        # A satellite's feature store reads its OpenSearch through a federated client, here on OpenSearch 1.3.
        client = MagicMock()
        client.indices.get_field_mapping.side_effect = mapping_error

        def opensearch_search(body, index, params):
            if "knn" in body["query"]:
                raise self._opensearch_error(
                    "parsing_exception",
                    "[knn] unknown token [START_OBJECT] after [filter]",
                )
            return self.mock_os_wrapper._search.return_value

        client.search.side_effect = opensearch_search
        mocker.patch.object(OpenSearchClientSingleton, "_instance", None)
        mocker.patch.object(OpenSearchClientSingleton, "_wrapper_cache", {})
        create = mocker.patch.object(
            OpenSearchClientSingleton,
            "_get_or_create_opensearch_client",
            return_value=(False, client),
        )
        mocker.patch("hopsworks_common.core.opensearch.OpenSearchApi")
        mocker.patch(
            "hsfs.core.vector_db_client.OpenSearchClientSingleton",
            OpenSearchClientSingleton,
        )

        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        create.assert_called_once()
        client.close.assert_not_called()
        client.indices.get_field_mapping.assert_called_once()
        assert [
            self._knn_shape(call.kwargs["body"])
            for call in client.search.call_args_list
        ] == [("inside", 2), ("outside", 2), ("outside", 2)]

    def test_find_neighbors_moves_filter_out_of_knn_after_a_refresh(self, mocker):
        # An expired token makes the wrapper refresh the connection and send the search again.
        mocker.patch.object(ProjectOpenSearchClient, "_refresh_opensearch_connection")
        bodies = self._opensearch_1_3(
            mocker,
            first_error=AuthenticationException(401, "security_exception", {}),
        )

        results = self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        assert len(results) == 1
        assert [self._knn_shape(body) for body in bodies] == [
            ("inside", 2),
            ("inside", 2),
            ("outside", 2),
        ]

    def test_find_neighbors_refusal_cache_is_scoped_to_client(self, mocker):
        old_bodies = self._opensearch_1_3(mocker)
        old_wrapper = vector_db_client.OpenSearchClientSingleton.return_value
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        modern_client = MagicMock()
        modern_client.indices.get_field_mapping.return_value = self._field_mapping(
            "f2", "faiss"
        )
        modern_client.search.return_value = self.mock_os_wrapper._search.return_value
        modern_wrapper = ProjectOpenSearchClient(
            modern_client, feature_store_id=99, is_cluster_client=False
        )
        singleton = mocker.patch(
            "hsfs.core.vector_db_client.OpenSearchClientSingleton",
            return_value=modern_wrapper,
        )

        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        modern_body = modern_client.search.call_args.kwargs["body"]
        assert self._knn_shape(modern_body) == ("inside", 2)
        modern_client.indices.get_field_mapping.assert_called_once()

        singleton.return_value = old_wrapper
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)
        assert [self._knn_shape(body) for body in old_bodies] == [
            ("inside", 2),
            ("outside", 2),
            ("outside", 2),
        ]

    def test_find_neighbors_tries_filter_in_knn_again_once_the_refusal_expires(
        self, mocker
    ):
        bodies = self._opensearch_1_3(mocker)
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)
        key = (99, "2249__embedding_default_embedding", "f2")
        wrapper = vector_db_client.OpenSearchClientSingleton.return_value
        vector_db_client.VectorDbClient._knn_filter_refused_until[wrapper][key] = (
            time.monotonic() - 1
        )

        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        assert [self._knn_shape(body) for body in bodies[2:]] == [
            ("inside", 2),
            ("outside", 2),
        ]

    def test_find_neighbors_keeps_refusal_after_token_refresh(self, mocker):
        self._opensearch_1_3(mocker)
        wrapper = vector_db_client.OpenSearchClientSingleton.return_value
        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)
        transport = wrapper._opensearch_client
        refresh = mocker.patch.object(wrapper, "_refresh_opensearch_connection")
        transport.search.side_effect = [
            AuthenticationException(401, "security_exception", {}),
            self.mock_os_wrapper._search.return_value,
        ]

        self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        refresh.assert_called_once()
        assert [
            self._knn_shape(call.kwargs["body"])
            for call in transport.search.call_args_list[2:]
        ] == [("outside", 2), ("outside", 2)]

    def test_knn_caches_release_discarded_client(self):
        client = MagicMock()
        client.indices.get_field_mapping.return_value = self._field_mapping(
            "f2", "faiss"
        )
        client.search.side_effect = [
            self._opensearch_error(
                "parsing_exception", "[knn] unknown token [START_OBJECT] after [filter]"
            ),
            self.mock_os_wrapper._search.return_value,
        ]
        wrapper = ProjectOpenSearchClient(
            client, feature_store_id=99, is_cluster_client=False
        )
        self.target._knn_search(
            wrapper,
            99,
            "2249__embedding_default_embedding",
            "f2",
            {"vector": [1.0, 2.0, 3.0], "k": 2},
            [{"exists": {"field": "f2"}}],
            {"size": 2},
            None,
        )
        assert vector_db_client.VectorDbClient._field_knn_filter
        assert vector_db_client.VectorDbClient._knn_filter_refused_until
        reference = weakref.ref(wrapper)

        del wrapper, client
        gc.collect()

        assert reference() is None
        assert not vector_db_client.VectorDbClient._field_knn_filter
        assert not vector_db_client.VectorDbClient._knn_filter_refused_until

    def test_find_neighbors_keeps_filter_out_of_knn_for_every_search_of_the_call(
        self, mocker
    ):
        # In a default project index fewer than k can come back, which repeats the search with a larger k after probing the largest k allowed.
        mocker.patch.object(self.embedding_index, "_col_prefix", "1_")
        mocker.patch.object(
            vector_db_client.VectorDbClient, "_index_result_limit_k", {}
        )
        hits = {"hits": {"hits": [{"_score": 1.0, "_source": {"1_f1": 4}}]}}

        def search(body):
            if self._knn_shape(body, "1_f2")[1] == 2**31 - 1:
                raise self._opensearch_error(
                    "search_phase_execution_exception",
                    "all shards failed",
                    caused_by={
                        "type": "illegal_argument_exception",
                        "reason": "[knn] requires k <= 10000",
                    },
                )
            return hits

        bodies = self._opensearch_1_3(mocker, col_name="1_f2", search=search)
        target = vector_db_client.VectorDbClient(self.fg.select_all())

        target._find_neighbors(
            [1.0, 2.0, 3.0], feature=self.f2, k=5, filter=self.f3 > 10
        )

        assert [self._knn_shape(body, "1_f2") for body in bodies] == [
            ("inside", 5),
            ("outside", 5),
            ("outside", 2**31 - 1),
            ("outside", 15),
        ]
        assert all(
            body["query"]["bool"]["filter"]
            == [{"exists": {"field": "1_f2"}}, {"range": {"1_f3": {"gt": 10}}}]
            for body in bodies[1:]
        )

    @pytest.mark.parametrize(
        "engine, error",
        [
            # Any other request the parser refuses is not the knn filter.
            ("faiss", ("parsing_exception", "unknown query [knnn]")),
            # Filters already outside the knn clause have nowhere else to go.
            (
                "nmslib",
                (
                    "parsing_exception",
                    "[knn] unknown token [START_OBJECT] after [filter]",
                ),
            ),
        ],
        ids=["other_parsing_error", "filter_already_outside"],
    )
    def test_find_neighbors_does_not_retry_other_errors(self, mocker, engine, error):
        client = MagicMock()
        client.indices.get_field_mapping.return_value = self._field_mapping(
            "f2", engine
        )
        client.search.side_effect = self._opensearch_error(*error)
        mocker.patch(
            "hsfs.core.vector_db_client.OpenSearchClientSingleton",
            return_value=ProjectOpenSearchClient(
                client, feature_store_id=99, is_cluster_client=False
            ),
        )

        with pytest.raises(VectorDatabaseException):
            self.target._find_neighbors([1.0, 2.0, 3.0], feature=self.f2, k=2)

        assert client.search.call_count == 1
        assert vector_db_client.VectorDbClient._knn_filter_refused_until == {}

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


class TestBatchedEmbeddingReads:
    """One round trip per embedding group, not one per entry.

    A batch of N entries joined to J embedding groups used to cost N times J
    sequential reads before the online store was touched at all.
    """

    def _client(self, mocker, hits_per_entry):
        client = vector_db_client.VectorDbClient.__new__(
            vector_db_client.VectorDbClient
        )
        client._fg_vdb_col_fg_col_map = {1: {"vdb_col": "col"}}
        client._fg_col_vdb_col_map = {1: {"col": "vdb_col"}}
        client._fg_vdb_col_td_col_map = {1: {"vdb_col": "col"}}
        client._fg_embedding_map = {1: mocker.Mock()}
        mocker.patch.object(
            vector_db_client.VectorDbClient,
            "_convert_to_pandas_type",
            side_effect=lambda _s, r: r,
        )
        searches = {}
        opensearch = mocker.Mock()
        opensearch._multi_search.side_effect = lambda body: (
            searches.update(body=body)
            or {"responses": [{"hits": {"hits": hits}} for hits in hits_per_entry]}
        )
        mocker.patch(
            "hsfs.core.vector_db_client.OpenSearchClientSingleton",
            return_value=opensearch,
        )
        return client, opensearch, searches

    def test_one_request_carries_every_entry(self, mocker):
        client, opensearch, searches = self._client(
            mocker,
            [[{"_source": {"vdb_col": 1}}], [{"_source": {"vdb_col": 2}}]],
        )

        results = client._read_many(
            1, ["col"], [{"col": "a"}, {"col": "b"}], index_name="idx"
        )

        assert results == [[{"col": 1}], [{"col": 2}]]
        assert opensearch._multi_search.call_count == 1
        # a header and a body per entry, and the query is the same match query
        assert len(searches["body"]) == 4
        assert searches["body"][0] == {"index": "idx"}
        assert searches["body"][1]["query"] == {
            "bool": {"must": [{"match": {"vdb_col": "a"}}]}
        }

    def test_an_entry_that_matched_nothing_keeps_its_place(self, mocker):
        client, _, _ = self._client(mocker, [[], [{"_source": {"vdb_col": 2}}]])

        results = client._read_many(
            1, ["col"], [{"col": "a"}, {"col": "b"}], index_name="idx"
        )

        assert results == [[], [{"col": 2}]]

    def test_no_entries_is_no_request(self, mocker):
        client, opensearch, _ = self._client(mocker, [])

        assert client._read_many(1, ["col"], [], index_name="idx") == []
        opensearch._multi_search.assert_not_called()

    def test_a_feature_group_without_an_embedding_is_refused(self, mocker):
        client, _, _ = self._client(mocker, [])

        with pytest.raises(FeatureStoreException, match="does not have embedding"):
            client._read_many(99, ["col"], [{"col": "a"}], index_name="idx")
