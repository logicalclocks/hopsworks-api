#
#   Copyright 2023 Logical Clocks AB
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
from __future__ import annotations

import base64
import contextlib
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from hopsworks_common.client.exceptions import (
    FeatureStoreException,
    VectorDatabaseException,
)
from hopsworks_common.util import convert_event_time_to_timestamp
from hsfs.constructor.filter import Filter, Logic
from hsfs.constructor.join import Join
from hsfs.core.opensearch import OpenSearchClientSingleton


if TYPE_CHECKING:
    import hsfs
    from hsfs.feature import Feature


class VectorDbClient:
    _filter_map = {
        Filter.GT: "gt",
        Filter.GE: "gte",
        Filter.LT: "lt",
        Filter.LE: "lte",
    }
    _index_result_limit_k = {}
    _index_result_limit_n = {}

    def __init__(self, query, serving_keys=None):
        self._query = query
        self._embedding_features = {}
        self._fg_vdb_col_fg_col_map = {}
        self._fg_vdb_col_td_col_map = {}
        self._fg_col_vdb_col_map = {}
        self._fg_embedding_map = {}
        self._td_embedding_feature_names = set()
        self._embedding_fg_by_join_index = {}
        self._fg_id_to_vdb_pks = {}
        self._serving_keys = serving_keys
        self._serving_key_by_serving_index: dict[int, hsfs.serving_key.ServingKey] = {}
        self.init()

    def init(self):
        for fg in self._query.featuregroups:
            if fg.embedding_index:
                for feat in fg.embedding_index.get_embeddings():
                    for fgf in fg.features:
                        if fgf.name == feat.name and fgf.feature_group_id == fg.id:
                            self._embedding_features[fgf] = feat
        for q in [self._query] + [j.query for j in self._query.joins]:
            fg = q._left_feature_group
            if fg.embedding_index:
                vdb_col_fg_col_map = {}
                fg_col_vdb_col_map = {}
                for f in q._left_features:
                    vdb_col_fg_col_map[fg.embedding_index.col_prefix + f.name] = f
                    fg_col_vdb_col_map[f.name] = fg.embedding_index.col_prefix + f.name
                # add primary key to the map in case it is not selected as feature
                for pk in q._left_feature_group.primary_key:
                    vdb_col_fg_col_map[fg.embedding_index.col_prefix + pk] = (
                        q._left_feature_group[pk]
                    )
                    fg_col_vdb_col_map[pk] = fg.embedding_index.col_prefix + pk
                self._fg_vdb_col_fg_col_map[fg.id] = vdb_col_fg_col_map
                self._fg_id_to_vdb_pks[fg.id] = [
                    fg_col_vdb_col_map[pk] for pk in fg.primary_key
                ]
                self._fg_col_vdb_col_map[fg.id] = fg_col_vdb_col_map
                self._fg_embedding_map[fg.id] = fg.embedding_index
        # create a join for the left fg so that the dict can be constructed in one loop
        fg_joins = [Join(self._query, None, None, None, None, "")] + self._query.joins
        # join in dex start from 0, 0 means left fg
        for i, join in enumerate(fg_joins):
            join_fg = join.query._left_feature_group
            if join_fg.embedding_index:
                if join_fg.id in self._fg_vdb_col_td_col_map:
                    # `self._fg_vdb_col_td_col_map` do not support join of same fg
                    raise FeatureStoreException(
                        "Do not support join of same fg multiple times."
                    )
                self._embedding_fg_by_join_index[i] = join_fg
                for embedding_feature in join_fg.embedding_index.get_embeddings():
                    self._td_embedding_feature_names.add(
                        (join.prefix or "") + embedding_feature.name
                    )
                vdb_col_td_col_map = {}
                for feat in join_fg.features:
                    vdb_col_td_col_map[
                        join_fg.embedding_index.col_prefix + feat.name
                    ] = (join.prefix or "") + feat.name  # join.prefix can be None
                self._fg_vdb_col_td_col_map[join_fg.id] = vdb_col_td_col_map

    def find_neighbors(
        self,
        embedding,
        feature: Feature = None,
        index_name=None,
        k=10,
        filter: Filter | Logic = None,
        options=None,
    ):
        if not feature:
            if not self._embedding_features:
                raise ValueError("embedding col is not defined.")
            if len(self._embedding_features) > 1:
                raise ValueError("More than 1 embedding columns but col is not defined")
            embedding_feature = list(self._embedding_features.values())[0]
        else:
            embedding_feature = self._embedding_features.get(feature, None)
            if embedding_feature is None:
                raise ValueError(
                    f"feature: {feature.name} is not an embedding feature."
                )
        self._check_filter(filter, embedding_feature.feature_group)
        col_name = embedding_feature.embedding_index.col_prefix + embedding_feature.name
        query = {
            "size": k,
            "query": {
                "bool": {
                    "must": [
                        {"knn": {col_name: {"vector": embedding, "k": k}}},
                        {"exists": {"field": col_name}},
                    ]
                    + self._get_query_filter(
                        filter, embedding_feature.embedding_index.col_prefix
                    )
                }
            },
            "_source": list(
                self._fg_vdb_col_fg_col_map.get(
                    embedding_feature.feature_group.id
                ).keys()
            ),
        }
        if not index_name:
            index_name = embedding_feature.embedding_index.index_name

        opensearch_client = OpenSearchClientSingleton(
            feature_store_id=embedding_feature.feature_group.feature_store_id
        )
        results = opensearch_client.search(
            body=query, index=index_name, options=options
        )

        # When using project index (`embedding_feature.embedding_index.col_prefix` is not empty), sometimes the total number of result returned is less than k. Possible reason is that when using project index, some embedding columns have null value if the row is from a different feature group. And opensearch filter out the result where embedding is null after retrieving the top k results. So search 3 times more data if it is using project index and size of result is not k.
        if (
            embedding_feature.embedding_index.col_prefix
            and len(results["hits"]["hits"]) != k
        ):
            # Get the max number of results allowed to request if it is not available.
            # This is expected to be executed once only.
            if not VectorDbClient._index_result_limit_k.get(index_name):
                query["query"]["bool"]["must"][0]["knn"][col_name]["k"] = 2**31 - 1
                try:
                    # It is expected that this request ALWAYS fails because requested k is too large.
                    # The purpose here is to get the max k allowed from the vector database, and cache it.
                    opensearch_client.search(
                        body=query, index=index_name, options=options
                    )
                except VectorDatabaseException as e:
                    if (
                        e.reason == VectorDatabaseException.REQUESTED_K_TOO_LARGE
                        and e.info.get(
                            VectorDatabaseException.REQUESTED_K_TOO_LARGE_INFO_K
                        )
                    ):
                        VectorDbClient._index_result_limit_k[index_name] = e.info.get(
                            VectorDatabaseException.REQUESTED_K_TOO_LARGE_INFO_K
                        )
                    else:
                        raise e
            query["query"]["bool"]["must"][0]["knn"][col_name]["k"] = min(
                VectorDbClient._index_result_limit_k.get(index_name, k), 3 * k
            )
            results = opensearch_client.search(
                body=query, index=index_name, options=options
            )

        # https://opensearch.org/docs/latest/search-plugins/knn/approximate-knn/#spaces
        return [
            (
                1 / item["_score"] - 1,
                self._convert_to_pandas_type(
                    embedding_feature.feature_group.features,
                    self._rewrite_result_key(
                        item["_source"],
                        self._fg_vdb_col_td_col_map[embedding_feature.feature_group.id],
                    ),
                ),
            )
            for item in results["hits"]["hits"]
        ]

    def _convert_to_pandas_type(self, schema, result):
        for feature in schema:
            feature_name = feature.name
            feature_type = feature.type.lower()
            feature_value = result.get(feature_name)
            if not feature_value:  # Feature value can be null
                continue
            if feature_type == "date":
                result[feature_name] = datetime.fromtimestamp(
                    feature_value // 10**3, tz=timezone.utc
                ).date()
            elif feature_type == "timestamp":
                # convert timestamp in ms to datetime in s
                result[feature_name] = datetime.fromtimestamp(
                    feature_value // 10**3, tz=timezone.utc
                )
            elif feature_type == "binary" or (
                feature.is_complex() and feature not in self._embedding_features
            ):
                result[feature_name] = base64.b64decode(feature_value)
        return result

    def _check_filter(self, filter, fg):
        if not filter:
            return
        if isinstance(filter, Filter):
            if filter.feature.feature_group_id != fg.id:
                raise FeatureStoreException(
                    f"filter feature should be from feature group '{fg.name}' version '{fg.version}'"
                )
        elif isinstance(filter, Logic):
            self._check_filter(filter.get_right_filter_or_logic(), fg)
            self._check_filter(filter.get_left_filter_or_logic(), fg)
        else:
            raise FeatureStoreException("filter should be of type `Filter` or `Logic`")

    def _get_query_filter(self, filter, col_prefix=None):
        if not filter:
            return []
        if isinstance(filter, Filter):
            return [self._convert_filter(filter, col_prefix)]
        if isinstance(filter, Logic):
            if filter.type == Logic.SINGLE:
                return self._get_query_filter(
                    filter.get_left_filter_or_logic(), col_prefix
                ) or self._get_query_filter(
                    filter.get_right_filter_or_logic(), col_prefix
                )
            if filter.type == Logic.AND:
                return [
                    {
                        "bool": {
                            "must": (
                                self._get_query_filter(
                                    filter.get_left_filter_or_logic(), col_prefix
                                )
                                + self._get_query_filter(
                                    filter.get_right_filter_or_logic(), col_prefix
                                )
                            )
                        }
                    }
                ]
            if filter.type == Logic.OR:
                return [
                    {
                        "bool": {
                            "should": (
                                self._get_query_filter(
                                    filter.get_left_filter_or_logic(), col_prefix
                                )
                                + self._get_query_filter(
                                    filter.get_right_filter_or_logic(), col_prefix
                                )
                            ),
                            "minimum_should_match": 1,
                        }
                    }
                ]
            raise FeatureStoreException(f"filter type {filter.type} not defined.")

        raise FeatureStoreException("filter should be of type `Filter` or `Logic`")

    def _convert_filter(self, filter, col_prefix=None):
        condition = filter.condition
        if col_prefix:
            feature_name = col_prefix + filter.feature.name
        else:
            feature_name = filter.feature.name

        # Get feature type for value conversion
        feature_type = filter.feature.type.lower()

        # Convert filter value based on feature type
        converted_value = self._convert_filter_value(
            filter.value, feature_type, condition
        )

        if condition == Filter.EQ:
            return {"term": {feature_name: converted_value}}
        if condition == filter.NE:
            return {"bool": {"must_not": [{"term": {feature_name: converted_value}}]}}
        if condition == filter.IN:
            return {"terms": {feature_name: converted_value}}
        if condition == filter.LK:
            # LIKE condition uses string value, no conversion needed
            return {
                "wildcard": {feature_name: {"value": "*" + str(filter.value) + "*"}}
            }
        if condition in self._filter_map:
            return {
                "range": {feature_name: {self._filter_map[condition]: converted_value}}
            }
        raise FeatureStoreException("Filter condition not defined.")

    def _convert_filter_value(self, value, feature_type, condition):
        """Convert filter value based on feature type for OpenSearch compatibility."""
        # Handle IN condition - value might be a JSON string or a list
        if condition == Filter.IN:
            # Parse JSON string if needed (as created by Feature.isin())
            if isinstance(value, str):
                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    value = json.loads(value)

            # Convert list items
            if isinstance(value, list):
                converted_list = []
                for item in value:
                    converted_list.append(
                        self._convert_filter_value(item, feature_type, Filter.EQ)
                    )
                return converted_list

        # Handle boolean type
        if feature_type == "boolean":
            if isinstance(value, bool):
                if value:
                    return 1
                return 0
            if isinstance(value, str):
                if value.lower() == "true":
                    return 1
                if value.lower() == "false":
                    return 0
            else:
                raise FeatureStoreException(f"Invalid boolean value: {value}")

        # Handle timestamp type
        elif feature_type == "timestamp":
            # Convert timestamp strings to epoch milliseconds for OpenSearch
            if isinstance(value, str):
                # Convert timestamp string to epoch milliseconds
                return convert_event_time_to_timestamp(value)
            # If already int (epoch milliseconds), return as-is
            return value

        # For other types, return value as-is
        return value

    def _rewrite_result_key(self, result_map, key_map):
        new_map = {}
        for key, value in result_map.items():
            new_key = key_map.get(key)
            if new_key is None:
                raise FeatureStoreException(
                    f"Feature '{key}' from embedding feature group is not found in the query"
                )
            new_map[new_key] = value
        return new_map

    def read(
        self,
        fg_id,
        schema,
        keys=None,
        pk=None,
        index_name=None,
        n=10,
        filter: Filter | Logic = None,
    ):
        if fg_id not in self._fg_vdb_col_fg_col_map:
            raise FeatureStoreException("Provided fg does not have embedding.")
        if not index_name:
            index_name = self._get_vector_db_index_name(fg_id)
        opensearch_client = OpenSearchClientSingleton(
            feature_store_id=self._fg_embedding_map[
                fg_id
            ].feature_group.feature_store_id
        )
        if keys:
            # Do not add filter query to keys match query
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {key: value}}
                            for key, value in self._rewrite_result_key(
                                keys, self._fg_col_vdb_col_map[fg_id]
                            ).items()
                        ]
                    }
                },
            }
        else:
            if not pk:
                raise FeatureStoreException("No pk provided.")

            if filter:
                self._check_filter(filter, self._fg_embedding_map[fg_id].feature_group)
                filter_query = self._get_query_filter(
                    filter,
                    self._fg_embedding_map[
                        fg_id
                    ].feature_group.embedding_index.col_prefix,
                )
            else:
                filter_query = []
            query = {
                "query": {"bool": {"must": [{"exists": {"field": pk}}] + filter_query}},
                "size": n,
            }
            if n is None:
                if VectorDbClient._index_result_limit_n.get(index_name) is None:
                    try:
                        query["size"] = 2**31 - 1
                        opensearch_client.search(body=query, index=index_name)
                    except VectorDatabaseException as e:
                        if (
                            e.reason
                            == VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE
                            and e.info.get(
                                VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE_INFO_N
                            )
                        ):
                            VectorDbClient._index_result_limit_n[index_name] = (
                                e.info.get(
                                    VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE_INFO_N
                                )
                            )
                        else:
                            raise e
                query["size"] = VectorDbClient._index_result_limit_n.get(index_name)
        query["_source"] = list(self._fg_vdb_col_fg_col_map.get(fg_id).keys())
        results = opensearch_client.search(body=query, index=index_name)
        # https://opensearch.org/docs/latest/search-plugins/knn/approximate-knn/#spaces
        return [
            self._convert_to_pandas_type(
                schema,
                self._rewrite_result_key(
                    item["_source"], self._fg_vdb_col_td_col_map[fg_id]
                ),
            )
            for item in results["hits"]["hits"]
        ]

    @staticmethod
    def read_feature_group(
        feature_group: hsfs.feature_group.FeatureGroup,
        n: int = None,
        filter: Filter | Logic = None,
    ) -> list:
        if feature_group.embedding_index:
            vector_db_client = VectorDbClient(feature_group.select_all())
            results = vector_db_client.read(
                feature_group.id,
                feature_group.features,
                pk=feature_group.embedding_index.col_prefix
                + feature_group.primary_key[0],
                index_name=feature_group.embedding_index.index_name,
                n=n,
                filter=filter,
            )
            return [
                [result[f.name] for f in feature_group.features] for result in results
            ]
        raise FeatureStoreException("Feature group does not have embedding.")

    def count(self, fg, options=None):
        query = {
            "query": {
                "bool": {
                    "must": {"exists": {"field": self._fg_id_to_vdb_pks[fg.id][0]}}
                }
            },
        }
        return OpenSearchClientSingleton(feature_store_id=fg.feature_store_id).count(
            self._get_vector_db_index_name(fg.id), query, options=options
        )

    def _get_vector_db_index_name(self, fg_id):
        embedding = self._fg_embedding_map.get(fg_id)
        if embedding is None:
            raise ValueError("No embedding fg available.")
        return embedding.index_name

    def filter_entry_by_join_index(
        self, entry: dict[str, Any], join_index: int
    ) -> tuple[bool, dict[str, Any]]:
        fg_entry = {}
        complete = True
        for sk in self.serving_key_by_serving_index[join_index]:
            fg_entry[sk.feature_name] = entry.get(sk.required_serving_key) or entry.get(
                sk.feature_name
            )  # fallback to use raw feature name
            if fg_entry[sk.feature_name] is None:
                complete = False
                break
        return complete, fg_entry

    @property
    def serving_keys(self) -> list[hsfs.serving_key.ServingKey] | None:
        return self._serving_keys

    @property
    def embedding_fg_by_join_index(self):
        return self._embedding_fg_by_join_index

    @property
    def serving_key_by_serving_index(self) -> dict[int, hsfs.serving_key.ServingKey]:
        if len(self._serving_key_by_serving_index) > 0:
            return self._serving_key_by_serving_index

        if self.serving_keys is not None:
            for sk in self.serving_keys:
                self._serving_key_by_serving_index[sk.join_index] = (
                    self._serving_key_by_serving_index.get(sk.join_index, []) + [sk]
                )
        else:
            self._serving_key_by_serving_index = {}
        return self._serving_key_by_serving_index

    @property
    def td_embedding_feature_names(self):
        return self._td_embedding_feature_names
