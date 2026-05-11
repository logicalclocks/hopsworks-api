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
from __future__ import annotations

from typing import Any

import humps
from hopsworks_apigen import public


INDEX_TYPE_HASH = "HASH"
INDEX_TYPE_BTREE = "BTREE"
INDEX_TYPE_HASH_AND_BTREE = "HASH_AND_BTREE"
_VALID_INDEX_TYPES = (INDEX_TYPE_HASH, INDEX_TYPE_BTREE, INDEX_TYPE_HASH_AND_BTREE)


@public
class OnlineConfig:
    """Metadata object used to provide online Feature Store configuration information for a feature group."""

    def __init__(
        self,
        online_comments: list[str] = None,
        table_space: str = None,
        primary_key_index_type: str = None,
        **kwargs,
    ):
        self._online_comments = online_comments
        self._table_space = table_space
        self._primary_key_index_type = self._normalize_primary_key_index_type(
            primary_key_index_type
        )

    @staticmethod
    def _normalize_primary_key_index_type(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.upper()
        if normalized not in _VALID_INDEX_TYPES:
            raise ValueError(
                f"Invalid primary_key_index_type '{value}'. "
                f"Allowed values: {', '.join(_VALID_INDEX_TYPES)}."
            )
        return normalized

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> OnlineConfig:
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "onlineComments": self._online_comments,
            "tableSpace": self._table_space,
            "primaryKeyIndexType": self._primary_key_index_type,
        }

    @public
    @property
    def online_comments(self) -> list[str]:
        """List of comments applied to online feature store table."""
        return self._online_comments

    @online_comments.setter
    def online_comments(self, online_comments: list[str]) -> None:
        self._online_comments = online_comments

    @public
    @property
    def table_space(self) -> str:
        """Table space of online feature store table for storing data on disk."""
        return self._table_space

    @table_space.setter
    def table_space(self, table_space: str) -> None:
        self._table_space = table_space

    @public
    @property
    def primary_key_index_type(self) -> str:
        """Primary key index type for the online feature store table.

        Controls which RonDB index structures back the primary key.

        Allowed values:
            * `"HASH"` — Hash-only index.
              Optimal for point lookups (`WHERE pk = ?`).
              Lowest memory footprint and write overhead.
              Range scans over the primary key are not supported.
              Choose this for pure online-serving feature groups that only retrieve by exact key.
            * `"BTREE"` — Ordered-only index (RonDB T-tree, exposed as `USING BTREE`).
              Supports range scans over the primary key but has slower point lookups than a hash index.
              Rarely needed for online feature groups; prefer this only when you query the primary key with range predicates such as `BETWEEN` or `>=`.
            * `"HASH_AND_BTREE"` — Both hash and ordered indexes on the primary key (RonDB default when no `USING` modifier is given).
              Supports point lookups via the hash index and range scans via the ordered index.
              Costs more memory and write throughput than either single index.
              Choose this when access patterns are mixed or unknown.
            * `None` (default, unset) — No modifier emitted.
              The server applies a TTL-driven fallback: hash-only (`USING HASH`) when TTL is disabled, and `HASH_AND_BTREE` when TTL is enabled so that the TTL cleaner can range-scan by event time.

        Warning: Create-only
            Set at feature group creation time only.
            The primary key index type cannot be changed after the table has been created.
            Changing it would require `DROP`/`ADD PRIMARY KEY` which is not in-place safe under concurrent ingestion.
        """
        return self._primary_key_index_type

    @primary_key_index_type.setter
    def primary_key_index_type(self, primary_key_index_type: str) -> None:
        self._primary_key_index_type = self._normalize_primary_key_index_type(
            primary_key_index_type
        )
