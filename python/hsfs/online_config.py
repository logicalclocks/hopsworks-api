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
INDEX_TYPE_ORDERED = "ORDERED"
_VALID_INDEX_TYPES = (INDEX_TYPE_HASH, INDEX_TYPE_ORDERED)


@public
class OnlineConfig:
    """Metadata object used to provide online Feature Store configuration information for a feature group."""

    def __init__(
        self,
        online_comments: list[str] = None,
        table_space: str = None,
        primary_key_index_type: str = None,
        secondary_indexes: list[list[str]] = None,
        **kwargs,
    ):
        self._online_comments = online_comments
        self._table_space = table_space
        self._primary_key_index_type = self._normalize_primary_key_index_type(
            primary_key_index_type
        )
        self._secondary_indexes = self._validate_secondary_indexes(secondary_indexes)

    @staticmethod
    def _normalize_primary_key_index_type(value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(
                f"primary_key_index_type must be a string or None, "
                f"got {type(value).__name__}."
            )
        normalized = value.upper()
        if normalized not in _VALID_INDEX_TYPES:
            raise ValueError(
                f"Invalid primary_key_index_type '{value}'. "
                f"Allowed values: {', '.join(_VALID_INDEX_TYPES)}."
            )
        return normalized

    @staticmethod
    def _validate_secondary_indexes(
        value: list[list[str]] | None,
    ) -> list[list[str]] | None:
        if value is None:
            return None
        if not isinstance(value, list):
            raise TypeError(
                f"secondary_indexes must be a list of column-name lists or None, "
                f"got {type(value).__name__}."
            )
        seen_indexes = set()
        for i, idx in enumerate(value):
            if not isinstance(idx, list) or len(idx) == 0:
                raise ValueError(
                    f"secondary_indexes[{i}] must be a non-empty list of column names."
                )
            seen_columns = set()
            for col in idx:
                if not isinstance(col, str) or not col:
                    raise ValueError(
                        f"secondary_indexes[{i}] contains an invalid column name: {col!r}."
                    )
                if col in seen_columns:
                    raise ValueError(
                        f"secondary_indexes[{i}] contains duplicate column name: {col!r}."
                    )
                seen_columns.add(col)
            index_definition = tuple(idx)
            if index_definition in seen_indexes:
                raise ValueError(
                    f"secondary_indexes contains duplicate index definition at position {i}: {idx!r}."
                )
            seen_indexes.add(index_definition)
        return value

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> OnlineConfig:
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        d = {
            "onlineComments": self._online_comments,
            "tableSpace": self._table_space,
            "primaryKeyIndexType": self._primary_key_index_type,
        }
        if self._secondary_indexes is not None:
            d["secondaryIndexes"] = self._secondary_indexes
        return d

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
    def primary_key_index_type(self) -> str | None:
        """Primary key index type for the online feature store table.

        Controls which RonDB index structures back the primary key.

        Allowed values:
            * `"HASH"` — Hash-only index.
              Optimal for point lookups (`WHERE pk = ?`).
              Lowest memory footprint and write overhead.
              Range scans over the primary key are not supported.
              Choose this for pure online-serving feature groups that only retrieve by exact key.
            * `"ORDERED"` — Both hash and ordered indexes on the primary key (RonDB default; emitted as `PRIMARY KEY (...)` with no `USING` modifier).
              Supports point lookups via the hash index and range scans via the ordered index.
              Costs more memory and write throughput than a hash-only index.
              Choose this when access patterns are mixed or when range scans over the primary key are needed.
            * `None` (default, unset) — The server applies a TTL-driven fallback: hash-only (`USING HASH`) when TTL is disabled, and the NDB default (hash + ordered) when TTL is enabled.

        Warning: Create-only
            Set at feature group creation time only.
            The primary key index type cannot be changed after the table has been created.
            Changing it would require `DROP`/`ADD PRIMARY KEY` which is not in-place safe under concurrent ingestion.
        """
        return self._primary_key_index_type

    @primary_key_index_type.setter
    def primary_key_index_type(self, primary_key_index_type: str | None) -> None:
        self._primary_key_index_type = self._normalize_primary_key_index_type(
            primary_key_index_type
        )

    @public
    @property
    def secondary_indexes(self) -> list[list[str]] | None:
        """Secondary indexes for the online feature store table.

        Each element is a list of feature names that form one index.
        The backend names each index automatically as `idx_<col1>_<col2>_...`.

        Example:
            ```python
            OnlineConfig(secondary_indexes=[["user_id"], ["country", "city"]])
            ```

        Generates DDL:
            ```sql
            KEY `idx_user_id`(`user_id`),
            KEY `idx_country_city`(`country`,`city`)
            ```

        Warning: Create-only
            Set at feature group creation time only.
            Indexes cannot be added or removed after the table has been created without recreating it.
        """
        return self._secondary_indexes

    @secondary_indexes.setter
    def secondary_indexes(self, value: list[list[str]] | None) -> None:
        self._secondary_indexes = self._validate_secondary_indexes(value)
