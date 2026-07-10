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

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core import variable_api
from hopsworks_common.util import AsyncTask, AsyncTaskThread
from hsfs import util
from hsfs.core import (
    feature_view_api,
    storage_connector_api,
    training_dataset_api,
)
from hsfs.core.constants import HAS_AIOMYSQL, HAS_SQLALCHEMY


if TYPE_CHECKING:
    import aiomysql
    import aiomysql.utils
    from hsfs import feature_view, storage_connector, training_dataset
    from hsfs.constructor.serving_prepared_statement import ServingPreparedStatement
    from hsfs.serving_key import ServingKey


if HAS_AIOMYSQL:
    pass

if HAS_SQLALCHEMY:
    from sqlalchemy import bindparam, exc, sql, text

if HAS_AIOMYSQL and HAS_SQLALCHEMY:
    from hsfs.core import util_sql


_logger = logging.getLogger(__name__)


class OnlineStoreSqlClient:
    # Alias of the ROW_NUMBER column the backend adds to a collect (most recent N rows per
    # entity) prepared statement. It is dropped when folding the N rows into list features.
    COLLECT_RANK_ALIAS = "hopsworks_collect_rank"
    # collect batch scans (review X9): extra pool connections so per-entry scans run
    # concurrently (a collect-only view would otherwise serialize on a one-connection
    # pool), and the per-wave task bound that caps in-flight cursors for large batches
    _COLLECT_SCAN_POOL_SIZE = 8
    _COLLECT_SCAN_CHUNK = 64
    # Bind-parameter name of an aggregation statement's trailing window bound.
    WINDOW_BOUND_PARAM = "hw_window_bound"
    # Bind-parameter name of a collect statement's rank cap (<= collect N; scans narrow it).
    RANK_CAP_PARAM = "hw_rank_cap"

    BATCH_HELPER_KEY = "batch_helper_column"
    SINGLE_HELPER_KEY = "single_helper_column"
    BATCH_VECTOR_KEY = "batch_feature_vectors"
    SINGLE_VECTOR_KEY = "single_feature_vector"
    SINGLE_LOGGING_VECTOR_KEY = "single_logging_feature_vector"
    BATCH_LOGGING_VECTOR_KEY = "batch_logging_feature_vector"
    # Combined keys for features + inference helpers in single query
    SINGLE_VECTOR_WITH_INFERENCE_HELPERS_KEY = (
        "single_feature_vector_with_inference_helpers"
    )
    BATCH_VECTOR_WITH_INFERENCE_HELPERS_KEY = (
        "batch_feature_vectors_with_inference_helpers"
    )

    def __init__(
        self,
        feature_store_id: id,
        skip_fg_ids: set[int] | None,
        external: bool,
        serving_keys: set[ServingKey] | None = None,
        connection_options: dict[str, Any] | None = None,
    ):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Initialising Online Store Sql Client")
        self._feature_store_id = feature_store_id
        self._skip_fg_ids: set[int] = skip_fg_ids or set()
        self._external = external

        self._prefix_by_serving_index = None
        self._pkname_by_serving_index = None
        # prepared_statement_index -> collect N (None when the statement is a regular point read)
        self._collect_n_by_serving_index: dict[int, int] = {}
        self._collect_name_by_serving_index: dict[int, str] = {}
        # prepared_statement_index -> aggregation window seconds, for statements whose
        # trailing ? is the window's lower bound (bound at read time from client UTC)
        self._aggregate_window_by_serving_index: dict[int, int] = {}
        # prepared_statement_index -> collect N, for statements whose trailing ? caps
        # the rank filter (folds bind N; scans narrow to min(limit, N))
        self._rank_cap_by_serving_index: dict[int, int] = {}
        # statement label -> prepared_statement_index -> direct single-entity scan
        # statement (ORDER BY order_col DESC LIMIT ?), preferred over the windowed
        # collect statement by _get_scan_rows and the single-vector fold. Keyed per
        # label because each label's scan projects that label's columns (a logging
        # variant's scan carries helper columns the plain vector must not return).
        # Empty for backends predating the field.
        self._scan_prepared_statements: dict[str, dict[int, Any]] = {}
        # prepared_statement_index -> collect_ascending, to order direct-scan output
        self._collect_ascending_by_serving_index: dict[int, bool] = {}
        # prepared_statement_index -> prefixed aggregate output names (empty-set defaults)
        self._aggregate_names_by_serving_index: dict[int, list[str]] = {}
        self._serving_key_by_serving_index: dict[str, ServingKey] = {}
        self._serving_keys: set[ServingKey] = set(serving_keys or [])

        self._prepared_statements: dict[str, list[ServingPreparedStatement]] = {}
        self._parametrised_prepared_statements = {}
        self._prepared_statement_engine = None

        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()
        self._online_connector = None
        self._hostname = None
        self._connection_options = None

        self._async_task_thread = None

    def __del__(self):
        # Safely stop the async task thread.
        # The connection pool will be closed during garbage collection by aiomysql.
        task_thread = getattr(self, "_async_task_thread", None)
        if task_thread is not None and task_thread.is_alive():
            task_thread._stop()

    def _fetch_prepared_statements(
        self,
        entity: feature_view.FeatureView | training_dataset.TrainingDataset,
        inference_helper_columns: bool,
        with_logging_meta_data: bool = False,
        feature_vector_with_inference_helpers: bool = False,
    ) -> None:
        """Fetch prepared statement for feature vector retrival from the backend.

        Parameters:
            entity : FeatureView or TrainingDataset object to fetch prepared statements for.
            inference_helper_columns : Fetch prepared statements for inference helper columns.
            with_logging_meta_data : Fetch prepared statements to include logging meta data.
                i.e The fetched data will include the features along with inference helper columns.
            feature_vector_with_inference_helpers : Fetch prepared statements for combined
                features + inference helpers in single query for on-demand transformation support.
        """
        if hasattr(entity, "_feature_view_engine"):
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Initialising prepared statements for feature view {entity.name} version {entity.version}."
                )
            for key in self._get_prepared_statement_labels(
                inference_helper_columns,
                with_logging_meta_data,
                feature_vector_with_inference_helpers,
            ):
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(f"Fetching prepared statement for key {key}")
                self.prepared_statements[key] = (
                    self.feature_view_api._get_serving_prepared_statement(
                        entity.name,
                        entity.version,
                        batch=key.startswith("batch"),
                        inference_helper_columns=key.endswith("helper_column"),
                        logging_meta_data="logging" in key,
                        feature_vector_with_inference_helpers="_with_inference_helpers"
                        in key,
                    )
                )
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(f"{self.prepared_statements[key]}")
        elif hasattr(entity, "_training_dataset_type"):
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Initialising prepared statements for training dataset {entity.name} version {entity.version}."
                )
            for key in self._get_prepared_statement_labels(
                with_inference_helper_column=False,
                with_logging_meta_data=with_logging_meta_data,
            ):
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(f"Fetching prepared statement for key {key}")
                self.prepared_statements[key] = (
                    self.training_dataset_api._get_serving_prepared_statement(
                        entity, batch=key.startswith("batch")
                    )
                )
        else:
            raise ValueError(
                "Object type needs to be `feature_view.FeatureView` or `training_dataset.TrainingDataset`."
            )

        if len(self.skip_fg_ids) > 0:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Skip feature groups {self.skip_fg_ids} when initialising prepared statements."
                )
            self.prepared_statements[key] = {
                ps
                for ps in self.prepared_statements[key]
                if ps.feature_group_id not in self.skip_fg_ids
            }

    def _init_prepared_statements(
        self,
        entity: feature_view.FeatureView | training_dataset.TrainingDataset,
        inference_helper_columns: bool,
        with_logging_meta_data: bool = False,
        feature_vector_with_inference_helpers: bool = False,
    ) -> None:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Fetch and reset prepared statements and external as user may be re-initialising with different parameters"
            )
        # derived bind/statement state is rebuilt below; a re-initialization must
        # not inherit entries from the previous statement set (a stale window or
        # rank-cap index would mis-bind the new statements)
        self._aggregate_window_by_serving_index = {}
        self._rank_cap_by_serving_index = {}
        self._parametrised_prepared_statements = {}
        self._scan_prepared_statements = {}
        self._fetch_prepared_statements(
            entity,
            inference_helper_columns,
            with_logging_meta_data=with_logging_meta_data,
            feature_vector_with_inference_helpers=feature_vector_with_inference_helpers,
        )

        self._init_parametrize_and_serving_utils(
            self.prepared_statements[self.BATCH_VECTOR_KEY]
        )

        for key in self._get_prepared_statement_labels(
            inference_helper_columns,
            with_logging_meta_data,
            feature_vector_with_inference_helpers,
        ):
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(f"Parametrize prepared statements for key {key}")
            self._parametrised_prepared_statements[key] = (
                self._parametrize_prepared_statements(
                    self.prepared_statements[key],
                    batch=key.startswith("batch"),
                    label=key,
                )
            )

    def _init_parametrize_and_serving_utils(
        self,
        prepared_statements: list[ServingPreparedStatement],
    ) -> None:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Initializing parametrize and serving utils property using %s",
                json.dumps(prepared_statements, default=lambda x: x.__dict__, indent=2),
            )
        self.prefix_by_serving_index = {
            statement.prepared_statement_index: statement.prefix
            for statement in prepared_statements
        }
        self._collect_n_by_serving_index = {
            statement.prepared_statement_index: statement.collect_n
            for statement in prepared_statements
            if statement.collect_n is not None
        }
        # v2 C1: the feature-view feature name (prefix applied, like every other column
        # alias in the statement) that the collect rows fold into as a list of structs.
        self._collect_name_by_serving_index = {
            statement.prepared_statement_index: (statement.prefix or "")
            + statement.collect_feature_name
            for statement in prepared_statements
            if statement.collect_n is not None
            and statement.collect_feature_name is not None
        }
        self._collect_ascending_by_serving_index = {
            statement.prepared_statement_index: bool(
                getattr(statement, "collect_ascending", False)
            )
            for statement in prepared_statements
            if statement.collect_n is not None
        }
        # aggregate output names, for synthesizing the empty-set defaults an entity
        # missed by the batch GROUP BY would have received from the single statement
        self._aggregate_names_by_serving_index = {
            statement.prepared_statement_index: list(
                getattr(statement, "aggregate_feature_names", None) or []
            )
            for statement in prepared_statements
            if getattr(statement, "aggregate_feature_names", None)
        }
        self._feature_name_order_by_psp = {
            statement.prepared_statement_index: {
                param.name: param.index
                for param in statement.prepared_statement_parameters
            }
            for statement in prepared_statements
        }
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Build serving keys by PreparedStatementParameter.index")
        # rebuild from scratch: the additive loop below would otherwise duplicate
        # every serving key on re-initialization
        self._serving_key_by_serving_index = {}
        for sk in self._serving_keys:
            self.serving_key_by_serving_index[sk.join_index] = (
                self.serving_key_by_serving_index.get(sk.join_index, []) + [sk]
            )

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Sort serving keys by PreparedStatementParameter.index")

        for join_index in self.serving_key_by_serving_index:
            # feature_name_order_by_psp do not include the join index when the joint feature only contains label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if join_index in self._feature_name_order_by_psp:
                self.serving_key_by_serving_index[join_index] = sorted(
                    self.serving_key_by_serving_index[join_index],
                    key=lambda _sk, join_index=join_index: (
                        self.feature_name_order_by_psp[join_index].get(
                            _sk.feature_name, 0
                        )
                    ),
                )

    def _parametrize_prepared_statements(
        self,
        prepared_statements: list[ServingPreparedStatement],
        batch: bool,
        label: str = SINGLE_VECTOR_KEY,
    ) -> dict[int, sql.text]:
        prepared_statements_dict = {}
        for prepared_statement in prepared_statements:
            if prepared_statement.feature_group_id in self._skip_fg_ids:
                continue
            query_online = str(prepared_statement.query_online).replace("\n", " ")

            if not batch:
                for param in prepared_statement.prepared_statement_parameters:
                    query_online = self._parametrize_query(param.name, query_online)
            else:
                query_online = self._parametrize_query("batch_ids", query_online)
            if (
                getattr(prepared_statement, "aggregate_window", None) is not None
                and self._has_unquoted_placeholder(query_online)
            ):
                # pushdown aggregation (FSTORE-2059): the trailing ? is the window's
                # lower bound, bound at read time as now - window from the client's
                # UTC clock — the same reference the RonSQL path substitutes, so the
                # two serving paths share one clock. A backend predating the
                # parameter inlines NOW(6) and leaves no marker here.
                query_online = self._parametrize_query(
                    self.WINDOW_BOUND_PARAM, query_online
                )
                self._aggregate_window_by_serving_index[
                    prepared_statement.prepared_statement_index
                ] = prepared_statement.aggregate_window
            if (
                getattr(prepared_statement, "collect_n", None) is not None
                and self._has_unquoted_placeholder(query_online)
            ):
                # collect (most recent N rows per entity): the trailing ? caps the
                # rank filter. Vector folds bind the full collect N; scan_vectors
                # narrows it to min(limit, N) so MySQL stops returning rows the
                # caller would discard. A backend predating the parameter inlines
                # the N literal and leaves no marker here.
                query_online = self._parametrize_query(
                    self.RANK_CAP_PARAM, query_online
                )
                self._rank_cap_by_serving_index[
                    prepared_statement.prepared_statement_index
                ] = prepared_statement.collect_n
            scan_query = getattr(prepared_statement, "query_online_scan", None)
            if not batch and scan_query:
                # direct single-entity scan (backward ordered-index read stopping at
                # the cap): same pk markers, then the trailing LIMIT ? binds through
                # the same rank-cap parameter. Serves scan_vectors and the
                # single-vector collect fold.
                scan_query = str(scan_query).replace("\n", " ")
                for param in prepared_statement.prepared_statement_parameters:
                    scan_query = self._parametrize_query(param.name, scan_query)
                scan_query = self._parametrize_query(self.RANK_CAP_PARAM, scan_query)
                self._scan_prepared_statements.setdefault(label, {})[
                    prepared_statement.prepared_statement_index
                ] = sql.text(scan_query)
                self._rank_cap_by_serving_index.setdefault(
                    prepared_statement.prepared_statement_index,
                    prepared_statement.collect_n,
                )
            query_online = sql.text(query_online)
            if batch:
                query_online = query_online.bindparams(
                    batch_ids=bindparam("batch_ids", expanding=True)
                )

            prepared_statements_dict[prepared_statement.prepared_statement_index] = (
                query_online
            )

        return prepared_statements_dict

    def _init_async_mysql_connection(self, options=None):
        assert self._prepared_statements.get(self.SINGLE_VECTOR_KEY) is not None, (
            "Prepared statements are not initialized. "
            "Please call `init_prepared_statement` method first."
        )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Fetching storage connector for sql connection to Online Feature Store."
            )
        self._online_connector = self._storage_connector_api._get_online_connector(
            self._feature_store_id
        )
        self._connection_options = options
        self._hostname = (
            variable_api.VariableApi()._get_loadbalancer_external_domain("mysqld")
            if self._external
            else None
        )

        if not self._async_task_thread:
            # Create the async event thread if it is not already running and start it.
            # The pool minimum is one connection per statement (the single-vector
            # fan-out); collect statements add per-entry batch scans, so the pool
            # MAXIMUM is raised to let those run concurrently instead of serializing
            # on a one-connection pool (review X9). User minsize/maxsize options win.
            statement_count = len(self._prepared_statements[self.SINGLE_VECTOR_KEY])
            pool_max = statement_count
            if self._collect_n_by_serving_index:
                pool_max = max(statement_count, self._COLLECT_SCAN_POOL_SIZE)
            self._async_task_thread = AsyncTaskThread(
                connection_pool_initializer=self._get_connection_pool,
                connection_test=self._test_connection,
                connection_pool_params=(statement_count, pool_max),
            )
            self._async_task_thread.start()

    def _get_single_feature_vector(
        self,
        entry: dict[str, Any],
        logging_data: bool = False,
        feature_vector_with_inference_helpers: bool = False,
    ) -> dict[str, Any]:
        """Retrieve single vector with parallel queries using aiomysql engine.

        If `logging_data` is True, it will use the prepared statement that includes logging metadata.
        i.e. The fetched feature vector will also include inference helper columns.
        If `feature_vector_with_inference_helpers` is True, it will use the prepared statement that includes
        inference helper columns along with regular features in a single query.

        Parameters:
            entry: Primary key values used to look up the feature vector.
            logging_data: Whether to include inference helper columns for logging.
            feature_vector_with_inference_helpers: Whether to include inference helper columns with regular features.

        Returns:
            A dictionary mapping feature names to their values.
        """
        if logging_data:
            key = self.SINGLE_LOGGING_VECTOR_KEY
        elif feature_vector_with_inference_helpers:
            key = self.SINGLE_VECTOR_WITH_INFERENCE_HELPERS_KEY
        else:
            key = self.SINGLE_VECTOR_KEY
        return self._single_vector_result(
            entry,
            self.parametrised_prepared_statements[key],
            # collect statements execute the label's direct scan when the backend
            # provides one, bounding the read at N instead of ranking all history
            scan_statements=self._scan_prepared_statements.get(key),
        )

    def _get_batch_feature_vectors(
        self,
        entries: list[dict[str, Any]],
        logging_data: bool = False,
        feature_vector_with_inference_helpers: bool = False,
    ) -> list[dict[str, Any]]:
        """Retrieve batch vector with parallel queries using aiomysql engine.

        If `logging_data` is True, it will use the prepared statement that includes logging metadata.
        i.e. The fetched feature vector will also include inference helper columns.
        If `feature_vector_with_inference_helpers` is True, it will use the prepared statement that includes
        inference helper columns along with regular features in a single query.

        Parameters:
            entries: List of primary key value dicts used to look up each feature vector.
            logging_data: Whether to include inference helper columns for logging.
            feature_vector_with_inference_helpers: Whether to include inference helper columns with regular features.

        Returns:
            A list of dictionaries, each mapping feature names to their values.
        """
        if logging_data:
            key = self.BATCH_LOGGING_VECTOR_KEY
            scan_label = self.SINGLE_LOGGING_VECTOR_KEY
        elif feature_vector_with_inference_helpers:
            key = self.BATCH_VECTOR_WITH_INFERENCE_HELPERS_KEY
            scan_label = self.SINGLE_VECTOR_WITH_INFERENCE_HELPERS_KEY
        else:
            key = self.BATCH_VECTOR_KEY
            scan_label = self.SINGLE_VECTOR_KEY
        return self._batch_vector_results(
            entries,
            self.parametrised_prepared_statements[key],
            # collect statements run one direct scan per entry (bounded by the
            # connection pool) instead of the windowed IN plan, which reads and
            # ranks the WHOLE history of every entity in the list. The scan
            # statement is single-entity, so it lives under the matching
            # single-vector label (statement indexes align across labels).
            scan_statements=self._scan_prepared_statements.get(scan_label),
        )

    def _get_inference_helper_vector(self, entry: dict[str, Any]) -> dict[str, Any]:
        """Retrieve single inference helper vector with parallel queries using aiomysql engine.

        Parameters:
            entry: Primary key values used to look up the inference helper vector.

        Returns:
            A dictionary mapping inference helper feature names to their values.
        """
        return self._single_vector_result(
            entry, self.parametrised_prepared_statements[self.SINGLE_HELPER_KEY]
        )

    def _get_batch_inference_helper_vectors(
        self, entries: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Retrieve batch inference helper vectors with parallel queries using aiomysql engine.

        Parameters:
            entries: List of primary key value dicts used to look up each inference helper vector.

        Returns:
            A list of dictionaries, each mapping inference helper feature names to their values.
        """
        return self._batch_vector_results(
            entries, self.parametrised_prepared_statements[self.BATCH_HELPER_KEY]
        )

    def _get_scan_rows(
        self, entry: dict[str, Any], limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Return the most-recent rows of the collect feature group(s) for one entity.

        This is the row-level (un-folded) view of the collect operation used by
        `FeatureView.scan_vectors`: the online query already orders by the collect order
        column and caps at the feature view's collect N, so this returns up to N rows per
        collect feature group, newest-first, with the internal rank column dropped.
        Only the collect statements execute (the point-read and aggregate statements
        contribute nothing to scan output), and `limit` narrows the statement's rank
        cap to min(limit, N) so MySQL stops returning rows the caller would discard
        (statements from a backend predating the rank-cap parameter return N rows and
        are sliced here instead).

        Parameters:
            entry: Entity-key values (e.g. {"user_id": 123}).
            limit: Optional cap on the number of rows returned (<= the FV collect N).

        Returns:
            A list of row dicts (feature name -> value), newest-first.
        """
        collect_statements = {
            index: statement
            for index, statement in self.parametrised_prepared_statements[
                self.SINGLE_VECTOR_KEY
            ].items()
            if self._collect_n_by_serving_index.get(index) is not None
        }
        if len(collect_statements) > 1:
            # a flat row list over several collect sources is ambiguous, and a
            # global limit would silently drop later sources
            raise FeatureStoreException(
                "scan_vectors is ambiguous for a feature view with multiple "
                "collect feature groups; read each feature group's rows "
                "directly, or use get_feature_vector for the folded features."
            )
        # prefer the direct scan statements (backward ordered-index read stopping
        # at the cap) over the windowed plan, which ranks the whole entity history
        scan_statements = self._scan_prepared_statements.get(
            self.SINGLE_VECTOR_KEY, {}
        )
        direct = {
            index: scan_statements[index]
            for index in collect_statements
            if index in scan_statements
        }
        rows = self._single_vector_result(
            entry,
            direct or collect_statements,
            raw_rows=True,
            scan_limit=limit,
        )
        if direct:
            # the direct statement returns newest-first; the windowed one already
            # carries the feature view's output order
            for index in direct:
                if self._collect_ascending_by_serving_index.get(index):
                    rows = list(reversed(rows))
        return rows[:limit] if limit is not None else rows

    def _single_vector_result(
        self,
        entry: dict[str, Any],
        prepared_statement_objects: dict[int, sql.text],
        raw_rows: bool = False,
        scan_limit: int | None = None,
        scan_statements: dict[int, sql.text] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Retrieve single vector with parallel queries using aiomysql engine.

        When `raw_rows` is True, return the collect feature group's rows as a list of dicts
        (the un-folded scan result) instead of the assembled single feature vector.
        `scan_limit` narrows the collect statements' rank-cap bind to min(scan_limit, N).
        `scan_statements` substitutes the direct single-entity scan for the matching
        statement indexes: the backward ordered-index read stops at the rank cap
        instead of ranking the entity's whole history, and the rows fold identically.
        The scan returns newest-first, so an ascending collect is reversed before
        folding (the windowed statement carries its output order server-side).
        """
        if all(isinstance(val, list) for val in entry.values()):
            raise ValueError(
                "Entry is expected to be single value per primary key. "
                "If you have already initialised prepared statements for single vector and now want to retrieve "
                "batch vector please reinitialise prepared statements with  "
                "`training_dataset.init_prepared_statement()` "
                "or `feature_view.init_serving()`"
            )
        # Initialize the set of values
        serving_vector = {}
        bind_entries = {}
        prepared_statement_execution = {}
        # one UTC reference for every aggregation window in this read, shared with
        # what the RonSQL path would substitute (naive UTC, microseconds preserved — X2-R14)
        window_reference = (
            datetime.now(timezone.utc).replace(tzinfo=None)
            if self._aggregate_window_by_serving_index
            else None
        )
        for prepared_statement_index in prepared_statement_objects:
            pk_entry = {}
            next_statement = False
            for sk in self.serving_key_by_serving_index[prepared_statement_index]:
                if sk.required_serving_key not in entry:
                    # Check if there is any entry matched with feature name.
                    if sk.feature_name in entry:
                        pk_entry[sk.feature_name] = entry[sk.feature_name]
                    else:
                        # User did not provide the necessary serving keys, we expect they have
                        # provided the necessary features as passed_features.
                        # We are going to check later if this is true
                        next_statement = True
                        break
                else:
                    pk_entry[sk.feature_name] = entry[sk.required_serving_key]
            if next_statement:
                continue
            window = self._aggregate_window_by_serving_index.get(
                prepared_statement_index
            )
            if window is not None:
                pk_entry[self.WINDOW_BOUND_PARAM] = window_reference - timedelta(
                    seconds=window
                )
            rank_cap = self._rank_cap_by_serving_index.get(prepared_statement_index)
            if rank_cap is not None:
                pk_entry[self.RANK_CAP_PARAM] = (
                    rank_cap if scan_limit is None else min(scan_limit, rank_cap)
                )
            bind_entries[prepared_statement_index] = pk_entry
            statement = prepared_statement_objects[prepared_statement_index]
            if scan_statements is not None and prepared_statement_index in (
                scan_statements
            ):
                # collect fold: the direct scan stops at the rank cap instead of
                # ranking the whole history; the windowed statement stays the
                # old-backend fallback (no scan statement -> no substitution)
                statement = scan_statements[prepared_statement_index]
            prepared_statement_execution[prepared_statement_index] = statement

        # run all the prepared statements in parallel using aiomysql engine
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Executing prepared statements {sorted(prepared_statement_execution)} "
                f"binding {[sorted(binds) for binds in bind_entries.values()]}"
            )
        results_dict = self._async_task_thread._submit(
            AsyncTask(
                task_function=self._execute_prep_statements,
                task_args=(
                    prepared_statement_execution,
                    bind_entries,
                ),
                requires_connection_pool=True,
            )
        )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Retrieved rows per statement: %s",
                {key: len(rows) for key, rows in results_dict.items()},
            )
            _logger.debug("Constructing serving vector from results")
        if raw_rows:
            # Return the un-folded rows of the collect statement(s) for this entity,
            # newest-first, with the internal rank column dropped. Used by scan_vectors.
            scan_rows: list[dict[str, Any]] = []
            for key in results_dict:
                if self._collect_n_by_serving_index.get(key) is None:
                    continue
                for row in results_dict[key]:
                    scan_rows.append(
                        {
                            col: val
                            for col, val in dict(row).items()
                            if col != self.COLLECT_RANK_ALIAS
                        }
                    )
            return scan_rows

        for key in results_dict:
            collect_n = self._collect_n_by_serving_index.get(key)
            if collect_n is not None:
                # collect feature group: fold the up-to-N rows into list-typed
                # features. Statement columns carry the join prefix (the feature
                # view's names), so entity keys match by prefixed name and struct
                # fields strip the prefix back to the source names (review X3).
                prefix = self.prefix_by_serving_index.get(key) or ""
                pk_names = {
                    prefix + sk.feature_name
                    for sk in self.serving_key_by_serving_index.get(key, [])
                }
                rows = results_dict[key]
                if (
                    scan_statements is not None
                    and key in scan_statements
                    and self._collect_ascending_by_serving_index.get(key)
                ):
                    # the direct scan returns newest-first; an ascending collect
                    # folds oldest-first (offline parity), which the windowed
                    # statement orders server-side
                    rows = list(reversed(rows))
                serving_vector.update(
                    self._fold_collect_rows(
                        rows,
                        pk_names,
                        self._collect_name_by_serving_index.get(key),
                        prefix=prefix,
                    )
                )
                continue
            for row in results_dict[key]:
                # row values are feature data: never logged, even at DEBUG
                result_dict = dict(row)
                serving_vector.update(result_dict)

        return serving_vector

    def _fold_collect_rows(
        self,
        rows: list[Any],
        pk_names: set[str],
        collect_feature_name: str | None,
        prefix: str = "",
    ) -> dict[str, Any]:
        """Fold the up-to-N rows of a collect feature group into one feature dict.

        Entity-key columns stay scalar (the entity id, identical across rows) under their
        feature-view (prefixed) names; the remaining columns of each row become one struct
        (dict) whose FIELD names are the source column names — the statement's join prefix
        is stripped, because the persisted array<struct<...>> schema uses the raw source
        names (review X3). The structs form a list ordered most-recent-first as returned
        by the query, assigned to the single array<struct<...>> feature named
        `collect_feature_name` (v2 Design Contract C1). The hopsworks_collect_rank helper
        column is dropped.

        When `collect_feature_name` is None (statement from a backend predating the
        collapsed schema), each column folds into its own list instead, with nulls
        preserved so the lists stay row-aligned.
        """
        folded: dict[str, Any] = {}
        structs: list[dict[str, Any]] = []
        for row in rows:
            struct_row: dict[str, Any] = {}
            for col, val in dict(row).items():
                if col == self.COLLECT_RANK_ALIAS:
                    continue
                if col in pk_names:
                    folded[col] = val
                    continue
                field = (
                    col[len(prefix):] if prefix and col.startswith(prefix) else col
                )
                if collect_feature_name is None:
                    folded.setdefault(field, []).append(val)
                else:
                    struct_row[field] = val
            if collect_feature_name is not None:
                structs.append(struct_row)
        if collect_feature_name is not None:
            folded[collect_feature_name] = structs
        return folded

    def _batch_vector_results(
        self,
        entries: list[dict[str, Any]],
        prepared_statement_objects: dict[int, sql.text],
        scan_statements: dict[int, sql.text] | None = None,
    ):
        """Execute prepared statements in parallel using aiomysql engine.

        Collect statements with a direct scan statement in `scan_statements` run one
        backward-index scan per entry (concurrency bounded by the connection pool)
        instead of the windowed IN plan: the window reads and ranks the WHOLE history
        of every entity in the list, so its cost scales with total history and one
        hot key dominates the batch, while per-entry scans stop at N rows each.
        The windowed IN statement stays the fallback for backends predating
        `queryOnlineScan`.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Starting batch vector retrieval for {len(entries)} entries via aiomysql engine."
            )
        # create dict object that will have of order of the vector as key and values as
        # vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
        # expect that backend will return correctly ordered vectors.
        batch_results = [{} for _ in range(len(entries))]
        entry_values = {}
        serving_keys_all_fg = []
        prepared_stmts_to_execute = {}
        scan_executions = {}
        # one UTC reference for every aggregation window in this read, shared with
        # what the RonSQL path would substitute (naive UTC, microseconds preserved — X2-R14)
        window_reference = (
            datetime.now(timezone.utc).replace(tzinfo=None)
            if self._aggregate_window_by_serving_index
            else None
        )
        # construct the list of entry values for binding to query
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Parametrize prepared statements for {len(entries)} entries"
            )
        for prepared_statement_index in prepared_statement_objects:
            # prepared_statement_index include fg with label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if prepared_statement_index not in self._serving_key_by_serving_index:
                continue
            if (
                scan_statements is not None
                and self._collect_n_by_serving_index.get(prepared_statement_index)
                is not None
                and prepared_statement_index in scan_statements
            ):
                # per-entry direct scans: bind each entry's serving keys plus the rank
                # cap, deduplicating identical entries so repeated keys share one scan
                serving_keys = self.serving_key_by_serving_index[
                    prepared_statement_index
                ]
                rank_cap = self._rank_cap_by_serving_index.get(
                    prepared_statement_index
                ) or self._collect_n_by_serving_index.get(prepared_statement_index)
                unique_binds: list[dict[str, Any]] = []
                entry_to_unique: list[int] = []
                seen_binds: dict[Any, int] = {}
                for entry in entries:
                    binds = {
                        sk.feature_name: self._entry_serving_key_value(entry, sk)
                        for sk in serving_keys
                    }
                    binds[self.RANK_CAP_PARAM] = rank_cap
                    try:
                        bind_key = tuple(sorted(binds.items()))
                        position = seen_binds.get(bind_key)
                        if position is None:
                            position = len(unique_binds)
                            seen_binds[bind_key] = position
                            unique_binds.append(binds)
                    except TypeError:
                        # unhashable key value: run it unshared
                        position = len(unique_binds)
                        unique_binds.append(binds)
                    entry_to_unique.append(position)
                scan_executions[prepared_statement_index] = (
                    scan_statements[prepared_statement_index],
                    unique_binds,
                    entry_to_unique,
                )
                continue

            prepared_stmts_to_execute[prepared_statement_index] = (
                prepared_statement_objects[prepared_statement_index]
            )
            entry_values_tuples = list(
                map(
                    lambda e, prepared_statement_index=prepared_statement_index: tuple(
                        [
                            self._entry_serving_key_value(e, sk)
                            for sk in self.serving_key_by_serving_index[
                                prepared_statement_index
                            ]
                        ]
                    ),
                    entries,
                )
            )
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Prepared statement {prepared_statement_index} with "
                    f"{len(entry_values_tuples)} entry tuples"
                )
            entry_values[prepared_statement_index] = {"batch_ids": entry_values_tuples}
            window = self._aggregate_window_by_serving_index.get(
                prepared_statement_index
            )
            if window is not None:
                entry_values[prepared_statement_index][self.WINDOW_BOUND_PARAM] = (
                    window_reference - timedelta(seconds=window)
                )
            rank_cap = self._rank_cap_by_serving_index.get(prepared_statement_index)
            if rank_cap is not None:
                # batch folds always need the full collect N per entity
                entry_values[prepared_statement_index][self.RANK_CAP_PARAM] = rank_cap
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Executing batch prepared statements %s and %d per-entry scan sets",
                sorted(entry_values),
                len(scan_executions),
            )
        # ONE wave: the batch IN statements and every per-entry collect scan share the
        # bounded pool concurrently instead of running in sequential waves (review X9)
        parallel_results, scan_results = (
            self._async_task_thread._submit(
                AsyncTask(
                    task_function=self._execute_batch_reads,
                    task_args=(
                        prepared_stmts_to_execute,
                        entry_values,
                        {
                            index: (statement, unique_binds)
                            for index, (
                                statement,
                                unique_binds,
                                _,
                            ) in scan_executions.items()
                        },
                    ),
                    requires_connection_pool=True,
                )
            )
            if prepared_stmts_to_execute or scan_executions
            else ({}, {})
        )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Retrieved rows per statement: %s; stitching.",
                {key: len(rows) for key, rows in parallel_results.items()},
            )
        # construct the results
        for prepared_statement_index in prepared_stmts_to_execute:
            statement_results = {}
            serving_keys = self.serving_key_by_serving_index[prepared_statement_index]
            serving_keys_all_fg += serving_keys
            prefix_features = [
                (self.prefix_by_serving_index[prepared_statement_index] or "")
                + sk.feature_name
                for sk in self.serving_key_by_serving_index[prepared_statement_index]
            ]
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Use prefix from prepare statement because prefix from serving key is collision adjusted {prefix_features}."
                )
                _logger.debug("iterate over results by index of the prepared statement")
            collect_n = self._collect_n_by_serving_index.get(prepared_statement_index)
            if collect_n is not None:
                # collect feature group: group the up-to-N rows per entity, then fold each
                # group into list-typed features keyed by the entity's serving key.
                grouped: dict[Any, list[dict[str, Any]]] = {}
                for row in parallel_results[prepared_statement_index]:
                    row_dict = dict(row)
                    grouped.setdefault(
                        self._get_result_key(prefix_features, row_dict), []
                    ).append(row_dict)
                pk_names = set(prefix_features)
                for result_key, group_rows in grouped.items():
                    statement_results[result_key] = self._fold_collect_rows(
                        group_rows,
                        pk_names,
                        self._collect_name_by_serving_index.get(
                            prepared_statement_index
                        ),
                        prefix=self.prefix_by_serving_index.get(
                            prepared_statement_index
                        )
                        or "",
                    )
            else:
                # rows and stitch keys carry feature data and entity keys:
                # never logged, even at DEBUG
                for row in parallel_results[prepared_statement_index]:
                    row_dict = dict(row)
                    # can primary key be complex feature? No, not supported.
                    statement_results[
                        self._get_result_key(prefix_features, row_dict)
                    ] = row_dict
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Stitching %d result rows from statement %d into %d entries",
                    len(statement_results),
                    prepared_statement_index,
                    len(entries),
                )
            for i, entry in enumerate(entries):
                batch_results[i].update(
                    statement_results.get(
                        self._get_result_key_serving_key(serving_keys, entry), {}
                    )
                )
        for prepared_statement_index, (
            _,
            _unique_binds,
            entry_to_unique,
        ) in scan_executions.items():
            # per-entry direct scans: results align with entries through the dedup
            # mapping, so folding needs no result-key stitching. Duplicate entries
            # fold one shared unique result.
            serving_keys = self.serving_key_by_serving_index[prepared_statement_index]
            serving_keys_all_fg += serving_keys
            unique_rows = scan_results.get(prepared_statement_index, [])
            prefix = self.prefix_by_serving_index.get(prepared_statement_index) or ""
            pk_names = {prefix + sk.feature_name for sk in serving_keys}
            collect_name = self._collect_name_by_serving_index.get(
                prepared_statement_index
            )
            ascending = self._collect_ascending_by_serving_index.get(
                prepared_statement_index
            )
            folded_by_unique: dict[int, dict[str, Any]] = {}
            for i, unique_position in enumerate(entry_to_unique):
                rows = (
                    unique_rows[unique_position]
                    if unique_position < len(unique_rows)
                    else []
                )
                if not rows:
                    # the empty-semantics pass below folds this entity to an
                    # empty collected array, like the single-vector fold
                    continue
                folded = folded_by_unique.get(unique_position)
                if folded is None:
                    # the scan returns newest-first; an ascending collect folds
                    # oldest-first (offline parity)
                    ordered = list(reversed(rows)) if ascending else list(rows)
                    folded = self._fold_collect_rows(
                        ordered, pk_names, collect_name, prefix=prefix
                    )
                    folded_by_unique[unique_position] = folded
                batch_results[i].update(folded)
        # pin the empty semantics to the single-read contract (review X5): an entity
        # with no matching history reads as an EMPTY collected array (the windowed IN
        # plan and the per-entry scans both surface nothing for it), and an entity the
        # batch aggregate's GROUP BY misses reads as the SQL aggregate over an empty
        # set (COUNT 0, every other function NULL) exactly like the single statement,
        # which has no GROUP BY and always returns one row.
        for index, collect_name in self._collect_name_by_serving_index.items():
            if (
                index not in prepared_statement_objects
                or index not in self._serving_key_by_serving_index
            ):
                continue
            for entry_result in batch_results:
                entry_result.setdefault(collect_name, [])
        for index, output_names in self._aggregate_names_by_serving_index.items():
            if (
                index not in prepared_statement_objects
                or index not in self._serving_key_by_serving_index
            ):
                continue
            prefix = self.prefix_by_serving_index.get(index) or ""
            for entry_result in batch_results:
                for name in output_names:
                    default = (
                        0
                        if name == prefix + "count" or name.endswith("_count")
                        else None
                    )
                    entry_result.setdefault(name, default)
        return batch_results, serving_keys_all_fg

    async def _execute_per_entry_scans(
        self,
        statement,
        binds_list: list[dict[str, Any]],
        connection_pool: aiomysql.utils._ConnectionContextManager,
    ):
        """Run one direct collect scan per entry, bounded by the connection pool.

        Each scan is the single-entity `ORDER BY order_col DESC LIMIT ?` statement,
        so the per-batch cost scales with `len(entries) * N` instead of the total
        history of the selected entities. Scans run in bounded waves of
        `_COLLECT_SCAN_CHUNK` tasks (the query timeout applies per wave), capping
        in-flight cursors and task objects for large batches (review X9).
        """
        timeout = (
            self.connection_options.get("query_timeout", 120)
            if self.connection_options
            else 120
        )
        results: list[Any] = []
        for start in range(0, len(binds_list), self._COLLECT_SCAN_CHUNK):
            tasks = [
                asyncio.create_task(
                    self._query_async_sql(statement, binds, connection_pool),
                    name="query_collect_scan_" + str(start + i),
                )
                for i, binds in enumerate(
                    binds_list[start : start + self._COLLECT_SCAN_CHUNK]
                )
            ]
            results.extend(
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
            )
        return results

    async def _execute_batch_reads(
        self,
        prepared_statements: dict[int, str],
        entries: dict[int, dict[str, Any]],
        scan_specs: dict[int, tuple[Any, list[dict[str, Any]]]],
        connection_pool: aiomysql.utils._ConnectionContextManager,
    ):
        """Run the batch IN statements and every per-entry collect scan in ONE wave.

        Point-read/aggregate statements and collect scans share the bounded pool
        concurrently instead of completing in sequential waves (review X9).
        """

        async def _no_statements():
            return {}

        statement_results, *scan_results = await asyncio.gather(
            self._execute_prep_statements(
                prepared_statements, entries, connection_pool
            )
            if prepared_statements
            else _no_statements(),
            *[
                self._execute_per_entry_scans(statement, binds_list, connection_pool)
                for statement, binds_list in scan_specs.values()
            ],
        )
        return statement_results, dict(
            zip(scan_specs.keys(), scan_results, strict=False)
        )

    def _refresh_mysql_connection(self):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Refreshing MySQL connection.")
        try:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Checking if the connection is still alive.")
            with self._prepared_statement_engine.connect():
                # This will raise an exception if the connection is closed
                pass
        except exc.OperationalError:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Connection is closed, re-establishing connection.")
            self._set_mysql_connection()

    def _make_preview_statement(self, statement, n):
        return text(statement.text[: statement.text.find(" WHERE ")] + f" LIMIT {n}")

    def _set_mysql_connection(self, options=None):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Retrieve MySQL connection details from the online storage connector."
            )
        online_conn = self._storage_connector_api._get_online_connector(
            self._feature_store_id
        )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Creating MySQL {'external' if self.external is True else ''}engine with options: {options}."
            )
        self._prepared_statement_engine = util_sql._create_mysql_engine(
            online_conn, self._external, options=options
        )

    @staticmethod
    def _first_unquoted_placeholder(query_online: str) -> int:
        """Index of the first `?` bind marker outside any SQL quoting, or -1.

        Tracks single-quoted string literals (with `''` escapes) and
        backtick-quoted identifiers, so a `?` inside a filter literal (for
        example `LIKE 'vip?%'`) is never treated as a marker.
        """
        quote = None
        i = 0
        while i < len(query_online):
            char = query_online[i]
            if quote is None:
                if char == "?":
                    return i
                if char in ("'", "`"):
                    quote = char
            elif quote == char:
                if char == "'" and query_online[i + 1 : i + 2] == "'":
                    # '' inside a string literal is an escaped quote
                    i += 1
                else:
                    quote = None
            i += 1
        return -1

    @classmethod
    def _has_unquoted_placeholder(cls, query_online: str) -> bool:
        return cls._first_unquoted_placeholder(query_online) != -1

    @classmethod
    def _parametrize_query(cls, name: str, query_online: str) -> str:
        """Replace the next `?` bind marker with `:name`.

        Iterating in parameter order consumes markers left to right, so this only
        works if the parameter names are sorted by their statement position. The
        scan is quote-aware: a `?` inside a string literal or a backtick-quoted
        identifier is never replaced (a filter literal containing `?` would
        otherwise be corrupted and the real marker left unbound).
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Parametrizing name {name} in query {query_online}")
        index = cls._first_unquoted_placeholder(query_online)
        if index == -1:
            return query_online
        return query_online[:index] + ":" + name + query_online[index + 1 :]

    @staticmethod
    def _get_result_key(
        primary_keys: list[str], result_dict: dict[str, str]
    ) -> tuple[str]:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Get result key for primary keys {primary_keys}"
            )
        result_key = []
        for pk in primary_keys:
            result_key.append(result_dict.get(pk))
        return tuple(result_key)

    @staticmethod
    def _entry_serving_key_value(entry: dict[str, Any], sk: ServingKey) -> Any:
        """The entry's value for one serving key.

        Falls back from the required serving key to the feature name only when
        the required key is ABSENT: falsy values (0, False, "") are valid
        entity keys and must not trigger the fallback.
        """
        if sk.required_serving_key in entry:
            return entry[sk.required_serving_key]
        return entry.get(sk.feature_name)

    @classmethod
    def _get_result_key_serving_key(
        cls, serving_keys: list[ServingKey], result_dict: dict[str, dict[str, Any]]
    ) -> tuple[str]:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Get result key for serving keys"
            )
        result_key = []
        for sk in serving_keys:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Get result key for serving key {sk.required_serving_key} or {sk.feature_name}"
                )
            result_key.append(cls._entry_serving_key_value(result_dict, sk))
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Built result key of length %d", len(result_key))
        return tuple(result_key)

    @staticmethod
    def _get_prepared_statement_labels(
        with_inference_helper_column: bool = False,
        with_logging_meta_data: bool = False,
        feature_vector_with_inference_helpers: bool = False,
    ) -> list[str]:
        if with_inference_helper_column:
            prepared_statements_list = [
                OnlineStoreSqlClient.SINGLE_VECTOR_KEY,
                OnlineStoreSqlClient.BATCH_VECTOR_KEY,
                OnlineStoreSqlClient.SINGLE_HELPER_KEY,
                OnlineStoreSqlClient.BATCH_HELPER_KEY,
            ]
        else:
            prepared_statements_list = [
                OnlineStoreSqlClient.SINGLE_VECTOR_KEY,
                OnlineStoreSqlClient.BATCH_VECTOR_KEY,
            ]
        if with_logging_meta_data:
            prepared_statements_list += [
                OnlineStoreSqlClient.SINGLE_LOGGING_VECTOR_KEY,
                OnlineStoreSqlClient.BATCH_LOGGING_VECTOR_KEY,
            ]
        if feature_vector_with_inference_helpers:
            prepared_statements_list += [
                OnlineStoreSqlClient.SINGLE_VECTOR_WITH_INFERENCE_HELPERS_KEY,
                OnlineStoreSqlClient.BATCH_VECTOR_WITH_INFERENCE_HELPERS_KEY,
            ]
        return prepared_statements_list

    async def _get_connection_pool(
        self, default_min_size: int, default_max_size: int | None = None
    ) -> None:
        return await util_sql._create_async_engine(
            self._online_connector,
            self._external,
            default_min_size,
            options=self._connection_options,
            hostname=self._hostname,
            default_max_size=default_max_size,
        )

    async def _test_connection(
        self, connection_pool: aiomysql.utils._ConnectionContextManager
    ):
        """Test the connection to the MySQL database."""
        try:
            async with connection_pool.acquire() as conn:
                await conn._connection.ping(reconnect=True)
        except Exception as e:
            _logger.error(f"Failed to connect to MySQL: {e}")
            raise e

    async def _query_async_sql(
        self,
        stmt,
        bind_params,
        connection_pool: aiomysql.utils._ConnectionContextManager,
    ):
        """Query prepared statement together with bind params using aiomysql connection pool."""
        # create connection pool
        async with connection_pool.acquire() as conn:
            # Execute the prepared statement. Bind VALUES are entity keys and
            # passed features: log parameter names only, never values.
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Executing prepared statement binding %s", sorted(bind_params)
                )
            cursor = await conn.execute(stmt, bind_params)
            # Fetch the result
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Waiting for resultset.")
            resultset = await cursor.fetchall()
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Retrieved %d rows. Closing cursor.", len(resultset))
            await cursor.close()

        return resultset

    async def _execute_prep_statements(
        self,
        prepared_statements: dict[int, str],
        entries: list[dict[str, Any]] | dict[str, Any],
        connection_pool: aiomysql.utils._ConnectionContextManager,  # The connection pool required is passed as a parameter from the AsyncTaskThread.
    ):
        """Iterate over prepared statements to create async tasks and gather all tasks results for a given list of entries."""
        # validate if prepared_statements and entries have the same keys
        if prepared_statements.keys() != entries.keys():
            # iterate over prepared_statements and entries to find the missing key
            # remove missing keys from prepared_statements
            for key in list(prepared_statements.keys()):
                if key not in entries:
                    prepared_statements.pop(key)

        try:
            tasks = [
                asyncio.create_task(
                    self._query_async_sql(
                        prepared_statements[key], entries[key], connection_pool
                    ),
                    name="query_prep_statement_key" + str(key),
                )
                for key in prepared_statements
            ]
            # Run the queries in parallel using asyncio.gather
            results = await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=self.connection_options.get("query_timeout", 120)
                if self.connection_options
                else 120,
            )
        except asyncio.CancelledError as e:
            if _logger.isEnabledFor(logging.ERROR):
                _logger.error(f"Failed executing prepared statements: {e}")
            raise e
        except asyncio.TimeoutError as e:
            if _logger.isEnabledFor(logging.ERROR):
                _logger.error(f"Query timed out: {e}")
            raise e

        # Create a dict of results with the prepared statement index as key
        results_dict = {}
        for i, key in enumerate(prepared_statements):
            results_dict[key] = results[i]

        return results_dict

    @property
    def feature_store_id(self) -> int:
        return self._feature_store_id

    @property
    def prepared_statement_engine(self) -> Any | None:
        """JDBC connection engine to retrieve connections to online features store from."""
        return self._prepared_statement_engine

    @prepared_statement_engine.setter
    def prepared_statement_engine(self, prepared_statement_engine: Any) -> None:
        self._prepared_statement_engine = prepared_statement_engine

    @property
    def prepared_statements(
        self,
    ) -> dict[str, list[ServingPreparedStatement]]:
        """Contains up to 4 prepared statements for single and batch vector retrieval, and single or batch inference helpers.

        The keys are the labels for the prepared statements, and the values are dictionaries of prepared statements
        with the prepared statement index as the key.
        """
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(
        self,
        prepared_statements: dict[str, list[ServingPreparedStatement]],
    ) -> None:
        self._prepared_statements = prepared_statements

    @property
    def parametrised_prepared_statements(
        self,
    ) -> dict[str, dict[int, sql.text]]:
        """The dict object of prepared_statements as values and keys as indices of positions in the query for selecting features from feature groups of the training dataset, used for batch retrieval."""
        return self._parametrised_prepared_statements

    @parametrised_prepared_statements.setter
    def parametrised_prepared_statements(
        self,
        parametrised_prepared_statements: dict[str, dict[int, sql.text]],
    ) -> None:
        self._parametrised_prepared_statements = parametrised_prepared_statements

    @property
    def prefix_by_serving_index(self) -> dict[int, str]:
        """The dict object of prefixes as values and keys as indices of positions in the query for selecting features from feature groups of the training dataset."""
        return self._prefix_by_serving_index

    @prefix_by_serving_index.setter
    def prefix_by_serving_index(self, prefix_by_serving_index: dict[int, str]) -> None:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Setting prefix by serving index {prefix_by_serving_index}.")
        self._prefix_by_serving_index = prefix_by_serving_index

    @property
    def serving_key_by_serving_index(
        self,
    ) -> dict[int, list[ServingKey]]:
        """The dict object of serving keys as values and keys as indices of positions in the query for selecting features from feature groups of the training dataset."""
        return self._serving_key_by_serving_index

    @property
    def feature_name_order_by_psp(self) -> dict[int, dict[str, int]]:
        """The dict object of feature names as values and keys as indices of positions in the query for selecting features from feature groups of the training dataset."""
        return self._feature_name_order_by_psp

    @property
    def skip_fg_ids(self) -> set[int]:
        """The list of feature group ids to skip when retrieving feature vectors.

        The retrieval of Feature values stored in Feature Group with embedding is handled via a separate client
        as there are not stored in RonDB.
        """
        return self._skip_fg_ids

    @property
    def serving_keys(self) -> set[ServingKey]:
        if len(self._serving_keys) > 0:
            return self._serving_keys

        if len(self.prepared_statements) == 0:
            raise ValueError(
                "Prepared statements are not initialized. Please call `init_prepared_statement` method first."
            )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Build serving keys from prepared statements ignoring prefix to ensure compatibility with older version."
            )
        self._serving_keys = util._build_serving_keys_from_prepared_statements(
            self.prepared_statements[
                self.BATCH_VECTOR_KEY
            ],  # use batch to avoid issue with label_fg
            ignore_prefix=True,  # if serving_keys are not set it is because the feature view is anterior to 3.3, this ensures compatibility
        )
        return self._serving_keys

    @property
    def training_dataset_api(self) -> training_dataset_api.TrainingDatasetApi:
        return self._training_dataset_api

    @property
    def feature_view_api(self) -> feature_view_api.FeatureViewApi:
        return self._feature_view_api

    @property
    def storage_connector_api(self) -> storage_connector_api.StorageConnectorApi:
        return self._storage_connector_api

    @property
    def hostname(self) -> str:
        return self._hostname

    @property
    def connection_options(self) -> dict[str, Any]:
        return self._connection_options

    @property
    def online_connector(self) -> storage_connector.StorageConnector:
        return self._online_connector
