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
import asyncio
import logging

from hsfs.constructor.serving_prepared_statement import ServingPreparedStatement
from hsfs.core.online_store_sql_engine import OnlineStoreSqlClient


def make_engine():
    engine = OnlineStoreSqlClient.__new__(OnlineStoreSqlClient)
    engine._skip_fg_ids = set()
    engine._aggregate_window_by_serving_index = {}
    engine._rank_cap_by_serving_index = {}
    engine._scan_prepared_statements = {}
    engine._collect_ascending_by_serving_index = {}
    engine._aggregate_names_by_serving_index = {}
    return engine


class _ServingKey:
    def __init__(self, feature_name, required_serving_key=None, join_index=0):
        self.feature_name = feature_name
        self.required_serving_key = required_serving_key or feature_name
        self.join_index = join_index


class _FakeTaskThread:
    """Records the submitted statements/binds and returns canned rows."""

    def __init__(self, results, scan_results=None):
        self.results = results
        self.scan_results = scan_results
        self.calls = []
        self.functions = []

    def _submit(self, task):
        self.functions.append(task.task_function.__name__)
        self.calls.append(task.task_args)
        name = task.task_function.__name__
        if name == "_execute_batch_reads":
            scan_specs = task.task_args[2]
            per_index = {}
            for index in scan_specs:
                if isinstance(self.scan_results, dict):
                    per_index[index] = self.scan_results.get(index, [])
                else:
                    per_index[index] = self.scan_results or []
            return self.results, per_index
        if name == "_execute_per_entry_scans":
            return self.scan_results
        return self.results

    def is_alive(self):
        # engine.__del__ probes the executor thread before stopping it
        return False


def make_serving_engine(results, collect_ascending=False):
    """An engine wired for one collect statement (index 0) with a fake executor."""
    engine = make_engine()
    engine._serving_key_by_serving_index = {0: [_ServingKey("user_id")]}
    engine._prefix_by_serving_index = {0: None}
    engine._collect_n_by_serving_index = {0: 3}
    engine._collect_name_by_serving_index = {0: "t_collect"}
    engine._collect_ascending_by_serving_index = {0: collect_ascending}
    engine._rank_cap_by_serving_index = {0: 3}
    engine._async_task_thread = _FakeTaskThread(results)
    return engine


def make_statement(
    query_online, aggregate_window=None, collect_n=None, query_online_scan=None
):
    return ServingPreparedStatement(
        feature_group_id=1,
        prepared_statement_index=0,
        prepared_statement_parameters=[{"name": "user_id", "index": 1}],
        query_online=query_online,
        query_online_scan=query_online_scan,
        aggregate_window=aggregate_window,
        collect_n=collect_n,
    )


class TestAggregateWindowParameter:
    """The aggregation window's trailing ? binds from the client's UTC clock.

    The backend emits `event_time >= ?` on both the MySQL and RonSQL statements so
    the two serving paths resolve the bound from ONE client-computed UTC reference.
    """

    def test_trailing_placeholder_becomes_the_window_bind(self):
        engine = make_engine()
        statements = engine._parametrize_prepared_statements(
            [
                make_statement(
                    "SELECT SUM(`amount`) AS `amount_sum` FROM `db`.`t_1` "
                    "WHERE `user_id` = ? AND `event_time` >= ?",
                    aggregate_window=3600,
                )
            ],
            batch=False,
        )
        assert ":hw_window_bound" in str(statements[0])
        assert "?" not in str(statements[0])
        assert engine._aggregate_window_by_serving_index == {0: 3600}

    def test_batch_placeholder_order_is_in_list_then_window(self):
        engine = make_engine()
        statements = engine._parametrize_prepared_statements(
            [
                make_statement(
                    "SELECT `user_id`, SUM(`amount`) AS `amount_sum` FROM `db`.`t_1` "
                    "WHERE `user_id` IN (?) AND `event_time` >= ? GROUP BY `user_id`",
                    aggregate_window=3600,
                )
            ],
            batch=True,
        )
        rendered = str(statements[0])
        assert rendered.index(":batch_ids") < rendered.index(":hw_window_bound")
        assert engine._aggregate_window_by_serving_index == {0: 3600}

    def test_old_backend_now6_statement_is_untouched(self):
        # a backend predating the parameter inlines NOW(6) and leaves no marker:
        # nothing to parametrize, nothing to bind
        engine = make_engine()
        statements = engine._parametrize_prepared_statements(
            [
                make_statement(
                    "SELECT SUM(`amount`) AS `amount_sum` FROM `db`.`t_1` "
                    "WHERE `user_id` = ? AND `event_time` >= "
                    "NOW(6) - INTERVAL 3600 SECOND",
                    aggregate_window=3600,
                )
            ],
            batch=False,
        )
        assert ":hw_window_bound" not in str(statements[0])
        assert engine._aggregate_window_by_serving_index == {}

    def test_regular_statement_records_no_window(self):
        engine = make_engine()
        engine._parametrize_prepared_statements(
            [make_statement("SELECT `amount` FROM `db`.`t_1` WHERE `user_id` = ?")],
            batch=False,
        )
        assert engine._aggregate_window_by_serving_index == {}

    def test_placeholder_inside_string_literal_is_not_consumed(self):
        # the backend can place string filter literals before the trailing window
        # marker; a quote-blind replacement would corrupt the literal and leave
        # the real marker unbound
        engine = make_engine()
        statements = engine._parametrize_prepared_statements(
            [
                make_statement(
                    "SELECT SUM(`amount`) AS `amount_sum` FROM `db`.`t_1` "
                    "WHERE `user_id` = ? AND `category` LIKE 'vip?%' "
                    "AND `event_time` >= ?",
                    aggregate_window=3600,
                )
            ],
            batch=False,
        )
        rendered = str(statements[0])
        assert "LIKE 'vip?%'" in rendered
        assert "`event_time` >= :hw_window_bound" in rendered
        assert engine._first_unquoted_placeholder(rendered) == -1

    def test_escaped_quote_inside_literal_is_tracked(self):
        query = OnlineStoreSqlClient._parametrize_query(
            "user_id", "SELECT 1 FROM `t` WHERE `name` = 'o''brien?' AND `id` = ?"
        )
        assert "'o''brien?'" in query
        assert query.endswith("`id` = :user_id")


class TestCollectRankCapParameter:
    """The collect rank cap binds N for folds and min(limit, N) for scans."""

    COLLECT_SQL = (
        "SELECT * FROM (SELECT `amount`, ROW_NUMBER() OVER (PARTITION BY "
        "`user_id` ORDER BY `ts` DESC) AS `hopsworks_collect_rank` FROM "
        "`db`.`t_1` WHERE `user_id` = ?) AS `hw_c` WHERE "
        "`hopsworks_collect_rank` <= ? ORDER BY `hopsworks_collect_rank`"
    )

    def test_trailing_placeholder_becomes_the_rank_cap(self):
        engine = make_engine()
        statements = engine._parametrize_prepared_statements(
            [make_statement(self.COLLECT_SQL, collect_n=100)], batch=False
        )
        rendered = str(statements[0])
        assert ":hw_rank_cap" in rendered
        assert engine._first_unquoted_placeholder(rendered) == -1
        assert engine._rank_cap_by_serving_index == {0: 100}

    def test_old_backend_literal_cap_is_untouched(self):
        engine = make_engine()
        statements = engine._parametrize_prepared_statements(
            [
                make_statement(
                    self.COLLECT_SQL.replace(
                        "`hopsworks_collect_rank` <= ?",
                        "`hopsworks_collect_rank` <= 100",
                    ),
                    collect_n=100,
                )
            ],
            batch=False,
        )
        assert ":hw_rank_cap" not in str(statements[0])
        assert engine._rank_cap_by_serving_index == {}

    def test_reinit_clears_derived_bind_state(self):
        # re-initialization must not inherit window/rank-cap indexes or
        # parametrized statements from the previous statement set: a stale
        # index would mis-bind the new statements
        engine = make_engine()
        engine._aggregate_window_by_serving_index = {9: 3600}
        engine._rank_cap_by_serving_index = {9: 100}
        engine._parametrised_prepared_statements = {"stale": {}}
        plain = make_statement("SELECT `amount` FROM `db`.`t_1` WHERE `user_id` = ?")
        engine._prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: [plain],
            OnlineStoreSqlClient.BATCH_VECTOR_KEY: [plain],
        }
        engine._fetch_prepared_statements = lambda *args, **kwargs: None
        engine._init_parametrize_and_serving_utils = lambda *args, **kwargs: None
        engine._get_prepared_statement_labels = lambda *args, **kwargs: [
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY
        ]
        engine._init_prepared_statements(entity=None, inference_helper_columns=False)
        assert engine._aggregate_window_by_serving_index == {}
        assert engine._rank_cap_by_serving_index == {}
        assert set(engine._parametrised_prepared_statements) == {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY
        }

    def test_serving_keys_do_not_duplicate_on_reinit(self):
        class _Key:
            join_index = 0
            feature_name = "user_id"
            required_serving_key = "user_id"

        engine = make_engine()
        engine._serving_keys = [_Key()]
        statements = [
            make_statement("SELECT `amount` FROM `db`.`t_1` WHERE `user_id` = ?")
        ]
        engine._init_parametrize_and_serving_utils(statements)
        engine._init_parametrize_and_serving_utils(statements)
        assert len(engine.serving_key_by_serving_index[0]) == 1

    SCAN_SQL = (
        "SELECT `amount`, `ts` FROM `db`.`t_1` WHERE `user_id` = ? "
        "ORDER BY `ts` DESC LIMIT ?"
    )

    def test_direct_scan_statement_is_parametrized(self):
        engine = make_engine()
        engine._parametrize_prepared_statements(
            [
                make_statement(
                    self.COLLECT_SQL, collect_n=100, query_online_scan=self.SCAN_SQL
                )
            ],
            batch=False,
        )
        rendered = str(
            engine._scan_prepared_statements[OnlineStoreSqlClient.SINGLE_VECTOR_KEY][0]
        )
        assert ":user_id" in rendered
        assert rendered.endswith("LIMIT :hw_rank_cap")
        assert engine._first_unquoted_placeholder(rendered) == -1
        assert engine._rank_cap_by_serving_index == {0: 100}

    def test_direct_scan_literal_placeholder_is_not_consumed(self):
        # a filter literal containing ? inside the scan statement must survive
        # both the pk and the LIMIT parametrization
        engine = make_engine()
        engine._parametrize_prepared_statements(
            [
                make_statement(
                    self.COLLECT_SQL,
                    collect_n=100,
                    query_online_scan=(
                        "SELECT `amount`, `ts` FROM `db`.`t_1` WHERE `user_id` = ? "
                        "AND `category` LIKE 'vip?%' ORDER BY `ts` DESC LIMIT ?"
                    ),
                )
            ],
            batch=False,
        )
        rendered = str(
            engine._scan_prepared_statements[OnlineStoreSqlClient.SINGLE_VECTOR_KEY][0]
        )
        assert "LIKE 'vip?%'" in rendered
        assert "`user_id` = :user_id" in rendered
        assert rendered.endswith("LIMIT :hw_rank_cap")
        assert engine._first_unquoted_placeholder(rendered) == -1

    def test_scan_rows_prefer_the_direct_statement(self):
        engine = make_engine()
        engine._collect_n_by_serving_index = {0: 100}
        engine._collect_ascending_by_serving_index = {0: False}
        engine._parametrised_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "windowed-statement"}
        }
        engine._scan_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "direct-statement"}
        }
        seen = {}

        def fake_single_vector_result(entry, statements, raw_rows=False,
                                      scan_limit=None):
            seen["statements"] = statements
            return [{"ts": 3}, {"ts": 2}, {"ts": 1}]

        engine._single_vector_result = fake_single_vector_result
        rows = engine._get_scan_rows({"user_id": 7}, limit=3)
        assert seen["statements"] == {0: "direct-statement"}
        assert [r["ts"] for r in rows] == [3, 2, 1]

    def test_direct_scan_reverses_for_ascending_views(self):
        engine = make_engine()
        engine._collect_n_by_serving_index = {0: 100}
        engine._collect_ascending_by_serving_index = {0: True}
        engine._parametrised_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "windowed-statement"}
        }
        engine._scan_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "direct-statement"}
        }
        engine._single_vector_result = (
            lambda entry, statements, raw_rows=False, scan_limit=None: [
                {"ts": 3},
                {"ts": 2},
                {"ts": 1},
            ]
        )
        rows = engine._get_scan_rows({"user_id": 7})
        assert [r["ts"] for r in rows] == [1, 2, 3]

    def test_scan_rows_refuse_multiple_collect_sources(self):
        import pytest
        from hopsworks_common.client.exceptions import FeatureStoreException

        engine = make_engine()
        engine._collect_n_by_serving_index = {0: 100, 1: 50}
        engine._parametrised_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "collect-a", 1: "collect-b"}
        }
        with pytest.raises(FeatureStoreException, match="multiple"):
            engine._get_scan_rows({"user_id": 7}, limit=5)

    def test_scan_rows_execute_only_collect_statements(self):
        engine = make_engine()
        engine._collect_n_by_serving_index = {0: 100}
        engine._parametrised_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {
                0: "collect-statement",
                1: "point-read-statement",
            }
        }
        seen = {}

        def fake_single_vector_result(entry, statements, raw_rows=False,
                                      scan_limit=None):
            seen["statements"] = statements
            seen["raw_rows"] = raw_rows
            seen["scan_limit"] = scan_limit
            return [{"amount": 1.0}] * 3

        engine._single_vector_result = fake_single_vector_result
        rows = engine._get_scan_rows({"user_id": 7}, limit=2)
        # the point-read statement contributes nothing to scan output
        assert seen["statements"] == {0: "collect-statement"}
        assert seen["raw_rows"] is True
        assert seen["scan_limit"] == 2
        assert len(rows) == 2


class TestSingleVectorDirectScan:
    """get_feature_vector's collect fold executes the direct scan statement.

    The windowed ROW_NUMBER plan ranks the entity's WHOLE history to return the
    newest N; the direct `ORDER BY order_col DESC LIMIT ?` statement walks the
    ordered index backward and stops at the cap. The windowed statement stays
    the fallback for backends predating `queryOnlineScan`.
    """

    def test_fold_prefers_direct_scan_and_binds_full_n(self):
        rows = [{"user_id": 7, "ts": 3}, {"user_id": 7, "ts": 2}]
        engine = make_serving_engine({0: rows})
        vector = engine._single_vector_result(
            {"user_id": 7}, {0: "windowed"}, scan_statements={0: "direct"}
        )
        executed, binds = engine._async_task_thread.calls[0]
        assert executed == {0: "direct"}
        # vector folds bind the FULL collect N, not a narrowed scan limit
        assert binds[0]["hw_rank_cap"] == 3
        assert [s["ts"] for s in vector["t_collect"]] == [3, 2]
        assert vector["user_id"] == 7

    def test_fold_reverses_ascending_scan_rows(self):
        # the direct scan always returns newest-first; an ascending collect
        # folds oldest-first, matching offline output order
        rows = [{"user_id": 7, "ts": 3}, {"user_id": 7, "ts": 2}]
        engine = make_serving_engine({0: rows}, collect_ascending=True)
        vector = engine._single_vector_result(
            {"user_id": 7}, {0: "windowed"}, scan_statements={0: "direct"}
        )
        assert [s["ts"] for s in vector["t_collect"]] == [2, 3]

    def test_fold_keeps_windowed_statement_without_scan(self):
        # old backend: no queryOnlineScan -> the windowed statement executes and
        # already carries the output order server-side (no client reversal)
        rows = [{"user_id": 7, "ts": 2}, {"user_id": 7, "ts": 3}]
        engine = make_serving_engine({0: rows}, collect_ascending=True)
        vector = engine._single_vector_result(
            {"user_id": 7}, {0: "windowed"}, scan_statements=None
        )
        executed, _ = engine._async_task_thread.calls[0]
        assert executed == {0: "windowed"}
        assert [s["ts"] for s in vector["t_collect"]] == [2, 3]

    def test_prefixed_collect_fold_strips_struct_prefix(self):
        # statement columns carry the join prefix (the feature view's names):
        # struct fields fold back to the SOURCE names, matching the persisted
        # array<struct<...>> schema, and the entity key stays scalar under its
        # prefixed name
        rows = [{"txn_user_id": 7, "txn_ts": 3, "txn_amount": 5.0}]
        engine = make_serving_engine({0: rows})
        engine._prefix_by_serving_index = {0: "txn_"}
        engine._collect_name_by_serving_index = {0: "txn_t_collect"}
        vector = engine._single_vector_result({"user_id": 7}, {0: "windowed"})
        assert vector["txn_user_id"] == 7
        assert vector["txn_t_collect"] == [{"ts": 3, "amount": 5.0}]

    def test_get_single_feature_vector_passes_label_scans(self):
        engine = make_engine()
        engine._parametrised_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "windowed"}
        }
        engine._scan_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "direct"}
        }
        captured = {}

        def fake_single_vector_result(entry, statements, scan_statements=None):
            captured["scan_statements"] = scan_statements
            return {}

        engine._single_vector_result = fake_single_vector_result
        engine._get_single_feature_vector({"user_id": 7})
        assert captured["scan_statements"] == {0: "direct"}

    def test_scan_statements_are_kept_per_label(self):
        # each label's scan projects that label's columns: a logging variant's
        # scan (helper columns included) must never serve the plain vector
        engine = make_engine()
        engine._parametrize_prepared_statements(
            [
                make_statement(
                    TestCollectRankCapParameter.COLLECT_SQL,
                    collect_n=100,
                    query_online_scan=(
                        "SELECT `amount`, `ts` FROM `db`.`t_1` "
                        "WHERE `user_id` = ? ORDER BY `ts` DESC LIMIT ?"
                    ),
                )
            ],
            batch=False,
        )
        engine._parametrize_prepared_statements(
            [
                make_statement(
                    TestCollectRankCapParameter.COLLECT_SQL,
                    collect_n=100,
                    query_online_scan=(
                        "SELECT `amount`, `ts`, `helper_col` FROM `db`.`t_1` "
                        "WHERE `user_id` = ? ORDER BY `ts` DESC LIMIT ?"
                    ),
                )
            ],
            batch=False,
            label=OnlineStoreSqlClient.SINGLE_LOGGING_VECTOR_KEY,
        )
        single = engine._scan_prepared_statements[
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY
        ]
        logging_scans = engine._scan_prepared_statements[
            OnlineStoreSqlClient.SINGLE_LOGGING_VECTOR_KEY
        ]
        assert "`helper_col`" not in str(single[0])
        assert "`helper_col`" in str(logging_scans[0])


class TestBatchCollectDirectScans:
    """The batch collect path runs one direct scan per entry.

    The windowed IN plan it replaces reads and ranks the whole history of every
    entity in the list, so its cost scales with total history, not entries * N.
    """

    def test_batch_collect_uses_per_entry_scans(self):
        scan_results = [
            [{"user_id": 0, "ts": 2}, {"user_id": 0, "ts": 1}],
            [],
            [{"user_id": 2, "ts": 9}],
        ]
        engine = make_serving_engine({})
        engine._async_task_thread = _FakeTaskThread({}, scan_results=scan_results)
        batch_results, serving_keys = engine._batch_vector_results(
            [{"user_id": 0}, {"user_id": 1}, {"user_id": 2}],
            {0: "windowed-batch"},
            scan_statements={0: "direct"},
        )
        # point statements and scans run in ONE wave; the windowed IN never executes
        assert engine._async_task_thread.functions == ["_execute_batch_reads"]
        statements, _, scan_specs = engine._async_task_thread.calls[0]
        assert statements == {}
        statement, binds_list = scan_specs[0]
        assert statement == "direct"
        assert binds_list == [
            {"user_id": 0, "hw_rank_cap": 3},
            {"user_id": 1, "hw_rank_cap": 3},
            {"user_id": 2, "hw_rank_cap": 3},
        ]
        assert [s["ts"] for s in batch_results[0]["t_collect"]] == [2, 1]
        # empty semantics pinned to the single-read contract: no history -> []
        assert batch_results[1] == {"t_collect": []}
        assert [s["ts"] for s in batch_results[2]["t_collect"]] == [9]
        assert [sk.feature_name for sk in serving_keys] == ["user_id"]

    def test_batch_scan_deduplicates_repeated_entries(self):
        scan_results = [
            [{"user_id": 0, "ts": 2}],
            [{"user_id": 1, "ts": 9}],
        ]
        engine = make_serving_engine({})
        engine._async_task_thread = _FakeTaskThread({}, scan_results=scan_results)
        batch_results, _ = engine._batch_vector_results(
            [{"user_id": 0}, {"user_id": 0}, {"user_id": 1}],
            {0: "windowed-batch"},
            scan_statements={0: "direct"},
        )
        _, _, scan_specs = engine._async_task_thread.calls[0]
        # the repeated entity issues ONE scan, both entries fold its result
        assert scan_specs[0][1] == [
            {"user_id": 0, "hw_rank_cap": 3},
            {"user_id": 1, "hw_rank_cap": 3},
        ]
        assert [s["ts"] for s in batch_results[0]["t_collect"]] == [2]
        assert [s["ts"] for s in batch_results[1]["t_collect"]] == [2]
        assert [s["ts"] for s in batch_results[2]["t_collect"]] == [9]

    def test_batch_scan_reverses_for_ascending_views(self):
        scan_results = [[{"user_id": 0, "ts": 3}, {"user_id": 0, "ts": 2}]]
        engine = make_serving_engine({}, collect_ascending=True)
        engine._async_task_thread = _FakeTaskThread({}, scan_results=scan_results)
        batch_results, _ = engine._batch_vector_results(
            [{"user_id": 0}], {0: "windowed-batch"}, scan_statements={0: "direct"}
        )
        assert [s["ts"] for s in batch_results[0]["t_collect"]] == [2, 3]

    def test_batch_collect_keeps_windowed_plan_without_scan(self):
        # old backend: no queryOnlineScan -> the windowed IN statement executes
        rows = [{"user_id": 0, "ts": 2}]
        engine = make_serving_engine({0: rows})
        batch_results, _ = engine._batch_vector_results(
            [{"user_id": 0}], {0: "windowed-batch"}, scan_statements=None
        )
        assert engine._async_task_thread.functions == ["_execute_batch_reads"]
        statements, _, scan_specs = engine._async_task_thread.calls[0]
        assert statements == {0: "windowed-batch"}
        assert scan_specs == {}
        assert [s["ts"] for s in batch_results[0]["t_collect"]] == [2]

    def test_batch_synthesizes_aggregate_defaults_for_missed_entities(self):
        # the grouped IN statement emits no row for an entity with no matching
        # rows; the single statement returns COUNT 0 and NULLs, so the batch
        # synthesizes exactly that
        engine = make_serving_engine(
            {0: [{"user_id": 0, "amount_sum": 5.0, "amount_count": 2, "count": 2}]}
        )
        engine._collect_n_by_serving_index = {}
        engine._collect_name_by_serving_index = {}
        engine._aggregate_names_by_serving_index = {
            0: ["amount_sum", "amount_count", "count"]
        }
        batch_results, _ = engine._batch_vector_results(
            [{"user_id": 0}, {"user_id": 1}], {0: "batch-aggregate"}
        )
        assert batch_results[0]["amount_sum"] == 5.0
        assert batch_results[1] == {
            "amount_sum": None,
            "amount_count": 0,
            "count": 0,
        }

    def test_get_batch_feature_vectors_passes_single_label_scans(self):
        engine = make_engine()
        engine._parametrised_prepared_statements = {
            OnlineStoreSqlClient.BATCH_VECTOR_KEY: {0: "windowed-batch"}
        }
        engine._scan_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "direct"}
        }
        captured = {}

        def fake_batch_vector_results(entries, statements, scan_statements=None):
            captured["scan_statements"] = scan_statements
            return [], []

        engine._batch_vector_results = fake_batch_vector_results
        engine._get_batch_feature_vectors([{"user_id": 0}])
        assert captured["scan_statements"] == {0: "direct"}


class TestFalsyServingKeys:
    """Falsy entity keys (0, False, "") are valid and must not trigger fallbacks."""

    def test_entry_value_keeps_falsy_required_keys(self):
        sk = _ServingKey("user_id")
        for falsy in (0, False, ""):
            entry = {"user_id": falsy}
            value = OnlineStoreSqlClient._entry_serving_key_value(entry, sk)
            assert value is falsy or value == falsy

    def test_entry_value_falls_back_only_when_absent(self):
        sk = _ServingKey("user_id", required_serving_key="fg2_user_id")
        assert (
            OnlineStoreSqlClient._entry_serving_key_value(
                {"fg2_user_id": 0, "user_id": 5}, sk
            )
            == 0
        )
        assert (
            OnlineStoreSqlClient._entry_serving_key_value({"user_id": 5}, sk) == 5
        )

    def test_batch_binds_and_stitching_keep_falsy_keys(self):
        rows = [
            {"user_id": 0, "ts": 2, "amount": 5.0},
            {"user_id": 0, "ts": 1, "amount": 3.0},
            {"user_id": 1, "ts": 9, "amount": 7.0},
        ]
        engine = make_serving_engine({0: rows})
        batch_results, _ = engine._batch_vector_results(
            [{"user_id": 0}, {"user_id": 1}], {0: "batch-collect-statement"}
        )
        _, entry_values, _ = engine._async_task_thread.calls[0]
        # entity key 0 binds as 0, not as the feature-name fallback (None)
        assert entry_values[0]["batch_ids"] == [(0,), (1,)]
        assert [s["ts"] for s in batch_results[0]["t_collect"]] == [2, 1]
        assert [s["ts"] for s in batch_results[1]["t_collect"]] == [9]
        assert batch_results[0]["user_id"] == 0


class TestDebugRedaction:
    """Serving DEBUG logs carry indexes, parameter names, and counts: never values."""

    SENTINEL = "SENTINEL-VALUE-0xC0FFEE"

    def test_single_and_batch_paths_never_log_values(self, caplog):
        rows = [{"user_id": self.SENTINEL, "amount": self.SENTINEL}]
        engine = make_serving_engine({0: rows})
        # a point-read statement exercises the non-collect stitching loops
        engine._collect_n_by_serving_index = {}
        engine._collect_name_by_serving_index = {}
        with caplog.at_level(
            logging.DEBUG, logger="hsfs.core.online_store_sql_engine"
        ):
            engine._single_vector_result({"user_id": self.SENTINEL}, {0: "stmt"})
            engine._batch_vector_results([{"user_id": self.SENTINEL}], {0: "stmt"})
        assert caplog.text
        assert self.SENTINEL not in caplog.text

    def test_query_async_sql_logs_parameter_names_only(self, caplog):
        sentinel = self.SENTINEL

        class _StubCursor:
            async def fetchall(self):
                return [{"amount": sentinel}]

            async def close(self):
                return None

        class _StubConn:
            async def execute(self, stmt, params):
                return _StubCursor()

        class _StubAcquire:
            async def __aenter__(self):
                return _StubConn()

            async def __aexit__(self, *args):
                return False

        class _StubPool:
            def acquire(self):
                return _StubAcquire()

        engine = make_engine()
        with caplog.at_level(
            logging.DEBUG, logger="hsfs.core.online_store_sql_engine"
        ):
            result = asyncio.run(
                engine._query_async_sql(
                    "SELECT `amount` FROM `t` WHERE `user_id` = :user_id",
                    {"user_id": sentinel},
                    _StubPool(),
                )
            )
        assert result == [{"amount": sentinel}]
        assert "user_id" in caplog.text
        assert sentinel not in caplog.text
