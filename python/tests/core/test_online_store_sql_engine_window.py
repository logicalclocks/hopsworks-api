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
from hsfs.constructor.serving_prepared_statement import ServingPreparedStatement
from hsfs.core.online_store_sql_engine import OnlineStoreSqlClient


def make_engine():
    engine = OnlineStoreSqlClient.__new__(OnlineStoreSqlClient)
    engine._skip_fg_ids = set()
    engine._aggregate_window_by_serving_index = {}
    engine._rank_cap_by_serving_index = {}
    engine._scan_prepared_statements = {}
    engine._collect_ascending_by_serving_index = {}
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
        rendered = str(engine._scan_prepared_statements[0])
        assert ":user_id" in rendered
        assert rendered.endswith("LIMIT :hw_rank_cap")
        assert engine._first_unquoted_placeholder(rendered) == -1
        assert engine._rank_cap_by_serving_index == {0: 100}

    def test_scan_rows_prefer_the_direct_statement(self):
        engine = make_engine()
        engine._collect_n_by_serving_index = {0: 100}
        engine._collect_ascending_by_serving_index = {0: False}
        engine._parametrised_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {0: "windowed-statement"}
        }
        engine._scan_prepared_statements = {0: "direct-statement"}
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
        engine._scan_prepared_statements = {0: "direct-statement"}
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
