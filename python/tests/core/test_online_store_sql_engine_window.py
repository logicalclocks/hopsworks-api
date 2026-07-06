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
    return engine


def make_statement(query_online, aggregate_window=None):
    return ServingPreparedStatement(
        feature_group_id=1,
        prepared_statement_index=0,
        prepared_statement_parameters=[{"name": "user_id", "index": 1}],
        query_online=query_online,
        aggregate_window=aggregate_window,
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
