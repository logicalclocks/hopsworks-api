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

from __future__ import annotations

import pytest
from hsfs.core import online_store_sql_engine
from hsfs.core.online_store_sql_engine import OnlineStoreSqlClient


class TestConcurrencyFollowsTheConfiguredPool:
    """A caller that asked for a larger pool asked to read more at once.

    The limit was the number of prepared statements, which is the number of
    feature groups in the view. A view over one feature group therefore read one
    at a time however large a pool `options={"maxsize": N}` asked for, and the
    dispatcher's concurrency could not be reached through the public client.
    """

    def _client(self, mocker, statements=1):
        client = OnlineStoreSqlClient.__new__(OnlineStoreSqlClient)
        client._feature_store_id = 1
        client._external = False
        client._async_task_thread = None
        client._prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: dict.fromkeys(
                range(statements), "q"
            )
        }
        client._storage_connector_api = mocker.Mock()
        mocker.patch("hsfs.core.online_store_sql_engine.variable_api.VariableApi")
        started = {}
        mocker.patch.object(
            online_store_sql_engine.AsyncTaskThread,
            "start",
            autospec=True,
            side_effect=lambda self: started.update(thread=self),
        )
        return client, started

    def test_a_bigger_pool_means_more_reads_at_once(self, mocker):
        client, started = self._client(mocker, statements=1)

        client._init_async_mysql_connection(options={"maxsize": 16})

        assert started["thread"]._max_concurrent_tasks == 16

    def test_the_default_is_one_read_per_prepared_statement(self, mocker):
        client, started = self._client(mocker, statements=3)

        client._init_async_mysql_connection(options=None)

        assert started["thread"]._max_concurrent_tasks == 3

    def test_a_view_over_one_group_still_gets_a_slot(self, mocker):
        client, started = self._client(mocker, statements=1)

        client._init_async_mysql_connection(options={})

        assert started["thread"]._max_concurrent_tasks == 1


class TestAFailedStatementSettlesTheOthers:
    """A retry must not queue behind the first attempt's remaining queries."""

    def test_the_other_queries_are_cancelled_before_the_error_is_raised(self, mocker):
        import asyncio

        client = OnlineStoreSqlClient.__new__(OnlineStoreSqlClient)
        client._connection_options = None
        mocker.patch.object(
            OnlineStoreSqlClient,
            "connection_options",
            new_callable=mocker.PropertyMock,
            return_value=None,
        )
        settled = []

        async def query(stmt, _bind, _pool):
            if stmt == "fails":
                raise ConnectionError("gone")
            try:
                await asyncio.sleep(3600)
            finally:
                settled.append(stmt)

        client._query_async_sql = query

        async def run():
            with pytest.raises(ConnectionError):
                await client._execute_prep_statements(
                    {1: "fails", 2: "slow", 3: "slower"},
                    {1: {}, 2: {}, 3: {}},
                    connection_pool=None,
                )
            # Settled by the time the error reached the caller, not later.
            return sorted(settled)

        assert asyncio.run(run()) == ["slow", "slower"]
