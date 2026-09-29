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

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import PropertyMock

import pytest
from hopsworks_common.core.constants import HAS_POLARS
from hsfs.core.vector_server import VectorServer, _with_entry_values


class TestVectorServer:
    # Schema of a chained-MDT feature view: primary key + passthrough features +
    # the three transformation outputs (two dropped raw inputs are absent).
    COLS = [
        "index",
        "data3",
        "data4",
        "category1",
        "add_one_data1_",
        "add_one_data2_",
        "add_two_data1_data2",
    ]

    def _server(self, mocker):
        server = VectorServer.__new__(VectorServer)
        mocker.patch.object(
            VectorServer,
            "transformed_feature_vector_col_name",
            new_callable=PropertyMock,
            return_value=self.COLS,
        )
        return server

    def test_logging_metadata_rows_stay_parallel_without_request_parameters(
        self, mocker
    ):
        # A logged view with no request parameters used to extend the parallel
        # request-parameter list with None and raise TypeError, which reached
        # clients as TRANSFORMATION_FAILED on every prediction.
        server = VectorServer.__new__(VectorServer)
        server._feature_view_logging_enabled = True
        server._fetch_inference_helpers_for_transformations = False
        server._root_feature_group = SimpleNamespace(event_time="event_time")
        server._inference_helper_col_name = []
        entries = [{"customer_id": 1}, {"customer_id": 2}]
        captured = {}

        mocker.patch.object(
            VectorServer, "_which_client_and_ensure_initialised", return_value="rest"
        )
        mocker.patch.object(VectorServer, "_raise_transformation_warnings")
        mocker.patch.object(
            VectorServer, "_validate_entry", side_effect=lambda entry, **kwargs: entry
        )
        mocker.patch.object(
            VectorServer,
            "rest_client_engine",
            new_callable=PropertyMock,
            return_value=mocker.MagicMock(
                **{"_get_batch_feature_vectors.return_value": [{"a": 1}, {"a": 2}]}
            ),
        )
        mocker.patch.object(VectorServer, "_assemble_feature_vector", return_value=[1])
        mocker.patch.object(
            VectorServer,
            "_handle_feature_vector_return_type",
            side_effect=lambda vectors, **kwargs: (
                captured.update(metadata=kwargs.get("logging_meta_data")) or vectors
            ),
        )

        server._get_feature_vectors(
            entries=entries, vector_db_features=[], logging_data=True
        )

        metadata = captured["metadata"]
        assert metadata.serving_keys == entries
        assert metadata.request_parameters == [{}, {}]

    def test_handle_return_type_empty_single_vector_pandas_does_not_crash(self, mocker):
        # An online lookup that misses makes assemble_feature_vector return None.
        # The single-vector pandas path must emit a one-row all-missing frame
        # matching the schema, not raise "Shape of passed values is (1, 1),
        # indices imply (1, 7)" from pandas internals.
        server = self._server(mocker)

        df = server._handle_feature_vector_return_type(
            None,
            batch=False,
            inference_helper=False,
            return_type="pandas",
            transform=True,
        )

        assert list(df.columns) == self.COLS
        assert len(df) == 1
        assert df.iloc[0].isna().all()

    @pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
    def test_handle_return_type_empty_single_vector_polars_does_not_crash(self, mocker):
        # Polars raises a ShapeError on the same input; the guard must cover it too.
        server = self._server(mocker)

        df = server._handle_feature_vector_return_type(
            None,
            batch=False,
            inference_helper=False,
            return_type="polars",
            transform=True,
        )

        assert df.columns == self.COLS
        assert df.height == 1

    def test_handle_return_type_populated_single_vector_pandas(self, mocker):
        # A populated chained-MDT vector still builds the correct seven-column row.
        server = self._server(mocker)
        values = [1, 3, 4, "a", 11, 21, 32]

        df = server._handle_feature_vector_return_type(
            values,
            batch=False,
            inference_helper=False,
            return_type="pandas",
            transform=True,
        )

        assert list(df.columns) == self.COLS
        assert len(df) == 1
        assert df.iloc[0]["add_two_data1_data2"] == 32

    def test_setup_rest_client_binds_real_init_signature(self, mocker):
        # Other tests mock _init_or_reset_online_store_rest_client, so a keyword
        # mismatch at this call site (e.g. the over-renamed _reset_client= from the
        # HWORKS-2849 privatization port) only surfaced on a live cluster. Run the
        # real function with only the singleton class mocked.
        import hopsworks_common.client.online_store_rest_client as rest_client_module

        mocker.patch.object(rest_client_module, "_online_store_rest_client", None)
        singleton = mocker.patch.object(
            rest_client_module, "OnlineStoreRestClientSingleton"
        )
        mocker.patch("hsfs.core.vector_server.online_store_rest_client_engine")

        server = VectorServer.__new__(VectorServer)
        server._feature_store_name = "test_featurestore"
        entity = mocker.Mock()
        entity.name = "fv_test"
        entity.version = 1
        entity.features = []

        server._setup_rest_client_and_engine(entity, reset_rest_client=True)

        singleton.assert_called_once_with(transport=None, optional_config=None)

    @pytest.mark.parametrize(
        "timestamp_value, expected",
        [
            ("2024-04-18 12:00:25", datetime(2024, 4, 18, 12, 0, 25)),
            # fractional seconds appear when the online type has sub-second
            # precision, e.g. timestamp(3) (FSTORE-2061)
            ("2024-04-18 12:00:25.789", datetime(2024, 4, 18, 12, 0, 25, 789000)),
            ("2024-04-18 12:00:25.789000", datetime(2024, 4, 18, 12, 0, 25, 789000)),
            (1713441625789, datetime(2024, 4, 18, 12, 0, 25, 789000)),
            (
                datetime(2024, 4, 18, 12, 0, 25, 789000),
                datetime(2024, 4, 18, 12, 0, 25, 789000),
            ),
            (None, None),
        ],
    )
    def test_handle_timestamp_based_on_dtype(self, timestamp_value, expected):
        server = VectorServer.__new__(VectorServer)

        assert server._handle_timestamp_based_on_dtype(timestamp_value) == expected


class TestRequestParametersAreRequestLocal:
    """A caller's dictionaries are read, never written.

    The entry's values back the on-demand features a retrieved vector does not
    carry, but merging them used to write into the caller's own dictionary, so
    one request left its parameters changed for the next and two concurrent
    requests through one feature view could see each other's.
    """

    def test_a_single_entry_is_merged_into_a_new_dictionary(self):
        parameters = {"factor": 2.0}
        entry = {"id": 1, "amount": 5}

        merged = _with_entry_values(parameters, entry)

        assert merged == {"id": 1, "amount": 5, "factor": 2.0}
        assert parameters == {"factor": 2.0}, "the caller's parameters were written to"
        assert merged is not parameters

    def test_a_batch_is_merged_into_new_dictionaries(self):
        parameters = [{"factor": 2.0}, {"factor": 3.0}]
        entries = [{"id": 1}, {"id": 2}]

        merged = _with_entry_values(parameters, entries)

        assert merged == [{"id": 1, "factor": 2.0}, {"id": 2, "factor": 3.0}]
        assert parameters == [{"factor": 2.0}, {"factor": 3.0}]

    def test_one_shared_parameter_dictionary_serves_a_single_entry_batch(self):
        parameters = {"factor": 2.0}
        entries = [{"id": 1}]

        assert _with_entry_values(parameters, entries) == {"id": 1, "factor": 2.0}
        assert parameters == {"factor": 2.0}

    def test_an_explicit_parameter_wins_over_the_entry(self):
        assert _with_entry_values({"amount": 9}, {"amount": 5}) == {"amount": 9}

    @pytest.mark.parametrize(
        "parameters, entries",
        [
            (None, {"id": 1}),
            ({}, {"id": 1}),
            ({"factor": 2.0}, None),
            ({"factor": 2.0}, []),
            # lengths that do not line up are left alone, as before
            ([{"factor": 2.0}], [{"id": 1}, {"id": 2}]),
        ],
    )
    def test_nothing_to_merge_returns_the_parameters_unchanged(
        self, parameters, entries
    ):
        assert _with_entry_values(parameters, entries) is parameters

    def test_a_falsy_serving_key_value_is_still_merged(self):
        """0, False and the empty string are values, not absences."""
        merged = _with_entry_values({"factor": 1}, {"a": 0, "b": False, "c": ""})

        assert merged == {"a": 0, "b": False, "c": "", "factor": 1}


class TestReadDeadline:
    """A caller's deadline reaches the dispatcher, which is where the wait was."""

    def _client(self, mocker):
        from hsfs.core.online_store_sql_engine import OnlineStoreSqlClient

        client = OnlineStoreSqlClient.__new__(OnlineStoreSqlClient)
        client._async_task_thread = mocker.Mock()
        client._async_task_thread._submit.return_value = {}
        client._prepared_statements = {}
        client._parametrised_prepared_statements = {
            OnlineStoreSqlClient.SINGLE_VECTOR_KEY: {},
            OnlineStoreSqlClient.BATCH_VECTOR_KEY: {},
        }
        return client

    def test_a_single_read_passes_its_timeout(self, mocker):
        client = self._client(mocker)
        mocker.patch.object(client, "_single_vector_result", return_value={})

        client._get_single_feature_vector({"id": 1}, timeout=2.5)

        assert client._single_vector_result.call_args.kwargs["timeout"] == 2.5

    def test_a_batch_read_passes_its_timeout(self, mocker):
        client = self._client(mocker)
        mocker.patch.object(client, "_batch_vector_results", return_value=([], None))

        client._get_batch_feature_vectors([{"id": 1}], timeout=2.5)

        assert client._batch_vector_results.call_args.kwargs["timeout"] == 2.5

    def test_no_timeout_still_means_wait(self, mocker):
        """Unset keeps what a caller that names no timeout got before."""
        client = self._client(mocker)
        mocker.patch.object(client, "_single_vector_result", return_value={})

        client._get_single_feature_vector({"id": 1})

        assert client._single_vector_result.call_args.kwargs["timeout"] is None


class TestNativeAsyncLookup:
    """The awaitable drivers reach the client's async methods, not a worker thread."""

    def _server(self, mocker):
        server = VectorServer.__new__(VectorServer)
        sql = mocker.MagicMock()
        mocker.patch.object(
            VectorServer, "sql_client", new_callable=PropertyMock, return_value=sql
        )
        mocker.patch.object(
            VectorServer,
            "rest_client_engine",
            new_callable=PropertyMock,
            return_value=mocker.MagicMock(),
        )
        server._fetch_inference_helpers_for_transformations = False
        return server, sql

    def test_the_batch_driver_awaits_the_sql_client(self, mocker):
        import asyncio

        server, sql = self._server(mocker)

        async def awaited(entries, **kwargs):
            return ([{"a": 1}], None)

        sql._get_batch_feature_vectors_async = awaited
        mocker.patch.object(VectorServer, "_batch_lookup_arguments", return_value=False)

        def steps(*args, **kwargs):
            # The body hands out everything the fetch needs, including the two options
            # that shape it.
            return (yield ["entry"], "sql", False, False)

        mocker.patch.object(VectorServer, "_feature_vectors_steps", steps)

        assert asyncio.run(server._get_feature_vectors_async()) == [{"a": 1}]
        # The blocking client must not be touched on the awaited path.
        sql._get_batch_feature_vectors.assert_not_called()

    def test_the_single_driver_awaits_the_sql_client(self, mocker):
        import asyncio

        server, sql = self._server(mocker)

        async def awaited(entry, **kwargs):
            return {"a": 1}

        sql._get_single_feature_vector_async = awaited
        mocker.patch.object(
            VectorServer, "_single_lookup_arguments", return_value=False
        )

        def steps(*args, **kwargs):
            return (yield {"pk": 1}, "sql", False, False)

        mocker.patch.object(VectorServer, "_feature_vector_steps", steps)

        assert asyncio.run(server._get_feature_vector_async()) == {"a": 1}
        sql._get_single_feature_vector.assert_not_called()

    def test_the_options_come_from_the_body_not_the_call(self, mocker):
        # The body already has allow_missing and logging_data bound, so the drivers take
        # them from the yield. Recovering them from the call instead cost an
        # inspect.signature bind on every request and could disagree with the body.
        import asyncio

        server, sql = self._server(mocker)
        seen = {}

        async def awaited(entries, **kwargs):
            seen.update(kwargs)
            return ([{"a": 1}], None)

        sql._get_batch_feature_vectors_async = awaited
        mocker.patch.object(VectorServer, "_batch_lookup_arguments", return_value=False)

        def steps(*args, **kwargs):
            return (yield ["entry"], "sql", True, True)

        mocker.patch.object(VectorServer, "_feature_vectors_steps", steps)

        asyncio.run(server._get_feature_vectors_async())
        assert seen["logging_data"] is True

    def test_a_body_that_suspends_twice_is_refused(self):
        from hsfs.core import vector_server

        def steps():
            yield "first"
            yield "second"

        gen = steps()
        next(gen)
        with pytest.raises(RuntimeError, match="more than once"):
            vector_server._resume(gen, None)

    def test_the_async_drivers_take_a_timeout(self, mocker):
        # The public async methods forward every keyword of the blocking ones,
        # timeout included, and the step bodies do not accept it.
        import asyncio

        server, sql = self._server(mocker)

        async def awaited(entries, **kwargs):
            return ([{"a": 1}], None)

        sql._get_batch_feature_vectors_async = awaited
        mocker.patch.object(VectorServer, "_batch_lookup_arguments", return_value=False)

        def steps(*args, **kwargs):
            assert "timeout" not in kwargs
            return (yield ["entry"], "sql", False, False)

        mocker.patch.object(VectorServer, "_feature_vectors_steps", steps)

        result = asyncio.run(server._get_feature_vectors_async(timeout=5))
        assert result == [{"a": 1}]

    def test_a_slow_async_read_raises_timeout_error(self, mocker):
        import asyncio

        server, sql = self._server(mocker)

        async def slow(entry, **kwargs):
            await asyncio.sleep(10)

        sql._get_single_feature_vector_async = slow
        mocker.patch.object(
            VectorServer, "_single_lookup_arguments", return_value=False
        )

        def steps(*args, **kwargs):
            return (yield {"pk": 1}, "sql", False, False)

        mocker.patch.object(VectorServer, "_feature_vector_steps", steps)

        with pytest.raises(TimeoutError):
            asyncio.run(server._get_feature_vector_async(timeout=0.01))
