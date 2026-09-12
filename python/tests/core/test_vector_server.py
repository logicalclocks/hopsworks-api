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
from hsfs.core.vector_server import VectorServer


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

    # `default_client` is the only argument that carries the request for a client:
    # init_rest_client defaults to False, so naming "rest" used to fall through to the
    # sql branch and serve every read over SQL with no error and no warning.
    def test_default_client_rest_initialises_the_rest_client(self):
        server = VectorServer(1, [])

        server._set_default_client(
            init_rest_client=False, init_sql_client=None, default_client="rest"
        )

        assert server.default_client == VectorServer.DEFAULT_REST_CLIENT
        assert server._init_rest_client is True
        # brought along so a statement that fails the RonSQL EXPLAIN gate has a client
        # to reclassify onto
        assert server._init_sql_client is True

    def test_default_client_rest_respects_an_explicitly_declined_sql_client(self):
        server = VectorServer(1, [])

        server._set_default_client(
            init_rest_client=False, init_sql_client=False, default_client="rest"
        )

        assert server.default_client == VectorServer.DEFAULT_REST_CLIENT
        assert server._init_rest_client is True
        assert server._init_sql_client is False

    def test_default_client_defaults_to_sql(self):
        server = VectorServer(1, [])

        server._set_default_client(
            init_rest_client=False, init_sql_client=None, default_client=None
        )

        assert server.default_client == VectorServer.DEFAULT_SQL_CLIENT
        assert server._init_sql_client is True

    def test_default_client_sql_is_unchanged(self):
        server = VectorServer(1, [])

        server._set_default_client(
            init_rest_client=True, init_sql_client=True, default_client="sql"
        )

        assert server.default_client == VectorServer.DEFAULT_SQL_CLIENT

    def test_no_client_at_all_still_raises(self):
        server = VectorServer(1, [])

        with pytest.raises(ValueError, match="At least one of the clients"):
            server._set_default_client(
                init_rest_client=False, init_sql_client=False, default_client=None
            )

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


class TestBatchLoggingMetaData:
    """Logging metadata must stay aligned with the entries it describes.

    `_get_feature_vectors` appends one serving-key entry per row and one
    request-parameter entry per row; the feature logger zips them. A request
    carrying no parameters left the copy as None, and extending a list with
    None raised TypeError, which surfaced as TRANSFORMATION_FAILED and took
    down every prediction of a logging-enabled deployment.
    """

    def _server(self, mocker, entries, captured):
        """A VectorServer stubbed down to the batch assembly path under test."""
        server = VectorServer.__new__(VectorServer)
        server._feature_view_logging_enabled = True
        server._inference_helper_col_name = []
        server._fetch_inference_helpers_for_transformations = False
        server._root_feature_group = SimpleNamespace(event_time="event_time")

        mocker.patch.object(
            server, "_which_client_and_ensure_initialised", return_value="sql"
        )
        mocker.patch.object(server, "_raise_transformation_warnings", return_value=None)
        # Every entry validates to itself, so none is skipped.
        mocker.patch.object(
            server, "_validate_entry", side_effect=lambda entry, **k: entry
        )
        # sql_client is a read-only property, so patch it on the class.
        mocker.patch.object(
            VectorServer,
            "sql_client",
            new_callable=PropertyMock,
            return_value=SimpleNamespace(
                _get_batch_feature_vectors=lambda *a, **k: ([{} for _ in entries], None)
            ),
        )

        def capture(*args, **kwargs):
            captured["meta"] = kwargs.get("logging_meta_data")
            return {"f": 1}

        mocker.patch.object(server, "_assemble_feature_vector", side_effect=capture)
        mocker.patch.object(
            server, "_handle_feature_vector_return_type", side_effect=lambda v, **k: v
        )
        return server

    def test_no_request_parameters_does_not_raise_and_stays_aligned(self, mocker):
        """The regression: request_parameters=None must not reach list.extend."""
        entries = [{"user_id": 1}, {"user_id": 2}, {"user_id": 3}]
        captured = {}
        server = self._server(mocker, entries, captured)

        server._get_feature_vectors(
            entries=entries,
            passed_features=[],
            vector_db_features=[],
            request_parameters=None,
            logging_data=True,
        )

        meta = captured["meta"]
        assert meta is not None
        assert meta.request_parameters == [{}, {}, {}]
        assert len(meta.request_parameters) == len(meta.serving_keys) == len(entries)

    def test_dict_request_parameters_are_broadcast_per_row(self, mocker):
        entries = [{"user_id": 1}, {"user_id": 2}]
        captured = {}
        server = self._server(mocker, entries, captured)

        server._get_feature_vectors(
            entries=entries,
            passed_features=[],
            vector_db_features=[],
            request_parameters={"now": 5},
            logging_data=True,
        )

        meta = captured["meta"]
        assert meta.request_parameters == [{"now": 5}, {"now": 5}]
        assert len(meta.request_parameters) == len(meta.serving_keys) == len(entries)
