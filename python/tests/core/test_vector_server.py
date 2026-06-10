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
