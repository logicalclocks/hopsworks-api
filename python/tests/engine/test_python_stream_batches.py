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

import decimal

import pyarrow as pa
from hsfs import feature
from hsfs.engine.python import Engine


class TestStreamedBatchesShareOneSchema:
    """The batches of one read describe their columns the same way.

    Each batch used to be built from the values it happened to hold, so a
    nullable text column was `null` in an all-null batch and `string` in the
    next, and a decimal's precision and scale followed its values. Putting such
    batches back together raises, which makes the stream unusable for the thing
    it exists for.
    """

    def _batches(self, engine, rows_per_batch, names, schema, batch_size=2):
        """Drive the online branch with a stubbed cursor."""

        class _Result:
            def __init__(self):
                self._batches = list(rows_per_batch)

            def keys(self):
                return names

            def fetchmany(self, _size):
                return self._batches.pop(0) if self._batches else []

        class _Connection:
            def execute(self, _statement):
                return _Result()

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        class _Engine:
            def connect(self):
                return self

            def execution_options(self, **_kwargs):
                return _Connection()

        engine._mysql_online_fs_engine = _Engine()
        return list(
            engine._stream_batches(
                "SELECT 1",
                None,
                {},
                online=True,
                batch_size=batch_size,
                schema=schema,
            )
        )

    def test_an_all_null_batch_keeps_the_declared_type(self):
        engine = Engine.__new__(Engine)
        batches = self._batches(
            engine,
            [[(None,), (None,)], [("present",), ("also",)]],
            ["text"],
            [feature.Feature("text", type="string")],
        )

        assert [b.schema.field("text").type for b in batches] == [
            pa.string(),
            pa.string(),
        ]
        assert pa.Table.from_batches(batches).num_rows == 4

    def test_a_decimal_keeps_its_declared_precision_and_scale(self):
        engine = Engine.__new__(Engine)
        batches = self._batches(
            engine,
            [
                [(decimal.Decimal("1.5"),), (decimal.Decimal("2.5"),)],
                [(decimal.Decimal("123.456"),), (decimal.Decimal("0.001"),)],
            ],
            ["amount"],
            [feature.Feature("amount", type="decimal(6,3)")],
        )

        assert {b.schema.field("amount").type for b in batches} == {pa.decimal128(6, 3)}
        assert pa.Table.from_batches(batches).num_rows == 4

    def test_batches_can_be_put_back_together_without_a_declared_type(self):
        """An undeclared column is settled by the first batch, not by each one."""
        engine = Engine.__new__(Engine)
        batches = self._batches(
            engine,
            [[("a",), ("b",)], [(None,), (None,)]],
            ["thing"],
            None,
        )

        assert pa.Table.from_batches(batches).num_rows == 4

    def test_a_boolean_arriving_as_tinyint_is_a_boolean(self):
        """The online store keeps a boolean as TINYINT, so rows carry 0 and 1."""
        engine = Engine.__new__(Engine)
        batches = self._batches(
            engine,
            [[(1,), (0,)], [(None,), (None,)]],
            ["flag"],
            [feature.Feature("flag", type="boolean")],
        )

        table = pa.Table.from_batches(batches)
        assert table.schema.field("flag").type == pa.bool_()
        assert table.column("flag").to_pylist() == [True, False, None, None]

    def test_a_complex_feature_keeps_the_bytes_the_online_store_holds(self):
        """An array is VARBINARY online; reading its bytes as a list would corrupt it."""
        engine = Engine.__new__(Engine)
        batches = self._batches(
            engine,
            [[(b"\x02\x04",), (None,)]],
            ["items"],
            [feature.Feature("items", type="array<int>")],
        )

        assert batches[0].schema.field("items").type == pa.binary()
        assert batches[0].column(0).to_pylist() == [b"\x02\x04", None]

    def test_a_name_two_features_share_is_not_declared(self):
        declared = Engine._declared_arrow_types(
            [
                feature.Feature("amount", type="double"),
                feature.Feature("amount", type="string"),
                feature.Feature("id", type="bigint"),
            ]
        )

        assert declared == {"id": pa.int64()}
