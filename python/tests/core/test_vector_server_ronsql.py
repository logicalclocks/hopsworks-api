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

import pytest
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor.serving_prepared_statement import ServingPreparedStatement
from hsfs.core.vector_server import VectorServer


TEMPLATE = (
    "WITH t AS (SELECT `amount`, `event_time` FROM `transactions_1` "
    "WHERE `user_id` = ? ORDER BY `event_time` DESC LIMIT 100) "
    "SELECT `amount`, `event_time` FROM t;"
)


def make_statement(**overrides):
    kwargs = {
        "feature_group_id": 1,
        "prepared_statement_index": 0,
        "prepared_statement_parameters": [{"name": "user_id", "index": 1}],
        "query_online": "ignored",
        "query_ronsql": TEMPLATE,
        "ronsql_database": "proj_featurestore",
        "collect_n": 100,
    }
    kwargs.update(overrides)
    return ServingPreparedStatement(**kwargs)


def make_server():
    server = VectorServer.__new__(VectorServer)
    server._serving_keys = []
    return server


class TestRonsqlLiteral:
    def test_int_inlines_bare(self):
        assert VectorServer._ronsql_literal(7) == "7"

    def test_bool_before_int(self):
        assert VectorServer._ronsql_literal(True) == "1"
        assert VectorServer._ronsql_literal(False) == "0"

    def test_float(self):
        assert VectorServer._ronsql_literal(3.5) == "3.5"

    def test_nan_rejected(self):
        with pytest.raises(FeatureStoreException):
            VectorServer._ronsql_literal(float("nan"))

    def test_string_quote_doubling(self):
        assert VectorServer._ronsql_literal("o'brien") == "'o''brien'"

    def test_string_control_chars_rejected(self):
        with pytest.raises(FeatureStoreException):
            VectorServer._ronsql_literal("x\x01y")

    def test_string_backslash_rejected(self):
        with pytest.raises(FeatureStoreException):
            VectorServer._ronsql_literal("a\\b")

    def test_datetime(self):
        assert (
            VectorServer._ronsql_literal(datetime(2026, 6, 21, 10, 0, 0))
            == "'2026-06-21 10:00:00'"
        )

    def test_unsupported_type_rejected(self):
        with pytest.raises(FeatureStoreException):
            VectorServer._ronsql_literal(object())


class TestRonsqlTemplateSubstitution:
    def test_substitutes_entity_key(self):
        query = make_server()._substitute_ronsql_template(
            make_statement(), {"user_id": 7}
        )
        assert "`user_id` = 7 " in query
        assert "?" not in query

    def test_missing_entity_key_raises(self):
        with pytest.raises(FeatureStoreException):
            make_server()._substitute_ronsql_template(make_statement(), {})

    def test_string_key_is_escaped_not_injected(self):
        stmt = make_statement(
            prepared_statement_parameters=[{"name": "user_name", "index": 1}],
            query_ronsql=TEMPLATE.replace("`user_id` = ?", "`user_name` = ?"),
        )
        query = make_server()._substitute_ronsql_template(
            stmt, {"user_name": "a'; DROP TABLE x; --"}
        )
        # quote-doubled, so the payload stays inside one string literal
        assert "'a''; DROP TABLE x; --'" in query


class _StubRestEngine:
    def __init__(self, feature_names_per_fg_id):
        self._feature_names_per_fg_id = feature_names_per_fg_id


class TestRonsqlCollectOverlay:
    def make_collect_server(self, rows):
        server = make_server()
        server._ronsql_scan_statement = make_statement()
        server._rest_client_engine = _StubRestEngine(
            {1: ["user_id", "amount", "event_time"]}
        )
        server._scan_rows_ronsql = lambda entry, limit=None: rows
        return server

    def test_overlay_folds_collect_columns_aligned(self):
        rows = [
            {"user_id": 7, "amount": 10.0, "event_time": "t3"},
            {"user_id": 7, "amount": None, "event_time": "t2"},
            {"user_id": 7, "amount": 30.0, "event_time": "t1"},
        ]
        server = self.make_collect_server(rows)
        vector = server._overlay_ronsql_collect(
            {"user_id": 7, "amount": None, "event_time": None, "tier": "gold"},
            {"user_id": 7},
        )
        # nulls preserved so columns stay row-aligned; entity key stays scalar
        assert vector["amount"] == [10.0, None, 30.0]
        assert vector["event_time"] == ["t3", "t2", "t1"]
        assert vector["user_id"] == 7
        # joined features untouched
        assert vector["tier"] == "gold"

    def test_overlay_noop_without_collect_statement(self):
        server = make_server()
        server._ronsql_scan_statement = None
        original = {"a": 1}
        assert server._overlay_ronsql_collect(original, {"user_id": 7}) == {"a": 1}
