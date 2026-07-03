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
        "collect_feature_name": "transactions_collect",
        "collect_order_by": "event_time",
        "collect_ascending": False,
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
    def make_ronsql_server(self, statement, rows):
        server = make_server()
        server._ronsql_statements = [statement]
        server._rest_client_engine = _StubRestEngine(
            {1: ["user_id", "amount", "event_time"]}
        )
        server._execute_ronsql_statement = lambda stmt, entry: rows
        return server

    def test_overlay_folds_rows_into_struct_array(self):
        rows = [
            {"user_id": 7, "amount": 10.0, "event_time": "t3"},
            {"user_id": 7, "amount": None, "event_time": "t2"},
            {"user_id": 7, "amount": 30.0, "event_time": "t1"},
        ]
        server = self.make_ronsql_server(make_statement(), rows)
        vector = server._overlay_ronsql_collect(
            {"user_id": 7, "transactions_collect": None, "tier": "gold"},
            {"user_id": 7},
        )
        # one array<struct> feature (v2 C1), newest-first, entity key stripped from rows
        assert vector["transactions_collect"] == [
            {"amount": 10.0, "event_time": "t3"},
            {"amount": None, "event_time": "t2"},
            {"amount": 30.0, "event_time": "t1"},
        ]
        # entity key stays scalar; joined features untouched
        assert vector["user_id"] == 7
        assert vector["tier"] == "gold"

    def test_overlay_applies_statement_prefix_to_collect_feature(self):
        rows = [{"user_id": 7, "amount": 10.0, "event_time": "t1"}]
        server = self.make_ronsql_server(make_statement(prefix="txn_"), rows)
        vector = server._overlay_ronsql_collect({}, {"user_id": 7})
        assert vector["txn_transactions_collect"] == [
            {"amount": 10.0, "event_time": "t1"}
        ]

    def test_overlay_legacy_backend_folds_per_column(self):
        # statements from a backend predating the collapsed schema carry no
        # collect_feature_name; each column folds into its own row-aligned list
        rows = [
            {"user_id": 7, "amount": 10.0, "event_time": "t2"},
            {"user_id": 7, "amount": None, "event_time": "t1"},
        ]
        server = self.make_ronsql_server(
            make_statement(collect_feature_name=None), rows
        )
        vector = server._overlay_ronsql_collect(
            {"user_id": 7, "amount": None, "event_time": None}, {"user_id": 7}
        )
        assert vector["amount"] == [10.0, None]
        assert vector["event_time"] == ["t2", "t1"]
        assert vector["user_id"] == 7

    def test_overlay_merges_aggregate_row_scalars(self):
        statement = make_statement(
            collect_n=None,
            aggregate_window=30 * 24 * 3600,
            query_ronsql=(
                "SELECT COUNT(`amount`) AS `amount_count`, SUM(`amount`) AS "
                "`amount_sum` FROM `transactions_1` WHERE `user_id` = ? AND "
                "`event_time` >= ?;"
            ),
        )
        rows = [{"amount_count": 41, "amount_sum": 812.5}]
        server = self.make_ronsql_server(statement, rows)
        vector = server._overlay_ronsql_collect({"tier": "gold"}, {"user_id": 7})
        assert vector["amount_count"] == 41
        assert vector["amount_sum"] == 812.5
        assert vector["tier"] == "gold"

    def test_overlay_batch_per_entry(self):
        # each entry gets its own RonSQL execution; results stay aligned with entries
        statement = make_statement()
        server = make_server()
        server._ronsql_statements = [statement]
        server._rest_client_engine = _StubRestEngine({1: ["user_id", "amount"]})
        server._execute_ronsql_statement = lambda stmt, entry: [
            {"user_id": entry["user_id"], "amount": float(entry["user_id"]) * 10}
        ]
        results = [
            server._overlay_ronsql_collect(dict(result), entry)
            for result, entry in zip(
                [{"tier": "gold"}, {"tier": "bronze"}],
                [{"user_id": 1}, {"user_id": 2}],
                strict=True,
            )
        ]
        assert results[0]["transactions_collect"] == [{"amount": 10.0}]
        assert results[1]["transactions_collect"] == [{"amount": 20.0}]
        assert results[0]["tier"] == "gold"

    def test_overlay_noop_without_ronsql_statements(self):
        server = make_server()
        server._ronsql_statements = []
        original = {"a": 1}
        assert server._overlay_ronsql_collect(original, {"user_id": 7}) == {"a": 1}


class TestRonsqlAggregateWindow:
    def test_window_bound_substituted_after_entity_keys(self):
        statement = make_statement(
            collect_n=None,
            aggregate_window=3600,
            query_ronsql=(
                "SELECT COUNT(`amount`) AS `amount_count` FROM `transactions_1` "
                "WHERE `user_id` = ? AND `event_time` >= ?;"
            ),
        )
        query = make_server()._substitute_ronsql_template(statement, {"user_id": 7})
        assert "`user_id` = 7 " in query
        # the trailing ? became a quoted datetime literal (now - window)
        assert "?" not in query
        assert "`event_time` >= '2" in query


class TestRonsqlPlaceholderAwareness:
    def test_question_mark_in_template_string_literal_is_not_a_placeholder(self):
        stmt = make_statement(
            query_ronsql=TEMPLATE.replace(
                "WHERE `user_id` = ?",
                "WHERE `category` LIKE 'who?%' AND `user_id` = ?",
            ),
        )
        query = make_server()._substitute_ronsql_template(stmt, {"user_id": 7})
        assert "LIKE 'who?%'" in query
        assert "`user_id` = 7 " in query

    def test_substituted_value_containing_question_mark_is_not_reconsumed(self):
        stmt = make_statement(
            prepared_statement_parameters=[
                {"name": "user_name", "index": 1},
                {"name": "region", "index": 2},
            ],
            query_ronsql=(
                "WITH t AS (SELECT `amount` FROM `transactions_1` WHERE "
                "`user_name` = ? AND `region` = ? ORDER BY `event_time` DESC "
                "LIMIT 100) SELECT `amount` FROM t;"
            ),
        )
        query = make_server()._substitute_ronsql_template(
            stmt, {"user_name": "wh?t", "region": "eu"}
        )
        assert "`user_name` = 'wh?t'" in query
        assert "`region` = 'eu'" in query

    def test_escaped_quote_inside_template_literal(self):
        stmt = make_statement(
            query_ronsql=TEMPLATE.replace(
                "WHERE `user_id` = ?",
                "WHERE `note` = 'it''s?fine' AND `user_id` = ?",
            ),
        )
        query = make_server()._substitute_ronsql_template(stmt, {"user_id": 7})
        assert "'it''s?fine'" in query
        assert "`user_id` = 7 " in query

    def test_placeholder_count_mismatch_raises(self):
        stmt = make_statement(
            prepared_statement_parameters=[
                {"name": "user_id", "index": 1},
                {"name": "region", "index": 2},
            ],
        )
        with pytest.raises(FeatureStoreException, match="placeholders"):
            make_server()._substitute_ronsql_template(
                stmt, {"user_id": 7, "region": "eu"}
            )


class _Feature:
    def __init__(self, name, type):
        self.name = name
        self.type = type


class TestRonsqlSchemaTypedLiterals:
    def test_numeric_string_for_int_column_inlines_bare(self):
        assert VectorServer._ronsql_literal("7", "bigint") == "7"

    def test_non_numeric_string_for_int_column_rejected(self):
        with pytest.raises(FeatureStoreException):
            VectorServer._ronsql_literal("abc", "bigint")

    def test_int_for_string_column_is_quoted(self):
        assert VectorServer._ronsql_literal(7, "string") == "'7'"

    def test_numeric_string_for_double_column(self):
        assert VectorServer._ronsql_literal("1.5", "double") == "1.5"

    def test_numeric_for_timestamp_column_rejected(self):
        with pytest.raises(FeatureStoreException):
            VectorServer._ronsql_literal(1700000000, "timestamp")

    def test_unknown_type_keeps_runtime_rendering(self):
        assert VectorServer._ronsql_literal(7, None) == "7"
        assert VectorServer._ronsql_literal("x", None) == "'x'"

    def test_string_length_cap(self):
        with pytest.raises(FeatureStoreException, match="longer than"):
            VectorServer._ronsql_literal("x" * 1025)

    def test_schema_type_resolved_from_features(self):
        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        query = server._substitute_ronsql_template(make_statement(), {"user_id": "7"})
        assert "`user_id` = 7 " in query


class TestSortCollectRows:
    ROWS = [
        {"amount": 1.0, "event_time": "t1"},
        {"amount": 3.0, "event_time": "t3"},
        {"amount": 2.0, "event_time": "t2"},
    ]

    def test_sorts_newest_first_by_default(self):
        rows = VectorServer._sort_collect_rows(list(self.ROWS), make_statement())
        assert [r["event_time"] for r in rows] == ["t3", "t2", "t1"]

    def test_sorts_oldest_first_when_ascending(self):
        rows = VectorServer._sort_collect_rows(
            list(self.ROWS), make_statement(collect_ascending=True)
        )
        assert [r["event_time"] for r in rows] == ["t1", "t2", "t3"]

    def test_legacy_statement_without_order_column_keeps_arrival_order(self):
        rows = VectorServer._sort_collect_rows(
            list(self.ROWS), make_statement(collect_order_by=None)
        )
        assert [r["event_time"] for r in rows] == ["t1", "t3", "t2"]

    def test_order_column_missing_from_rows_keeps_arrival_order(self):
        rows = [{"amount": 1.0}, {"amount": 3.0}]
        assert (
            VectorServer._sort_collect_rows(list(rows), make_statement()) == rows
        )

    def test_scan_rows_are_sorted(self):
        server = make_server()
        server._ronsql_statements = [make_statement()]
        server._execute_ronsql_statement = lambda stmt, entry: list(self.ROWS)
        rows = server._scan_rows_ronsql({"user_id": 7})
        assert [r["event_time"] for r in rows] == ["t3", "t2", "t1"]

    def test_overlay_sorts_before_folding(self):
        server = make_server()
        server._ronsql_statements = [make_statement()]
        server._rest_client_engine = _StubRestEngine(
            {1: ["user_id", "amount", "event_time"]}
        )
        server._execute_ronsql_statement = lambda stmt, entry: [
            {"user_id": 7, "amount": 1.0, "event_time": "t1"},
            {"user_id": 7, "amount": 3.0, "event_time": "t3"},
        ]
        vector = server._overlay_ronsql_collect({}, {"user_id": 7})
        assert vector["transactions_collect"] == [
            {"amount": 3.0, "event_time": "t3"},
            {"amount": 1.0, "event_time": "t1"},
        ]


class TestRonsqlTemplateValidation:
    def make_validating_server(self, monkeypatch, outcome):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        calls = []

        def fake_execute(api_self, query, database, explain_mode="REMOVE"):
            calls.append({
                "query": query,
                "database": database,
                "explain_mode": explain_mode,
            })
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_execute,
        )
        return server, calls

    def test_valid_template_kept_and_planned_with_force(self, monkeypatch):
        server, calls = self.make_validating_server(monkeypatch, [])
        statements = server._validate_ronsql_templates([make_statement()])
        assert len(statements) == 1
        assert len(calls) == 1
        assert calls[0]["explain_mode"] == "FORCE"
        assert calls[0]["database"] == "proj_featurestore"
        assert "?" not in calls[0]["query"]
        assert "`user_id` = 0 " in calls[0]["query"]

    def test_rejected_template_dropped_with_warning(self, monkeypatch):
        from hsfs.client.exceptions import RestAPIError

        error = RestAPIError.__new__(RestAPIError)
        server, _ = self.make_validating_server(monkeypatch, error)
        with pytest.warns(UserWarning, match="conformance check"):
            statements = server._validate_ronsql_templates([make_statement()])
        assert statements == []

    def test_transport_error_keeps_template(self, monkeypatch):
        server, _ = self.make_validating_server(monkeypatch, ConnectionError("down"))
        statements = server._validate_ronsql_templates([make_statement()])
        assert len(statements) == 1

    def test_placeholder_values_follow_schema_types(self):
        server = make_server()
        server._features = [_Feature("user_id", "string")]
        entry = server._ronsql_placeholder_entry(make_statement())
        assert entry == {"user_id": "0"}


class TestServingPreparedStatementWireParsing:
    def test_camelized_wire_payload_round_trips(self):
        # The REAL wire shape: humps decamelizes "collectN" to "collectn" (trailing
        # single capitals do not split), which must still land on collect_n.
        payload = {
            "count": 1,
            "items": [
                {
                    "featureGroupId": 1,
                    "preparedStatementIndex": 0,
                    "preparedStatementParameters": [{"name": "user_id", "index": 1}],
                    "queryOnline": "SELECT ...",
                    "queryRonsql": TEMPLATE,
                    "ronsqlDatabase": "proj",
                    "collectN": 100,
                    "collectFeatureName": "transactions_collect",
                    "collectOrderBy": "event_time",
                    "collectAscending": True,
                    "aggregateWindow": 3600,
                }
            ],
        }
        statement = ServingPreparedStatement.from_response_json(payload)[0]
        assert statement.collect_n == 100
        assert statement.collect_feature_name == "transactions_collect"
        assert statement.collect_order_by == "event_time"
        assert statement.collect_ascending is True
        assert statement.aggregate_window == 3600
        assert statement.query_ronsql == TEMPLATE


class _StubFeatureGroup:
    id = 1

    def _get_feature_avro_schema(self, name):
        return None


class _StubComplexFeature:
    """A synthesized complex feature (the collect fold) with no avro schema."""

    name = "transactions_collect"
    feature_group_feature_name = "transactions_collect"
    feature_group = _StubFeatureGroup()
    _feature_group = feature_group

    @staticmethod
    def is_complex():
        return True


class TestComplexDecoderSkipsSchemalessFeatures:
    def test_collect_feature_without_avro_schema_is_skipped(self):
        server = make_server()
        server._features = [_StubComplexFeature()]
        server._skip_feature_decoding_fg_ids = set()
        assert server._build_complex_feature_decoders() == {}


class TestBatchOverlayConcurrency:
    def test_batch_overlay_preserves_entry_alignment(self):
        server = make_server()
        server._ronsql_statements = [make_statement()]
        server._rest_client_engine = _StubRestEngine({1: ["user_id", "amount"]})
        server._execute_ronsql_statement = lambda stmt, entry: [
            {"user_id": entry["user_id"], "amount": float(entry["user_id"]) * 10}
        ]
        entries = [{"user_id": i} for i in range(1, 21)]
        results = server._overlay_ronsql_collect_batch(
            [{"tier": str(i)} for i in range(1, 21)], entries
        )
        for i, result in enumerate(results, start=1):
            assert result["tier"] == str(i)
            assert result["transactions_collect"] == [{"amount": float(i) * 10}]
