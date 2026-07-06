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
import json
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
        "collect_filter_applied": False,
    }
    kwargs.update(overrides)
    return ServingPreparedStatement(**kwargs)


def make_server():
    server = VectorServer.__new__(VectorServer)
    server._serving_keys = []
    return server


def _rest_error(status_code):
    from hsfs.client.exceptions import RestAPIError

    class _Response:
        pass

    error = RestAPIError.__new__(RestAPIError)
    error.response = _Response()
    error.response.status_code = status_code
    return error


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
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: rows
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
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: [
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
    @staticmethod
    def window_statement(**overrides):
        kwargs = {
            "collect_n": None,
            "aggregate_window": 3600,
            "query_ronsql": (
                "SELECT COUNT(`amount`) AS `amount_count` FROM `transactions_1` "
                "WHERE `user_id` = ? AND `event_time` >= ?;"
            ),
        }
        kwargs.update(overrides)
        return make_statement(**kwargs)

    def test_window_bound_substituted_after_entity_keys(self):
        query = make_server()._substitute_ronsql_template(
            self.window_statement(), {"user_id": 7}
        )
        assert "`user_id` = 7 " in query
        # the trailing ? became a quoted datetime literal (now - window)
        assert "?" not in query
        assert "`event_time` >= '2" in query

    def test_window_bound_resolves_from_the_shared_utc_reference(self):
        # every statement of one read shares a single UTC reference, so all
        # windows (and the SQL client's MySQL binds) agree on one clock
        from datetime import timezone

        reference = datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)
        query = make_server()._substitute_ronsql_template(
            self.window_statement(), {"user_id": 7}, now=reference
        )
        assert "`event_time` >= '2026-07-06 11:00:00'" in query

    def test_batch_window_bound_resolves_from_the_shared_utc_reference(self):
        from datetime import timezone

        statement = self.window_statement(
            query_ronsql=(
                "SELECT `user_id`, COUNT(`amount`) AS `amount_count` FROM "
                "`transactions_1` WHERE `user_id` IN (?) AND `event_time` >= ? "
                "GROUP BY `user_id`;"
            ),
        )
        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        reference = datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)
        query = server._substitute_ronsql_batch_template(
            statement, [{"user_id": 1}, {"user_id": 2}], now=reference
        )
        assert "IN (1, 2)" in query
        assert "`event_time` >= '2026-07-06 11:00:00'" in query


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
        assert VectorServer._sort_collect_rows(list(rows), make_statement()) == rows

    def test_scan_rows_are_sorted(self):
        server = make_server()
        server._ronsql_statements = [make_statement()]
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: list(
            self.ROWS
        )
        rows = server._scan_rows_ronsql({"user_id": 7})
        assert [r["event_time"] for r in rows] == ["t3", "t2", "t1"]

    def test_overlay_sorts_before_folding(self):
        server = make_server()
        server._ronsql_statements = [make_statement()]
        server._rest_client_engine = _StubRestEngine(
            {1: ["user_id", "amount", "event_time"]}
        )
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: [
            {"user_id": 7, "amount": 1.0, "event_time": "t1"},
            {"user_id": 7, "amount": 3.0, "event_time": "t3"},
        ]
        vector = server._overlay_ronsql_collect({}, {"user_id": 7})
        assert vector["transactions_collect"] == [
            {"amount": 3.0, "event_time": "t3"},
            {"amount": 1.0, "event_time": "t1"},
        ]


class TestRonsqlTemplateValidation:
    """EXPLAIN classification, exercised on statements that still need a probe.

    A collect whose filters are /scan-expressible skips the EXPLAIN entirely
    (designed /scan routing, see the dedicated tests below), so these use a
    filtered collect without structured filters: /scan cannot serve it, and the
    EXPLAIN is its last chance at /ronsql.
    """

    def make_validating_server(self, monkeypatch, outcome):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        calls = []

        def fake_execute(api_self, query, database, explain_mode="REMOVE"):
            calls.append(
                {
                    "query": query,
                    "database": database,
                    "explain_mode": explain_mode,
                }
            )
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_execute,
        )
        return server, calls

    @staticmethod
    def probed_statement(**overrides):
        return make_statement(collect_filter_applied=True, **overrides)

    def test_valid_template_kept_and_planned_with_force(self, monkeypatch):
        server, calls = self.make_validating_server(monkeypatch, [])
        statements = server._validate_ronsql_templates([self.probed_statement()], [])
        assert len(statements) == 1
        assert len(calls) == 1
        assert calls[0]["explain_mode"] == "FORCE"
        assert calls[0]["database"] == "proj_featurestore"
        assert "?" not in calls[0]["query"]
        assert "`user_id` = 0 " in calls[0]["query"]

    def test_scan_servable_collect_skips_explain(self, monkeypatch):
        # an unfiltered (or structured-filter) collect routes straight to the /scan
        # index read: this RonDB generation refuses row-returning CTEs, so the
        # EXPLAIN round trip is dead weight — and the routing is designed, not a
        # degradation, so there is no warning
        import warnings as warnings_module

        server, calls = self.make_validating_server(monkeypatch, [])
        rejected = []
        with warnings_module.catch_warnings():
            warnings_module.simplefilter("error")
            statements = server._validate_ronsql_templates(
                [make_statement()], rejected
            )
        assert statements == []
        assert len(rejected) == 1
        assert calls == []

    def test_identical_probes_explained_once(self, monkeypatch):
        # the verdict cache dedups identical (database, statement) probes across
        # validations — e.g. the single and batch statement sets
        server, calls = self.make_validating_server(monkeypatch, [])
        first = server._validate_ronsql_templates([self.probed_statement()], [])
        second = server._validate_ronsql_templates([self.probed_statement()], [])
        assert len(first) == len(second) == 1
        assert len(calls) == 1

    def test_rejected_template_dropped_with_warning(self, monkeypatch):
        # a definitive planner refusal (4xx) rejects the template
        server, _ = self.make_validating_server(monkeypatch, _rest_error(400))
        rejected = []
        with pytest.warns(UserWarning, match="conformance check"):
            statements = server._validate_ronsql_templates(
                [self.probed_statement()], rejected
            )
        assert statements == []
        assert len(rejected) == 1

    def test_auth_error_raises_instead_of_rejecting(self, monkeypatch):
        from hsfs.client.exceptions import RestAPIError

        for status in (401, 403):
            server, _ = self.make_validating_server(monkeypatch, _rest_error(status))
            with pytest.raises(RestAPIError):
                server._validate_ronsql_templates([self.probed_statement()], [])

    def test_server_error_keeps_template_unvalidated(self, monkeypatch):
        # a 5xx proves nothing about conformance: the template stays servable
        server, _ = self.make_validating_server(monkeypatch, _rest_error(503))
        rejected = []
        statements = server._validate_ronsql_templates(
            [self.probed_statement()], rejected
        )
        assert len(statements) == 1
        assert rejected == []

    def test_transient_verdicts_are_not_cached(self, monkeypatch):
        # a transient failure proves nothing, so the next validation re-probes
        server, calls = self.make_validating_server(monkeypatch, _rest_error(503))
        server._validate_ronsql_templates([self.probed_statement()], [])
        server._validate_ronsql_templates([self.probed_statement()], [])
        assert len(calls) == 2

    def test_statusless_rest_error_keeps_template(self, monkeypatch):
        from hsfs.client.exceptions import RestAPIError

        error = RestAPIError.__new__(RestAPIError)
        server, _ = self.make_validating_server(monkeypatch, error)
        statements = server._validate_ronsql_templates([self.probed_statement()], [])
        assert len(statements) == 1

    def test_transport_error_keeps_template(self, monkeypatch):
        server, _ = self.make_validating_server(monkeypatch, ConnectionError("down"))
        statements = server._validate_ronsql_templates([self.probed_statement()], [])
        assert len(statements) == 1

    def test_planner_refusal_wrapped_as_500_rejects_template(self, monkeypatch):
        # the live RDRS wraps RonSQL planner refusals as 500s; the body signature
        # makes them definitive conformance verdicts, unlike a plain 5xx
        error = _rest_error(500)
        error.response.text = (
            "CTE 't' must contain at least one aggregate function.\n"
            "Error handling: RPE\n"
            "Caught exception: CTE without aggregate function.\n"
        )
        server, _ = self.make_validating_server(monkeypatch, error)
        rejected = []
        with pytest.warns(UserWarning, match="conformance check"):
            statements = server._validate_ronsql_templates(
                [self.probed_statement()], rejected
            )
        assert statements == []
        assert len(rejected) == 1

    def test_placeholder_values_follow_schema_types(self):
        server = make_server()
        server._features = [_Feature("user_id", "string")]
        entry = server._ronsql_placeholder_entry(make_statement())
        assert entry == {"user_id": "0"}

    def test_explain_requests_text_output_and_skips_json_parsing(self, monkeypatch):
        # the server refuses JSON output for EXPLAIN plans; sending JSON turned every
        # valid template into a false planner refusal
        from hsfs.client import online_store_rest_client
        from hsfs.core import online_store_rest_client_api

        sent = []

        class _Client:
            def _send_request(self, method, path_params, headers, data):
                sent.append(json.loads(data))

                class _Resp:
                    status_code = 200

                    @staticmethod
                    def json():
                        raise AssertionError("EXPLAIN plan body must not be parsed")

                return _Resp()

        monkeypatch.setattr(
            online_store_rest_client, "_get_instance", lambda: _Client()
        )
        api = online_store_rest_client_api.OnlineStoreRestClientApi()
        assert api._execute_ronsql("SELECT 1;", "db", explain_mode="FORCE") == []
        assert sent[0]["outputFormat"] == "TEXT"
        assert sent[0]["explainMode"] == "FORCE"


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
                    "collectFilterApplied": True,
                    "collectSourceFeatures": ["user_id", "event_time", "amount"],
                    "collectFilters": [
                        {
                            "feature": "amount",
                            "condition": "GREATER_THAN",
                            "value": "0",
                        }
                    ],
                    "aggregateWindow": 3600,
                }
            ],
        }
        payload["items"][0]["snowflakeTemplate"] = False
        payload["items"][0]["snowflakeTemplates"] = ["WITH `b` AS (...) SELECT ...;"]
        statement = ServingPreparedStatement.from_response_json(payload)[0]
        assert statement.snowflake_template is False
        assert statement.snowflake_templates == ["WITH `b` AS (...) SELECT ...;"]
        assert statement.collect_n == 100
        assert statement.collect_feature_name == "transactions_collect"
        assert statement.collect_order_by == "event_time"
        assert statement.collect_ascending is True
        assert statement.collect_filter_applied is True
        assert statement.collect_source_features == ["user_id", "event_time", "amount"]
        assert statement.collect_filters == [
            {"feature": "amount", "condition": "GREATER_THAN", "value": "0"}
        ]
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


class _ScanFeatureGroup:
    id = 1
    name = "transactions"
    version = 1


class _ScanFeature:
    feature_group = _ScanFeatureGroup()
    name = "user_id"
    type = "bigint"


class TestScanFallback:
    def make_scan_server(self, monkeypatch, rows):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_ScanFeature()]
        calls = []

        def fake_scan(api_self, database, table, body):
            calls.append({"database": database, "table": table, "body": body})
            return rows

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_scan",
            fake_scan,
        )
        return server, calls

    def test_scan_request_shape(self, monkeypatch):
        server, calls = self.make_scan_server(monkeypatch, [])
        server._scan_rows_rest_scan(make_statement(), {"user_id": 7})
        assert calls[0]["database"] == "proj_featurestore"
        assert calls[0]["table"] == "transactions_1"
        body = calls[0]["body"]
        assert body["limit"] == 100
        index = body["index"]
        assert index["name"] == "PRIMARY"
        assert index["key_columns"] == ["user_id", "event_time"]
        assert index["order"] == "desc"
        assert index["ranges"] == [
            {
                "lower": {"values": [7], "inclusive": True},
                "upper": {"values": [7], "inclusive": True},
            }
        ]
        # no collect_source_features on the statement: scan all columns, as before
        assert "readColumns" not in body

    def test_scan_projects_only_source_columns(self, monkeypatch):
        server, calls = self.make_scan_server(monkeypatch, [])
        statement = make_statement(
            collect_source_features=["user_id", "event_time", "amount", "category"]
        )
        server._scan_rows_rest_scan(statement, {"user_id": 7})
        assert calls[0]["body"]["readColumns"] == [
            {"column": "user_id"},
            {"column": "event_time"},
            {"column": "amount"},
            {"column": "category"},
        ]

    def test_scan_projection_always_includes_key_columns(self, monkeypatch):
        server, calls = self.make_scan_server(monkeypatch, [])
        statement = make_statement(collect_source_features=["amount"])
        server._scan_rows_rest_scan(statement, {"user_id": 7})
        assert calls[0]["body"]["readColumns"] == [
            {"column": "amount"},
            {"column": "user_id"},
            {"column": "event_time"},
        ]

    def test_rejected_unfiltered_collect_falls_back_to_scan(self, monkeypatch):
        # the ordered index scan returns newest-first; the client trusts that
        # order instead of re-sorting
        rows = [
            {"amount": 3.0, "event_time": "t3"},
            {"amount": 1.0, "event_time": "t1"},
        ]
        server, calls = self.make_scan_server(monkeypatch, rows)
        server._ronsql_statements = []
        server._ronsql_rejected = [make_statement()]
        result = server._scan_rows_ronsql({"user_id": 7})
        assert len(calls) == 1
        assert [r["event_time"] for r in result] == ["t3", "t1"]

    def test_scan_limit_narrows_the_request(self, monkeypatch):
        # scan_vectors(limit=k) pushes min(k, collect_n) into the scan body instead
        # of reading collect_n rows and slicing client-side
        server, calls = self.make_scan_server(monkeypatch, [])
        server._ronsql_statements = []
        server._ronsql_rejected = [make_statement()]
        server._scan_rows_ronsql({"user_id": 7}, limit=5)
        assert calls[0]["body"]["limit"] == 5

    def test_scan_limit_never_widens_beyond_collect_n(self, monkeypatch):
        server, calls = self.make_scan_server(monkeypatch, [])
        server._scan_rows_rest_scan(make_statement(), {"user_id": 7}, limit=500)
        assert calls[0]["body"]["limit"] == 100

    def test_ascending_scan_rows_reverse_without_sorting(self):
        # ascending output is a single reversal of the desc-ordered scan
        rows = [
            {"amount": 3.0, "event_time": "t3"},
            {"amount": 1.0, "event_time": "t1"},
        ]
        ordered = VectorServer._order_scan_rows(
            rows, make_statement(collect_ascending=True)
        )
        assert [r["event_time"] for r in ordered] == ["t1", "t3"]

    def test_scan_rows_without_order_column_fall_back_to_sort(self):
        # a backend predating collect_order_by: no order column, arrival order kept
        rows = [{"amount": 1.0}, {"amount": 3.0}]
        ordered = VectorServer._order_scan_rows(
            rows, make_statement(collect_order_by=None)
        )
        assert ordered == rows

    def test_filtered_collect_refuses_scan_fallback(self, monkeypatch):
        server, calls = self.make_scan_server(monkeypatch, [])
        server._ronsql_statements = []
        server._ronsql_rejected = [make_statement(collect_filter_applied=True)]
        with pytest.raises(FeatureStoreException, match="filters"):
            server._scan_rows_ronsql({"user_id": 7})
        assert calls == []


class TestRejectedStatementVectorOverlay:
    """Vectors must never silently omit features after a RonSQL rejection.

    Unfiltered collects fold via /scan; everything else fails loudly.
    """

    def make_rejected_server(self, monkeypatch, rows, rejected):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_ScanFeature()]
        server._ronsql_statements = []
        server._ronsql_rejected = rejected
        server._rest_client_engine = _StubRestEngine({1: ["user_id", "amount"]})
        calls = []

        def fake_scan(api_self, database, table, body):
            calls.append({"database": database, "table": table, "body": body})
            return rows

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_scan",
            fake_scan,
        )
        return server, calls

    def test_rejected_unfiltered_collect_folds_via_scan(self, monkeypatch):
        # the ordered index scan returns newest-first; the fold trusts that order
        rows = [
            {"user_id": 7, "amount": 3.0, "event_time": "t3"},
            {"user_id": 7, "amount": 1.0, "event_time": "t1"},
        ]
        server, calls = self.make_rejected_server(monkeypatch, rows, [make_statement()])
        vector = server._overlay_ronsql_collect({"tier": "gold"}, {"user_id": 7})
        assert len(calls) == 1
        # folded newest-first, entity keys excluded from the structs
        assert vector["transactions_collect"] == [
            {"amount": 3.0, "event_time": "t3"},
            {"amount": 1.0, "event_time": "t1"},
        ]
        assert vector["tier"] == "gold"

    def test_rejected_filtered_collect_raises(self, monkeypatch):
        server, calls = self.make_rejected_server(
            monkeypatch, [], [make_statement(collect_filter_applied=True)]
        )
        with pytest.raises(FeatureStoreException, match="filters"):
            server._overlay_ronsql_collect({}, {"user_id": 7})
        assert calls == []

    def test_rejected_aggregate_raises(self, monkeypatch):
        aggregate = make_statement(
            collect_n=None,
            collect_feature_name=None,
            collect_order_by=None,
            aggregate_window=3600,
        )
        server, calls = self.make_rejected_server(monkeypatch, [], [aggregate])
        with pytest.raises(FeatureStoreException, match="aggregation"):
            server._overlay_ronsql_collect({}, {"user_id": 7})
        assert calls == []


class TestCollectUnservableMetadataGuidance:
    """The RDRS refusal of synthesized collect features must raise guidance.

    Feature vectors of collect feature views point users at the SQL client instead
    of surfacing an opaque 400 from /feature_store.
    """

    def _error_with_body(self, body):
        error = _rest_error(400)
        error.response.text = body
        return error

    def test_schema_refusal_for_collect_feature_raises_guidance(self):
        server = make_server()
        err = self._error_with_body(
            '{"code": 6, "reason": "Reading feature view failed.", '
            '"message": "Cannot find schema for feature transactions_collect"}'
        )
        with pytest.raises(FeatureStoreException, match="SQL client"):
            server._raise_if_collect_unservable_metadata(err)

    def test_other_rest_errors_pass_through(self):
        server = make_server()
        for body in (
            '{"message": "Cannot find schema for feature user_embedding"}',
            '{"message": "table not found"}',
            "",
        ):
            server._raise_if_collect_unservable_metadata(self._error_with_body(body))


SNOWFLAKE_TEMPLATE = (
    "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `profiles_1` "
    "WHERE `user_id` = ? GROUP BY `region_id`) "
    "SELECT `j2`.`region_name` AS `r_region_name` "
    "FROM `b` JOIN `regions_1` AS `j2` ON `j2`.`region_id` = `b`.`region_id`;"
)


def make_snowflake_statement(**overrides):
    kwargs = {
        "collect_n": None,
        "collect_feature_name": None,
        "collect_order_by": None,
        "collect_filter_applied": None,
        "snowflake_template": True,
        "prefix": "r_",
        "query_ronsql": None,
        "snowflake_templates": [SNOWFLAKE_TEMPLATE],
    }
    kwargs.update(overrides)
    return make_statement(**kwargs)


class TestSnowflakeOverlay:
    """Snowflake nested-subtree statements overlay their joined row verbatim.

    Outputs arrive already aliased to the prefixed feature-view names (FSTORE-2060).
    """

    def make_snowflake_server(self, rows_by_template):
        server = make_server()
        server._ronsql_statements = [
            make_snowflake_statement(snowflake_templates=list(rows_by_template.keys()))
        ]
        server._ronsql_rejected = []
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: (
            rows_by_template[template]
        )
        return server

    def test_joined_row_overlays_verbatim(self):
        server = self.make_snowflake_server(
            {SNOWFLAKE_TEMPLATE: [{"r_region_name": "emea"}]}
        )
        vector = server._overlay_ronsql_collect(
            {"tier": "gold", "r_region_name": None}, {"user_id": 1}
        )
        # the statement prefix is NOT re-applied: names arrive pre-aliased
        assert vector["r_region_name"] == "emea"
        assert vector["tier"] == "gold"

    def test_hop_miss_leaves_features_missing(self):
        server = self.make_snowflake_server({SNOWFLAKE_TEMPLATE: []})
        vector = server._overlay_ronsql_collect({"r_region_name": None}, {"user_id": 1})
        assert vector["r_region_name"] is None

    def test_left_chains_serve_independently(self):
        # per-chain templates: a miss on the deep chain must NOT lose the features
        # that the shallow chain already matched
        server = self.make_snowflake_server(
            {
                "chain-regions": [{"r_region_name": "emea"}],
                "chain-countries": [],
            }
        )
        vector = server._overlay_ronsql_collect(
            {"r_region_name": None, "c_country_name": None}, {"user_id": 1}
        )
        assert vector["r_region_name"] == "emea"
        assert vector["c_country_name"] is None

    def test_old_backend_single_template_field_still_serves(self):
        server = make_server()
        server._ronsql_statements = [
            make_snowflake_statement(
                snowflake_templates=None, query_ronsql=SNOWFLAKE_TEMPLATE
            )
        ]
        server._ronsql_rejected = []
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: [
            {"r_region_name": "emea"}
        ]
        vector = server._overlay_ronsql_collect({}, {"user_id": 1})
        assert vector["r_region_name"] == "emea"

    def test_rejected_snowflake_statement_raises_guidance(self):
        server = make_server()
        server._ronsql_statements = []
        server._ronsql_rejected = [make_snowflake_statement()]
        with pytest.raises(FeatureStoreException, match="snowflake"):
            server._overlay_ronsql_collect({}, {"user_id": 1})

    def test_batch_snowflake_rows_map_without_prefixing(self, monkeypatch):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        server._ronsql_statements = []
        server._ronsql_rejected = []
        server._ronsql_batch_statements = [
            make_snowflake_statement(
                snowflake_templates=[
                    "WITH `b` AS (SELECT `user_id`, `region_id`, COUNT(*) AS `hw_cnt` "
                    "FROM `profiles_1` WHERE `user_id` IN (?) GROUP BY `user_id`, `region_id`) "
                    "SELECT `j2`.`region_name` AS `r_region_name`, `b`.`user_id` AS `user_id` "
                    "FROM `b` JOIN `regions_1` AS `j2` ON `j2`.`region_id` = `b`.`region_id`;"
                ]
            )
        ]
        rows = [
            {"user_id": 1, "r_region_name": "emea"},
            {"user_id": 2, "r_region_name": "apac"},
        ]

        def fake_execute(api_self, query, database, explain_mode="REMOVE"):
            assert "IN (1, 2)" in query
            return rows

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_execute,
        )
        results = [{}, {}]
        served = server._overlay_batch_aggregates(
            results, [{"user_id": 1}, {"user_id": 2}]
        )
        assert results[0] == {"r_region_name": "emea"}
        assert results[1] == {"r_region_name": "apac"}
        # snowflake misses are legitimate (hop miss): every entry counts as served
        assert served == {0: {(1, "r_")}, 1: {(1, "r_")}}

    def test_batch_in_list_is_chunked(self, monkeypatch):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        server._ronsql_statements = []
        server._ronsql_rejected = []
        server._ronsql_batch_statements = [make_batch_aggregate_statement()]
        monkeypatch.setattr(VectorServer, "_RONSQL_IN_CHUNK", 2)
        queries = []

        def fake_execute(api_self, query, database, explain_mode="REMOVE"):
            queries.append(query)
            if "IN (1, 2)" in query:
                return [
                    {"user_id": 1, "amount_sum": 10.0},
                    {"user_id": 2, "amount_sum": 20.0},
                ]
            return [{"user_id": 3, "amount_sum": 30.0}]

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_execute,
        )
        results = [{}, {}, {}]
        served = server._overlay_batch_aggregates(
            results, [{"user_id": 1}, {"user_id": 2}, {"user_id": 3}]
        )
        # chunks run concurrently, so assert membership rather than arrival order
        assert len(queries) == 2
        assert any("IN (1, 2)" in q for q in queries)
        assert any("IN (3)" in q for q in queries)
        assert results[0]["amount_sum"] == 10.0
        assert results[2]["amount_sum"] == 30.0
        assert served == {0: {(1, None)}, 1: {(1, None)}, 2: {(1, None)}}

    def test_batch_in_list_dedups_repeated_keys(self, monkeypatch):
        # one IN literal serves every entry sharing the key value; the grouped row
        # still lands on all of them
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        server._ronsql_statements = []
        server._ronsql_rejected = []
        server._ronsql_batch_statements = [make_batch_aggregate_statement()]
        queries = []

        def fake_execute(api_self, query, database, explain_mode="REMOVE"):
            queries.append(query)
            return [{"user_id": 1, "amount_sum": 10.0}]

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_execute,
        )
        results = [{}, {}]
        served = server._overlay_batch_aggregates(
            results, [{"user_id": 1}, {"user_id": 1}]
        )
        assert queries == [
            "SELECT `user_id`, SUM(`amount`) AS `amount_sum` FROM `transactions_1` "
            "WHERE `user_id` IN (1) GROUP BY `user_id`;"
        ]
        assert results[0]["amount_sum"] == 10.0
        assert results[1]["amount_sum"] == 10.0
        assert served == {0: {(1, None)}, 1: {(1, None)}}

    def test_chunking_is_byte_aware(self):
        # long literals split a chunk before the count cap is reached
        literals = ["'" + "a" * 40000 + "'", "'" + "b" * 40000 + "'"]
        chunks = VectorServer._chunk_ronsql_literals(literals)
        assert chunks == [[literals[0]], [literals[1]]]


class TestScanVectorsLimitValidation:
    def make_limit_server(self):
        server = make_server()
        server._which_client_and_ensure_initialised = lambda **kwargs: "sql"

        class _SqlClient:
            def _get_scan_rows(self, entry, limit=None):
                return []

        server._sql_client = _SqlClient()
        return server

    @pytest.mark.parametrize("bad_limit", [0, -1, True, "5", 2.5])
    def test_invalid_limits_rejected(self, bad_limit):
        server = self.make_limit_server()
        with pytest.raises(ValueError, match="positive integer"):
            server._scan_vectors({"user_id": 7}, limit=bad_limit, return_type="list")

    @pytest.mark.parametrize("good_limit", [None, 1, 100])
    def test_valid_limits_accepted(self, good_limit):
        server = self.make_limit_server()
        assert (
            server._scan_vectors({"user_id": 7}, limit=good_limit, return_type="list")
            == []
        )


class TestBatchOverlayConcurrency:
    def test_batch_overlay_preserves_entry_alignment(self):
        server = make_server()
        server._ronsql_statements = [make_statement()]
        server._ronsql_batch_statements = []
        server._rest_client_engine = _StubRestEngine({1: ["user_id", "amount"]})
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: [
            {"user_id": entry["user_id"], "amount": float(entry["user_id"]) * 10}
        ]
        entries = [{"user_id": i} for i in range(1, 21)]
        results = server._overlay_ronsql_collect_batch(
            [{"tier": str(i)} for i in range(1, 21)], entries
        )
        for i, result in enumerate(results, start=1):
            assert result["tier"] == str(i)
            assert result["transactions_collect"] == [{"amount": float(i) * 10}]


def make_batch_aggregate_statement(**overrides):
    kwargs = {
        "collect_n": None,
        "collect_feature_name": None,
        "collect_order_by": None,
        "aggregate_window": None,
        "query_ronsql": (
            "SELECT `user_id`, SUM(`amount`) AS `amount_sum` FROM `transactions_1` "
            "WHERE `user_id` IN (?) GROUP BY `user_id`;"
        ),
    }
    kwargs.update(overrides)
    return make_statement(**kwargs)


class TestBatchAggregateGrouping:
    def make_grouping_server(self, monkeypatch, rows, batch_statement=None):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_Feature("user_id", "bigint")]
        server._ronsql_statements = []
        server._ronsql_rejected = []
        server._ronsql_batch_statements = [
            batch_statement
            if batch_statement is not None
            else make_batch_aggregate_statement()
        ]
        calls = []

        def fake_execute(api_self, query, database, explain_mode="REMOVE"):
            calls.append(query)
            if isinstance(rows, Exception):
                raise rows
            return rows

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_execute,
        )
        return server, calls

    def test_one_grouped_statement_serves_the_batch(self, monkeypatch):
        rows = [
            {"user_id": 1, "amount_sum": 10.0},
            {"user_id": 2, "amount_sum": 20.0},
        ]
        server, calls = self.make_grouping_server(monkeypatch, rows)
        results = [{"tier": "a"}, {"tier": "b"}]
        entries = [{"user_id": 1}, {"user_id": 2}]
        served = server._overlay_batch_aggregates(results, entries)
        assert len(calls) == 1
        assert "IN (1, 2)" in calls[0]
        assert results[0]["amount_sum"] == 10.0
        assert results[1]["amount_sum"] == 20.0
        # the key column itself is not overlaid; served statements are skipped per-entry
        assert "user_id" not in results[0]
        assert served == {0: {(1, None)}, 1: {(1, None)}}

    def test_grouped_outputs_carry_the_statement_prefix(self, monkeypatch):
        rows = [{"user_id": 1, "amount_sum": 10.0}]
        server, _ = self.make_grouping_server(
            monkeypatch, rows, make_batch_aggregate_statement(prefix="txn_")
        )
        results = [{}]
        server._overlay_batch_aggregates(results, [{"user_id": 1}])
        assert results[0] == {"txn_amount_sum": 10.0}

    def test_missing_entity_defers_to_per_entry_pass(self, monkeypatch):
        # GROUP BY omits entities with no rows: ONLY those entries stay uncovered,
        # so the per-entry pass computes their empty-window aggregates without
        # re-fetching the entries the grouped read already served
        rows = [{"user_id": 1, "amount_sum": 10.0}]
        server, _ = self.make_grouping_server(monkeypatch, rows)
        results = [{}, {}]
        served = server._overlay_batch_aggregates(
            results, [{"user_id": 1}, {"user_id": 2}]
        )
        assert results[0]["amount_sum"] == 10.0
        assert served == {0: {(1, None)}}

    def test_planner_refusal_falls_back_to_per_entry(self, monkeypatch):
        error = _rest_error(500)
        error.response.text = "Error handling: RPE\nCaught exception: nope\n"
        server, _ = self.make_grouping_server(monkeypatch, error)
        served = server._overlay_batch_aggregates([{}], [{"user_id": 1}])
        assert served == {}

    def test_composite_key_statement_stays_per_entry(self, monkeypatch):
        statement = make_batch_aggregate_statement(
            prepared_statement_parameters=[
                {"name": "user_id", "index": 1},
                {"name": "region", "index": 2},
            ]
        )
        server, calls = self.make_grouping_server(monkeypatch, [], statement)
        served = server._overlay_batch_aggregates([{}], [{"user_id": 1, "region": 2}])
        assert served == {}
        assert calls == []


class TestRejectedAggregatePrefix:
    def test_single_entity_aggregate_outputs_carry_prefix(self):
        statement = make_statement(
            collect_n=None,
            collect_feature_name=None,
            collect_order_by=None,
            prefix="txn_",
            query_ronsql=(
                "SELECT SUM(`amount`) AS `amount_sum` FROM `transactions_1` "
                "WHERE `user_id` = ?;"
            ),
        )
        server = make_server()
        server._ronsql_statements = [statement]
        server._ronsql_rejected = []
        server._execute_ronsql_statement = lambda stmt, entry, template=None, now=None: [
            {"amount_sum": 5.0}
        ]
        vector = server._overlay_ronsql_collect({"tier": "gold"}, {"user_id": 7})
        assert vector["txn_amount_sum"] == 5.0
        assert "amount_sum" not in vector


class TestScanFilterTree:
    def test_single_condition_is_a_bare_cmp_node(self):
        # the statement carries the column type (struct sources are not FV features)
        statement = make_statement(
            collect_filter_applied=True,
            collect_filters=[
                {
                    "feature": "amount",
                    "condition": "GREATER_THAN",
                    "value": "0",
                    "feature_type": "double",
                }
            ],
        )
        server = make_server()
        server._features = []
        tree = server._scan_filter_tree(statement)
        assert tree == {"op": "CMP", "column": "amount", "cond": "GT", "value": 0.0}

    def test_untyped_condition_falls_back_to_fv_schema(self):
        # older backends omit feature_type; the FV schema types what it can
        statement = make_statement(
            collect_filter_applied=True,
            collect_filters=[
                {"feature": "amount", "condition": "GREATER_THAN", "value": "0"}
            ],
        )
        server = make_server()
        server._features = [_Feature("amount", "double")]
        tree = server._scan_filter_tree(statement)
        assert tree["value"] == 0.0

    def test_decimal_literal_stays_an_exact_string(self):
        # a decimal filter must not lose precision through a binary float; it is
        # validated and sent as a decimal string
        value = VectorServer._scan_filter_value(
            "1234567890.123456789", "decimal(19,9)"
        )
        assert value == "1234567890.123456789"

    def test_invalid_decimal_literal_refused(self):
        with pytest.raises(FeatureStoreException, match="decimal"):
            VectorServer._scan_filter_value("not-a-number", "decimal(10,2)")

    def test_multiple_conditions_fold_into_binary_and_tree(self):
        statement = make_statement(
            collect_filter_applied=True,
            collect_filters=[
                {
                    "feature": "amount",
                    "condition": "GREATER_THAN",
                    "value": "0",
                    "feature_type": "bigint",
                },
                {"feature": "category", "condition": "EQUALS", "value": "atm"},
                {
                    "feature": "amount",
                    "condition": "LESS_THAN_OR_EQUAL",
                    "value": "10",
                    "feature_type": "bigint",
                },
            ],
        )
        server = make_server()
        server._features = []
        tree = server._scan_filter_tree(statement)
        assert tree["op"] == "AND"
        assert tree["args"][1] == {
            "op": "CMP",
            "column": "amount",
            "cond": "LE",
            "value": 10,
        }
        inner = tree["args"][0]
        assert inner["op"] == "AND"
        assert inner["args"][0]["cond"] == "GT"
        assert inner["args"][0]["value"] == 0
        # untyped (non-numeric) columns keep the string literal
        assert inner["args"][1]["value"] == "atm"

    def test_scan_request_carries_the_filter_tree(self, monkeypatch):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_ScanFeature()]
        calls = []

        def fake_scan(api_self, database, table, body):
            calls.append(body)
            return []

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_scan",
            fake_scan,
        )
        statement = make_statement(
            collect_filter_applied=True,
            collect_filters=[
                {"feature": "amount", "condition": "GREATER_THAN", "value": "0"}
            ],
        )
        server._scan_rows_rest_scan(statement, {"user_id": 7})
        assert calls[0]["filters"]["op"] == "CMP"

    def test_filtered_collect_with_structured_filters_is_scan_servable(self):
        with_filters = make_statement(
            collect_filter_applied=True,
            collect_filters=[
                {"feature": "amount", "condition": "GREATER_THAN", "value": "0"}
            ],
        )
        without = make_statement(collect_filter_applied=True)
        assert VectorServer._scan_can_serve(with_filters) is True
        assert VectorServer._scan_can_serve(without) is False
        assert VectorServer._scan_can_serve(make_statement()) is True


class TestReadTimeReclassification:
    def test_planner_refusal_at_read_time_reroutes_to_scan(self, monkeypatch):
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        server._features = [_ScanFeature()]
        statement = make_statement()
        server._ronsql_statements = [statement]
        server._ronsql_rejected = []
        scan_rows = [{"amount": 1.0, "event_time": "t1"}]
        scan_calls = []

        error = _rest_error(500)
        error.response.text = "Error handling: RPE\nCaught exception: nope\n"

        def fake_ronsql(api_self, query, database, explain_mode="REMOVE"):
            raise error

        def fake_scan(api_self, database, table, body):
            scan_calls.append(body)
            return scan_rows

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_ronsql,
        )
        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_scan",
            fake_scan,
        )
        with pytest.warns(UserWarning, match="reclassified"):
            vector = server._overlay_ronsql_collect({}, {"user_id": 7})
        assert vector["transactions_collect"] == [{"amount": 1.0, "event_time": "t1"}]
        assert server._ronsql_statements == []
        assert server._ronsql_rejected == [statement]
        assert len(scan_calls) == 1

    def test_non_planner_read_error_propagates(self, monkeypatch):
        from hsfs.client.exceptions import RestAPIError
        from hsfs.core import online_store_rest_client_api

        server = make_server()
        statement = make_statement()
        server._ronsql_statements = [statement]
        server._ronsql_rejected = []
        error = _rest_error(503)

        def fake_ronsql(api_self, query, database, explain_mode="REMOVE"):
            raise error

        monkeypatch.setattr(
            online_store_rest_client_api.OnlineStoreRestClientApi,
            "_execute_ronsql",
            fake_ronsql,
        )
        with pytest.raises(RestAPIError):
            server._overlay_ronsql_collect({}, {"user_id": 7})
        assert server._ronsql_statements == [statement]


class TestUnservableSnowflakeInitGate:
    """A snowflake subtree without servable templates must fail REST init.

    The backend marks every nested-subtree statement with snowflake_template; one
    carrying no templates (mixed INNER/LEFT joins, partial-key hops, ...) cannot be
    served correctly by the typed REST read, so continuing would silently return
    wrong nested features.
    """

    def test_marked_statement_without_templates_raises_guidance(self):
        unservable = make_statement(
            collect_n=None,
            collect_feature_name=None,
            collect_order_by=None,
            query_ronsql=None,
            snowflake_template=True,
            snowflake_templates=None,
        )
        with pytest.raises(FeatureStoreException, match="SQL client"):
            VectorServer._raise_if_unservable_snowflake([unservable])

    def test_statement_with_templates_passes(self):
        VectorServer._raise_if_unservable_snowflake([make_snowflake_statement()])

    def test_old_backend_statement_without_marker_passes(self):
        # an old backend never sets the marker without templates: no false failures
        VectorServer._raise_if_unservable_snowflake(
            [make_statement(query_ronsql=None)]
        )


class TestSharedRonsqlPool:
    def test_task_results_preserve_item_order(self):
        import time

        def slow_first(item):
            if item == 0:
                time.sleep(0.05)
            return item * 10

        results = make_server()._run_ronsql_tasks(slow_first, [0, 1, 2, 3])
        assert results == [0, 10, 20, 30]

    def test_single_item_runs_inline(self):
        import threading as threading_module

        caller = threading_module.current_thread().name
        seen = []
        make_server()._run_ronsql_tasks(
            lambda item: seen.append(threading_module.current_thread().name), [1]
        )
        assert seen == [caller]

    def test_exceptions_surface_in_item_order(self):
        def fail_on_two(item):
            if item == 2:
                raise ValueError("two")
            if item == 1:
                raise KeyError("one")
            return item

        with pytest.raises(KeyError):
            make_server()._run_ronsql_tasks(fail_on_two, [0, 1, 2])
