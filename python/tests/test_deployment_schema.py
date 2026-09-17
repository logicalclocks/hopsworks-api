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

import datetime
import decimal
from types import SimpleNamespace

import numpy as np
import pytest
from hsml import deployment_schema as ds
from hsml.deployment_schema import DeploymentSchema, SchemaField
from hsml.schema import Schema


def _schema(**overrides):
    kwargs = {
        "serving_keys": [{"name": "cc_num", "type": "bigint"}],
        "passed_features": [{"name": "amount", "type": "double"}],
        "request_parameters": ["transaction_time"],
        "extra_logging_features": [{"name": "channel", "type": "string"}],
        "feature_view": {"name": "fv", "version": 1},
        "training_dataset_version": 3,
    }
    kwargs.update(overrides)
    return DeploymentSchema(**kwargs)


class TestConstruction:
    def test_groups_and_columns_order(self):
        schema = _schema()

        assert schema.names == ["cc_num", "amount", "transaction_time", "channel"]
        assert schema.required_names == ["cc_num", "amount", "transaction_time"]
        assert schema.unresolved == ["transaction_time"]
        assert schema.serving_keys[0].nullable is False
        assert schema.passed_features[0].nullable is True

    def test_accepts_names_dicts_and_schema_objects(self):
        columnar = Schema(
            [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]
        )
        schema = DeploymentSchema(serving_keys=["k"], passed_features=columnar)

        assert [f.name for f in schema.serving_keys] == ["k"]
        assert [(f.name, f.type) for f in schema.passed_features] == [
            ("a", "int"),
            ("b", "string"),
        ]

    def test_rejects_duplicate_names(self):
        with pytest.raises(ValueError, match="more than one group"):
            DeploymentSchema(serving_keys=["a"], passed_features=["a"])

    def test_rejects_tensor_schema_group(self):
        with pytest.raises(ValueError, match="columnar"):
            DeploymentSchema(passed_features=Schema(np.zeros((2, 3))))

    def test_rejects_unknown_output_kind(self):
        with pytest.raises(ValueError, match="output kind"):
            DeploymentSchema(output={"kind": "tensors"})

    def test_round_trip_and_schema_id(self):
        schema = _schema()
        as_dict = schema.to_dict()

        restored = DeploymentSchema.from_response_json(as_dict)

        assert restored == schema
        assert restored.schema_id == schema.schema_id == as_dict["schemaId"]
        assert len(schema.schema_id) == 16

    def test_schema_id_covers_field_descriptions(self):
        # Content addressing covers everything that is persisted, descriptions included.
        a = _schema()
        b = _schema(
            serving_keys=[{"name": "cc_num", "type": "bigint", "description": "card"}]
        )

        assert a.schema_id != b.schema_id

    def test_schema_id_is_order_sensitive(self):
        a = DeploymentSchema(passed_features=["a", "b"])
        b = DeploymentSchema(passed_features=["b", "a"])

        assert a.schema_id != b.schema_id

    def test_types_are_normalised(self):
        field = SchemaField("x", " BIGINT ")

        assert field.type == "bigint"


class TestValidation:
    def test_valid_object_and_array_rows(self):
        schema = _schema()

        assert (
            schema.validate_instances(
                [{"cc_num": 1, "amount": 2.5, "transaction_time": "x"}]
            )
            == []
        )
        assert schema.validate_instances([[1, 2.5, "x", None]]) == []

    def test_extra_logging_field_optional_in_objects_but_positional_in_arrays(self):
        schema = _schema()

        assert (
            schema.validate_instances(
                [{"cc_num": 1, "amount": None, "transaction_time": 1}]
            )
            == []
        )
        errors = schema.validate_instances([[1, 2.5, "x"]])

        assert errors[0]["reason"] == "expected 4 values, got 3"

    def test_missing_unknown_and_null_key(self):
        schema = _schema()
        errors = schema.validate_instances(
            [
                {"cc_num": 1, "ammount": 2.5, "transaction_time": 1},
                {"cc_num": None, "amount": 1.0, "transaction_time": 1},
            ]
        )

        assert {"row": 0, "field": "amount", "reason": "missing"} in errors
        assert {"row": 0, "field": "ammount", "reason": "unknown field"} in errors
        assert {"row": 1, "field": "cc_num", "reason": "must not be null"} in errors

    def test_mixed_row_forms_rejected(self):
        schema = _schema()
        errors = schema.validate_instances(
            [{"cc_num": 1, "amount": 1.0, "transaction_time": 1}, [1, 1.0, 1, None]]
        )

        assert errors[0]["reason"] == "rows must all be objects or all be arrays"

    def test_batch_limits(self):
        schema = _schema()

        assert (
            schema.validate_instances([])[0]["reason"] == "instances must not be empty"
        )
        assert (
            schema.validate_instances("no")[0]["reason"] == "instances must be an array"
        )
        assert (
            "limit is 1"
            in schema.validate_instances([[1, 1.0, 1, None]] * 2, max_rows=1)[0][
                "reason"
            ]
        )

    @pytest.mark.parametrize(
        "type_,good,bad",
        [
            ("int", 3, 3.5),
            ("int", 3, True),
            ("bigint", 2**60, 1.5),
            ("bigint", "18446744073709551616", "1.5"),
            ("double", 1, "1"),
            ("double", 1.5, float("nan")),
            ("decimal(10,2)", "12.50", "abc"),
            ("string", "s", 1),
            ("varchar(10)", "s", 1),
            ("boolean", True, 1),
            ("timestamp", "2026-09-06T10:00:00Z", "yesterday"),
            ("timestamp", 1757152800000, True),
            ("date", "2026-09-06", "2026/09/06"),
            ("binary", "aGVsbG8=", "not base64!"),
            ("array<int>", [1, 2], [1, "2"]),
            ("array<int>", [1, None], "1,2"),
            ("map<string,double>", {"a": 1.0}, {"a": "x"}),
            ("struct<a:int,b:string>", {"a": 1, "b": "x"}, {"a": 1}),
            ("struct<a:int,b:string>", {"a": 1, "b": None}, {"a": 1, "b": "x", "c": 2}),
        ],
    )
    def test_type_checks(self, type_, good, bad):
        schema = DeploymentSchema(passed_features=[{"name": "f", "type": type_}])

        assert schema.validate_instances([{"f": good}]) == []
        assert schema.validate_instances([{"f": bad}]) != []

    def test_unresolved_type_accepts_anything(self):
        schema = DeploymentSchema(request_parameters=["p"])

        assert schema.validate_instances([{"p": {"any": ["thing"]}}]) == []

    def test_error_message_lists_expected_fields_and_rows(self):
        schema = _schema()

        with pytest.raises(ds.DeploymentSchemaError) as info:
            schema._raise_if_invalid([{"cc_num": 1}], "fraud")

        assert "deployment schema of 'fraud'" in str(info.value)
        assert "cc_num, amount, transaction_time, channel" in str(info.value)
        assert "instance 0: 'amount' missing" in str(info.value)
        assert info.value.schema_id == schema.schema_id

    def test_rows_and_split_row(self):
        schema = _schema()
        rows = schema.rows(
            [
                [1, 2.5, "t", "web"],
                {"cc_num": 2, "amount": 1.0, "transaction_time": "u"},
            ]
        )

        assert rows[0] == {
            "cc_num": 1,
            "amount": 2.5,
            "transaction_time": "t",
            "channel": "web",
        }
        assert rows[1]["channel"] is None
        assert schema.split_row(rows[0]) == {
            "serving_keys": {"cc_num": 1},
            "passed_features": {"amount": 2.5},
            "request_parameters": {"transaction_time": "t"},
            "extra_logging_features": {"channel": "web"},
        }


class TestEncoding:
    def test_encodes_python_values_to_wire_form(self):
        naive = datetime.datetime(2026, 9, 6, 10, 0, 0)
        aware = datetime.datetime(
            2026, 9, 6, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
        )

        assert ds._encode_value(naive) == "2026-09-06T10:00:00Z"
        assert ds._encode_value(aware) == "2026-09-06T10:00:00Z"
        assert ds._encode_value(datetime.date(2026, 9, 6)) == "2026-09-06"
        assert ds._encode_value(b"hello") == "aGVsbG8="
        assert ds._encode_value(decimal.Decimal("12.50")) == "12.50"
        assert ds._encode_value(np.int64(3)) == 3
        assert ds._encode_value(np.array([1.5, 2.5])) == [1.5, 2.5]
        assert ds._encode_value({"k": (1, np.float32(2.0))}) == {"k": [1, 2.0]}

    def test_encode_instances_keeps_row_shapes(self):
        rows = ds._encode_instances([{"a": np.int32(1)}, [np.float64(2.0)]])

        assert rows == [{"a": 1}, [2.0]]


class TestJsonSchema:
    def test_request_schema_shape(self):
        schema = _schema()
        request = schema.to_json_schema(max_rows=7)["request"]

        batches = request["properties"]["instances"]["oneOf"]
        assert [b["items"]["$ref"] for b in batches] == [
            "#/$defs/row_object",
            "#/$defs/row_array",
        ]
        assert all(b["maxItems"] == 7 and b["minItems"] == 1 for b in batches)
        row_object = request["$defs"]["row_object"]
        assert row_object["required"] == ["cc_num", "amount", "transaction_time"]
        assert row_object["additionalProperties"] is False
        assert row_object["properties"]["cc_num"] == {
            "type": ["integer", "string"],
            "pattern": "^-?[0-9]+$",
        }
        assert row_object["properties"]["amount"] == {"type": ["number", "null"]}
        assert row_object["properties"]["transaction_time"] == {
            "x-hopsworks-unresolved": True
        }
        row_array = request["$defs"]["row_array"]
        assert row_array["minItems"] == row_array["maxItems"] == 4

    def test_response_schema_for_predictions_and_feature_vectors(self):
        predictions = _schema(
            output={
                "kind": "predictions",
                "columns": [{"name": "fraud", "type": "int"}],
            }
        )
        vectors = _schema(
            output={
                "kind": "feature_vectors",
                "columns": [
                    {"name": "a", "type": "double"},
                    {"name": "b", "type": "string"},
                ],
            }
        )

        p = predictions.to_json_schema()["response"]
        v = vectors.to_json_schema()["response"]

        assert p["required"] == ["predictions"]
        assert v["required"] == ["predictions", "columns"]
        assert v["properties"]["columns"]["const"] == ["a", "b"]
        assert v["properties"]["predictions"]["items"]["prefixItems"][1] == {
            "type": ["string", "null"]
        }

    def test_complex_types_render(self):
        schema = DeploymentSchema(
            passed_features=[
                {"name": "a", "type": "array<int>"},
                {"name": "s", "type": "struct<x:int,y:string>"},
                {"name": "m", "type": "map<string,double>"},
                {"name": "t", "type": "timestamp"},
                {"name": "b", "type": "binary"},
            ]
        )
        props = schema.to_json_schema()["request"]["$defs"]["row_object"]["properties"]

        assert props["a"]["items"] == {"type": ["integer", "null"]}
        assert props["s"]["required"] == ["x", "y"]
        assert props["m"]["additionalProperties"] == {"type": ["number", "null"]}
        assert {"type": "string", "format": "date-time"} in props["t"]["oneOf"]
        assert props["b"]["contentEncoding"] == "base64"

    def test_validator_agrees_with_json_schema(self):
        jsonschema = pytest.importorskip("jsonschema")
        schema = _schema(
            passed_features=[
                {"name": "amount", "type": "double"},
                {"name": "tags", "type": "array<string>"},
                {"name": "ts", "type": "timestamp"},
            ]
        )
        request_schema = schema.to_json_schema()["request"]
        validator = jsonschema.Draft202012Validator(request_schema)
        cases = [
            {
                "instances": [
                    {
                        "cc_num": 1,
                        "amount": 1.0,
                        "tags": ["a"],
                        "ts": 1,
                        "transaction_time": None,
                    }
                ]
            },
            {"instances": [[1, 1.0, ["a"], "2026-09-06T00:00:00Z", None, None]]},
            {
                "instances": [
                    {
                        "cc_num": None,
                        "amount": 1.0,
                        "tags": [],
                        "ts": 1,
                        "transaction_time": None,
                    }
                ]
            },
            {
                "instances": [
                    {
                        "cc_num": 1,
                        "amount": "1",
                        "tags": [],
                        "ts": 1,
                        "transaction_time": None,
                    }
                ]
            },
            {
                "instances": [
                    {
                        "cc_num": 1,
                        "amount": 1.0,
                        "tags": [1],
                        "ts": 1,
                        "transaction_time": None,
                    }
                ]
            },
            {
                "instances": [
                    {
                        "cc_num": 1,
                        "amount": 1.0,
                        "tags": [],
                        "ts": 1,
                        "transaction_time": None,
                        "x": 1,
                    }
                ]
            },
            {"instances": [[1, 1.0, [], 1, None]]},
            # mixed rows: one object, one array
            {
                "instances": [
                    {
                        "cc_num": 1,
                        "amount": 1.0,
                        "tags": [],
                        "ts": 1,
                        "transaction_time": None,
                    },
                    [1, 1.0, [], 1, None, None],
                ]
            },
            {"instances": []},
        ]

        for case in cases:
            ours = schema.validate_instances(case["instances"]) == []
            theirs = validator.is_valid(case)
            assert ours == theirs, case

    def test_configured_batch_limit_in_both_validators(self):
        jsonschema = pytest.importorskip("jsonschema")
        schema = _schema()
        validator = jsonschema.Draft202012Validator(
            schema.to_json_schema(max_rows=2)["request"]
        )
        rows = [{"cc_num": 1, "amount": 1.0, "transaction_time": None}] * 3

        assert not validator.is_valid({"instances": rows})
        assert schema.validate_instances(rows, max_rows=2)[0]["reason"].startswith(
            "batch has 3 rows"
        )
        assert schema.validate_instances(rows, max_rows=3) == []

    def test_invalid_calendar_dates_rejected(self):
        schema = _schema(
            passed_features=[
                {"name": "d", "type": "date"},
                {"name": "ts", "type": "timestamp"},
            ]
        )
        ok = {
            "cc_num": 1,
            "d": "2026-09-06",
            "ts": "2026-09-06T10:00:00Z",
            "transaction_time": None,
        }
        assert schema.validate_instances([ok]) == []
        for bad in ("2026-99-99", "2026-02-30"):
            errors = schema.validate_instances([{**ok, "d": bad}])
            assert errors and errors[0]["field"] == "d", bad
        for bad in ("2026-13-01T00:00:00Z", "2026-09-06T25:00:00Z"):
            errors = schema.validate_instances([{**ok, "ts": bad}])
            assert errors and errors[0]["field"] == "ts", bad

    def test_response_schema_prediction_shapes(self):
        jsonschema = pytest.importorskip("jsonschema")
        one_column = _schema(
            output={
                "kind": "predictions",
                "columns": [{"name": "fraud", "type": "int"}],
            }
        )
        validator = jsonschema.Draft202012Validator(
            one_column.to_json_schema()["response"]
        )
        assert validator.is_valid({"predictions": [1, 0]})
        assert validator.is_valid({"predictions": [[1], [0]]})
        assert validator.is_valid({"predictions": [{"fraud": 1}]})
        assert not validator.is_valid({"predictions": ["fraud"]})
        assert not validator.is_valid({"predictions": [[1, 2]]})

        two_columns = _schema(
            output={
                "kind": "predictions",
                "columns": [
                    {"name": "a", "type": "int"},
                    {"name": "b", "type": "double"},
                ],
            }
        )
        validator = jsonschema.Draft202012Validator(
            two_columns.to_json_schema()["response"]
        )
        assert validator.is_valid({"predictions": [[1, 0.5]]})
        assert validator.is_valid({"predictions": [{"a": 1, "b": 0.5}]})
        assert not validator.is_valid({"predictions": [1]})

    def test_openapi_request_schema_is_self_contained(self):
        jsonschema = pytest.importorskip("jsonschema")
        doc = _schema().to_openapi("fraud")

        def refs(node):
            if isinstance(node, dict):
                if "$ref" in node:
                    yield node["$ref"]
                for value in node.values():
                    yield from refs(value)
            elif isinstance(node, list):
                for value in node:
                    yield from refs(value)

        assert all(r.startswith("#/components/schemas/") for r in refs(doc)), list(
            refs(doc)
        )
        request = doc["components"]["schemas"]["PredictRequest"]
        assert "$defs" not in request
        validator = jsonschema.Draft202012Validator(request)
        assert validator.is_valid(
            {"instances": [{"cc_num": 1, "amount": 1.0, "transaction_time": None}]}
        )
        assert not validator.is_valid({"instances": [{"cc_num": 1}]})
        error = doc["components"]["schemas"]["Error"]
        assert "request_id" in error["properties"]["detail"]["properties"]

    def test_batch_limit_is_part_of_the_contract(self):
        a = _schema()
        b = a._with_max_batch_rows(7)

        assert a.max_batch_rows == 512 and b.max_batch_rows == 7
        assert a.schema_id != b.schema_id
        assert DeploymentSchema.from_response_json(b.to_dict()) == b
        assert b.to_dict()["maxBatchRows"] == 7
        assert {
            x["maxItems"]
            for x in b.to_json_schema()["request"]["properties"]["instances"]["oneOf"]
        } == {7}
        assert b.validate_instances([[1, 1.0, 1, None]] * 8)[0]["reason"].startswith(
            "batch has 8 rows, the limit is 7"
        )
        assert (
            DeploymentSchema(serving_keys=["k"], max_batch_rows="junk").max_batch_rows
            == 512
        )

    def test_max_batch_rows_from_env_vars(self):
        from hsml.deployment_schema import _max_batch_rows

        assert _max_batch_rows(None) == 512
        assert _max_batch_rows({"SERVING_MAX_BATCH_ROWS": "64"}) == 64
        assert _max_batch_rows({"SERVING_MAX_BATCH_ROWS": "many"}) == 512

    def test_openapi_document(self):
        schema = _schema()
        doc = schema.to_openapi(
            "fraud", url="https://h/v1/ns/fraud/v1/models/fraud:predict"
        )

        assert doc["openapi"] == "3.1.0"
        assert doc["servers"] == [{"url": "https://h/v1/ns/fraud"}]
        assert "/v1/models/fraud:predict" in doc["paths"]
        responses = doc["paths"]["/v1/models/fraud:predict"]["post"]["responses"]
        assert set(responses) == {"200", "400", "404", "413", "422", "500", "503"}
        assert doc["info"]["version"] == schema.schema_id


def _fv(**overrides):
    def feature(name, type_, label=False, helper=False):
        return SimpleNamespace(
            name=name,
            type=type_,
            label=label,
            inference_helper_column=helper,
            training_helper_column=False,
            on_demand_transformation_function=None,
        )

    def serving_key(name, required=True, prefix=""):
        return SimpleNamespace(
            required_serving_key=prefix + name if required else None,
            feature_name=name,
            feature_group=SimpleNamespace(features=[feature(name, "string")]),
        )

    features = [
        feature("cc_num", "bigint"),
        feature("amount", "double"),
        feature("fraud", "int", label=True),
        feature("helper", "string", helper=True),
    ]
    defaults = {
        "name": "fv",
        "version": 2,
        "features": features,
        "labels": ["fraud"],
        "serving_keys": [serving_key("cc_num"), serving_key("account_id")],
        "request_parameters": ["transaction_time"],
        "logging_enabled": False,
        "feature_logging": None,
        "transformation_functions": [],
        "get_training_dataset_schema": lambda version: (
            features + [feature("amount_scaled", "double")]
        ),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _tf(name, features, statistics_required, kind="model_dependent"):
    return SimpleNamespace(
        transformation_type=SimpleNamespace(value=kind),
        hopsworks_udf=SimpleNamespace(
            function_name=name,
            transformation_features=features,
            statistics_required=statistics_required,
        ),
    )


class TestInference:
    def test_infers_groups_sorted_with_types(self):
        schema = ds._infer_deployment_schema(
            _fv(), passed_features=["amount"], training_dataset_version=3
        )

        assert [(f.name, f.type) for f in schema.serving_keys] == [
            ("account_id", "string"),
            ("cc_num", "bigint"),
        ]
        assert [(f.name, f.type) for f in schema.passed_features] == [
            ("amount", "double")
        ]
        assert [(f.name, f.type) for f in schema.request_parameters] == [
            ("transaction_time", None)
        ]
        assert schema.extra_logging_features == []
        assert schema.feature_view == {"name": "fv", "version": 2}
        assert schema.training_dataset_version == 3
        assert schema.inferred is True
        assert schema.output == {"kind": "predictions", "columns": None}

    def test_rejects_unknown_and_label_passed_features(self):
        with pytest.raises(ValueError, match="not a feature of feature view 'fv' v2"):
            ds._infer_deployment_schema(_fv(), passed_features=["nope"])
        with pytest.raises(ValueError, match="is a label"):
            ds._infer_deployment_schema(_fv(), passed_features=["fraud"])

    def test_extra_logging_columns_minus_reserved(self):
        logging = SimpleNamespace(
            extra_logging_columns=[
                SimpleNamespace(name="channel", type="string"),
                SimpleNamespace(name="deployment_name", type="string"),
            ]
        )
        schema = ds._infer_deployment_schema(
            _fv(logging_enabled=True, feature_logging=logging)
        )

        assert [f.name for f in schema.extra_logging_features] == ["channel"]

    def test_serving_key_type_from_the_query_feature_group(self):
        stub = SimpleNamespace(id=53, name="iris", features=[])
        full = SimpleNamespace(
            id=53,
            name="iris",
            features=[
                SimpleNamespace(name="id", type="bigint"),
                SimpleNamespace(name="amount", type="double"),
            ],
        )
        serving_key = SimpleNamespace(
            required_serving_key="id", feature_name="id", feature_group=stub
        )
        fv = _fv(
            serving_keys=[serving_key], query=SimpleNamespace(featuregroups=[full])
        )

        schema = ds._infer_deployment_schema(fv)

        assert [(f.name, f.type) for f in schema.serving_keys] == [("id", "bigint")]
        assert "id" not in schema.unresolved

    def test_extra_logging_column_type_taken_from_logging_feature_group(self):
        fg = SimpleNamespace(
            features=[
                SimpleNamespace(name="channel", type="string"),
                SimpleNamespace(name="cc_num", type="bigint"),
            ]
        )
        logging = SimpleNamespace(
            extra_logging_columns=[SimpleNamespace(name="channel", type=None)],
            get_feature_group=lambda transformed: fg,
        )
        schema = ds._infer_deployment_schema(
            _fv(logging_enabled=True, feature_logging=logging)
        )

        assert [(f.name, f.type) for f in schema.extra_logging_features] == [
            ("channel", "string")
        ]
        assert schema.unresolved == ["transaction_time"]

    def test_feature_vector_output_columns_exclude_labels_and_helpers(self):
        schema = ds._infer_deployment_schema(
            _fv(), output_kind="feature_vectors", training_dataset_version=1
        )

        assert schema.output["kind"] == "feature_vectors"
        assert [c["name"] for c in schema.output["columns"]] == [
            "cc_num",
            "amount",
            "amount_scaled",
        ]

    def test_no_serving_keys_is_fine(self):
        schema = ds._infer_deployment_schema(_fv(serving_keys=[]))

        assert schema.serving_keys == []


class TestTrainingDatasetCheck:
    def test_raises_naming_statistics_dependent_transformations(self):
        fv = _fv(
            transformation_functions=[
                _tf("min_max_scaler", ["amount"], True),
                _tf("add_one", ["amount"], False),
                _tf("on_demand_thing", ["transaction_time"], True, kind="on_demand"),
            ]
        )

        with pytest.raises(ValueError) as info:
            ds._check_training_dataset_version(
                fv, None, "feature view 'fv' v2", "Pass it."
            )

        message = str(info.value)
        assert "min_max_scaler(amount)" in message
        assert "add_one" not in message
        assert "on_demand_thing" not in message
        assert message.endswith("Pass it.")

    def test_passes_with_version_or_without_statistics(self):
        fv = _fv(transformation_functions=[_tf("min_max_scaler", ["amount"], True)])

        ds._check_training_dataset_version(fv, 1, "x", "y")
        ds._check_training_dataset_version(
            _fv(transformation_functions=[_tf("f", ["a"], False)]), None, "x", "y"
        )


class TestRefinement:
    def test_same_fields_may_change_types_and_order(self):
        inferred = ds._infer_deployment_schema(_fv(), passed_features=["amount"])
        given = DeploymentSchema(
            serving_keys=[
                {"name": "cc_num", "type": "bigint"},
                {"name": "account_id", "type": "string"},
            ],
            passed_features=["amount"],
            request_parameters=[{"name": "transaction_time", "type": "timestamp"}],
        )

        ds._check_schema_refinement(inferred, given)

    def test_added_or_removed_fields_rejected(self):
        inferred = ds._infer_deployment_schema(_fv(), passed_features=["amount"])
        given = DeploymentSchema(
            serving_keys=["cc_num", "account_id"], passed_features=["amount", "extra"]
        )

        with pytest.raises(
            ValueError, match=r"passed_features differ.*unexpected \['extra'\]"
        ):
            ds._check_schema_refinement(inferred, given)
        with pytest.raises(ValueError, match=r"request_parameters differ.*missing"):
            ds._check_schema_refinement(
                inferred,
                DeploymentSchema(
                    serving_keys=["cc_num", "account_id"], passed_features=["amount"]
                ),
            )


class TestNoLookupSchemas:
    def test_all_stored_features_passed_needs_no_serving_keys(self):
        from hsml.deployment_schema import _infer_deployment_schema

        fv = _fv()
        looked_up = _infer_deployment_schema(fv, passed_features=["amount"])
        assert [f.name for f in looked_up.serving_keys] == ["account_id", "cc_num"]

        passed_all = _infer_deployment_schema(
            fv, passed_features=["amount", "cc_num", "helper"]
        )
        assert passed_all.serving_keys == []
        assert [f.name for f in passed_all.passed_features] == [
            "amount",
            "cc_num",
            "helper",
        ]
        assert [f.name for f in passed_all.request_parameters] == ["transaction_time"]
        assert passed_all.schema_id != looked_up.schema_id

    def test_on_demand_features_are_never_looked_up(self):
        from hsml.deployment_schema import _infer_deployment_schema

        fv = _fv()
        fv.features.append(
            SimpleNamespace(
                name="amount_x_rate",
                type="double",
                label=False,
                inference_helper_column=False,
                training_helper_column=False,
                on_demand_transformation_function=object(),
            )
        )
        schema = _infer_deployment_schema(
            fv, passed_features=["amount", "cc_num", "helper"]
        )
        assert schema.serving_keys == []

    def test_model_without_feature_view_serves_its_passed_features(self):
        from hsml.deployment_schema import (
            _infer_model_deployment_schema,
            _offline_type_for_model_type,
        )

        schema = _infer_model_deployment_schema("m", ["b", "a"])
        assert schema.serving_keys == [] and schema.request_parameters == []
        assert schema.feature_view is None and schema.inferred
        assert [(f.name, f.type) for f in schema.passed_features] == [
            ("b", None),
            ("a", None),
        ]
        assert schema.unresolved == ["b", "a"]

        legacy = [
            {"name": "b", "type": "int64"},
            {"name": "a", "type": "float64"},
            {"name": "c", "type": "object"},
            {"name": "d", "type": "weird"},
        ]
        schema = _infer_model_deployment_schema(
            "m",
            None,
            model_input_columns=legacy,
            output_columns=[{"name": "y", "type": "int64"}],
        )
        assert [(f.name, f.type) for f in schema.passed_features] == [
            ("b", "bigint"),
            ("a", "double"),
            ("c", "string"),
            ("d", None),
        ]
        assert schema.unresolved == ["d"]
        assert schema.output["columns"][0]["name"] == "y"
        assert _offline_type_for_model_type("bigint") == "bigint"
        assert _offline_type_for_model_type(None) is None
        with pytest.raises(ValueError, match="passed_features"):
            _infer_model_deployment_schema("m", None)
        with pytest.raises(ValueError, match="exactly its input columns"):
            _infer_model_deployment_schema(
                "m", ["b"], model_input_columns=[{"name": "a", "type": "int64"}]
            )


def _odt(source, features):
    return SimpleNamespace(
        transformation_type=SimpleNamespace(value="on_demand"),
        hopsworks_udf=SimpleNamespace(
            function_name="odt",
            _function_source=source,
            transformation_features=features,
            statistics_required=False,
        ),
    )


class TestRequestParameterTypes:
    def test_annotated_arguments_type_the_request_parameters(self):
        source = (
            "from datetime import datetime\n\n"
            "@udf(float, drop=['transaction_time'])\n"
            "def age(amount: float, transaction_time: datetime, context) -> float:\n"
            "    return 1.0\n"
        )
        fv = _fv(
            _on_demand_transformation_functions=[
                _odt(source, ["amount", "transaction_time"])
            ]
        )

        schema = ds._infer_deployment_schema(
            fv, passed_features=["amount"], training_dataset_version=3
        )

        assert [(f.name, f.type) for f in schema.request_parameters] == [
            ("transaction_time", "timestamp")
        ]
        assert schema.unresolved == []

    def test_bound_names_map_positionally_and_keywords_are_skipped(self):
        source = (
            "@udf(float)\n"
            "def f(x: int, y: str | None, z: Optional[float], statistics=None, context=None):\n"
            "    return 1.0\n"
        )

        types = ds._udf_argument_types(_odt(source, ["a", "b", "c"]).hopsworks_udf)

        assert types == {"a": "bigint", "b": "string", "c": "double"}

    @pytest.mark.parametrize(
        "annotation", ["", ": pd.Series", ": list[float]", ": int | str"]
    )
    def test_unknown_annotations_stay_unresolved(self, annotation):
        source = f"@udf(float)\ndef f(x{annotation}):\n    return 1.0\n"

        assert ds._udf_argument_types(_odt(source, ["a"]).hopsworks_udf) == {}

    def test_unparseable_or_missing_source_resolves_nothing(self):
        assert ds._udf_argument_types(_odt("def (:", ["a"]).hopsworks_udf) == {}
        assert (
            ds._udf_argument_types(SimpleNamespace(transformation_features=["a"])) == {}
        )
        assert ds._udf_argument_types(None) == {}

    def test_falls_back_to_the_transformations_attached_to_features(self):
        source = "@udf(float)\ndef f(amount: float, transaction_time: int):\n    return 1.0\n"
        tf = _odt(source, ["amount", "transaction_time"])
        features = [
            SimpleNamespace(
                name=name,
                type=type_,
                label=False,
                inference_helper_column=False,
                training_helper_column=False,
                on_demand_transformation_function=odt,
            )
            for name, type_, odt in [("cc_num", "bigint", None), ("age", "double", tf)]
        ]
        fv = _fv(features=features, labels=[])

        schema = ds._infer_deployment_schema(fv, training_dataset_version=3)

        assert [(f.name, f.type) for f in schema.request_parameters] == [
            ("transaction_time", "bigint")
        ]
