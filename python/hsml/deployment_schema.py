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

import ast
import base64
import datetime
import decimal
import hashlib
import json
import math
import re
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hopsworks_common.client.exceptions import ModelServingException
from hopsworks_common.constants import MODEL_SERVING


if TYPE_CHECKING:
    from hsml.schema import Schema


SCHEMA_VERSION = 1

GROUPS = (
    "serving_keys",
    "passed_features",
    "request_parameters",
    "extra_logging_features",
)

# Extra logging columns the default predictor fills itself when the feature
# view declares them; they never appear in the request contract.
RESERVED_LOGGING_COLUMNS = {
    "deployment_name": "string",
    "deployment_version": "int",
    "deployment_schema_id": "string",
    "request_row": "int",
}

OUTPUT_PREDICTIONS = "predictions"
OUTPUT_FEATURE_VECTORS = "feature_vectors"

_INTEGER_TYPES = {"tinyint", "smallint", "int", "integer"}
_FLOAT_TYPES = {"float", "double"}
_STRING_TYPES = {"string"}
_TIMESTAMP_TYPES = {"timestamp"}
_DATE_TYPES = {"date"}

_RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:?\d{2})?$"
)
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DECIMAL_STRING = re.compile(r"^-?\d+(\.\d+)?$")
_INTEGER_STRING = re.compile(r"^-?\d+$")
_BASE64 = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")


@public
class SchemaField:
    """One field of a deployment schema: a name, an optional feature store type, and nullability.

    A `None` type means the type could not be resolved (request parameters of
    on-demand transformations); such fields are not type-checked.
    """

    def __init__(
        self,
        name: str,
        type: str | None = None,
        nullable: bool = True,
        description: str | None = None,
        **kwargs,
    ):
        if not name or not isinstance(name, str):
            raise ValueError(f"A schema field needs a non-empty name, got {name!r}")
        self.name = name
        self.type = _normalize_type(type)
        self.nullable = bool(nullable)
        self.description = description

    def to_dict(self) -> dict[str, Any]:
        """Return the field as the dict stored in the schema document.

        Returns:
            Name, type and nullability, plus the description when one is set.
        """
        d = {"name": self.name, "type": self.type, "nullable": self.nullable}
        if self.description is not None:
            d["description"] = self.description
        return d

    def __eq__(self, other):
        return isinstance(other, SchemaField) and self.to_dict() == other.to_dict()

    def __repr__(self):
        return f"SchemaField({self.name!r}, {self.type!r}, nullable={self.nullable})"


@public
class DeploymentSchema:
    """The request and response contract of a deployment.

    Serving keys, passed features, and request parameters are required in every
    request; extra logging features are optional. `columns` gives the
    positional order for list rows. Types are feature store offline types
    (`bigint`, `string`, `array<double>`, ...); see `to_json_schema()` for the
    JSON encoding each one accepts.

    Example:
        ```python
        # a custom predictor script with an explicit contract
        schema = DeploymentSchema(
            serving_keys=[{"name": "cc_num", "type": "bigint", "nullable": False}],
            passed_features=[{"name": "amount", "type": "double"}],
        )
        deployment = model.deploy(script_file="predictor.py", schema=schema)

        deployment.schema.validate_instances([{"cc_num": 1, "amount": "x"}])  # -> errors
        print(deployment.schema.to_openapi()["paths"])
        ```
    """

    def __init__(
        self,
        serving_keys: list[dict | str] | Schema | None = None,
        passed_features: list[dict | str] | Schema | None = None,
        request_parameters: list[dict | str] | Schema | None = None,
        extra_logging_features: list[dict | str] | Schema | None = None,
        feature_view: dict[str, Any] | None = None,
        training_dataset_version: int | None = None,
        output: dict[str, Any] | None = None,
        inferred: bool = False,
        max_batch_rows: int | None = None,
        **kwargs,
    ):
        self._serving_keys = _to_fields(serving_keys, nullable_default=False)
        self._passed_features = _to_fields(passed_features)
        self._request_parameters = _to_fields(request_parameters)
        self._extra_logging_features = _to_fields(extra_logging_features)
        self._feature_view = _normalize_feature_view(feature_view)
        self._training_dataset_version = training_dataset_version
        self._output = _normalize_output(output)
        self._inferred = bool(inferred)
        self._max_batch_rows = _positive_int(max_batch_rows)
        self._check_unique_names()

    # region Construction and serialization

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> DeploymentSchema:
        """Build a schema from a camelCase schema document.

        Parameters:
            json_dict: The document as served by the backend or read from the deployment's resources.

        Returns:
            The schema the document describes.
        """
        return cls.from_json(humps.decamelize(json_dict))

    @classmethod
    def from_json(cls, json_decamelized: dict[str, Any]) -> DeploymentSchema:
        """Build a schema from a decamelized document, ignoring its version and id.

        Parameters:
            json_decamelized: The document with snake_case keys.

        Returns:
            The schema the document describes.
        """
        json_decamelized = dict(json_decamelized)
        json_decamelized.pop("schema_version", None)
        json_decamelized.pop("schema_id", None)
        return cls(**json_decamelized)

    def _with_max_batch_rows(self, max_batch_rows: int) -> DeploymentSchema:
        """The same contract with another batch limit, hence another id."""
        content = humps.decamelize(self._content_dict())
        content["max_batch_rows"] = max_batch_rows
        return DeploymentSchema.from_json(content)

    def to_dict(self) -> dict[str, Any]:
        """Return the schema document with its content-addressed id.

        Returns:
            The camelCase document including `schemaId`.
        """
        d = self._content_dict()
        d["schemaId"] = self.schema_id
        return d

    def _content_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "servingKeys": [f.to_dict() for f in self._serving_keys],
            "passedFeatures": [f.to_dict() for f in self._passed_features],
            "requestParameters": [f.to_dict() for f in self._request_parameters],
            "extraLoggingFeatures": [f.to_dict() for f in self._extra_logging_features],
            "featureView": self._feature_view,
            "trainingDatasetVersion": self._training_dataset_version,
            "output": self._output,
            "inferred": self._inferred,
            "maxBatchRows": self.max_batch_rows,
        }

    def json(self) -> str:
        """Return the schema document as indented JSON.

        Returns:
            The document from `to_dict()` with two-space indentation.
        """
        return json.dumps(self.to_dict(), indent=2)

    @public
    def describe(self) -> None:
        """Print the schema as JSON."""
        util._pretty_print(self)

    # endregion

    # region Properties

    @public
    @property
    def serving_keys(self) -> list[SchemaField]:
        """Fields identifying the entity to look up; required and non-null."""
        return list(self._serving_keys)

    @public
    @property
    def passed_features(self) -> list[SchemaField]:
        """Feature view features whose values the client provides; required."""
        return list(self._passed_features)

    @public
    @property
    def request_parameters(self) -> list[SchemaField]:
        """Parameters of on-demand transformations; required."""
        return list(self._request_parameters)

    @public
    @property
    def extra_logging_features(self) -> list[SchemaField]:
        """Client-supplied values that are only logged; optional."""
        return list(self._extra_logging_features)

    @public
    @property
    def columns(self) -> list[SchemaField]:
        """All fields in positional order: serving keys, passed features, request parameters, extra logging features."""
        return (
            self._serving_keys
            + self._passed_features
            + self._request_parameters
            + self._extra_logging_features
        )

    @public
    @property
    def names(self) -> list[str]:
        """Names of all fields in positional order."""
        return [f.name for f in self.columns]

    @public
    @property
    def required_names(self) -> list[str]:
        """Names a request row must contain."""
        return [
            f.name
            for f in self._serving_keys
            + self._passed_features
            + self._request_parameters
        ]

    @public
    @property
    def unresolved(self) -> list[str]:
        """Names whose type is unknown and therefore not validated."""
        return [f.name for f in self.columns if f.type is None]

    @public
    @property
    def feature_view(self) -> dict[str, Any] | None:
        """`{"name", "version"}` of the feature view the schema was inferred from."""
        return self._feature_view

    @public
    @property
    def training_dataset_version(self) -> int | None:
        """Training dataset version whose statistics the deployment uses."""
        return self._training_dataset_version

    @public
    @property
    def output(self) -> dict[str, Any]:
        """Response contract: `{"kind": "predictions" | "feature_vectors", "columns": [...] | None}`."""
        return dict(self._output)

    @public
    @property
    def inferred(self) -> bool:
        """Whether the schema was inferred from the feature view rather than given."""
        return self._inferred

    @public
    @property
    def max_batch_rows(self) -> int:
        """Largest batch a request may carry; part of the published contract, so changing it changes the schema id."""
        return self._max_batch_rows or MODEL_SERVING.DEFAULT_MAX_BATCH_ROWS

    @public
    @property
    def schema_id(self) -> str:
        """Content hash of the schema; equal schemas have equal ids."""
        canonical = json.dumps(
            self._content_dict(), sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    # endregion

    # region Validation

    def _group_of(self, name: str) -> str | None:
        for group in GROUPS:
            if any(f.name == name for f in getattr(self, f"_{group}")):
                return group
        return None

    @public
    def validate_instance(self, instance: Any, row: int = 0) -> list[dict[str, Any]]:
        """Validate one request row.

        Parameters:
            instance: A row as an object keyed by field name or an array in `columns` order.
            row: Index of the row in its batch, reported in the errors.

        Returns:
            The errors found, each `{"row", "field", "reason"}`; empty when the row is valid.
        """
        errors: list[dict[str, Any]] = []
        columns = self.columns
        if isinstance(instance, dict):
            known = {f.name for f in columns}
            for name in self.required_names:
                if name not in instance:
                    errors.append({"row": row, "field": name, "reason": "missing"})
            for key in instance:
                if key not in known:
                    errors.append({"row": row, "field": key, "reason": "unknown field"})
            values = {
                f.name: instance.get(f.name) for f in columns if f.name in instance
            }
        elif isinstance(instance, (list, tuple)):
            if len(instance) != len(columns):
                errors.append(
                    {
                        "row": row,
                        "field": None,
                        "reason": f"expected {len(columns)} values, got {len(instance)}",
                    }
                )
                return errors
            values = {f.name: v for f, v in zip(columns, instance, strict=True)}
        else:
            return [
                {
                    "row": row,
                    "field": None,
                    "reason": f"a row must be an object or an array, got {type(instance).__name__}",
                }
            ]

        for field in columns:
            if field.name not in values:
                continue
            value = values[field.name]
            if value is None:
                if not field.nullable:
                    errors.append(
                        {"row": row, "field": field.name, "reason": "must not be null"}
                    )
                continue
            reason = _check_value(field.type, value)
            if reason is not None:
                errors.append({"row": row, "field": field.name, "reason": reason})
        return errors

    @public
    def validate_instances(
        self, instances: Any, max_rows: int | None = None
    ) -> list[dict[str, Any]]:
        """Validate a batch of rows.

        Rows must all be objects or all be arrays.

        Parameters:
            instances: The rows of a request.
            max_rows: Largest accepted batch; defaults to `max_batch_rows`.

        Returns:
            All errors found across the batch; empty when the batch is valid.
        """
        if max_rows is None:
            max_rows = self.max_batch_rows
        if not isinstance(instances, (list, tuple)):
            return [
                {"row": None, "field": None, "reason": "instances must be an array"}
            ]
        if len(instances) == 0:
            return [
                {"row": None, "field": None, "reason": "instances must not be empty"}
            ]
        if len(instances) > max_rows:
            return [
                {
                    "row": None,
                    "field": None,
                    "reason": f"batch has {len(instances)} rows, the limit is {max_rows}",
                }
            ]
        forms = {isinstance(i, dict) for i in instances}
        if len(forms) > 1:
            return [
                {
                    "row": None,
                    "field": None,
                    "reason": "rows must all be objects or all be arrays",
                }
            ]
        errors: list[dict[str, Any]] = []
        for row, instance in enumerate(instances):
            errors.extend(self.validate_instance(instance, row))
        return errors

    def _raise_if_invalid(
        self, instances: Any, deployment_name: str | None, max_rows: int | None = None
    ) -> None:
        errors = self.validate_instances(instances, max_rows=max_rows)
        if errors:
            raise DeploymentSchemaError(
                _format_errors(deployment_name, self, errors), errors, self.schema_id
            )

    @public
    def rows(self, instances: list[Any]) -> list[dict[str, Any]]:
        """Convert validated rows to dicts keyed by field name.

        Parameters:
            instances: Rows that passed `validate_instances`, as objects or arrays.

        Returns:
            One dict per row with every field of the schema.
        """
        columns = self.columns
        rows = []
        for instance in instances:
            if isinstance(instance, dict):
                rows.append({f.name: instance.get(f.name) for f in columns})
            else:
                rows.append(dict(zip((f.name for f in columns), instance, strict=True)))
        return rows

    @public
    def split_row(self, row: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Split one row dict into its groups.

        Parameters:
            row: A row as returned by `rows`.

        Returns:
            `{"serving_keys": {...}, "passed_features": {...}, "request_parameters": {...}, "extra_logging_features": {...}}`.
        """
        return {
            group: {f.name: row.get(f.name) for f in getattr(self, f"_{group}")}
            for group in GROUPS
        }

    # endregion

    # region JSON Schema and OpenAPI

    @public
    def to_json_schema(self, max_rows: int | None = None) -> dict[str, Any]:
        """Render the request and response contracts as JSON Schema (draft 2020-12).

        Parameters:
            max_rows: Largest accepted batch; defaults to `max_batch_rows`.

        Returns:
            `{"request": <JSON Schema>, "response": <JSON Schema>}`.
        """
        if max_rows is None:
            max_rows = self.max_batch_rows
        columns = self.columns
        properties = {f.name: _json_schema_for(f) for f in columns}
        row_object = {
            "type": "object",
            "properties": properties,
            "required": list(self.required_names),
            "additionalProperties": False,
        }
        row_array = {
            "type": "array",
            "prefixItems": [_json_schema_for(f) for f in columns],
            "minItems": len(columns),
            "maxItems": len(columns),
        }

        def batch_of(row_ref: str) -> dict[str, Any]:
            return {
                "type": "array",
                "minItems": 1,
                "maxItems": max_rows,
                "items": {"$ref": row_ref},
            }

        request = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["instances"],
            "properties": {
                # all objects or all arrays, as the validator requires
                "instances": {
                    "oneOf": [
                        batch_of("#/$defs/row_object"),
                        batch_of("#/$defs/row_array"),
                    ]
                }
            },
            "$defs": {"row_object": row_object, "row_array": row_array},
        }
        if self._output["kind"] == OUTPUT_FEATURE_VECTORS:
            out_columns = self._output.get("columns") or []
            response = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["predictions", "columns"],
                "properties": {
                    "predictions": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "prefixItems": [
                                _json_schema_for_type(c.get("type"), nullable=True)
                                for c in out_columns
                            ],
                            "minItems": len(out_columns),
                            "maxItems": len(out_columns),
                        },
                    },
                    "columns": {
                        "type": "array",
                        "const": [c["name"] for c in out_columns],
                    },
                },
            }
        else:
            out_columns = self._output.get("columns")
            items: dict[str, Any] = {}
            if out_columns:
                # one prediction per row: an object or an array over the output
                # columns, or the bare value when the model has one output
                shapes: list[dict[str, Any]] = [
                    {
                        "type": "object",
                        "properties": {
                            c["name"]: _json_schema_for_type(c.get("type"), True)
                            for c in out_columns
                        },
                    },
                    {
                        "type": "array",
                        "prefixItems": [
                            _json_schema_for_type(c.get("type"), True)
                            for c in out_columns
                        ],
                        "minItems": len(out_columns),
                        "maxItems": len(out_columns),
                    },
                ]
                if len(out_columns) == 1:
                    shapes.append(
                        _json_schema_for_type(out_columns[0].get("type"), True)
                    )
                items = {"anyOf": shapes}
            response = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["predictions"],
                "properties": {"predictions": {"type": "array", "items": items}},
            }
        return {"request": request, "response": response}

    @public
    def to_openapi(
        self, name: str, url: str | None = None, max_rows: int | None = None
    ) -> dict[str, Any]:
        """Render an OpenAPI 3.1 document for the deployment's `:predict` endpoint.

        Parameters:
            name: Deployment name.
            url: Full inference URL when known; otherwise the path is rendered relative to the server the client already uses.
            max_rows: Largest accepted batch; defaults to `max_batch_rows`.

        Returns:
            The OpenAPI document as a dict.
        """
        schemas = self.to_json_schema(max_rows=max_rows)
        # `#/$defs/...` would resolve against the OpenAPI document root, so the
        # row schemas are inlined instead
        request = _inline_defs(schemas["request"])
        request.pop("$schema", None)
        response = dict(schemas["response"])
        response.pop("$schema", None)
        error = {
            "type": "object",
            "properties": {
                "detail": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "message": {"type": "string"},
                        "schema_id": {"type": "string"},
                        "request_id": {"type": "string"},
                        "errors": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "row": {"type": ["integer", "null"]},
                                    "field": {"type": ["string", "null"]},
                                    "reason": {"type": "string"},
                                },
                            },
                        },
                    },
                },
                "error": {"type": "string"},
            },
        }
        path = f"/v1/models/{name}:predict"
        servers = []
        if url:
            if url.endswith(path):
                servers = [{"url": url[: -len(path)]}]
            else:
                servers = [{"url": url}]
                path = "/"
        return {
            "openapi": "3.1.0",
            "info": {
                "title": f"Hopsworks deployment {name}",
                "version": self.schema_id,
                "description": "Prediction endpoint contract generated from the deployment schema.",
            },
            "servers": servers,
            "components": {
                "securitySchemes": {
                    "ApiKey": {
                        "type": "apiKey",
                        "in": "header",
                        "name": "Authorization",
                        "description": "`ApiKey <key>` with the SERVING scope.",
                    }
                },
                "schemas": {
                    "PredictRequest": request,
                    "PredictResponse": response,
                    "Error": error,
                },
            },
            "security": [{"ApiKey": []}],
            "paths": {
                path: {
                    "post": {
                        "summary": "Predict",
                        "parameters": [
                            {
                                "name": "x-request-id",
                                "in": "header",
                                "required": False,
                                "schema": {"type": "string"},
                                "description": "Correlation id stored with every logged row.",
                            }
                        ],
                        "requestBody": {
                            "required": True,
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/PredictRequest"
                                    }
                                }
                            },
                        },
                        "responses": {
                            "200": {
                                "description": "Predictions, one per request row, in request order.",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": "#/components/schemas/PredictResponse"
                                        }
                                    }
                                },
                            },
                            **{
                                str(status): {
                                    "description": desc,
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "$ref": "#/components/schemas/Error"
                                            }
                                        }
                                    },
                                }
                                for status, desc in ERROR_RESPONSES.items()
                            },
                        },
                    }
                }
            },
        }

    # endregion

    def _check_unique_names(self):
        seen: set[str] = set()
        for field in self.columns:
            if field.name in seen:
                raise ValueError(
                    f"Field '{field.name}' appears in more than one group of the deployment schema"
                )
            seen.add(field.name)

    def __eq__(self, other):
        return isinstance(other, DeploymentSchema) and self.to_dict() == other.to_dict()

    def __repr__(self):
        return (
            f"DeploymentSchema(id={self.schema_id!r}, fields={len(self.columns)}, "
            f"inferred={self._inferred})"
        )


ERROR_RESPONSES = {
    400: "SCHEMA_VALIDATION or FEATURE_LOOKUP_FAILED: the request does not match the schema or cannot be resolved.",
    404: "ENTITY_NOT_FOUND: at least one row's serving keys match no entity.",
    413: "BATCH_TOO_LARGE: more rows than the deployment accepts.",
    422: "TRANSFORMATION_FAILED: a transformation raised.",
    500: "MODEL_FAILED or CONTRACT_VIOLATION: the model raised, or the pod produced a result that does not match the contract.",
    503: "FEATURE_STORE_UNAVAILABLE: the online store or the feature store API could not be reached.",
}


def _inline_defs(schema: dict[str, Any]) -> dict[str, Any]:
    """Copy a JSON Schema with every `#/$defs/<name>` reference replaced by its definition."""
    defs = schema.get("$defs") or {}
    prefix = "#/$defs/"

    def walk(node: Any) -> Any:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith(prefix):
                return walk(defs[ref[len(prefix) :]])
            return {k: walk(v) for k, v in node.items() if k != "$defs"}
        if isinstance(node, list):
            return [walk(v) for v in node]
        return node

    return walk(schema)


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _configured_batch_rows(env_vars: dict[str, str] | None) -> int | None:
    """The batch limit a component's env vars configure, or None when they do not."""
    return _positive_int((env_vars or {}).get(MODEL_SERVING.MAX_BATCH_ROWS_ENV_VAR))


def _max_batch_rows(env_vars: dict[str, str] | None) -> int:
    """The batch limit a deployment's env vars configure, else the default."""
    return _configured_batch_rows(env_vars) or MODEL_SERVING.DEFAULT_MAX_BATCH_ROWS


def _parses_as_timestamp(value: str) -> bool:
    try:
        datetime.datetime.fromisoformat(value.replace("z", "Z").replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _parses_as_date(value: str) -> bool:
    try:
        datetime.date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _logging_feature_group_types(logging: Any) -> dict[str, str]:
    """Column types of the logging feature groups, for extra columns whose metadata carries none."""
    types: dict[str, str] = {}
    getter = getattr(logging, "get_feature_group", None)
    if getter is None:
        return types
    for transformed in (False, True):
        try:
            features = getattr(getter(transformed), "features", None) or []
        except Exception:  # noqa: BLE001 - metadata only; the column stays unresolved
            continue
        for feature in features:
            if getattr(feature, "type", None):
                types.setdefault(feature.name, feature.type)
    return types


@public
class DeploymentSchemaError(ModelServingException):
    """A request does not match a deployment schema. `errors` lists `{"row", "field", "reason"}`."""

    def __init__(self, message: str, errors: list[dict[str, Any]], schema_id: str):
        super().__init__(message)
        self.errors = errors
        self.schema_id = schema_id


def _format_errors(
    deployment_name: str | None, schema: DeploymentSchema, errors: list[dict[str, Any]]
) -> str:
    target = f" of '{deployment_name}'" if deployment_name else ""
    lines = [
        f"Prediction request does not match the deployment schema{target}.",
        "Expected fields (in order): " + ", ".join(schema.names),
    ]
    by_row: dict[Any, list[str]] = {}
    for e in errors:
        text = f"'{e['field']}' {e['reason']}" if e.get("field") else e["reason"]
        by_row.setdefault(e.get("row"), []).append(text)
    for row, texts in by_row.items():
        prefix = f"instance {row}: " if row is not None else ""
        lines.append(prefix + "; ".join(texts))
    return "\n".join(lines)


# region Fields and types


def _to_fields(value: Any, nullable_default: bool = True) -> list[SchemaField]:
    if value is None:
        return []
    if hasattr(value, "columnar_schema"):
        value = [
            {
                "name": getattr(c, "name", None),
                "type": getattr(c, "type", None),
                "description": getattr(c, "description", None),
            }
            for c in value.columnar_schema
        ]
    elif hasattr(value, "tensor_schema"):
        raise ValueError(
            "A deployment schema group must be columnar, not a tensor schema"
        )
    fields = []
    for item in value:
        if isinstance(item, SchemaField):
            fields.append(item)
        elif isinstance(item, str):
            fields.append(SchemaField(item, None, nullable_default))
        elif isinstance(item, dict):
            item = dict(item)
            item.setdefault("nullable", nullable_default)
            fields.append(SchemaField(**item))
        else:
            raise ValueError(
                f"A schema field must be a name, a dict, or a SchemaField, got {type(item).__name__}"
            )
    return fields


def _normalize_type(type_: Any) -> str | None:
    if type_ is None:
        return None
    return str(type_).strip().lower()


def _normalize_feature_view(fv: Any) -> dict[str, Any] | None:
    if fv is None:
        return None
    if isinstance(fv, dict):
        return {"name": fv.get("name"), "version": fv.get("version")}
    return {"name": getattr(fv, "name", None), "version": getattr(fv, "version", None)}


def _normalize_output(output: Any) -> dict[str, Any]:
    if output is None:
        return {"kind": OUTPUT_PREDICTIONS, "columns": None}
    output = humps.decamelize(dict(output))
    kind = output.get("kind") or OUTPUT_PREDICTIONS
    if kind not in (OUTPUT_PREDICTIONS, OUTPUT_FEATURE_VECTORS):
        raise ValueError(f"Unknown output kind {kind!r}")
    columns = output.get("columns")
    if columns is not None:
        columns = [
            {"name": c["name"], "type": _normalize_type(c.get("type"))} for c in columns
        ]
    return {"kind": kind, "columns": columns}


def _split_top_level(text: str) -> list[str]:
    parts, depth, current = [], 0, []
    for ch in text:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return parts


def _parse_complex(type_: str) -> tuple[str, Any] | None:
    if type_.startswith("array<") and type_.endswith(">"):
        return "array", type_[6:-1].strip()
    if type_.startswith("map<") and type_.endswith(">"):
        key_value = _split_top_level(type_[4:-1])
        if len(key_value) == 2:
            return "map", (key_value[0], key_value[1])
    if type_.startswith("struct<") and type_.endswith(">"):
        fields = []
        for part in _split_top_level(type_[7:-1]):
            if ":" in part:
                name, ftype = part.split(":", 1)
                fields.append((name.strip(), ftype.strip()))
        return "struct", fields
    return None


def _family(type_: str | None) -> str | None:
    """Type family used for compatibility checks between model inputs and feature view columns."""
    if type_ is None:
        return None
    if type_ in _INTEGER_TYPES or type_ == "bigint":
        return "integer"
    if type_ in _FLOAT_TYPES or type_.startswith("decimal"):
        return "float"
    if type_ in _STRING_TYPES or type_.startswith(("varchar", "char")):
        return "string"
    if type_ == "boolean":
        return "boolean"
    if type_ in _TIMESTAMP_TYPES or type_ in _DATE_TYPES:
        return "timestamp"
    if type_ == "binary":
        return "binary"
    complex_ = _parse_complex(type_)
    return complex_[0] if complex_ else None


def _check_value(type_: str | None, value: Any) -> str | None:
    """Return why `value` is not an acceptable JSON value for `type_`, or `None` when it is."""
    if type_ is None:
        return None
    if type_ in _INTEGER_TYPES:
        if isinstance(value, bool) or not isinstance(value, int):
            return f"must be an integer ({type_})"
        return None
    if type_ == "bigint":
        if isinstance(value, bool):
            return "must be an integer (bigint)"
        if isinstance(value, int):
            return None
        if isinstance(value, str) and _INTEGER_STRING.match(value):
            return None
        return "must be an integer or a decimal string (bigint)"
    if type_ in _FLOAT_TYPES:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return f"must be a number ({type_})"
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return "must be a finite number"
        return None
    if type_.startswith("decimal"):
        if isinstance(value, bool):
            return "must be a number (decimal)"
        if isinstance(value, (int, float)):
            return None
        if isinstance(value, str) and _DECIMAL_STRING.match(value):
            return None
        return "must be a number or a decimal string (decimal)"
    if type_ in _STRING_TYPES or type_.startswith(("varchar", "char")):
        return None if isinstance(value, str) else "must be a string"
    if type_ == "boolean":
        return None if isinstance(value, bool) else "must be a boolean"
    if type_ in _TIMESTAMP_TYPES:
        if isinstance(value, bool):
            return "must be an RFC 3339 string or epoch milliseconds (timestamp)"
        if isinstance(value, int):
            return None
        if (
            isinstance(value, str)
            and _RFC3339.match(value)
            and _parses_as_timestamp(value)
        ):
            return None
        return "must be an RFC 3339 string or epoch milliseconds (timestamp)"
    if type_ in _DATE_TYPES:
        if isinstance(value, bool):
            return "must be a YYYY-MM-DD string or days since epoch (date)"
        if isinstance(value, int):
            return None
        if isinstance(value, str) and _DATE.match(value) and _parses_as_date(value):
            return None
        return "must be a YYYY-MM-DD string or days since epoch (date)"
    if type_ == "binary":
        if isinstance(value, str) and _BASE64.match(value) and len(value) % 4 == 0:
            return None
        return "must be a base64 string (binary)"
    complex_ = _parse_complex(type_)
    if complex_ is None:
        return None
    kind, spec = complex_
    if kind == "array":
        if not isinstance(value, list):
            return f"must be an array ({type_})"
        for i, element in enumerate(value):
            if element is None:
                continue
            reason = _check_value(spec, element)
            if reason:
                return f"element {i} {reason}"
        return None
    if kind == "map":
        if not isinstance(value, dict):
            return f"must be an object ({type_})"
        for key, element in value.items():
            if element is None:
                continue
            reason = _check_value(spec[1], element)
            if reason:
                return f"value of '{key}' {reason}"
        return None
    if kind == "struct":
        if not isinstance(value, dict):
            return f"must be an object ({type_})"
        expected = {name for name, _ in spec}
        unknown = set(value) - expected
        if unknown:
            return f"has unknown fields {sorted(unknown)}"
        for name, ftype in spec:
            if name not in value:
                return f"is missing field '{name}'"
            if value[name] is None:
                continue
            reason = _check_value(ftype, value[name])
            if reason:
                return f"field '{name}' {reason}"
        return None
    return None


def _json_schema_for(field: SchemaField) -> dict[str, Any]:
    return _json_schema_for_type(field.type, field.nullable, field.description)


def _json_schema_for_type(
    type_: str | None, nullable: bool, description: str | None = None
) -> dict[str, Any]:
    schema = _json_schema_for_known_type(type_)
    if type_ is None:
        schema = {"x-hopsworks-unresolved": True}
    elif nullable:
        schema = _nullable(schema)
    if description:
        schema = {**schema, "description": description}
    return schema


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if "null" not in types:
            return {**schema, "type": [*types, "null"]}
        return schema
    if "oneOf" in schema:
        return {**schema, "oneOf": [*schema["oneOf"], {"type": "null"}]}
    return {"oneOf": [schema, {"type": "null"}]}


def _json_schema_for_known_type(type_: str | None) -> dict[str, Any]:
    if type_ is None:
        return {}
    if type_ in _INTEGER_TYPES:
        return {"type": "integer"}
    if type_ == "bigint":
        return {"type": ["integer", "string"], "pattern": "^-?[0-9]+$"}
    if type_ in _FLOAT_TYPES:
        return {"type": "number"}
    if type_.startswith("decimal"):
        return {"type": ["number", "string"], "pattern": "^-?[0-9]+(\\.[0-9]+)?$"}
    if type_ in _STRING_TYPES or type_.startswith(("varchar", "char")):
        return {"type": "string"}
    if type_ == "boolean":
        return {"type": "boolean"}
    if type_ in _TIMESTAMP_TYPES:
        return {
            "oneOf": [
                {"type": "string", "format": "date-time"},
                {"type": "integer", "description": "epoch milliseconds"},
            ]
        }
    if type_ in _DATE_TYPES:
        return {
            "oneOf": [
                {"type": "string", "format": "date"},
                {"type": "integer", "description": "days since epoch"},
            ]
        }
    if type_ == "binary":
        return {"type": "string", "contentEncoding": "base64"}
    complex_ = _parse_complex(type_)
    if complex_ is None:
        return {}
    kind, spec = complex_
    if kind == "array":
        return {"type": "array", "items": _nullable(_json_schema_for_known_type(spec))}
    if kind == "map":
        return {
            "type": "object",
            "additionalProperties": _nullable(_json_schema_for_known_type(spec[1])),
        }
    return {
        "type": "object",
        "properties": {
            name: _nullable(_json_schema_for_known_type(ftype)) for name, ftype in spec
        },
        "required": [name for name, _ in spec],
        "additionalProperties": False,
    }


# endregion

# region Encoding


def _encode_value(value: Any) -> Any:
    """Encode a Python value into the JSON form the wire contract accepts.

    Datetimes become RFC 3339 UTC strings (naive datetimes are treated as
    UTC), dates `YYYY-MM-DD`, bytes base64, decimals strings, numpy scalars
    and pandas timestamps their native equivalents. Containers are encoded
    recursively; everything else is returned unchanged.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return (
            value.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        )
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _encode_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode_value(v) for v in value]
    if hasattr(value, "isoformat") and hasattr(value, "tz_localize"):
        # pandas.Timestamp
        return _encode_value(value.to_pydatetime())
    if hasattr(value, "dtype") and getattr(value, "ndim", 0) > 0:
        # numpy array
        return _encode_value(value.tolist())
    if hasattr(value, "item") and hasattr(value, "dtype"):
        # numpy scalar or 0-d array
        return _encode_value(value.item())
    if hasattr(value, "tolist"):
        return _encode_value(value.tolist())
    return value


def _encode_instances(instances: Any) -> Any:
    """Encode every value of a batch of rows with `_encode_value`."""
    if not isinstance(instances, (list, tuple)):
        return _encode_value(instances)
    return [_encode_value(instance) for instance in instances]


# endregion

# region Inference


def _transformation_type_value(tf: Any) -> str:
    type_ = getattr(tf, "transformation_type", None)
    return getattr(type_, "value", type_) or ""


def _check_training_dataset_version(
    feature_view: Any, training_dataset_version: int | None, subject: str, hint: str
) -> None:
    """Raise `ValueError` when the view has statistics-dependent model-dependent transformations and no training dataset version.

    `subject` names what is being deployed for the message; `hint` says how
    to supply the version on this path.
    """
    if training_dataset_version:  # the backend answers 0 for "none"
        return
    needing = []
    for tf in getattr(feature_view, "transformation_functions", None) or []:
        if _transformation_type_value(tf) != "model_dependent":
            continue
        udf = tf.hopsworks_udf
        if getattr(udf, "statistics_required", False):
            needing.append(
                f"{udf.function_name}({', '.join(udf.transformation_features)})"
            )
    if needing:
        raise ValueError(
            f"Cannot deploy {subject}: the model-dependent transformations "
            f"{', '.join(needing)} need training dataset statistics, but no training "
            f"dataset version is available. {hint}"
        )


_UDF_KEYWORDS = {"statistics", "context"}

_ANNOTATION_TO_OFFLINE = {
    "int": "bigint",
    "float": "double",
    "str": "string",
    "bool": "boolean",
    "bytes": "binary",
    "datetime": "timestamp",
    "datetime.datetime": "timestamp",
    "pd.Timestamp": "timestamp",
    "pandas.Timestamp": "timestamp",
    "date": "date",
    "datetime.date": "date",
}


def _on_demand_transformation_functions(feature_view: Any) -> list[Any]:
    chain = getattr(feature_view, "_on_demand_transformation_functions", None)
    if chain:
        return list(chain)
    attached = []
    for feature in getattr(feature_view, "features", None) or []:
        tf = getattr(feature, "on_demand_transformation_function", None)
        if tf is not None and tf not in attached:
            attached.append(tf)
    return attached


def _udf_argument_types(udf: Any) -> dict[str, str]:
    """Offline types of a UDF's arguments from their annotations, keyed by the bound feature name.

    Only scalar annotations resolve; a pandas Series or anything else leaves
    the argument unresolved, as does a source that does not parse.
    """
    source = getattr(udf, "_function_source", None)
    bound = list(getattr(udf, "transformation_features", None) or [])
    if not isinstance(source, str) or not bound:
        return {}
    try:
        function = next(
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.FunctionDef)
        )
    except (SyntaxError, ValueError, StopIteration):
        return {}
    arguments = [
        arg
        for arg in (
            function.args.posonlyargs + function.args.args + function.args.kwonlyargs
        )
        if arg.arg not in _UDF_KEYWORDS
    ]
    types = {}
    for name, arg in zip(bound, arguments, strict=False):
        type_ = _annotation_type(arg.annotation)
        if type_ is not None:
            types[name] = type_
    return types


def _annotation_type(annotation: Any) -> str | None:
    if annotation is None:
        return None
    if isinstance(annotation, ast.Subscript) and ast.unparse(annotation.value) in (
        "Optional",
        "typing.Optional",
    ):
        return _annotation_type(annotation.slice)
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        if (
            isinstance(annotation.right, ast.Constant)
            and annotation.right.value is None
        ):
            return _annotation_type(annotation.left)
        if isinstance(annotation.left, ast.Constant) and annotation.left.value is None:
            return _annotation_type(annotation.right)
        return None
    return _ANNOTATION_TO_OFFLINE.get(ast.unparse(annotation))


def _feature_type(feature_view: Any, name: str) -> str | None:
    for feature in getattr(feature_view, "features", None) or []:
        if feature.name == name:
            return feature.type
    return None


def _serving_key_type(feature_view: Any, serving_key: Any) -> str | None:
    type_ = _feature_type(feature_view, serving_key.required_serving_key)
    if type_ is None:
        type_ = _feature_type(feature_view, serving_key.feature_name)
    fg = getattr(serving_key, "feature_group", None)
    if type_ is None:
        type_ = _feature_group_feature_type(fg, serving_key.feature_name)
    if type_ is None:
        # A serving key's feature group comes back as a stub without features
        # when the key is not among the selected features; the query carries
        # the full feature group.
        query = getattr(feature_view, "query", None)
        for candidate in getattr(query, "featuregroups", None) or []:
            same = fg is None or (
                getattr(candidate, "id", None) == getattr(fg, "id", None)
                and getattr(candidate, "name", None) == getattr(fg, "name", None)
            )
            if same:
                type_ = _feature_group_feature_type(candidate, serving_key.feature_name)
            if type_ is not None:
                break
    return type_


def _feature_group_feature_type(feature_group: Any, name: str) -> str | None:
    for feature in getattr(feature_group, "features", None) or []:
        if feature.name == name:
            return feature.type
    return None


def _output_columns(
    feature_view: Any, training_dataset_version: int | None
) -> list[dict]:
    """Columns of the transformed feature vector: training dataset schema minus labels and helper columns."""
    features = feature_view.get_training_dataset_schema(training_dataset_version)
    return [
        {"name": f.name, "type": _normalize_type(f.type)}
        for f in features
        if not getattr(f, "label", False)
        and not getattr(f, "inference_helper_column", False)
        and not getattr(f, "training_helper_column", False)
    ]


def _infer_deployment_schema(
    feature_view: Any,
    passed_features: list[str] | None = None,
    training_dataset_version: int | None = None,
    output_kind: str = OUTPUT_PREDICTIONS,
    output_columns: list[dict] | None = None,
) -> DeploymentSchema:
    """Infer the deployment schema for `feature_view` served by the default predictor.

    Serving keys and request parameters come from the view, passed features
    from the argument (each must be a non-label feature of the view), extra
    logging features from the view's logging configuration minus the reserved
    columns. Groups are sorted by name. For `output_kind="feature_vectors"`
    the output columns are the transformed training dataset schema unless
    given. When every stored feature is passed, nothing is looked up and the
    schema has no serving keys: the view only transforms.
    """
    feature_names = {f.name: f for f in (getattr(feature_view, "features", None) or [])}
    labels = set(getattr(feature_view, "labels", None) or [])

    looked_up = [
        name
        for name, f in feature_names.items()
        if name not in labels
        and not getattr(f, "label", False)
        and not getattr(f, "on_demand_transformation_function", None)
        and name not in set(passed_features or [])
    ]
    # A join key that is also a primary key of the joined feature group yields one
    # serving key per side: required on the left, and on the right the same name
    # again through `join_on`. Both name the single field a client sends, so they
    # collapse to one column. Keeping the required side makes the resolved type
    # independent of the order the backend returns the keys in.
    by_name: dict[str, Any] = {}
    for sk in getattr(feature_view, "serving_keys", None) or [] if looked_up else []:
        name = sk.required_serving_key
        if isinstance(name, list):
            continue
        if name not in by_name or (
            getattr(sk, "required", False)
            and not getattr(by_name[name], "required", False)
        ):
            by_name[name] = sk
    serving_keys = [
        SchemaField(name, _serving_key_type(feature_view, sk), nullable=False)
        for name, sk in by_name.items()
    ]

    passed = []
    for name in passed_features or []:
        if name not in feature_names:
            raise ValueError(
                f"Passed feature '{name}' is not a feature of feature view "
                f"'{feature_view.name}' v{feature_view.version}. Available features: "
                f"{', '.join(sorted(feature_names))}"
            )
        if name in labels or getattr(feature_names[name], "label", False):
            raise ValueError(
                f"Passed feature '{name}' is a label of feature view "
                f"'{feature_view.name}' v{feature_view.version} and cannot be sent by clients"
            )
        passed.append(SchemaField(name, feature_names[name].type))

    parameter_types: dict[str, str] = {}
    for tf in _on_demand_transformation_functions(feature_view):
        parameter_types.update(_udf_argument_types(getattr(tf, "hopsworks_udf", None)))
    request_parameters = [
        SchemaField(name, parameter_types.get(name))
        for name in (getattr(feature_view, "request_parameters", None) or [])
    ]

    extra = []
    if getattr(feature_view, "logging_enabled", False):
        logging = getattr(feature_view, "feature_logging", None)
        logged_types = _logging_feature_group_types(logging)
        for column in getattr(logging, "extra_logging_columns", None) or []:
            if column.name in RESERVED_LOGGING_COLUMNS:
                continue
            type_ = getattr(column, "type", None) or logged_types.get(column.name)
            extra.append(SchemaField(column.name, type_))

    if output_kind == OUTPUT_FEATURE_VECTORS and output_columns is None:
        output_columns = _output_columns(feature_view, training_dataset_version)

    return DeploymentSchema(
        serving_keys=sorted(serving_keys, key=lambda f: f.name),
        passed_features=sorted(passed, key=lambda f: f.name),
        request_parameters=sorted(request_parameters, key=lambda f: f.name),
        extra_logging_features=sorted(extra, key=lambda f: f.name),
        feature_view={"name": feature_view.name, "version": feature_view.version},
        training_dataset_version=training_dataset_version,
        output={"kind": output_kind, "columns": output_columns},
        inferred=True,
    )


_MODEL_TYPE_TO_OFFLINE = {
    "int8": "int",
    "int16": "int",
    "int32": "int",
    "int64": "bigint",
    "uint8": "int",
    "uint16": "int",
    "uint32": "bigint",
    "uint64": "bigint",
    "float16": "float",
    "float32": "float",
    "float64": "double",
    "bool": "boolean",
    "object": "string",
    "str": "string",
    "category": "string",
    "datetime64[ns]": "timestamp",
}


def _offline_type_for_model_type(model_type: Any) -> str | None:
    """The feature store type a model schema column type maps to, or None when unknown."""
    if model_type is None:
        return None
    text = str(model_type).lower()
    if _family(text) is not None:
        return text
    return _MODEL_TYPE_TO_OFFLINE.get(text)


def _infer_model_deployment_schema(
    model_name: str,
    passed_features: list[str] | None,
    model_input_columns: list[dict[str, Any]] | None = None,
    output_columns: list[dict[str, Any]] | None = None,
) -> DeploymentSchema:
    """Infer the schema of a model deployed without a feature view: its inputs are the passed features.

    Nothing is looked up or transformed, so the schema has no serving keys and
    no request parameters. `passed_features` names the model's input columns in
    the order the model expects and is required unless the model carries a
    columnar model schema, in which case that schema's columns are the default
    and supply the types; without one the types are unresolved.
    """
    legacy = {
        c["name"]: c.get("type") for c in (model_input_columns or []) if c.get("name")
    }
    if passed_features is None:
        if not legacy:
            raise ValueError(
                f"Model '{model_name}' has no feature view, so its input columns must "
                "be given: deploy(default_predictor=True, passed_features=[...]) in "
                "the order the model expects, or register it with "
                "create_model(feature_view=...)."
            )
        names = list(legacy)
    else:
        names = list(passed_features)
        if legacy and sorted(names) != sorted(legacy):
            raise ValueError(
                f"Model '{model_name}' has no feature view, so its passed features "
                f"must be exactly its input columns {list(legacy)}; got {names}."
            )
    if not names:
        raise ValueError(
            f"Model '{model_name}' has no feature view and no passed features."
        )
    passed = [
        SchemaField(name, _offline_type_for_model_type(legacy.get(name)))
        for name in names
    ]
    return DeploymentSchema(
        passed_features=passed,
        output={"kind": OUTPUT_PREDICTIONS, "columns": output_columns},
        inferred=True,
    )


def _check_schema_refinement(
    inferred: DeploymentSchema, given: DeploymentSchema
) -> None:
    """Raise `ValueError` unless `given` has exactly the field names of `inferred` in each group.

    The default predictor looks up exactly the inferred fields, so a manual
    schema may change types, nullability, descriptions, and order, but not
    membership.
    """
    for group in GROUPS:
        expected = {f.name for f in getattr(inferred, f"_{group}")}
        actual = {f.name for f in getattr(given, f"_{group}")}
        if expected != actual:
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            parts = []
            if missing:
                parts.append(f"missing {missing}")
            if extra:
                parts.append(f"unexpected {extra}")
            raise ValueError(
                f"The given schema's {group} differ from the feature view's: "
                + ", ".join(parts)
                + ". A manual schema may refine types and descriptions but must keep the same fields."
            )


# endregion
