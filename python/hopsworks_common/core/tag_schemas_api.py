#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#

"""REST wrapper for schematized-tag (tag-schema) management.

The ``/tags`` REST resource lives outside any project — it manages the
*global* registry of typed tag schemas a Hopsworks instance allows.
Create, list, get, and delete are all guarded by ``HOPS_ADMIN``: a
non-admin caller gets a 403 and we surface that as a ``PermissionError``
naming the platform-administrator requirement explicitly. Once a schema
is registered, members of any project can attach instances of it to
feature groups, feature views, or training datasets via
``hopsworks_common.core.tags_api.TagsApi`` — which is unchanged.
"""

from __future__ import annotations

import json
from typing import Any

from hopsworks_apigen import public
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError


_ADMIN_HINT = (
    "Tag-schema management requires the platform-administrator role "
    "(HOPS_ADMIN). Ask a platform admin to run this — Data Owner role "
    "in a single project is not enough."
)

_OLD_BACKEND_CASCADE = (
    "This backend predates reference-checked tag-schema deletion: it ignores "
    "force=False and deletes every attached value of the schema. Refusing the "
    "delete. Upgrade the cluster, or pass force=True if losing the attached "
    "values is intended."
)


def _carries_error_code(err: RestAPIError) -> bool:
    """Whether the failure is a Hopsworks error payload rather than an unrouted path.

    A backend that has the endpoint answers a missing schema with a JSON body carrying
    ``errorCode``; one that has never heard of the path 404s from the JAX-RS router with no such
    body. That difference is what separates "no schema by that name" from "server too old".
    """
    try:
        return "errorCode" in (err.response.json() or {})
    except Exception:  # noqa: BLE001 - any parse failure means no error payload
        return False


@public(
    "hopsworks.core.tag_schemas_api.TagSchemasApi",
    "hsfs.core.tag_schemas_api.TagSchemasApi",
)
class TagSchemasApi:
    """Manage the platform's typed tag-schema registry.

    All write operations on this class require the
    **platform-administrator** role (``HOPS_ADMIN``); read operations
    require any authenticated user.
    """

    @staticmethod
    def _path() -> list[Any]:
        return ["tags"]

    @public
    def list(self) -> dict[str, Any]:
        """List all registered tag schemas.

        Each schema item carries its lifecycle state: ``deprecated``,
        ``deprecatedOn``, and ``deprecatedBy``. A deprecated schema keeps
        its existing values but refuses new attachments.

        Returns:
            The raw schema-list payload from the backend.
        """
        _client = client._get_instance()
        return _client._send_request("GET", self._path())

    @public
    def get(self, name: str) -> dict[str, Any]:
        """Fetch a single tag schema by name.

        The payload carries the schema's lifecycle state: ``deprecated``,
        ``deprecatedOn``, and ``deprecatedBy``. A deprecated schema keeps
        its existing values but refuses new attachments.

        Parameters:
            name: Schema name.

        Returns:
            The raw schema payload from the backend.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the schema does
                not exist.
        """
        if not name:
            raise ValueError("name must be a non-empty schema name")
        _client = client._get_instance()
        return _client._send_request("GET", self._path() + [name])

    @public
    def usage(self, name: str) -> dict[str, Any]:
        """Report what still references a schema, and whether it can be deleted.

        Requires **HOPS_ADMIN**. Project-level Data Owner is **not**
        sufficient.

        Parameters:
            name: Schema name.

        Returns:
            The raw usage payload from the backend, with per-artifact-type
            reference counts and a bounded list of the referencing
            artifacts.

        Raises:
            PermissionError: If the caller lacks ``HOPS_ADMIN``.
            hopsworks.client.exceptions.RestAPIError: If the schema does
                not exist or the backend otherwise rejects the request.
        """
        if not name:
            raise ValueError("name must be a non-empty schema name")
        _client = client._get_instance()
        try:
            return _client._send_request("GET", self._path() + [name, "usage"])
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(_ADMIN_HINT) from e
            raise

    @public
    def deprecate(self, name: str) -> dict[str, Any]:
        """Deprecate a tag schema, refusing new attachments of it.

        Existing values keep working; only new attachments are refused.
        The backend refuses to deprecate a schema that is registered as a
        mandatory tag.

        Requires **HOPS_ADMIN**. Project-level Data Owner is **not**
        sufficient.

        Parameters:
            name: Schema name.

        Returns:
            The updated schema payload, with ``deprecated``,
            ``deprecatedOn``, and ``deprecatedBy`` set.

        Raises:
            PermissionError: If the caller lacks ``HOPS_ADMIN``.
            hopsworks.client.exceptions.RestAPIError: If the schema does
                not exist or is registered as mandatory.
        """
        if not name:
            raise ValueError("name must be a non-empty schema name")
        _client = client._get_instance()
        try:
            return _client._send_request("POST", self._path() + [name, "deprecation"])
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(_ADMIN_HINT) from e
            raise

    @public
    def restore(self, name: str) -> dict[str, Any]:
        """Undo a deprecation, letting the schema accept new attachments again.

        Requires **HOPS_ADMIN**. Project-level Data Owner is **not**
        sufficient.

        Parameters:
            name: Schema name.

        Returns:
            The updated schema payload, with ``deprecated`` cleared.

        Raises:
            PermissionError: If the caller lacks ``HOPS_ADMIN``.
            hopsworks.client.exceptions.RestAPIError: If the schema does
                not exist.
        """
        if not name:
            raise ValueError("name must be a non-empty schema name")
        _client = client._get_instance()
        try:
            return _client._send_request("DELETE", self._path() + [name, "deprecation"])
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(_ADMIN_HINT) from e
            raise

    @public
    def create(self, name: str, schema: dict | str) -> dict[str, Any]:
        """Register a new schematized tag.

        Tag schemas are JSON-Schema definitions that constrain the value
        clients can attach via ``TagsApi.add(...)``. A schema must be
        valid JSON Schema (``type``, ``properties``, etc.); the backend
        rejects malformed payloads.

        Requires **HOPS_ADMIN** (platform administrator). Project-level
        Data Owner is **not** sufficient.

        Example:
            ```python
            from hopsworks_common.core.tag_schemas_api import TagSchemasApi

            schema = {
                "type": "object",
                "properties": {
                    "owner": {"type": "string"},
                    "version": {"type": "integer"},
                },
                "required": ["owner"],
            }
            TagSchemasApi().create("ownership", schema)
            ```

        Parameters:
            name: Unique schema name (used later by clients to attach
                tag instances to entities).
            schema: JSON-Schema definition. May be a Python dict (the
                preferred form) or a JSON string already serialized.

        Returns:
            The created schema payload as returned by the backend.

        Raises:
            PermissionError: If the caller lacks ``HOPS_ADMIN``.
            ValueError: If ``name`` is empty or ``schema`` is not a dict
                or a JSON string.
            hopsworks.client.exceptions.RestAPIError: If the schema
                payload is rejected by the backend (invalid JSON Schema,
                duplicate name, etc.).
        """
        if not name:
            raise ValueError("name must be a non-empty schema name")
        if isinstance(schema, dict):
            body = json.dumps(schema)
        elif isinstance(schema, str):
            # Validate it parses as JSON before sending — a 400 from the
            # backend with a generic "invalid schema" is much harder to
            # debug than a local JSONDecodeError.
            json.loads(schema)
            body = schema
        else:
            raise ValueError("schema must be a dict or a JSON string")

        _client = client._get_instance()
        try:
            return _client._send_request(
                "POST",
                self._path(),
                query_params={"name": name},
                headers={"content-type": "application/json"},
                data=body,
            )
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(_ADMIN_HINT) from e
            raise

    @public
    def delete(self, name: str, force: bool = False) -> None:
        """Remove a tag schema from the registry.

        The backend refuses the delete while artifacts still carry a value
        of the schema; ``usage(name)`` reports what still references it.
        With ``force`` the referencing values are deleted too.

        Requires **HOPS_ADMIN**. Project-level Data Owner is **not**
        sufficient.

        Danger: Potentially dangerous operation
            ``force=True`` silently removes every attached value of this
            schema across all projects, leaving no trace of them.

        Parameters:
            name: Schema name.
            force: Also delete all attached values of the schema.

        Raises:
            PermissionError: If the caller lacks ``HOPS_ADMIN``.
            RuntimeError: If the backend is too old to refuse a referenced
                schema, since there the safe delete would cascade.
            hopsworks.client.exceptions.RestAPIError: If the schema does
                not exist, is still referenced (without ``force``), or the
                backend otherwise rejects the request.
        """
        if not name:
            raise ValueError("name must be a non-empty schema name")
        _client = client._get_instance()
        if not force:
            # An old backend ignores the unknown 'force' parameter and cascade-deletes every
            # attached value, so asking for the safe delete would get the destructive one, silently.
            # The usage endpoint shipped together with the refusal, so its absence dates the server.
            try:
                self.usage(name)
            except RestAPIError as e:
                status = getattr(e.response, "status_code", None)
                if status in (404, 405) and not _carries_error_code(e):
                    raise RuntimeError(_OLD_BACKEND_CASCADE) from e
                raise
        query_params = {"force": "true"} if force else None
        try:
            _client._send_request(
                "DELETE", self._path() + [name], query_params=query_params
            )
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(_ADMIN_HINT) from e
            raise
