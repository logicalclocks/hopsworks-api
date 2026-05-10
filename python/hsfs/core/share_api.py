#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
"""REST wrappers for feature-store and feature-group sharing.

Both endpoints take the *target* project as an integer id, never a name —
the CLI/SDK callers usually only know the name, so this module does the
``getProjectInfo`` lookup once and converts ``PermissionError`` /
``RestAPIError`` from the backend into messages that name the source and
target projects explicitly. Both REST endpoints require Data Owner role
in the source project; a 403 is reported as a ``PermissionError``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import project_api


if TYPE_CHECKING:
    from collections.abc import Iterable


def _resolve_target_project_id(target_project: str | int) -> int:
    """Translate a project name to its int id, or pass an int id through.

    Parameters:
        target_project: Project name or numeric id.

    Returns:
        The numeric project id.

    Raises:
        ValueError: If ``target_project`` is empty.
        hopsworks.client.exceptions.RestAPIError: If the backend cannot
            resolve the project name (e.g. caller has no membership).
    """
    if not target_project and target_project != 0:
        raise ValueError("target_project must be a non-empty project name or id")
    if isinstance(target_project, int):
        return target_project
    proj = project_api.ProjectApi()._get_project(target_project)
    return proj.id


def _denied_msg(action: str, source_project: str, target_project: str | int) -> str:
    return (
        f"{action} from '{source_project}' to '{target_project}' requires the "
        f"Data Owner role in the source project '{source_project}'. Ask a data "
        f"owner of '{source_project}' to run this, or be granted that role."
    )


class ShareApi:
    """Thin REST wrapper for both feature-store-level and feature-group-level shares."""

    def __init__(self, feature_store_id: int) -> None:
        self._feature_store_id = feature_store_id

    def share_feature_store(self, target_project: str | int) -> None:
        """Share the entire feature store with another project.

        Parameters:
            target_project: Project name (preferred) or numeric id.

        Raises:
            PermissionError: If the caller lacks Data Owner in the source project.
        """
        target_id = _resolve_target_project_id(target_project)
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "share",
        ]
        try:
            _client._send_request(
                "POST", path_params, query_params={"project": target_id}
            )
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    _denied_msg(
                        "Sharing the feature store",
                        _client._project_name,
                        target_project,
                    )
                ) from e
            raise

    def unshare_feature_store(self, target_project: str | int) -> None:
        """Revoke a feature-store-level share with another project.

        Parameters:
            target_project: Project name or numeric id whose share is being revoked.
        """
        target_id = _resolve_target_project_id(target_project)
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "share",
        ]
        try:
            _client._send_request(
                "DELETE", path_params, query_params={"project": target_id}
            )
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    _denied_msg(
                        "Unsharing the feature store",
                        _client._project_name,
                        target_project,
                    )
                ) from e
            raise

    def share_feature_group(
        self,
        feature_group_id: int,
        target_project: str | int,
        features: Iterable[str] | None = None,
    ) -> None:
        """Share a single feature group with another project.

        Parameters:
            feature_group_id: Numeric feature-group id.
            target_project: Project name (preferred) or numeric id.
            features: Optional whitelist of feature names. When provided,
                only those columns are visible to the target project.
                When ``None`` (default), the full feature group is shared.
                Primary keys and the event-time column are always included
                by the backend regardless of this list.

        Raises:
            PermissionError: If the caller lacks Data Owner in the source project.
        """
        target_id = _resolve_target_project_id(target_project)
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "share",
            "featuregroups",
            feature_group_id,
        ]
        # JAX-RS @QueryParam Set<String> reads the same param key repeated
        # multiple times; requests serializes a list value the same way.
        query_params: dict[str, object] = {"project": target_id}
        if features:
            query_params["feature"] = list(features)
        try:
            _client._send_request("POST", path_params, query_params=query_params)
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    _denied_msg(
                        "Sharing the feature group",
                        _client._project_name,
                        target_project,
                    )
                ) from e
            raise

    def list_feature_store_shares(self) -> list[dict]:
        """List the projects this feature store has been shared with.

        Returns the ``items`` array from the backend's
        ``GET /featurestores/{id}/share`` response. Each entry has at
        least ``sharedWithProject`` (with ``name`` / ``id``),
        ``sharedBy``, ``sharedOn``, and ``sharedEntirely`` (true when
        the whole feature store was shared rather than an individual
        feature group).

        Returns:
            One dict per share, mirroring the backend payload above.

        Raises:
            PermissionError: If the caller lacks Data Owner in the source
                project.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "share",
        ]
        try:
            resp = _client._send_request("GET", path_params) or {}
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    "Listing feature-store shares requires the Data Owner "
                    f"role in project '{_client._project_name}'."
                ) from e
            raise
        # Empty/no-shares may come back as the bare DTO with no items field.
        return resp.get("items") or []

    def list_feature_group_shares(self, feature_group_id: int) -> list[dict]:
        """List the projects a single feature group has been shared with.

        Returns the ``items`` array from the backend's
        ``GET /featurestores/{id}/share/featuregroups/{fgId}`` response.
        Each entry includes ``sharedWithProject``, ``sharedBy``,
        ``sharedOn``, ``sharedEntirely`` (false when only specific
        columns were shared), and ``features`` (the column whitelist
        when not shared entirely; empty/null otherwise).

        Parameters:
            feature_group_id: Numeric feature-group id.

        Returns:
            One dict per share, mirroring the backend payload above.

        Raises:
            PermissionError: If the caller lacks Data Owner in the source
                project.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "share",
            "featuregroups",
            feature_group_id,
        ]
        try:
            resp = _client._send_request("GET", path_params) or {}
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    "Listing feature-group shares requires the Data Owner "
                    f"role in project '{_client._project_name}'."
                ) from e
            raise
        return resp.get("items") or []

    def unshare_feature_group(
        self, feature_group_id: int, target_project: str | int
    ) -> None:
        """Revoke a feature-group-level share with another project.

        Parameters:
            feature_group_id: Numeric feature-group id.
            target_project: Project name or numeric id whose share is being revoked.
        """
        target_id = _resolve_target_project_id(target_project)
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "share",
            "featuregroups",
            feature_group_id,
        ]
        try:
            _client._send_request(
                "DELETE", path_params, query_params={"project": target_id}
            )
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    _denied_msg(
                        "Unsharing the feature group",
                        _client._project_name,
                        target_project,
                    )
                ) from e
            raise
