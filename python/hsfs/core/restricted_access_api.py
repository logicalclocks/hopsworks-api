#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
"""REST wrapper for granting `Feature store restricted` project members access to a feature group.

Unlike cross-project sharing (see `share_api.py`), this grants access to an
individual member *within the same project* who otherwise has no feature
store access at all under the `Feature store restricted` role. The target
user must already hold that role in the project; the backend rejects the
grant otherwise. Requires Data Owner role in the project; a 403 is reported
as a `PermissionError`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError


if TYPE_CHECKING:
    from collections.abc import Iterable


class RestrictedAccessApi:
    """Thin REST wrapper for restricted-access grants on a single feature group."""

    def __init__(self, feature_store_id: int) -> None:
        self._feature_store_id = feature_store_id

    def _grant_restricted_access(
        self,
        feature_group_id: int,
        user_email: str,
        features: Iterable[str] | None = None,
    ) -> None:
        """Grant a restricted project member access to a feature group.

        Parameters:
            feature_group_id: Numeric feature-group id.
            user_email: Email of the project member to grant access to. The
                member must already hold the `Feature store restricted` role.
            features: Optional whitelist of feature names. When provided,
                the member can only read those columns. When `None`
                (default), the member can read the whole feature group.
                Primary keys and the event-time column are always included
                by the backend regardless of this list.

        Raises:
            PermissionError: If the caller lacks Data Owner in the project.
            hopsworks.client.exceptions.RestAPIError: If the target user
                doesn't exist, doesn't hold the `Feature store restricted`
                role, or already has restricted access to this feature group.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "restrictedaccess",
        ]
        # JAX-RS @QueryParam Set<String> reads the same param key repeated
        # multiple times; requests serializes a list value the same way.
        query_params: dict[str, object] = {"user": user_email}
        if features:
            query_params["feature"] = list(features)
        try:
            _client._send_request("POST", path_params, query_params=query_params)
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    "Granting restricted feature group access requires the "
                    f"Data Owner role in project '{_client._project_name}'."
                ) from e
            raise

    def _revoke_restricted_access(self, feature_group_id: int, user_email: str) -> None:
        """Revoke a previously-granted restricted-access grant.

        Parameters:
            feature_group_id: Numeric feature-group id.
            user_email: Email of the project member whose access is revoked.

        Raises:
            PermissionError: If the caller lacks Data Owner in the project.
            hopsworks.client.exceptions.RestAPIError: If the user doesn't
                exist or has no restricted-access grant on this feature group.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "restrictedaccess",
        ]
        try:
            _client._send_request(
                "DELETE", path_params, query_params={"user": user_email}
            )
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    "Revoking restricted feature group access requires the "
                    f"Data Owner role in project '{_client._project_name}'."
                ) from e
            raise

    def _get_restricted_access(self, feature_group_id: int) -> list[dict]:
        """List the restricted members granted access to a feature group.

        Returns the `items` array from the backend's
        `GET .../featuregroups/{fgId}/restrictedaccess` response. Each entry
        has `grantedToUser`, `grantedBy`, `grantedOn`, `grantedEntirely`
        (`False` when only specific columns were granted), and `features`
        (the column whitelist when not granted entirely; empty/null otherwise).

        Parameters:
            feature_group_id: Numeric feature-group id.

        Returns:
            One dict per grant, mirroring the backend payload above.

        Raises:
            PermissionError: If the caller lacks Data Owner in the project.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "restrictedaccess",
        ]
        try:
            resp = _client._send_request("GET", path_params) or {}
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 403:
                raise PermissionError(
                    "Listing restricted feature group access requires the "
                    f"Data Owner role in project '{_client._project_name}'."
                ) from e
            raise
        return resp.get("items") or []
