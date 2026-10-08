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

from typing import Any
from urllib.parse import quote

from hopsworks_common.client.exceptions import FeatureStoreException


class ElasticsearchApi:
    """Requests sent straight to an Elasticsearch cluster rather than to Hopsworks."""

    def _get_mapping(
        self,
        base_url: str,
        index: str,
        auth: tuple[str, str] | None,
        headers: dict[str, str],
        verify: bool | str,
    ) -> dict[str, Any]:
        import requests

        response = requests.get(
            f"{base_url}/{quote(index, safe='*,:-_.')}/_mapping",
            auth=auth,
            headers={"Accept": "application/json", **headers},
            verify=verify,
            timeout=60,
        )
        if response.status_code >= 300:
            raise FeatureStoreException(
                f"Elasticsearch returned HTTP {response.status_code} for the mapping of '{index}': "
                f"{response.text[:500]}"
            )
        return response.json()
