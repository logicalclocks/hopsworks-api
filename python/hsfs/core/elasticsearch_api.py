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

import json
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from hopsworks_common.client.exceptions import FeatureStoreException


if TYPE_CHECKING:
    from collections.abc import Callable


class ElasticsearchApi:
    """Requests sent straight to an Elasticsearch cluster rather than to Hopsworks."""

    def _get_mapping(
        self,
        base_url: str,
        index: str,
        auth: tuple[str, str] | None,
        headers: dict[str, str],
        verify: bool | str,
        run: Callable[[Callable[[], Any], str], Any] | None = None,
    ) -> dict[str, Any]:
        """Return the `_mapping` response of `index`.

        `run` sends the request instead of this process, taking the request function and the Spark DDL type of its `(status, text)` result.
        """
        url = f"{base_url}/{quote(index, safe='*,:-_.')}/_mapping"
        request_headers = {"Accept": "application/json", **headers}

        def fetch():
            import requests

            response = requests.get(
                url,
                auth=auth,
                headers=request_headers,
                verify=verify,
                timeout=60,
            )
            return response.status_code, response.text

        status, text = run(fetch, "struct<status:int,text:string>") if run else fetch()
        if status >= 300:
            raise FeatureStoreException(
                f"Elasticsearch returned HTTP {status} for the mapping of '{index}': "
                f"{text[:500]}"
            )
        return json.loads(text)
