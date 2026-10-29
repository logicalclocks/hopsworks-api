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

from typing import TYPE_CHECKING

from hopsworks_common import client
from hopsworks_common.decorators import _uses_openai


if TYPE_CHECKING:
    import openai


@_uses_openai
def _build_openai_client(url: str, /, **kwargs) -> openai.OpenAI:
    """Build an OpenAI client that authenticates against the serving endpoint.

    The token is sent both as the client API key and as an explicit `ApiKey` authorization header.
    The header keeps working with authenticators that only accept the `ApiKey` scheme.

    Parameters:
        url: OpenAI-compatible URL the client talks to.
        **kwargs: Passed to `openai.OpenAI`; `default_headers` are merged over the authorization header.
            Without an `http_client`, the client checks certificates as the Hopsworks connection does.

    Returns:
        The configured client.

    Raises:
        ValueError: If `base_url` or `api_key` is in `kwargs`.
    """
    for reserved in ("base_url", "api_key"):
        if reserved in kwargs:
            raise ValueError(
                f"'{reserved}' is set by Hopsworks and cannot be overridden."
            )

    import openai

    istio = client.istio._get_instance()
    token = istio._auth._token
    headers = {
        "Authorization": f"ApiKey {token}",
        **(kwargs.pop("default_headers", None) or {}),
    }
    # The connection's certificate checks (hostname_verification, trust_store_path) apply to
    # the client too, unless the caller brings its own transport.
    verify = istio._verify
    if verify is not True and "http_client" not in kwargs:
        # DefaultHttpxClient keeps the client's own connection defaults; openai releases before it have none.
        import httpx

        transport = getattr(openai, "DefaultHttpxClient", httpx.Client)
        kwargs["http_client"] = transport(verify=verify)
    return openai.OpenAI(base_url=url, api_key=token, default_headers=headers, **kwargs)
