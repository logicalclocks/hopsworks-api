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

from urllib.parse import urlparse

import pytest
from hopsworks_common.client.istio.external import Client


class TestIstioExternalClient:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("public.hopsworks.ai", "https://public.hopsworks.ai/p/1"),
            ("", "https://test.hopsworks.ai:443/p/1"),
            (None, "https://test.hopsworks.ai:443/p/1"),
        ],
    )
    def test_replace_public_host(self, monkeypatch, value, expected):
        if value is None:
            monkeypatch.delenv("HOPSWORKS_PUBLIC_HOST", raising=False)
        else:
            monkeypatch.setenv("HOPSWORKS_PUBLIC_HOST", value)
        client = Client.__new__(Client)

        url = client._replace_public_host(urlparse("https://test.hopsworks.ai:443/p/1"))

        assert url.geturl() == expected
