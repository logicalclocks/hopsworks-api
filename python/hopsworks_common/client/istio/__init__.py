#
#   Copyright 2022 Logical Clocks AB
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

from typing import Union

import hopsworks_common.client as _main
from hopsworks_common.client.istio import external, hopsworks


_client: Union[hopsworks.Client, external.Client, None] = None


def init(host, port, project=None, api_key_value=None, scheme="http"):
    global _client

    if _client:
        return
    if not _main._is_external():
        _client = hopsworks.Client(host, port)
    else:
        _client = external.Client(host, port, project, api_key_value, scheme=scheme)


def get_instance() -> Union[hopsworks.Client, external.Client, None]:
    global _client
    return _client
