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

import pytest
from hopsworks_common.client import hopsworks as internal_client
from hopsworks_common.client.istio import hopsworks as istio_internal_client


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in (
        "HOPSWORKS_PROJECT_BACKING_NAME",
        "HADOOP_USER_NAME",
        "HDFS_USER",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(params=[internal_client.Client, istio_internal_client.Client])
def client_cls(request):
    return request.param


def _bare(cls, project_name="display"):
    c = cls.__new__(cls)
    c._project_name = project_name
    return c


class TestInternalBackingName:
    def test_env_var_wins(self, client_cls, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_PROJECT_BACKING_NAME", "backing1")
        monkeypatch.setenv("HADOOP_USER_NAME", "other__meb10000")

        assert _bare(client_cls)._backing_name() == "backing1"

    def test_derived_from_hadoop_user_name(self, client_cls, monkeypatch):
        monkeypatch.setenv("HADOOP_USER_NAME", "proj__meb10000")

        assert _bare(client_cls)._backing_name() == "proj"

    def test_derived_from_hdfs_user(self, client_cls, monkeypatch):
        monkeypatch.setenv("HDFS_USER", "proj__meb10000")

        assert _bare(client_cls)._backing_name() == "proj"

    def test_falls_back_to_project_name(self, client_cls):
        assert _bare(client_cls, "display")._backing_name() == "display"
