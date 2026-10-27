"""The client package stays light until serving is used.

Lazy istio import, serving defaults on first read, single project and version
lookups.
"""

from __future__ import annotations

import subprocess
import sys
from unittest import mock

import hopsworks_common.client as client
import pytest
from hopsworks_common import connection as connection_mod
from hopsworks_common.client import external
from hopsworks_common.connection import Connection


def test_importing_hopsworks_does_not_import_istio_or_hsfs():
    code = (
        "import sys, hopsworks; "
        "print('hopsworks_common.client.istio' in sys.modules, 'hsfs' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert out.split() == ["False", "False"]


def test_istio_attribute_resolves_lazily():
    assert client.istio.__name__ == "hopsworks_common.client.istio"


def test_stop_without_istio_loaded(monkeypatch):
    monkeypatch.setattr(client, "_client", mock.MagicMock())
    monkeypatch.setitem(sys.modules, "hopsworks_common.client.istio", None)
    client._stop()  # must not import or touch istio
    assert client._client is None


def test_stop_forgets_serving_defaults(monkeypatch):
    monkeypatch.setattr(client, "_client", mock.MagicMock())
    monkeypatch.setattr(client, "_kserve_installed", True)
    monkeypatch.setattr(client, "_serving_num_instances_limits", [0, 4])
    monkeypatch.setattr(client, "_knative_domain", "old.example")
    client._stop()
    assert (
        client._kserve_installed,
        client._serving_num_instances_limits,
        client._knative_domain,
    ) == (None, None, None)


def test_serving_getters_load_defaults_on_first_read(monkeypatch):
    conn = mock.MagicMock()
    monkeypatch.setattr(client, "_connection", conn)
    monkeypatch.setattr(client, "_kserve_installed", None)
    monkeypatch.setattr(client, "_serving_num_instances_limits", None)
    monkeypatch.setattr(client, "_knative_domain", None)

    def load():
        client._set_kserve_installed(True)
        client._set_serving_num_instances_limits([1, 2])
        client._set_knative_domain("example.com")

    conn._load_serving_defaults.side_effect = load
    assert client._is_kserve_installed() is True
    assert client._get_serving_num_instances_limits() == [1, 2]
    assert client._get_knative_domain() == "example.com"
    conn._load_serving_defaults.assert_called_once()


def test_istio_instance_triggers_defaults(monkeypatch):
    conn = mock.MagicMock()
    monkeypatch.setattr(client, "_connection", conn)
    monkeypatch.setattr(client.istio, "_client", None)
    assert client.istio._get_instance() is None
    conn._load_serving_defaults.assert_called_once()


def test_external_provide_project_reuses_known_id(monkeypatch):
    # _provide_project ends by handing over to the connection; stub that hop.
    monkeypatch.setattr(client, "_get_connection", lambda: mock.MagicMock())
    c = external.Client.__new__(external.Client)
    c._engine = "none"
    c._get_project_info = mock.MagicMock()
    c._get_username = mock.MagicMock(return_value="meb")
    c._provide_project("proj", project_id=119)
    assert (c._project_name, c._project_id, c._username) == ("proj", "119", "meb")
    c._get_project_info.assert_not_called()
    c._provide_project("proj")
    c._get_project_info.assert_called_once_with("proj")


def test_check_compatibility_uses_cached_version():
    conn = Connection.__new__(Connection)
    conn._connected = True
    conn._backend_version = connection_mod.version.__version__
    conn._variable_api = mock.MagicMock()
    conn._check_compatibility()
    conn._variable_api._get_version.assert_not_called()


def test_engine_type_known_before_engine_is_built(monkeypatch):
    import hopsworks_common.connection as connection
    from hsfs import engine

    monkeypatch.setattr(engine, "_engine", None)
    monkeypatch.setattr(connection, "_hsfs_engine_type", "python")
    assert engine._get_type() == "python"
    monkeypatch.setattr(connection, "_hsfs_engine_type", None)
    with pytest.raises(Exception, match="Couldn't find execution engine"):
        engine._get_type()
    # An engine object built by hsfs.engine._init also fixes the type.
    monkeypatch.setattr(engine, "_engine", mock.MagicMock())
    monkeypatch.setattr(engine, "_engine_type", "spark")
    assert engine._get_type() == "spark"


def test_close_forgets_engine_type(monkeypatch):
    import hopsworks_common.connection as connection

    conn = Connection.__new__(Connection)
    conn._connected = True
    conn._feature_store_api_cache = None
    conn._model_registry_api_cache = None
    conn._model_serving_api_cache = None
    monkeypatch.setattr(connection, "_hsfs_engine_type", "python")
    monkeypatch.setattr(connection.client, "_stop", lambda: None)
    monkeypatch.delitem(sys.modules, "hsfs.engine", raising=False)
    conn._close()
    assert connection._hsfs_engine_type is None
    assert conn._connected is False


def test_set_active_project_passes_id(monkeypatch):
    import hopsworks

    hw_client = mock.MagicMock()
    hw_client._is_external.return_value = True
    monkeypatch.setattr(hopsworks.client, "_get_instance", lambda: hw_client)
    project = mock.MagicMock()
    project.name = "proj"
    project.id = 119
    hopsworks._set_active_project(project)
    hw_client._provide_project.assert_called_once_with("proj", project_id=119)
