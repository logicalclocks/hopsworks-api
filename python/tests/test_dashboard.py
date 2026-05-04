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
from unittest.mock import patch

import pytest
from hsfs.core.dashboard import Dashboard


class TestDashboard:
    def test_from_response_json_includes_type_and_bundle_path(self):
        """BUNDLE dashboards round-trip type and bundlePath through the JSON model."""
        payload = {
            "id": 7,
            "name": "sales-overview",
            "type": "BUNDLE",
            "bundlePath": "/Projects/demo/Resources/dashboards/sales-overview",
            "charts": [],
        }

        result = Dashboard.from_response_json(payload)

        assert result.id == 7
        assert result.name == "sales-overview"
        assert result.type == "BUNDLE"
        assert result.bundle_path == (
            "/Projects/demo/Resources/dashboards/sales-overview"
        )

    def test_from_response_json_grid_default(self):
        """GRID dashboards do not require type/bundle_path in the payload."""
        payload = {"id": 1, "name": "grid-demo", "charts": []}

        result = Dashboard.from_response_json(payload)

        assert result.type is None
        assert result.bundle_path is None

    def test_to_dict_omits_unset_bundle_fields(self):
        """A GRID dashboard's serialized payload must not carry empty bundle fields."""
        d = Dashboard(name="grid-demo", charts=[])

        out = d.to_dict()

        assert "type" not in out
        assert "bundlePath" not in out

    def test_to_dict_includes_type_when_set(self):
        """A BUNDLE dashboard serializes type; bundlePath is server-derived and may be unset."""
        d = Dashboard(name="b", type="BUNDLE")

        out = d.to_dict()

        assert out["type"] == "BUNDLE"


class TestCreateBundle:
    def test_rejects_invalid_name_with_slash(self, tmp_path):
        """Names that could escape the dashboards directory must fail before any upload."""
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)
        bundle = tmp_path / "dist"
        bundle.mkdir()
        (bundle / "index.html").write_text("ok")

        with pytest.raises(ValueError, match="invalid dashboard name"):
            fs.create_bundle(name="../etc/passwd", path=str(bundle))

    def test_rejects_invalid_name_with_dot(self, tmp_path):
        """Leading dots and dotted segments are rejected."""
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)
        bundle = tmp_path / "dist"
        bundle.mkdir()
        (bundle / "index.html").write_text("ok")

        with pytest.raises(ValueError, match="invalid dashboard name"):
            fs.create_bundle(name=".hidden", path=str(bundle))

    def test_rejects_missing_index_html(self, tmp_path):
        """A bundle directory without index.html fails before any upload."""
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)
        bundle = tmp_path / "no_index"
        bundle.mkdir()
        (bundle / "main.js").write_text("// no index")

        with pytest.raises(ValueError, match="index.html not found"):
            fs.create_bundle(name="x", path=str(bundle))

    def test_rejects_missing_directory(self, tmp_path):
        """A non-existent path fails fast with a clear message."""
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)

        with pytest.raises(ValueError, match="bundle directory not found"):
            fs.create_bundle(name="x", path=str(tmp_path / "missing"))

    def test_uploads_then_creates_bundle_dashboard(self, tmp_path):
        """Happy path: upload to Resources/dashboards/{name}, then POST a BUNDLE dashboard.

        The DTO does NOT carry bundlePath — the backend derives it from name.
        """
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)

        bundle = tmp_path / "dist"
        bundle.mkdir()
        (bundle / "index.html").write_text("<!doctype html><title>x</title>")

        captured: dict[str, object] = {}

        def fake_upload(self, local_path, upload_path, overwrite=False, **kwargs):
            captured["upload_path"] = upload_path
            captured["overwrite"] = overwrite
            captured["local_path"] = local_path

        def fake_create(self, dashboard):
            captured["sent"] = json.loads(dashboard.json())
            return Dashboard(
                id=99,
                name=dashboard.name,
                type=dashboard.type,
                bundle_path=f"/Projects/demo/Resources/dashboards/{dashboard.name}",
            )

        with (
            patch(
                "hopsworks_common.core.dataset_api.DatasetApi.upload",
                fake_upload,
            ),
            patch(
                "hsfs.core.dashboard_api.DashboardApi.create_dashboard",
                fake_create,
            ),
        ):
            result = fs.create_bundle(name="sales", path=str(bundle))

        assert captured["upload_path"] == "Resources/dashboards/sales"
        assert captured["overwrite"] is True
        assert captured["sent"]["type"] == "BUNDLE"
        # Client must not send bundlePath — the backend derives it.
        assert "bundlePath" not in captured["sent"]
        assert result.id == 99

    def test_cleans_up_upload_only_on_first_create_failure(self, tmp_path):
        """If create_dashboard fails on first registration, the SDK removes the bundle."""
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)

        bundle = tmp_path / "dist"
        bundle.mkdir()
        (bundle / "index.html").write_text("ok")

        removed: list[str] = []

        def fake_upload(self, local_path, upload_path, overwrite=False, **kwargs):
            pass

        def boom(self, dashboard):
            raise RuntimeError("backend rejected")

        def fake_remove(self, path):
            removed.append(path)

        def no_existing(self):
            return []

        with (
            patch(
                "hopsworks_common.core.dataset_api.DatasetApi.upload",
                fake_upload,
            ),
            patch(
                "hopsworks_common.core.dataset_api.DatasetApi.remove",
                fake_remove,
            ),
            patch(
                "hsfs.core.dashboard_api.DashboardApi.get_dashboards",
                no_existing,
            ),
            patch(
                "hsfs.core.dashboard_api.DashboardApi.create_dashboard",
                boom,
            ),
            pytest.raises(RuntimeError, match="backend rejected"),
        ):
            fs.create_bundle(name="will_fail", path=str(bundle))

        assert removed == ["Resources/dashboards/will_fail"]

    def test_returns_existing_dashboard_without_creating_duplicate(self, tmp_path):
        """If a BUNDLE dashboard with this name exists, return it; no second row, no remove."""
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)

        bundle = tmp_path / "dist"
        bundle.mkdir()
        (bundle / "index.html").write_text("v2 content")

        uploads: list[str] = []
        creates: list[Dashboard] = []
        removed: list[str] = []

        existing = Dashboard(
            id=42,
            name="rebuilt",
            type="BUNDLE",
            bundle_path="/Projects/demo/Resources/dashboards/rebuilt",
        )

        def fake_get_dashboards(self):
            return [existing]

        def fake_upload(self, local_path, upload_path, overwrite=False, **kwargs):
            uploads.append(upload_path)

        def fake_create(self, dashboard):
            creates.append(dashboard)
            return dashboard

        def fake_remove(self, path):
            removed.append(path)

        with (
            patch(
                "hsfs.core.dashboard_api.DashboardApi.get_dashboards",
                fake_get_dashboards,
            ),
            patch(
                "hopsworks_common.core.dataset_api.DatasetApi.upload",
                fake_upload,
            ),
            patch(
                "hopsworks_common.core.dataset_api.DatasetApi.remove",
                fake_remove,
            ),
            patch(
                "hsfs.core.dashboard_api.DashboardApi.create_dashboard",
                fake_create,
            ),
        ):
            result = fs.create_bundle(name="rebuilt", path=str(bundle))

        assert result is existing
        assert uploads == ["Resources/dashboards/rebuilt"]
        # Critical: we must not create a duplicate row, and we must not delete
        # the pre-existing bundle if we somehow hit the cleanup path.
        assert creates == []
        assert removed == []

    def test_existing_grid_dashboard_with_same_name_does_not_block_bundle(
        self, tmp_path
    ):
        """A GRID dashboard with the same name is not the same record — create the BUNDLE."""
        from hsfs.feature_store import FeatureStore

        fs = FeatureStore.__new__(FeatureStore)

        bundle = tmp_path / "dist"
        bundle.mkdir()
        (bundle / "index.html").write_text("ok")

        creates: list[Dashboard] = []

        # A GRID with the same name (a quirk of the existing dashboard model)
        # is NOT the same record. Only an existing BUNDLE should short-circuit.
        grid_with_same_name = Dashboard(id=1, name="dual", type="GRID", charts=[])

        def fake_get_dashboards(self):
            return [grid_with_same_name]

        def fake_upload(self, local_path, upload_path, overwrite=False, **kwargs):
            pass

        def fake_create(self, dashboard):
            creates.append(dashboard)
            return Dashboard(id=2, name=dashboard.name, type="BUNDLE")

        with (
            patch(
                "hsfs.core.dashboard_api.DashboardApi.get_dashboards",
                fake_get_dashboards,
            ),
            patch(
                "hopsworks_common.core.dataset_api.DatasetApi.upload",
                fake_upload,
            ),
            patch(
                "hsfs.core.dashboard_api.DashboardApi.create_dashboard",
                fake_create,
            ),
        ):
            result = fs.create_bundle(name="dual", path=str(bundle))

        assert result.id == 2
        assert len(creates) == 1
        assert creates[0].type == "BUNDLE"
