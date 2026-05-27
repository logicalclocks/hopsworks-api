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
"""Superset API for managing datasets, charts, and dashboards.

This module provides a Python client that talks directly to Superset's
REST API v1 from within Hopsworks (notebooks, jobs, terminals).
Authentication is handled automatically via the Hopsworks Superset login endpoint.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import requests
from hopsworks_apigen import public
from hopsworks_common import client, usage
from hopsworks_common.core.variable_api import VariableApi


if TYPE_CHECKING:
    from hopsworks_common import project


SUPERSET_SERVICE_NAME = "app.superset.service"
SUPERSET_PORT = 8088
SUPERSET_BASE_PATH = "/hopsworks-api/superset"


@public("hopsworks.core.superset_api.SupersetApi")
class SupersetApi:
    """API for managing Superset datasets, charts, and dashboards.

    This class provides methods to create, read, update, and delete
    Superset resources directly from within Hopsworks.
    Authentication is handled automatically using the project user's
    Superset credentials stored in Hopsworks secrets.

    Example:
        ```python
        import hopsworks
        import json

        project = hopsworks.login()
        superset = project.get_superset_api()

        # 1. List existing datasets in Superset
        datasets = superset.list_datasets()

        # 2. Create a dataset backed by an existing database table
        dataset = superset.create_dataset(
            database_id=1,                       # ID of the Superset database connection
            table_name="transactions_1",         # table in the online feature store
            schema="myproject_onlinedb",         # schema (project's online DB name)
        )
        dataset_id = dataset["id"]

        # 3. Create a chart that visualises the dataset
        chart = superset.create_chart(
            slice_name="Transactions over time",
            viz_type="echarts_timeseries_line",  # Superset viz type
            datasource_id=dataset_id,
            params=json.dumps({                  # chart config as a JSON string
                "metrics": [{"label": "count", "expressionType": "SQL", "sqlExpression": "COUNT(*)"}],
                "groupby": [],
                "time_column": "event_time",
                "time_grain_sqla": "P1D",
                "row_limit": 1000,
            }),
        )
        chart_id = chart["id"]

        # 4. Create a dashboard and add the chart to it
        dashboard = superset.create_dashboard(
            dashboard_title="Feature Monitoring",
            published=True,
        )
        dashboard_id = dashboard["id"]

        # Add the chart to the dashboard by updating the chart
        superset.update_chart(chart_id, dashboards=[dashboard_id])

        # 5. Clean up
        superset.delete_chart(chart_id)
        superset.delete_dataset(dataset_id)
        superset.delete_dashboard(dashboard_id)
        ```
    """

    def __init__(self, project: project.Project | None = None):
        self._variable_api: VariableApi = VariableApi()
        self._service_discovery_domain = (
            self._variable_api.get_service_discovery_domain()
        )
        if project is not None:
            self._project_id = project.id
        else:
            _client = client.get_instance()
            self._project_id = _client._project_id
        self._session_token: str | None = None
        self._csrf_token: str | None = None
        self._http = requests.Session()

    def _get_superset_url(self) -> str:
        if client._is_external():
            raise RuntimeError(
                "SupersetApi is only available from within the Hopsworks cluster. "
                "Connecting from an external client is not supported."
            )
        if not self._service_discovery_domain:
            raise RuntimeError(
                "Service discovery domain is not configured. "
                "Superset cannot be reached because "
                "'VariableApi.get_service_discovery_domain()' "
                "returned an empty value."
            )
        return f"http://{SUPERSET_SERVICE_NAME}.{self._service_discovery_domain}:{SUPERSET_PORT}"

    def _get_session_token(self) -> str:
        _client = client.get_instance()
        path_params = ["project", self._project_id, "superset", "login"]
        resp = _client._send_request("GET", path_params)
        self._session_token = resp["accessToken"]
        return self._session_token

    def _get_csrf_token(self) -> str:
        url = (
            self._get_superset_url()
            + SUPERSET_BASE_PATH
            + "/api/v1/security/csrf_token/"
        )
        headers = {
            "Content-Type": "application/json",
            "Cookie": f"session={self._session_token}",
        }
        resp = self._http.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        # Update session cookie if the server rotated it during CSRF token generation
        if "session" in resp.cookies:
            self._session_token = resp.cookies["session"]
        self._csrf_token = resp.json()["result"]
        return self._csrf_token

    def _request(
        self,
        method: str,
        path: str,
        json_data: dict | None = None,
        params: dict | None = None,
    ) -> dict:
        if not self._session_token:
            self._get_session_token()

        needs_csrf = method.upper() in ("POST", "PUT", "DELETE")
        if needs_csrf and not self._csrf_token:
            self._get_csrf_token()

        url = self._get_superset_url() + SUPERSET_BASE_PATH + path
        headers = {
            "Content-Type": "application/json",
            "Cookie": f"session={self._session_token}",
        }
        if needs_csrf:
            headers["X-CSRFToken"] = self._csrf_token

        resp = self._http.request(
            method, url, json=json_data, headers=headers, params=params, timeout=60
        )

        if resp.status_code == 401:
            self._get_session_token()
            self._csrf_token = None
            headers["Cookie"] = f"session={self._session_token}"
            if needs_csrf:
                self._get_csrf_token()
                headers["X-CSRFToken"] = self._csrf_token
            resp = self._http.request(
                method, url, json=json_data, headers=headers, params=params, timeout=60
            )

        resp.raise_for_status()
        if resp.status_code == 204:
            return {}
        return resp.json()

    # ------------------------------------------------------------------ #
    #  Databases                                                          #
    # ------------------------------------------------------------------ #

    @public
    @usage.method_logger
    def list_databases(self) -> dict:
        """List all Superset database connections visible to the current user.

        Returns:
            A dict containing the list of databases under the "result" key.
        """
        return self._request("GET", "/api/v1/database/")

    # ------------------------------------------------------------------ #
    #  Datasets                                                           #
    # ------------------------------------------------------------------ #

    @public
    @usage.method_logger
    def create_dataset(
        self,
        database_id: int,
        table_name: str,
        schema: str | None = None,
        sql: str | None = None,
        description: str | None = None,
        owners: list[int] | None = None,
    ) -> dict:
        """Create a Superset dataset.

        Parameters:
            database_id: ID of the Superset database connection.
            table_name: Name of the table (or virtual dataset).
            schema: Database schema name.
            sql: SQL expression for virtual datasets.
            description: Optional description.
            owners: Optional list of Superset user IDs as owners.

        Returns:
            The created dataset as a dict (Superset API response).
        """
        payload: dict[str, Any] = {
            "database": database_id,
            "table_name": table_name,
        }
        if schema is not None:
            payload["schema"] = schema
        if sql is not None:
            payload["sql"] = sql
        if description is not None:
            payload["description"] = description
        if owners is not None:
            payload["owners"] = owners
        return self._request("POST", "/api/v1/dataset/", json_data=payload)

    @public
    @usage.method_logger
    def get_dataset(self, dataset_id: int) -> dict:
        """Get a Superset dataset by ID.

        Parameters:
            dataset_id: The dataset ID.

        Returns:
            The dataset as a dict.
        """
        return self._request("GET", f"/api/v1/dataset/{dataset_id}")

    @public
    @usage.method_logger
    def list_datasets(self) -> dict:
        """List all Superset datasets visible to the current user.

        Returns:
            A dict containing the list of datasets under the "result" key.
        """
        return self._request("GET", "/api/v1/dataset/")

    @public
    @usage.method_logger
    def update_dataset(self, dataset_id: int, **kwargs: Any) -> dict:
        """Update a Superset dataset.

        Pass fields to update as keyword arguments (e.g., description, sql, table_name, owners).

        Parameters:
            dataset_id: The dataset ID.

        Returns:
            The updated dataset as a dict.
        """
        return self._request("PUT", f"/api/v1/dataset/{dataset_id}", json_data=kwargs)

    @public
    @usage.method_logger
    def delete_dataset(self, dataset_id: int) -> dict:
        """Delete a Superset dataset.

        Parameters:
            dataset_id: The dataset ID.

        Returns:
            Empty dict on success.
        """
        return self._request("DELETE", f"/api/v1/dataset/{dataset_id}")

    # ------------------------------------------------------------------ #
    #  Charts                                                             #
    # ------------------------------------------------------------------ #

    @public
    @usage.method_logger
    def create_chart(
        self,
        slice_name: str,
        viz_type: str,
        datasource_id: int,
        params: str,
        datasource_type: str = "table",
        description: str | None = None,
        dashboards: list[int] | None = None,
        owners: list[int] | None = None,
    ) -> dict:
        """Create a Superset chart.

        Parameters:
            slice_name: Display name of the chart.
            viz_type: Visualization type (e.g., "table", "big_number", "echarts_timeseries").
            datasource_id: ID of the dataset to use as data source.
            params: JSON string of chart configuration parameters.
            datasource_type: Type of datasource (default "table").
            description: Optional description.
            dashboards: Optional list of dashboard IDs to add this chart to.
            owners: Optional list of Superset user IDs as owners.

        Returns:
            The created chart as a dict.
        """
        payload: dict[str, Any] = {
            "slice_name": slice_name,
            "viz_type": viz_type,
            "datasource_id": datasource_id,
            "datasource_type": datasource_type,
            "params": params,
        }
        if description is not None:
            payload["description"] = description
        if dashboards is not None:
            payload["dashboards"] = dashboards
        if owners is not None:
            payload["owners"] = owners
        return self._request("POST", "/api/v1/chart/", json_data=payload)

    @public
    @usage.method_logger
    def get_chart(self, chart_id: int) -> dict:
        """Get a Superset chart by ID.

        Parameters:
            chart_id: The chart ID.

        Returns:
            The chart as a dict.
        """
        return self._request("GET", f"/api/v1/chart/{chart_id}")

    @public
    @usage.method_logger
    def list_charts(self) -> dict:
        """List all Superset charts visible to the current user.

        Returns:
            A dict containing the list of charts under the "result" key.
        """
        return self._request("GET", "/api/v1/chart/")

    @public
    @usage.method_logger
    def update_chart(self, chart_id: int, **kwargs: Any) -> dict:
        """Update a Superset chart.

        Pass fields to update as keyword arguments (e.g., slice_name, viz_type, params, owners).

        Parameters:
            chart_id: The chart ID.

        Returns:
            The updated chart as a dict.
        """
        return self._request("PUT", f"/api/v1/chart/{chart_id}", json_data=kwargs)

    @public
    @usage.method_logger
    def delete_chart(self, chart_id: int) -> dict:
        """Delete a Superset chart.

        Parameters:
            chart_id: The chart ID.

        Returns:
            Empty dict on success.
        """
        return self._request("DELETE", f"/api/v1/chart/{chart_id}")

    # ------------------------------------------------------------------ #
    #  Dashboards                                                         #
    # ------------------------------------------------------------------ #

    @public
    @usage.method_logger
    def create_dashboard(
        self,
        dashboard_title: str,
        published: bool = False,
        slug: str | None = None,
        position_json: str | None = None,
        json_metadata: str | None = None,
        css: str | None = None,
        owners: list[int] | None = None,
    ) -> dict:
        """Create a Superset dashboard.

        Parameters:
            dashboard_title: Display title of the dashboard.
            published: Whether the dashboard is published.
            slug: Optional URL slug.
            position_json: Optional JSON string defining the layout.
            json_metadata: Optional JSON string with dashboard metadata.
            css: Optional custom CSS.
            owners: Optional list of Superset user IDs as owners.

        Returns:
            The created dashboard as a dict.
        """
        payload: dict[str, Any] = {
            "dashboard_title": dashboard_title,
            "published": published,
        }
        if slug is not None:
            payload["slug"] = slug
        if position_json is not None:
            payload["position_json"] = position_json
        if json_metadata is not None:
            payload["json_metadata"] = json_metadata
        if css is not None:
            payload["css"] = css
        if owners is not None:
            payload["owners"] = owners
        return self._request("POST", "/api/v1/dashboard/", json_data=payload)

    @public
    @usage.method_logger
    def get_dashboard(self, dashboard_id: int) -> dict:
        """Get a Superset dashboard by ID.

        Parameters:
            dashboard_id: The dashboard ID.

        Returns:
            The dashboard as a dict.
        """
        return self._request("GET", f"/api/v1/dashboard/{dashboard_id}")

    @public
    @usage.method_logger
    def list_dashboards(self) -> dict:
        """List all Superset dashboards visible to the current user.

        Returns:
            A dict containing the list of dashboards under the "result" key.
        """
        return self._request("GET", "/api/v1/dashboard/")

    @public
    @usage.method_logger
    def update_dashboard(self, dashboard_id: int, **kwargs: Any) -> dict:
        """Update a Superset dashboard.

        Pass fields to update as keyword arguments (e.g., dashboard_title, published, position_json, json_metadata, css, owners).

        Parameters:
            dashboard_id: The dashboard ID.

        Returns:
            The updated dashboard as a dict.
        """
        return self._request(
            "PUT", f"/api/v1/dashboard/{dashboard_id}", json_data=kwargs
        )

    @public
    @usage.method_logger
    def delete_dashboard(self, dashboard_id: int) -> dict:
        """Delete a Superset dashboard.

        Parameters:
            dashboard_id: The dashboard ID.

        Returns:
            Empty dict on success.
        """
        return self._request("DELETE", f"/api/v1/dashboard/{dashboard_id}")
