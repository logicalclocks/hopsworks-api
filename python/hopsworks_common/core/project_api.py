#
#   Copyright 2020 Logical Clocks AB
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

from hopsworks_common import client, constants, project
from hopsworks_common.client.exceptions import RestAPIError


class ProjectApi:
    def _exists(self, name: str):
        """Check if a project exists.

        # Arguments
            name: Name of the project.
        # Returns
            `bool`: True if project exists, otherwise False
        """
        try:
            self._get_project(name)
            return True
        except RestAPIError:
            return False

    def _get_owned_projects(self):
        """Get all projects owned by the current user

        # Returns
            `List[Project]`: List of Project objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If unable to get the project teams
        """
        project_team_json = self._get_project_teams()
        projects = []
        if project_team_json:
            # This information can be retrieved calling the /users/profile endpoint but is avoided as that
            # requires an API key to have the USER scope which is not guaranteed on serverless
            # Until there is a better solution this code is used to get the current user_id to check project ownership
            current_user_uid = project_team_json[0]['user']['uid']
            for project_team in project_team_json:
                if project_team["project"]["owner"]["uid"] == current_user_uid:
                    projects.append(self._get_project(project_team["project"]["name"]))
        return projects


    def _get_project_teams(self):
        """Get all project teams for this user.

        # Returns
            `str`: List of Project teams
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If unable to get the project teams
        """
        _client = client.get_instance()
        path_params = [
            "project",
        ]
        return _client._send_request("GET", path_params)

    def _get_projects(self):
        """Get all projects accessible by the user.

        # Returns
            `List[Project]`: List of Project objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If unable to get the projects
        """
        project_team_json = self._get_project_teams()
        projects = []
        for project_team in project_team_json:
            projects.append(self._get_project(project_team["project"]["name"]))
        return projects

    def _get_project(self, name: str):
        """Get a project.

        # Arguments
            name: Name of the project.
        # Returns
            `Project`: The Project object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If unable to get the project
        """
        _client = client.get_instance()
        path_params = [
            "project",
            "getProjectInfo",
            name,
        ]
        project_json = _client._send_request("GET", path_params)
        return project.Project.from_response_json(project_json)

    def _create_project(
        self, name: str, description: str = None, feature_store_topic: str = None
    ):
        """Create a new project.

        # Arguments
            name: Name of the project.
            description: Description of the project.
            feature_store_topic: Feature store topic name.
        # Returns
            `Project`: The Project object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If unable to create the project
        """
        _client = client.get_instance()

        path_params = ["project"]
        query_params = {"projectName": name}
        headers = {"content-type": "application/json"}

        data = {
            "projectName": name,
            "services": constants.SERVICES.LIST,
            "description": description,
            "featureStoreTopic": feature_store_topic,
        }
        _client._send_request(
            "POST",
            path_params,
            headers=headers,
            query_params=query_params,
            data=json.dumps(data),
        )

        # The return of the project creation is not a ProjectDTO, so get the correct object after creation
        project = self._get_project(name)
        print("Project created successfully, explore it at " + project.get_url())
        return project

    def get_client(self):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "client",
        ]
        return _client._send_request("GET", path_params, stream=True)
