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

from hopsworks_common.project import Project


class TestProject:
    def test_home_path_external_client(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        mock_client.return_value._username = "alice"

        project = Project(project_name="my_project")

        assert project.home_path == "/Projects/my_project/Users/alice"

    def test_home_path_internal_client(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        # Internal client has no _username attribute
        del mock_client.return_value._username
        mock_client.return_value._project_user.return_value = "my_project__bob"

        project = Project(project_name="my_project")

        assert project.home_path == "/Projects/my_project/Users/bob"

    def test_home_path_internal_client_username_with_double_underscore(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        del mock_client.return_value._username
        mock_client.return_value._project_user.return_value = (
            "my_project__user__with__underscores"
        )

        project = Project(project_name="my_project")

        assert project.home_path == "/Projects/my_project/Users/user__with__underscores"

    def test_home_path_external_client_empty_username_falls_back(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        mock_client.return_value._username = ""
        mock_client.return_value._project_user.return_value = "my_project__bob"

        project = Project(project_name="my_project")

        assert project.home_path == "/Projects/my_project/Users/bob"
