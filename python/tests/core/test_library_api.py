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

from hopsworks_common.core import library_api


class TestLibraryApi:
    def test_uninstall_sends_delete_to_library_path(self, mocker):
        # Arrange
        api = library_api.LibraryApi()
        mock_client = mocker.MagicMock()
        mock_client._project_id = 99
        mocker.patch("hopsworks_common.client.get_instance", return_value=mock_client)

        # Act
        api._uninstall("matplotlib", "myenv")

        # Assert
        mock_client._send_request.assert_called_once_with(
            "DELETE",
            [
                "project",
                99,
                "python",
                "environments",
                "myenv",
                "libraries",
                "matplotlib",
            ],
            headers={"content-type": "application/json"},
        )
