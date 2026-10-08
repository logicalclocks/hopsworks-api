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

import json

from hopsworks_common.core import library_api


class TestLibraryApi:
    def test_uninstall_sends_delete_to_library_path(self, mocker):
        # Arrange
        api = library_api.LibraryApi()
        mock_client = mocker.MagicMock()
        mock_client._project_id = 99
        mocker.patch("hopsworks_common.client._get_instance", return_value=mock_client)

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

    def test_install_npm_posts_the_request_and_returns_the_items(self, mocker):
        # Arrange
        api = library_api.LibraryApi()
        mock_client = mocker.MagicMock()
        mock_client._project_id = 99
        mock_client._send_request.return_value = {
            "count": 2,
            "items": [
                {
                    "library": "left-pad",
                    "version": "1.3.0",
                    "channel": "npm",
                    "packageSource": "NPM",
                },
                {
                    "library": "@angular/cli",
                    "version": "latest",
                    "channel": "npm",
                    "packageSource": "NPM",
                },
            ],
        }
        mocker.patch("hopsworks_common.client._get_instance", return_value=mock_client)
        request = {
            "packages": [
                {"name": "left-pad", "version": "1.3.0"},
                {"name": "@angular/cli", "version": "latest"},
            ],
            "flags": ["--no-fund"],
        }

        # Act
        libraries = api._install_npm("myenv", request)

        # Assert
        mock_client._send_request.assert_called_once_with(
            "POST",
            [
                "project",
                99,
                "python",
                "environments",
                "myenv",
                "libraries",
                "npm",
                "install",
            ],
            headers={"content-type": "application/json"},
            data=json.dumps(request),
        )
        assert [lib._library for lib in libraries] == ["left-pad", "@angular/cli"]
        assert libraries[0]._package_source == "NPM"

    def test_resolve_and_inspect_post_to_their_paths(self, mocker):
        # Arrange
        api = library_api.LibraryApi()
        mock_client = mocker.MagicMock()
        mock_client._project_id = 99
        mock_client._send_request.return_value = {"packages": []}
        mocker.patch("hopsworks_common.client._get_instance", return_value=mock_client)

        # Act
        resolved = api._resolve_npm("myenv", {"packageJson": "{}"})
        inspected = api._inspect_npm_git(
            "myenv", {"url": "https://github.com/acme/tool"}
        )

        # Assert
        assert resolved == {"packages": []}
        assert inspected == {"packages": []}
        calls = mock_client._send_request.call_args_list
        assert calls[0].args[1][-2:] == ["npm", "resolve"]
        assert calls[1].args[1][-3:] == ["npm", "git", "inspect"]
        assert json.loads(calls[1].kwargs["data"]) == {
            "url": "https://github.com/acme/tool"
        }
