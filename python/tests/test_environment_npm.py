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

import pytest
from hopsworks_common import environment, library


def _lib(name, version="1.0.0"):
    return library.Library(
        channel="npm", package_source="NPM", library=name, version=version
    )


class TestEnvironmentNpm:
    @pytest.fixture
    def env(self, mocker):
        env = environment.Environment(name="myenv")
        mocker.patch.object(env._environment_engine, "_await_environment_command")
        mocker.patch.object(env._environment_engine, "_await_library_command")
        return env

    def test_install_npm_accepts_every_entry_form_and_waits_per_package(
        self, env, mocker
    ):
        # Arrange
        mock_install = mocker.patch.object(
            env._library_api,
            "_install_npm",
            return_value=[
                _lib("express"),
                _lib("lodash"),
                _lib("@types/node"),
                _lib("bare"),
                _lib("tool"),
            ],
        )

        # Act
        libraries = env.install_npm(
            [
                "express@4.19.2",
                ("lodash", "4.17.21"),
                {"name": "@types/node", "version": "20.11.0"},
                "bare",
                {
                    "name": "tool",
                    "git_url": "https://github.com/acme/tool",
                    "git_ref": "0123456789abcdef0123456789abcdef01234567",
                },
            ],
            flags=["--no-fund"],
            timeout=12,
        )

        # Assert
        env._environment_engine._await_environment_command.assert_called_once_with(
            "myenv", 12
        )
        mock_install.assert_called_once_with(
            "myenv",
            {
                "packages": [
                    {"name": "express", "version": "4.19.2"},
                    {"name": "lodash", "version": "4.17.21"},
                    {"name": "@types/node", "version": "20.11.0"},
                    {"name": "bare", "version": "latest"},
                    {
                        "name": "tool",
                        "gitUrl": "https://github.com/acme/tool",
                        "gitRef": "0123456789abcdef0123456789abcdef01234567",
                    },
                ],
                "flags": ["--no-fund"],
            },
        )
        assert len(libraries) == 5
        waited = [
            c.args[1]
            for c in env._environment_engine._await_library_command.call_args_list
        ]
        assert waited == ["express", "lodash", "@types/node", "bare", "tool"]
        assert (
            env._environment_engine._await_library_command.call_args_list[0].args[2]
            == 12
        )

    def test_install_npm_scoped_name_keeps_its_scope(self, env, mocker):
        mock_install = mocker.patch.object(
            env._library_api, "_install_npm", return_value=[]
        )
        env.install_npm("@angular/cli@17.0.0", await_installation=False)
        assert mock_install.call_args.args[1]["packages"] == [
            {"name": "@angular/cli", "version": "17.0.0"}
        ]
        env._environment_engine._await_library_command.assert_not_called()

    def test_install_npm_refuses_empty_and_malformed_entries(self, env, mocker):
        mocker.patch.object(env._library_api, "_install_npm", return_value=[])
        with pytest.raises(ValueError):
            env.install_npm([])
        with pytest.raises(ValueError):
            env.install_npm([("only-name",)])
        with pytest.raises(ValueError):
            env.install_npm([{"version": "1.0.0"}])

    def test_resolve_reads_local_files_and_sends_project_paths_otherwise(
        self, env, mocker, tmp_path
    ):
        # Arrange
        manifest = tmp_path / "package.json"
        manifest.write_text(json.dumps({"dependencies": {"lodash": "^4"}}))
        lock = tmp_path / "pnpm-lock.yaml"
        lock.write_text("lockfileVersion: '9.0'\n")
        mock_resolve = mocker.patch.object(
            env._library_api, "_resolve_npm", return_value={"packages": []}
        )
        mock_client = mocker.MagicMock()
        mock_client._project_name = "demo"
        mocker.patch("hopsworks_common.client._get_instance", return_value=mock_client)

        # Act: local files
        env.resolve_npm_package_json(
            str(manifest), lockfile_path=str(lock), include_dev_dependencies=True
        )
        # Act: project paths
        env.resolve_npm_package_json(
            "Resources/app/package.json", lockfile_path="Resources/app/yarn.lock"
        )

        # Assert
        local = mock_resolve.call_args_list[0].args[1]
        assert json.loads(local["packageJson"]) == {"dependencies": {"lodash": "^4"}}
        assert local["lockfile"] == "lockfileVersion: '9.0'\n"
        assert local["lockfileName"] == "pnpm-lock.yaml"
        assert local["includeDevDependencies"] is True
        assert "packageJsonPath" not in local
        remote = mock_resolve.call_args_list[1].args[1]
        assert remote == {
            "includeDevDependencies": False,
            "packageJsonPath": "/Projects/demo/Resources/app/package.json",
            "lockfilePath": "/Projects/demo/Resources/app/yarn.lock",
            "lockfileName": "yarn.lock",
        }

    def test_resolve_needs_a_manifest_and_a_lockfile_name_with_content(
        self, env, mocker
    ):
        mocker.patch.object(env._library_api, "_resolve_npm", return_value={})
        with pytest.raises(ValueError):
            env.resolve_npm_package_json()
        with pytest.raises(ValueError):
            env.resolve_npm_package_json(content="{}", lockfile_content="x")

    def test_install_from_package_json_installs_resolved_and_fails_on_unresolved(
        self, env, mocker
    ):
        # Arrange
        resolved = {
            "packages": [
                {
                    "name": "lodash",
                    "requested": "^4",
                    "version": "4.17.21",
                    "status": "RESOLVED",
                },
                {
                    "name": "shared",
                    "requested": "workspace:*",
                    "status": "SKIPPED",
                    "reason": "workspace",
                },
                {
                    "name": "ghost",
                    "requested": "^9",
                    "status": "UNRESOLVED",
                    "reason": "no such version",
                },
            ]
        }
        mocker.patch.object(env._library_api, "_resolve_npm", return_value=resolved)
        mock_install = mocker.patch.object(
            env._library_api, "_install_npm", return_value=[_lib("lodash")]
        )

        # Act / Assert: unresolved fails by default, naming the package
        with pytest.raises(ValueError, match="ghost@\\^9"):
            env.install_npm_from_package_json(content="{}")
        mock_install.assert_not_called()

        # Act: skipping the unresolved installs the rest, skipped entries never counted
        libraries = env.install_npm_from_package_json(
            content="{}", skip_unresolved=True, flags=["--no-audit"]
        )

        # Assert
        assert [lib._library for lib in libraries] == ["lodash"]
        mock_install.assert_called_once_with(
            "myenv",
            {
                "packages": [{"name": "lodash", "version": "4.17.21"}],
                "flags": ["--no-audit"],
            },
        )

    def test_install_from_git_selects_manifests_and_adds_the_repository(
        self, env, mocker
    ):
        # Arrange
        info = {
            "url": "https://github.com/acme/mono",
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "rootPackageName": "mono",
            "manifests": [
                {
                    "path": "package.json",
                    "resolved": {
                        "packages": [
                            {
                                "name": "lodash",
                                "version": "4.17.21",
                                "status": "RESOLVED",
                            }
                        ]
                    },
                },
                {
                    "path": "packages/cli/package.json",
                    "resolved": {
                        "packages": [
                            {
                                "name": "lodash",
                                "version": "4.17.20",
                                "status": "RESOLVED",
                            },
                            {
                                "name": "yargs",
                                "version": "17.7.2",
                                "status": "RESOLVED",
                            },
                        ]
                    },
                },
            ],
        }
        mock_inspect = mocker.patch.object(
            env._library_api, "_inspect_npm_git", return_value=info
        )
        mock_install = mocker.patch.object(
            env._library_api, "_install_npm", return_value=[]
        )

        # Act: everything, first manifest's version of a shared dependency wins, repository appended
        env.install_npm_from_git(
            "https://github.com/acme/mono",
            ref="main",
            install_repository=True,
            await_installation=False,
        )

        # Assert
        mock_inspect.assert_called_once_with(
            "myenv",
            {
                "url": "https://github.com/acme/mono",
                "includeDevDependencies": False,
                "ref": "main",
            },
        )
        assert mock_install.call_args.args[1]["packages"] == [
            {"name": "lodash", "version": "4.17.21"},
            {"name": "yargs", "version": "17.7.2"},
            {
                "name": "mono",
                "gitUrl": "https://github.com/acme/mono",
                "gitRef": "0123456789abcdef0123456789abcdef01234567",
            },
        ]

        # Act: one manifest only
        env.install_npm_from_git(
            "https://github.com/acme/mono",
            manifests=["packages/cli/package.json"],
            await_installation=False,
        )
        assert mock_install.call_args.args[1]["packages"] == [
            {"name": "lodash", "version": "4.17.20"},
            {"name": "yargs", "version": "17.7.2"},
        ]

        # Act / Assert: an unknown manifest, and nothing to install
        with pytest.raises(ValueError, match="Not in the repository"):
            env.install_npm_from_git(
                "https://github.com/acme/mono", manifests=["nope/package.json"]
            )
        with pytest.raises(ValueError, match="Nothing to install"):
            env.install_npm_from_git("https://github.com/acme/mono", manifests=[])

    def test_install_from_git_repository_needs_a_name(self, env, mocker):
        mocker.patch.object(
            env._library_api,
            "_inspect_npm_git",
            return_value={"commit": "abc", "manifests": []},
        )
        mocker.patch.object(env._library_api, "_install_npm", return_value=[])
        with pytest.raises(ValueError, match="no name"):
            env.install_npm_from_git(
                "https://github.com/acme/mono", install_repository=True
            )
