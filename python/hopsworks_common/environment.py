#
#   Copyright 2022 Hopsworks AB
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

import os

import humps
from hopsworks_apigen import public
from hopsworks_common import client, command, library, usage, util
from hopsworks_common.core import environment_api, library_api
from hopsworks_common.engine import environment_engine


@public("hopsworks.environment.Environment")
class Environment:
    NOT_FOUND_ERROR_CODE = 300000

    def __init__(
        self,
        name=None,
        description=None,
        python_version=None,
        python_conflicts=None,
        pip_search_enabled=None,
        conflicts=None,
        conda_channel=None,
        libraries=None,
        commands=None,
        href=None,
        type=None,
        **kwargs,
    ):
        self._name = name
        self._description = description
        self._python_version = python_version
        self._python_conflicts = python_conflicts
        self._pip_search_enabled = pip_search_enabled
        self._conflicts = conflicts
        self._libraries = libraries
        self._commands = (
            command.Command.from_response_json(commands) if commands else None
        )

        self._environment_engine = environment_engine.EnvironmentEngine()
        self._library_api = library_api.LibraryApi()
        self._environment_api = environment_api.EnvironmentApi()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            return [cls(**env) for env in json_decamelized["items"]]
        return cls(**json_decamelized)

    @public
    @property
    def python_version(self):
        """Python version of the environment."""
        return self._python_version

    @public
    @property
    def name(self):
        """Name of the environment."""
        return self._name

    @public
    @property
    def description(self):
        """Description of the environment."""
        return self._description

    @public
    @usage._method_logger
    def install_wheel(
        self,
        path: str,
        await_installation: bool | None = True,
        timeout: float | None = None,
    ):
        """Install a python library packaged in a wheel file.

        ```python
        import hopsworks

        project = hopsworks.login()

        # Upload to Hopsworks
        ds_api = project.get_dataset_api()
        whl_path = ds_api.upload("matplotlib-3.1.3-cp38-cp38-manylinux1_x86_64.whl", "Resources")

        # Install
        env_api = project.get_environment_api()
        env = env_api.get_environment("my_custom_environment")

        env.install_wheel(whl_path)
        ```

        Parameters:
            path: The path in Hopsworks where the wheel file is located.
            await_installation: If `True` the method returns only when the installation finishes.
            timeout: Seconds to wait for the installation to finish before raising.
                Falls back to the engine's default when unset.
                It also bounds the wait for any environment command already in flight, which happens before the install is submitted and therefore applies even when `await_installation` is `False`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        # Wait for any ongoing environment operations
        self._environment_engine._await_environment_command(self.name, timeout)

        library_name = os.path.basename(path)

        _client = client._get_instance()
        path = util._convert_to_abs(path, _client._project_name)

        library_spec = {
            "dependencyUrl": path,
            "channelUrl": "wheel",
            "packageSource": "WHEEL",
        }

        self._library_api._install(library_name, self.name, library_spec)

        if await_installation:
            self._environment_engine._await_library_command(
                self.name, library_name, timeout
            )

    @public
    @usage._method_logger
    def install_requirements(
        self,
        path: str,
        await_installation: bool | None = True,
        timeout: float | None = None,
    ):
        """Install libraries specified in a `requirements.txt` file.

        ```python
        import hopsworks

        project = hopsworks.login()

        # Upload to Hopsworks
        ds_api = project.get_dataset_api()
        requirements_path = ds_api.upload("requirements.txt", "Resources")

        # Install
        env_api = project.get_environment_api()
        env = env_api.get_environment("my_custom_environment")

        env.install_requirements(requirements_path)
        ```

        Parameters:
            path: The path in Hopsworks where the `requirements.txt` file is located.
            await_installation: If `True` the method returns only when the installation is finished.
            timeout: Seconds to wait for the installation to finish before raising.
                Falls back to the engine's default when unset.
                It also bounds the wait for any environment command already in flight, which happens before the install is submitted and therefore applies even when `await_installation` is `False`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        # Wait for any ongoing environment operations
        self._environment_engine._await_environment_command(self.name, timeout)

        library_name = os.path.basename(path)

        _client = client._get_instance()
        path = util._convert_to_abs(path, _client._project_name)

        library_spec = {
            "dependencyUrl": path,
            "channelUrl": "requirements_txt",
            "packageSource": "REQUIREMENTS_TXT",
        }

        self._library_api._install(library_name, self.name, library_spec)

        if await_installation:
            self._environment_engine._await_library_command(
                self.name, library_name, timeout
            )

    @public
    @usage._method_logger
    def install_npm(
        self,
        packages: str | list[str | tuple[str, str] | dict],
        flags: list[str] | None = None,
        await_installation: bool | None = True,
        timeout: float | None = None,
    ) -> list[library.Library]:
        """Install npm packages into the environment as one image build.

        The packages are installed globally with npm, so they are on `PATH` and importable from
        any Node.js app, job or deployment the environment runs. Every package gets its own library
        entry, but the whole list is one build: resolving and installing fifty packages takes one
        image layer, not fifty.

        ```python
        import hopsworks

        project = hopsworks.login()
        env_api = project.get_environment_api()
        env = env_api.get_environment("my_custom_environment")

        env.install_npm(["express@4.19.2", "lodash@4.17.21", "@types/node@20.11.0"])

        # Pin a package to a git commit, by the name its package.json declares.
        env.install_npm([{"name": "mytool",
                          "git_url": "https://github.com/acme/mytool",
                          "git_ref": "0123456789abcdef0123456789abcdef01234567"}])
        ```

        Parameters:
            packages: What to install. Each entry is `"name@version"` (an exact version or a
                dist-tag such as `latest`; a bare `"name"` installs `latest`), a `(name, version)`
                tuple, or a dict with `name` and `version`, or with `name`, `git_url` and `git_ref`
                for a repository installed as a package, pinned to a full commit.
            flags: npm flags for the install, from the supported set: `--ignore-scripts`,
                `--legacy-peer-deps`, `--no-audit`, `--no-fund`, `--no-optional`, `--strict-peer-deps`.
            await_installation: If `True` the method returns only when the installation finishes.
            timeout: Seconds to wait for the installation to finish before raising.
                Falls back to the engine's default when unset.
                It also bounds the wait for any environment command already in flight, which happens before the install is submitted and therefore applies even when `await_installation` is `False`.

        Returns:
            One library object per package, in the order given.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            ValueError: If an entry cannot be read as a package.
        """
        request = {
            "packages": [self._npm_package_entry(p) for p in self._as_list(packages)],
            "flags": list(flags or []),
        }
        if not request["packages"]:
            raise ValueError("No packages given.")

        # Wait for any ongoing environment operations
        self._environment_engine._await_environment_command(self.name, timeout)

        libraries = self._library_api._install_npm(self.name, request)

        if await_installation:
            self._await_npm_libraries(request["packages"], timeout)
        return libraries

    @public
    @usage._method_logger
    def resolve_npm_package_json(
        self,
        path: str | None = None,
        lockfile_path: str | None = None,
        include_dev_dependencies: bool = False,
        *,
        content: str | None = None,
        lockfile_content: str | None = None,
        lockfile_name: str | None = None,
    ) -> dict:
        """Resolve a package.json's dependencies to the exact versions `install_npm` takes, without installing.

        Pinned versions stand. Where a lockfile is given its recorded versions win, since they are
        what the project actually ran. Ranges and dist-tags without a lockfile entry resolve to
        the newest matching registry release. Dependencies that are not registry packages
        (workspace siblings, `npm:` aliases, git, file and URL specs) are reported as skipped with
        the reason.

        ```python
        result = env.resolve_npm_package_json("package.json", lockfile_path="package-lock.json")
        for entry in result["packages"]:
            print(entry["name"], entry["requested"], "->", entry.get("version"), entry["status"])
        ```

        Parameters:
            path: The package.json: a path on the local file system, or, when no such local file
                exists, a path in the project (for example `Resources/my-app/package.json`).
            lockfile_path: The lockfile next to it, local or in the project: `package-lock.json`,
                `npm-shrinkwrap.json`, `yarn.lock`, `pnpm-lock.yaml` or `bun.lock`.
            include_dev_dependencies: Whether `devDependencies` are part of the result.
            content: The package.json content, instead of `path`.
            lockfile_content: The lockfile content, instead of `lockfile_path`; needs `lockfile_name`.
            lockfile_name: The lockfile's file name, which selects its format, when the lockfile is passed as content.

        Returns:
            A dict with `name`, `version`, `packageManager`, `lockfileName`, `warnings` and
            `packages`, each package carrying `name`, `requested`, `version`, `status`
            (`RESOLVED`, `SKIPPED` or `UNRESOLVED`), `source`, `dependencyType` and `reason`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            ValueError: If neither a path nor content is given.
        """
        request = {"includeDevDependencies": bool(include_dev_dependencies)}
        if content is not None:
            request["packageJson"] = content
        elif path:
            self._put_file_input(request, path, "packageJson", "packageJsonPath")
        else:
            raise ValueError("Pass the package.json path or its content.")
        if lockfile_content is not None:
            if not lockfile_name:
                raise ValueError("lockfile_name is required with lockfile_content.")
            request["lockfile"] = lockfile_content
            request["lockfileName"] = lockfile_name
        elif lockfile_path:
            self._put_file_input(request, lockfile_path, "lockfile", "lockfilePath")
            request["lockfileName"] = lockfile_name or os.path.basename(lockfile_path)
        return self._library_api._resolve_npm(self.name, request)

    @public
    @usage._method_logger
    def install_npm_from_package_json(
        self,
        path: str | None = None,
        lockfile_path: str | None = None,
        include_dev_dependencies: bool = False,
        flags: list[str] | None = None,
        await_installation: bool | None = True,
        timeout: float | None = None,
        *,
        content: str | None = None,
        lockfile_content: str | None = None,
        lockfile_name: str | None = None,
        skip_unresolved: bool = False,
    ) -> list[library.Library]:
        """Install the dependencies a package.json lists, resolved to exact versions, as one build.

        See `resolve_npm_package_json` for how versions are chosen. Dependencies the resolver
        skips (workspace siblings, aliases, git, file and URL specs) are left out. A dependency
        it cannot resolve (the registry has no matching version, or could not be reached) fails
        the call unless `skip_unresolved` is set.

        ```python
        env.install_npm_from_package_json("package.json", lockfile_path="package-lock.json")
        ```

        Parameters:
            path: The package.json: a local path, or a path in the project when no such local file exists.
            lockfile_path: The lockfile next to it, local or in the project.
            include_dev_dependencies: Also install `devDependencies`. Off by default.
            flags: npm flags for the install; see `install_npm`.
            await_installation: If `True` the method returns only when the installation finishes.
            timeout: Seconds to wait for the installation to finish before raising; see `install_npm`.
            content: The package.json content, instead of `path`.
            lockfile_content: The lockfile content, instead of `lockfile_path`; needs `lockfile_name`.
            lockfile_name: The lockfile's file name when passed as content.
            skip_unresolved: Install what resolved and leave out what did not, instead of raising.

        Returns:
            One library object per installed package.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            ValueError: If a dependency could not be resolved and `skip_unresolved` is not set, or nothing is left to install.
        """
        resolved = self.resolve_npm_package_json(
            path,
            lockfile_path,
            include_dev_dependencies,
            content=content,
            lockfile_content=lockfile_content,
            lockfile_name=lockfile_name,
        )
        packages = self._installable(resolved, skip_unresolved)
        return self.install_npm(
            packages,
            flags=flags,
            await_installation=await_installation,
            timeout=timeout,
        )

    @public
    @usage._method_logger
    def inspect_npm_git(
        self,
        url: str,
        ref: str | None = None,
        include_dev_dependencies: bool = False,
    ) -> dict:
        """Read and resolve the package.json files of a git repository, without installing.

        The repository is cloned, without checkout, by a short-lived job in the project with the
        git credentials configured under your account settings, so a private repository works
        once its provider is set up there. Every package.json in the tree is returned, one for a
        plain repository and one per workspace for a monorepo, resolved against the nearest
        lockfile.

        ```python
        info = env.inspect_npm_git("https://github.com/acme/mytool", ref="main")
        print(info["commit"], info["rootPackageName"])
        for manifest in info["manifests"]:
            print(manifest["path"], [p["name"] for p in manifest["resolved"]["packages"]])
        ```

        Parameters:
            url: The https clone URL.
            ref: A branch, tag or commit; the repository's default branch when unset.
            include_dev_dependencies: Whether `devDependencies` are part of the result.

        Returns:
            A dict with `url`, `commit`, `branch`, `rootPackageName`, `rootPackageVersion`,
            `packageManager`, `warnings` and `manifests`, each manifest carrying its `path` and
            a `resolved` result in the shape `resolve_npm_package_json` returns.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        request = {"url": url, "includeDevDependencies": bool(include_dev_dependencies)}
        if ref:
            request["ref"] = ref
        return self._library_api._inspect_npm_git(self.name, request)

    @public
    @usage._method_logger
    def install_npm_from_git(
        self,
        url: str,
        ref: str | None = None,
        manifests: list[str] | None = None,
        install_repository: bool = False,
        include_dev_dependencies: bool = False,
        flags: list[str] | None = None,
        await_installation: bool | None = True,
        timeout: float | None = None,
        *,
        skip_unresolved: bool = False,
    ) -> list[library.Library]:
        """Install what a git repository's package.json files depend on, and optionally the repository itself, as one build.

        ```python
        # The dependencies of the repository's package.json, at the branch's current commit.
        env.install_npm_from_git("https://github.com/acme/mytool", ref="main")

        # One workspace of a monorepo, plus the repository installed as a package.
        env.install_npm_from_git("https://github.com/acme/mono", manifests=["packages/cli/package.json"],
                                 install_repository=True)
        ```

        Parameters:
            url: The https clone URL.
            ref: A branch, tag or commit; the repository's default branch when unset. The install is
                pinned to the commit the ref points at when this is called.
            manifests: The package.json paths to install the dependencies of, as `inspect_npm_git`
                lists them. All of them when unset; an empty list installs none, which with
                `install_repository` installs only the repository.
            install_repository: Also install the repository itself as a package, by the name its root
                package.json declares. The install runs npm; a repository that needs yarn, pnpm or
                Bun to build is reported but still attempted.
            include_dev_dependencies: Also install `devDependencies`. Off by default.
            flags: npm flags for the install; see `install_npm`.
            await_installation: If `True` the method returns only when the installation finishes.
            timeout: Seconds to wait for the installation to finish before raising; see `install_npm`.
            skip_unresolved: Install what resolved and leave out what did not, instead of raising.

        Returns:
            One library object per installed package.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            ValueError: If a manifest is not in the repository, a dependency could not be resolved and
                `skip_unresolved` is not set, the repository has no name to install it under, or nothing is left to install.
        """
        info = self.inspect_npm_git(url, ref, include_dev_dependencies)
        found = {m["path"]: m for m in info.get("manifests", []) or []}
        if manifests is None:
            selected = list(found.values())
        else:
            missing = [m for m in manifests if m not in found]
            if missing:
                raise ValueError(
                    f"Not in the repository: {', '.join(missing)}. It holds: {', '.join(found) or 'no package.json'}."
                )
            selected = [found[m] for m in manifests]
        packages: list[dict] = []
        seen: set[str] = set()
        for manifest in selected:
            for entry in self._installable(
                manifest.get("resolved") or {}, skip_unresolved
            ):
                if entry["name"] not in seen:
                    seen.add(entry["name"])
                    packages.append(entry)
        if install_repository:
            name = info.get("rootPackageName")
            if not name:
                raise ValueError(
                    "The repository's root package.json declares no name, so it cannot be installed as a package."
                )
            if name in seen:
                packages = [p for p in packages if p["name"] != name]
            packages.append({"name": name, "git_url": url, "git_ref": info["commit"]})
        if not packages:
            raise ValueError(
                "Nothing to install: the selected package.json files list no resolvable dependencies."
            )
        return self.install_npm(
            packages,
            flags=flags,
            await_installation=await_installation,
            timeout=timeout,
        )

    @staticmethod
    def _as_list(packages):
        if isinstance(packages, (str, tuple, dict)):
            return [packages]
        return list(packages)

    @staticmethod
    def _npm_package_entry(entry) -> dict:
        """One install request entry from the forms `install_npm` accepts."""
        if isinstance(entry, dict):
            name = entry.get("name")
            if not name:
                raise ValueError(f"A package entry needs a name: {entry!r}")
            if entry.get("git_url") or entry.get("gitUrl"):
                return {
                    "name": name,
                    "gitUrl": entry.get("git_url") or entry.get("gitUrl"),
                    "gitRef": entry.get("git_ref") or entry.get("gitRef"),
                }
            return {"name": name, "version": entry.get("version") or "latest"}
        if isinstance(entry, tuple):
            if len(entry) != 2:
                raise ValueError(f"A package tuple is (name, version): {entry!r}")
            return {"name": entry[0], "version": entry[1] or "latest"}
        if not isinstance(entry, str) or not entry.strip():
            raise ValueError(f"Not a package: {entry!r}")
        spec = entry.strip()
        # The scope's @ is not the version separator: @scope/name@1.0.0.
        at = spec.find("@", 1)
        if at > 0:
            return {"name": spec[:at], "version": spec[at + 1 :] or "latest"}
        return {"name": spec, "version": "latest"}

    @staticmethod
    def _installable(resolved: dict, skip_unresolved: bool) -> list[dict]:
        packages = []
        unresolved = []
        for entry in resolved.get("packages", []) or []:
            status = entry.get("status")
            if status == "RESOLVED":
                packages.append({"name": entry["name"], "version": entry["version"]})
            elif status == "UNRESOLVED":
                unresolved.append(
                    f"{entry['name']}@{entry.get('requested')}: {entry.get('reason')}"
                )
        if unresolved and not skip_unresolved:
            raise ValueError(
                "Could not resolve "
                + "; ".join(unresolved)
                + ". Fix the entries, provide a lockfile, or pass skip_unresolved=True to install the rest."
            )
        return packages

    def _put_file_input(
        self, request: dict, path: str, content_key: str, path_key: str
    ):
        """A local file goes as content; anything else is a path in the project."""
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                request[content_key] = f.read()
            return
        _client = client._get_instance()
        request[path_key] = util._convert_to_abs(path, _client._project_name)

    def _await_npm_libraries(self, packages: list[dict], timeout: float | None):
        """Waits for each package of one request. They share a build, so after the first the rest are quick."""
        for entry in packages:
            self._environment_engine._await_library_command(
                self.name, entry["name"], timeout
            )

    @public
    @usage._method_logger
    def uninstall(
        self,
        library_name: str,
        await_uninstallation: bool | None = True,
        timeout: float | None = None,
    ) -> None:
        """Uninstall a library from the environment.

        ```python
        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()
        env = env_api.get_environment("my_custom_environment")

        env.uninstall("matplotlib")
        ```

        Parameters:
            library_name: Name of the installed library to remove.
            await_uninstallation: If `True` the method returns only when the uninstallation finishes.
            timeout: Seconds to wait for the uninstallation to finish before raising.
                Falls back to the engine's default when unset.
                It also bounds the wait for any environment command already in flight, which happens before the removal is submitted and therefore applies even when `await_uninstallation` is `False`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        # Wait for any ongoing environment operations
        self._environment_engine._await_environment_command(self.name, timeout)

        self._library_api._uninstall(library_name, self.name)

        if await_uninstallation:
            self._environment_engine._await_library_command(
                self.name, library_name, timeout
            )

    @public
    @usage._method_logger
    def delete(self):
        """Delete the environment.

        Danger: Potentially dangerous operation
            This operation deletes the python environment.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._environment_api._delete(self.name)

    def __repr__(self):
        return f"Environment({self.name!r})"
