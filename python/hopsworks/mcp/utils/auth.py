#
#   Copyright 2025 Hopsworks AB
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

import builtins
from typing import TYPE_CHECKING, Literal

import hopsworks


if TYPE_CHECKING:
    from hopsworks_common.project import Project


def login(
    host: str | None = None,
    port: int = 443,
    project: str | None = None,
    api_key_value: str | None = None,
    api_key_file: str | None = None,
    hostname_verification: bool = False,
    trust_store_path: str | None = None,
    engine: Literal["spark", "python", "training", "spark-no-metastore", "spark-delta"]
    | None = "python",
) -> Project:
    """Login to Hopsworks and return the project.

    This function wraps hopsworks.login to simulate user input by overriding the built-in input function.

    Parameters:
        host: Hopsworks host URL.
        port: Hopsworks port (default 443).
        project: Project name to access.
        api_key_value: API key value for Hopsworks authentication.
        api_key_file: Path to a file containing the API key for Hopsworks authentication.
        hostname_verification: Enable hostname verification for Hopsworks authentication.
        trust_store_path: Path to the trust store for Hopsworks authentication.
        engine: Engine to use (default: python).

    Returns:
        Project: The project object after successful login.
    """
    # Backup original input
    original_input = builtins.input

    # Override input to simulate user input. Choose the first project by default.
    builtins.input = lambda _: 1

    try:
        project = hopsworks.login(
            host=host,
            port=port,
            api_key_value=api_key_value,
            api_key_file=api_key_file,
            hostname_verification=hostname_verification,
            trust_store_path=trust_store_path,
            engine=engine,
        )
    finally:
        # Restore original input function
        builtins.input = original_input

    return project
