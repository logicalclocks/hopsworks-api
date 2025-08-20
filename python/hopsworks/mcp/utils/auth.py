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
import builtins
from typing import Literal, Optional, Union

import hopsworks


def login(
    host: Optional[str] = None,
    port: int = 443,
    project: Optional[str] = None,
    api_key_value: Optional[str] = None,
    api_key_file: Optional[str] = None,
    hostname_verification: bool = False,
    trust_store_path: Optional[str] = None,
    engine: Union[
        None,
        Literal["spark"],
        Literal["python"],
        Literal["training"],
        Literal["spark-no-metastore"],
        Literal["spark-delta"],
    ] = "python",
):
    """Login to Hopsworks and return the project.
    This function wraps hopsworks.login to simulate user input by overriding the built-in input function.

    Args:
        host (Optional[str]): Hopsworks host URL.
        port (int): Hopsworks port (default 443).
        project (Optional[str]): Project name to access.
        api_key_value (Optional[str]): API key value for Hopsworks authentication.
        api_key_file (Optional[str]): Path to a file containing the API key for Hopsworks authentication.
        hostname_verification (bool): Enable hostname verification for Hopsworks authentication.
        trust_store_path (Optional[str]): Path to the trust store for Hopsworks authentication.
        engine (Union[None, Literal["spark"], Literal["python"], Literal["training"], Literal["spark-no-metastore"], Literal["spark-delta"]]): Engine to use (default: python).
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
