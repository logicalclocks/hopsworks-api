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

import importlib.util
import os
import pathlib
from unittest import mock

import pytest


HSFS_UTILS = (
    pathlib.Path(__file__).resolve().parents[3] / "utils" / "python" / "hsfs_utils.py"
)


@pytest.fixture(scope="module")
def hsfs_utils():
    pytest.importorskip("pyspark")
    # The job opens a HopsFS client for the user named in the environment as it is imported,
    # neither of which exists here.
    os.environ.setdefault("HADOOP_USER_NAME", "test_user")
    spec = importlib.util.spec_from_file_location("hsfs_utils_under_test", HSFS_UTILS)
    module = importlib.util.module_from_spec(spec)
    with mock.patch("fsspec.implementations.arrow.HadoopFileSystem"):
        spec.loader.exec_module(module)
    return module
