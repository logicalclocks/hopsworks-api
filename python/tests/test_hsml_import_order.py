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

import subprocess
import sys

import pytest


# Each import runs in a fresh interpreter: inside the test session these modules are already loaded,
# so an import cycle only shows when the module is the first hsml import of a process.
@pytest.mark.parametrize(
    "statement",
    [
        "from hsml.predictor import Predictor",
        "import hsml.deployment",
        "import hsml.core.model_api",
        "import hsml.model",
    ],
)
def test_first_hsml_import(statement):
    result = subprocess.run(
        [sys.executable, "-c", statement], capture_output=True, text=True, timeout=120
    )

    assert result.returncode == 0, result.stderr
