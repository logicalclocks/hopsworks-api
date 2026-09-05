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
import subprocess
import sys

import pytest


def test_importing_the_sdk_does_not_execute_great_expectations():
    # constants.py sits on the SDK's import spine; executing great_expectations there put
    # ~8 seconds of altair/scipy/jsonschema module init on every `import hopsworks`. The
    # GE version must come from package metadata, so GE stays out of sys.modules until a
    # validation module actually needs it. Run in a subprocess so this test's verdict
    # cannot be poisoned by whatever earlier tests already imported.
    code = (
        "import json, sys\n"
        "import hopsworks_common.core.constants as c\n"
        "print(json.dumps({'ge_loaded': 'great_expectations' in sys.modules,"
        " 'has_ge': c.HAS_GREAT_EXPECTATIONS, 'ge_major': c.GE_MAJOR}))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    result = json.loads(out.stdout.strip().splitlines()[-1])
    assert result["ge_loaded"] is False
    if result["has_ge"]:
        assert isinstance(result["ge_major"], int)


def test_loading_a_validation_module_does_not_log_ge_metric_chatter():
    # GE 1.x registers its core metrics from its own module init and logs "Multiple
    # declarations of metric ... for engine ..." at INFO for each one it declares twice.
    # The validation modules import GE long after `hopsworks` has pointed the root logger
    # at stderr on INFO, so those five lines land in every notebook that calls login().
    # constants.py pins that logger to WARNING before any GE import can happen; run in a
    # subprocess because logger levels are process-global state an earlier import decides.
    code = (
        "import json, sys\n"
        "import hopsworks\n"
        "try:\n"
        "    import hsfs.expectation_suite\n"
        "except Exception as e:\n"
        "    print(json.dumps({'ge_loaded': False, 'error': type(e).__name__}))\n"
        "else:\n"
        "    print(json.dumps({'ge_loaded': 'great_expectations' in sys.modules}))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    result = json.loads(out.stdout.strip().splitlines()[-1])
    if not result["ge_loaded"]:
        pytest.skip(
            f"great_expectations was not loaded: {result.get('error', 'absent')}"
        )
    assert "Multiple declarations of metric" not in out.stderr, out.stderr
