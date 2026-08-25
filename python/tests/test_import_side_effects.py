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
