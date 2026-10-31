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


HEAVY = ("pandas", "polars", "sqlalchemy", "pyarrow", "numpy", "boto3")


def test_the_cli_feature_store_path_loads_no_heavy_library():
    # A fresh interpreter: the test process has these libraries loaded already.
    code = (
        "import sys\n"
        "import hopsworks.cli.main, hsfs, hsfs.feature_store, hsfs.feature_group, "
        "hsfs.engine.python, hsfs.core.type_systems\n"
        f"print(','.join(m for m in {HEAVY!r} if m in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == ""
