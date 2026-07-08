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
import textwrap

import pytest


@pytest.mark.parametrize("module", ["hopsworks", "hopsworks_common.usage"])
def test_import_does_not_touch_global_random(module):
    """Importing the SDK must not reseed the interpreter-global `random` module.

    The telemetry sampler keeps its own private generator; if it ever reaches
    for the global `random` module again it would silently override the user's
    own randomness (data shuffling, train/test splits, etc.).
    The import side effect only fires on first import, so run a fresh
    interpreter and assert the global RNG state is byte-for-byte unchanged.
    """
    code = textwrap.dedent(f"""
        import random
        random.seed(12345)
        before = random.getstate()
        import {module}  # noqa: F401
        after = random.getstate()
        assert before == after, "{module} import mutated the global random state"
    """)
    subprocess.run([sys.executable, "-c", code], check=True)
