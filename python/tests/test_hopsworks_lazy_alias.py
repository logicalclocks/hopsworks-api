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
"""Tests for the ``hopsworks.hsfs`` / ``hopsworks.hsml`` alias contract.

These aliases have been part of the public surface since #292 (Aug 2024).
The PEP 562 lazy refactor in #920 (May 2026) inadvertently regressed the
dotted-import form because attribute hooks aren't consulted by Python's
import machinery — only by attribute access. The meta-path finder in
``hopsworks/__init__.py`` restores the contract.
"""

import subprocess
import sys
import textwrap


def _run_isolated(snippet: str) -> tuple[int, str, str]:
    """Execute ``snippet`` in a fresh interpreter for clean ``sys.modules``.

    The sentinel is asserted against stdout alone: optional dependencies log
    to stderr on import (great_expectations prints "Multiple declarations of
    metric ..." whenever it loads), and concatenating the streams put that
    noise after the sentinel, failing every test on environments that have
    the extras installed. The merged output is returned for failure messages.
    """
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(snippet)],
        capture_output=True,
        text=True,
    )
    merged = (proc.stdout + proc.stderr).strip()
    return proc.returncode, proc.stdout.strip(), merged


def test_import_hopsworks_does_not_load_hsfs():
    """``import hopsworks`` must not eagerly load ``hsfs`` or ``hsml``."""
    rc, out, log = _run_isolated(
        """
        import sys
        import hopsworks  # noqa: F401
        assert 'hsfs' not in sys.modules, sys.modules.keys()
        assert 'hsml' not in sys.modules
        assert 'hopsworks.hsfs' not in sys.modules
        assert 'hopsworks.hsml' not in sys.modules
        print('OK')
        """
    )
    assert rc == 0 and out.endswith("OK"), log


def test_dotted_submodule_import_works():
    """``from hopsworks.hsfs.X import Y`` resolves via the meta-path finder."""
    rc, out, log = _run_isolated(
        """
        from hopsworks.hsfs.builtin_transformations import one_hot_encoder
        assert one_hot_encoder is not None
        print('OK')
        """
    )
    assert rc == 0 and out.endswith("OK"), log


def test_dotted_submodule_import_lazily_loads_hsfs():
    """The dotted import is what triggers the real ``hsfs`` load, not ``import hopsworks``."""
    rc, out, log = _run_isolated(
        """
        import sys
        import hopsworks  # noqa: F401
        assert 'hsfs' not in sys.modules
        from hopsworks.hsfs.builtin_transformations import one_hot_encoder  # noqa: F401
        assert 'hsfs' in sys.modules
        assert sys.modules['hopsworks.hsfs'] is sys.modules['hsfs']
        print('OK')
        """
    )
    assert rc == 0 and out.endswith("OK"), log


def test_attribute_access_still_works():
    """The PEP 562 ``__getattr__`` path is unaffected by the finder."""
    rc, out, log = _run_isolated(
        """
        import hopsworks
        _ = hopsworks.hsfs
        _ = hopsworks.hsml
        _ = hopsworks.udf
        print('OK')
        """
    )
    assert rc == 0 and out.endswith("OK"), log


def test_alias_and_real_module_are_identical():
    """``hopsworks.hsfs`` and ``hsfs`` must be the same module object."""
    rc, out, log = _run_isolated(
        """
        import hopsworks.hsfs
        import hsfs
        assert hopsworks.hsfs is hsfs
        import hopsworks.hsml
        import hsml
        assert hopsworks.hsml is hsml
        print('OK')
        """
    )
    assert rc == 0 and out.endswith("OK"), log


def test_repeated_dotted_imports_are_idempotent():
    """Re-importing a dotted submodule must hand back the same object."""
    rc, out, log = _run_isolated(
        """
        from hopsworks.hsfs.builtin_transformations import one_hot_encoder as a
        from hopsworks.hsfs.builtin_transformations import one_hot_encoder as b
        assert a is b
        print('OK')
        """
    )
    assert rc == 0 and out.endswith("OK"), log
