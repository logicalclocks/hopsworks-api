# ruff: noqa: INP001
"""Job entry point for the book's unit tests, run in the jobs environment.

Copied into the system as tests/run_unit.py, beside the book's tests in
tests/book (as tests/ccfraud, pytest would import them as the `ccfraud` package
they test). The `ccfraud` package imports Faker and geoip2, which only
`<slug>-jobs-env` holds, so the tests run as the `<slug>-tests` job from this
file's HopsFS path, in place, like the pipelines:

    hops job deploy <slug>-tests hdfs:///Projects/<project>/<system dir>/tests/run_unit.py \
        --env <slug>-jobs-env --run --wait

Their conftest replaces the hopsworks modules with mocks for the whole process,
which is why they run in a process of their own, outside tests/unit.
Exits with pytest's status; nothing is written into the system directory.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).absolute().parents[1]

if __name__ == "__main__":
    sys.dont_write_bytecode = True
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))
    import pytest

    sys.exit(
        pytest.main(
            [
                "-q",
                "-p",
                "no:cacheprovider",
                "--rootdir",
                "tests/book",
                "tests/book",
            ]
        )
    )
