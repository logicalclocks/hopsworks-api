#
#   Copyright 2024 Hopsworks AB
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

import os
import sys
from functools import wraps


def changes_environ(f):
    @wraps(f)
    def g(*args, **kwds):
        old_environ = os.environ.copy()
        try:
            return f(*args, **kwds)
        finally:
            os.environ.clear()
            os.environ.update(old_environ)

    return g


def get_or_create_local_spark_session():
    # Single entry point for tests that need a real SparkSession. The worker
    # and driver interpreters are pinned to the running interpreter at session
    # creation: a session created here outlives the creating test (Spark keeps
    # one JVM per process), and an unpinned session makes every later
    # worker-forking test resolve `python3` from PATH, which on CI runners is
    # the system interpreter, not the venv (PYTHON_VERSION_MISMATCH).
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("hopsworks-unit-tests")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
