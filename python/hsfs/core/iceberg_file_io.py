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
"""PyIceberg file IO for S3 data sources on S3-compatible stores.

PyIceberg loads it by name through the `py-io-impl` property, so it is never imported directly.
"""

from __future__ import annotations

import os
import threading

from hopsworks_common.core.constants import HAS_PYARROW, HAS_PYICEBERG


_CHECKSUM_VARIABLES = (
    "AWS_REQUEST_CHECKSUM_CALCULATION",
    "AWS_RESPONSE_CHECKSUM_VALIDATION",
)
_WHEN_REQUIRED = "when_required"
_environment_lock = threading.RLock()

if HAS_PYICEBERG and HAS_PYARROW:
    from pyiceberg.io.pyarrow import PyArrowFileIO

    class _WhenRequiredChecksumFileIO(PyArrowFileIO):
        """PyArrow file IO whose S3 filesystems compute and validate checksums only when an operation requires them.

        Stores such as Dell ECS refuse the CRC64NVME trailer that PyArrow's aws-sdk-cpp adds to every upload by default.
        Neither PyArrow nor PyIceberg exposes the SDK's switch, and the SDK reads it from the environment when it builds a client.
        The variables are therefore set only while this file IO builds an S3 filesystem, and a value the user set is kept.
        An AWS client that another thread builds in that window takes them too, and keeps them for its lifetime.
        """

        def _initialize_s3_fs(self, netloc):
            with _environment_lock:
                unset = [name for name in _CHECKSUM_VARIABLES if name not in os.environ]
                for name in unset:
                    os.environ[name] = _WHEN_REQUIRED
                try:
                    return super()._initialize_s3_fs(netloc)
                finally:
                    for name in unset:
                        # Another thread may have set its own value meanwhile.
                        if os.environ.get(name) == _WHEN_REQUIRED:
                            del os.environ[name]
