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
"""Deprecated import path kept for one release.

The module moved to `hsml.deployment.predictor`; import from there.
"""

import warnings as _warnings

from hsml.deployment import predictor as _target
from hsml.deployment.predictor import *  # noqa: F401, F403


__all__ = getattr(
    _target, "__all__", [_n for _n in dir(_target) if not _n.startswith("_")]
)
_warnings.warn(
    "hsml.predictor has moved to hsml.deployment.predictor; the old import path will be removed in a future release.",
    DeprecationWarning,
    stacklevel=2,
)
