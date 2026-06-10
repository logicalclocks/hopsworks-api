#
#   Copyright 2021 Logical Clocks AB
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

import warnings
from typing import Any

from hopsworks_apigen import deprecated
from hopsworks_common import util, version
from hopsworks_common.connection import Connection


@deprecated("hopsworks.login", public_name="hsml.connection")
def connection(*args: Any, **kwargs: Any) -> Connection:
    """Create a connection to a Hopsworks instance.

    Deprecated, use [`hopsworks.login`][hopsworks.login] instead, which connects and returns a project handle directly.

    Parameters:
        *args: Positional arguments forwarded to the connection factory.
        **kwargs: Keyword arguments forwarded to the connection factory.

    Returns:
        A new connection to the Hopsworks instance.
    """
    return Connection._connection(*args, **kwargs)

__version__ = version.__version__


def ml_formatwarning(message, category, filename, lineno, line=None):
    return f"{category.__name__}: {message}\n"


warnings.formatwarning = ml_formatwarning
warnings.simplefilter("always", util.VersionWarning)

__all__ = ["connection"]
