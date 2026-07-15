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

from datetime import datetime

import pytest
from hsfs.core.vector_server import VectorServer


class TestVectorServer:
    @pytest.mark.parametrize(
        "timestamp_value, expected",
        [
            ("2024-04-18 12:00:25", datetime(2024, 4, 18, 12, 0, 25)),
            # fractional seconds appear when the online type has sub-second
            # precision, e.g. timestamp(3) (FSTORE-2061)
            ("2024-04-18 12:00:25.789", datetime(2024, 4, 18, 12, 0, 25, 789000)),
            ("2024-04-18 12:00:25.789000", datetime(2024, 4, 18, 12, 0, 25, 789000)),
            (1713441625789, datetime(2024, 4, 18, 12, 0, 25, 789000)),
            (
                datetime(2024, 4, 18, 12, 0, 25, 789000),
                datetime(2024, 4, 18, 12, 0, 25, 789000),
            ),
            (None, None),
        ],
    )
    def test_handle_timestamp_based_on_dtype(self, timestamp_value, expected):
        server = VectorServer.__new__(VectorServer)

        assert server._handle_timestamp_based_on_dtype(timestamp_value) == expected
