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

from hsfs.core.data_source_data import DataSourceData


ROWS = [
    {
        "values": [
            {"value0": "id", "value1": "1", "size": 2},
            {"value0": "word", "value1": "alpha", "size": 2},
        ]
    },
    {
        "values": [
            {"value0": "id", "value1": "2", "size": 2},
            {"value0": "word", "value1": "beta", "size": 2},
        ]
    },
]


def test_preview_unwraps_the_data_route_answer():
    # The shape GET .../data_source/data answered on dev (train 17, an S3 trust-link source).
    data = DataSourceData.from_response_json(
        {
            "limit": 50,
            "features": [
                {"name": "id", "type": "bigint"},
                {"name": "word", "type": "string"},
            ],
            "preview": {"preview": ROWS},
            "schemaFetchFailed": False,
        }
    )
    assert data.preview == ROWS
    assert [c["value1"] for c in data.preview[1]["values"]] == ["2", "beta"]


def test_preview_keeps_a_row_list():
    assert DataSourceData(preview=ROWS).preview == ROWS
    assert DataSourceData(preview={"preview": None}).preview == []
