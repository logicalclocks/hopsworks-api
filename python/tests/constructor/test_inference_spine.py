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
from datetime import date, timedelta

import pandas as pd
import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.constructor.inference_spine import ROW_ID_COLUMN, InferenceSpine
from hsfs.constructor.prediction_times import PredictionTimes


# The air quality shape: root air_quality (pk country/city/street, event time date, feature pm25)
# joined to weather (pk city) on city, so city is a non-required serving key.
class _Feature:
    def __init__(self, name, type_):
        self.name, self.type = name, type_


class _FeatureGroup:
    def __init__(self, name, features, event_time):
        self.name, self.features, self.event_time = name, features, event_time


class _ServingKey:
    def __init__(self, feature_name, feature_group, required):
        self.feature_name = feature_name
        self.feature_group = feature_group
        self.required = required
        self.required_serving_key = feature_name


class _Join:
    def __init__(self, fg):
        self.query = type("Q", (), {"_left_feature_group": fg})()


class _Query:
    def __init__(self, root, joined):
        self._left_feature_group = root
        self.joins = [_Join(fg) for fg in joined]

    @property
    def featuregroups(self):
        return [self._left_feature_group] + [j.query._left_feature_group for j in self.joins]


class _FeatureView:
    def __init__(self, root, joined, serving_keys):
        self.name = "air_quality_fv"
        self.query = _Query(root, joined)
        self.serving_keys = serving_keys


@pytest.fixture
def feature_view():
    root = _FeatureGroup(
        "air_quality",
        [
            _Feature("country", "string"),
            _Feature("city", "string"),
            _Feature("street", "string"),
            _Feature("date", "timestamp"),
            _Feature("pm25", "double"),
        ],
        "date",
    )
    weather = _FeatureGroup(
        "weather",
        [
            _Feature("city", "string"),
            _Feature("date", "timestamp"),
            _Feature("temp", "double"),
        ],
        "date",
    )
    keys = [
        _ServingKey("country", root, True),
        _ServingKey("city", root, True),
        _ServingKey("street", root, True),
        _ServingKey("city", weather, False),
    ]
    return _FeatureView(root, [weather], keys)


ENTRIES = [{"country": "SE", "city": "Stockholm", "street": "Sveavagen"}]


def _times(count=3):
    return PredictionTimes.every(
        "daily", offset="08:00", start=date(2026, 9, 14), count=count
    )


class TestConstruction:
    def test_cross_product_size_and_order(self, feature_view):
        entries = pd.DataFrame(
            [
                {"country": "SE", "city": "Stockholm", "street": "B"},
                {"country": "SE", "city": "Stockholm", "street": "A"},
            ]
        )
        spine = InferenceSpine(feature_view, entries, _times(3))
        df = spine.dataframe
        assert len(df) == 6
        # entries order is preserved, then ascending prediction time within each entry.
        assert list(df["street"]) == ["B", "B", "B", "A", "A", "A"]
        assert list(df[ROW_ID_COLUMN]) == [0, 1, 2, 3, 4, 5]
        assert list(df["date"].dt.day) == [14, 15, 16, 14, 15, 16]

    def test_duplicate_entries_are_preserved_with_distinct_row_ids(self, feature_view):
        entries = pd.DataFrame(ENTRIES * 2)
        spine = InferenceSpine(feature_view, entries, _times(1))
        assert len(spine.dataframe) == 2
        assert list(spine.dataframe[ROW_ID_COLUMN]) == [0, 1]

    def test_entries_may_carry_the_prediction_time_itself(self, feature_view):
        entries = pd.DataFrame(
            [{**ENTRIES[0], "date": pd.Timestamp("2026-09-14 08:00")}]
        )
        spine = InferenceSpine(feature_view, entries, None)
        assert len(spine.dataframe) == 1
        assert spine.event_time == "date"

    def test_a_list_of_dicts_is_accepted(self, feature_view):
        spine = InferenceSpine(feature_view, ENTRIES, _times(2))
        assert len(spine.dataframe) == 2

    def test_row_id_is_absent_from_the_supplied_columns(self, feature_view):
        spine = InferenceSpine(feature_view, ENTRIES, _times(1))
        assert ROW_ID_COLUMN not in spine.supplied_columns
        assert set(spine.supplied_columns) == {"country", "city", "street", "date"}


class TestValidation:
    @pytest.mark.parametrize(
        "entries, times, message",
        [
            (pd.DataFrame([]), _times(1), "at least one row"),
            ([{"citty": "Stockholm"}], _times(1), "match nothing"),
            ([{ROW_ID_COLUMN: 1}], _times(1), "match nothing"),
            (
                [{"city": "Stockholm", "date": pd.Timestamp("2026-09-14")}],
                _times(1),
                "not both",
            ),
            (ENTRIES, None, "No prediction times"),
        ],
    )
    def test_rejects(self, feature_view, entries, times, message):
        with pytest.raises(FeatureStoreException, match=message):
            InferenceSpine(feature_view, entries, times)

    def test_a_frame_of_only_the_event_time_has_nothing_to_bind(self, feature_view):
        with pytest.raises(FeatureStoreException, match="no serving key"):
            InferenceSpine(feature_view, [{"date": pd.Timestamp("2026-09-14")}], None)

    def test_an_absent_serving_key_warns_rather_than_failing(self, feature_view):
        with pytest.warns(
            UserWarning, match=r"Serving key\(s\) \['country', 'street'\]"
        ):
            spine = InferenceSpine(feature_view, [{"city": "Stockholm"}], _times(2))
        assert len(spine.dataframe) == 2

    def test_a_root_feature_may_be_passed_in(self, feature_view):
        spine = InferenceSpine(feature_view, [{**ENTRIES[0], "pm25": 9.5}], _times(1))
        assert "pm25" in spine.supplied_columns


class TestWireForm:
    def test_carries_the_schema_never_the_rows(self, feature_view):
        spine = InferenceSpine(feature_view, ENTRIES, _times(3))
        payload = spine.to_dict()
        assert payload["rowCount"] == 3
        assert payload["eventTimeColumn"] == "date"
        assert payload["tableName"].startswith("__hopsworks_spine_")
        assert "rows" not in payload
        types = {c["name"]: c["type"] for c in payload["columns"]}
        assert types[ROW_ID_COLUMN] == "bigint"
        assert types["date"] == "timestamp"
        assert types["country"] == "string"

    def test_the_file_is_named_only_once_it_has_been_staged(self, feature_view):
        """Spark reads a temporary view, not a file, and must not send a basename.

        The backend checks the caller may read the named file before it renders a path into
        SQL, so naming a file that was never uploaded is refused as unreadable.
        """
        spine = InferenceSpine(feature_view, ENTRIES, _times(1))
        assert "parquetBasename" not in spine.to_dict()
        spine.parquet_staged = True
        assert spine.to_dict()["parquetBasename"] == spine.basename

    def test_max_event_time_is_the_last_prediction_time(self, feature_view):
        spine = InferenceSpine(feature_view, ENTRIES, _times(3))
        last = pd.Timestamp("2026-09-16 08:00", tz="UTC")
        assert spine.to_dict()["maxEventTime"] == int(last.timestamp() * 1000)

    @pytest.mark.parametrize(
        "age, expected",
        [
            (timedelta(hours=1), {"*": 3600000}),
            ({"weather": timedelta(days=1)}, {"weather": 86400000}),
            (None, None),
        ],
    )
    def test_max_feature_age(self, feature_view, age, expected):
        spine = InferenceSpine(feature_view, ENTRIES, _times(1), max_feature_age=age)
        assert spine.to_dict().get("maxFeatureAgeMs") == expected

    def test_rejects_a_non_timedelta_age(self, feature_view):
        with pytest.raises(TypeError, match="must be a timedelta"):
            InferenceSpine(
                feature_view, ENTRIES, _times(1), max_feature_age={"weather": 3600}
            )

    def test_rejects_an_age_naming_no_feature_group(self, feature_view):
        # The bound is what makes a stale lookup visible. A key that matches nothing would apply
        # no bound and return rows carried forward for ever, with no error to notice.
        with pytest.raises(FeatureStoreException, match="not a feature group"):
            InferenceSpine(
                feature_view,
                ENTRIES,
                _times(1),
                max_feature_age={"wether": timedelta(days=1)},
            )

    def test_the_wildcard_key_is_always_accepted(self, feature_view):
        spine = InferenceSpine(
            feature_view, ENTRIES, _times(1), max_feature_age={"*": timedelta(hours=2)}
        )
        assert spine.to_dict()["maxFeatureAgeMs"] == {"*": 7200000}


class TestArrowTable:
    def test_types_follow_the_feature_view_schema(self, feature_view):
        import pyarrow as pa

        spine = InferenceSpine(feature_view, [{**ENTRIES[0], "pm25": 9.5}], _times(2))
        table = spine.arrow_table()
        schema = {f.name: f.type for f in table.schema}
        assert schema[ROW_ID_COLUMN] == pa.int64()
        assert schema["country"] == pa.string()
        assert schema["pm25"] == pa.float64()
        assert schema["date"] == pa.timestamp("ms")
        assert table.num_rows == 2

    def test_a_value_that_does_not_convert_is_an_error_naming_the_type(
        self, feature_view
    ):
        spine = InferenceSpine(
            feature_view, [{**ENTRIES[0], "pm25": "not-a-number"}], _times(1)
        )
        with pytest.raises(FeatureStoreException, match="does not convert"):
            spine.arrow_table()

    def test_writes_a_parquet_file_named_by_the_basename(self, feature_view, tmp_path):
        spine = InferenceSpine(feature_view, ENTRIES, _times(2))
        path = spine.write_parquet(str(tmp_path))
        assert path.endswith(spine.basename)
        import pyarrow.parquet as pq

        assert pq.read_table(path).num_rows == 2
