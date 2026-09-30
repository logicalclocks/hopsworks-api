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
from datetime import date, datetime, timezone

import pandas as pd
import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.constructor import inference_spine
from hsfs.constructor.inference_spine import ROW_ID_COLUMN, InferenceSpine
from hsfs.constructor.prediction_times import PredictionTimes


# The air quality shape: root air_quality (pk country/city/street, event time date, feature pm25)
# joined to weather (pk city) on city, so city is a non-required serving key.
class _Feature:
    def __init__(self, name, type_):
        self.name, self.type = name, type_


class _FeatureGroup:
    def __init__(self, name, features, event_time, primary_key=None):
        self.name, self.features, self.event_time = name, features, event_time
        self.primary_key = primary_key or []


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
    def __init__(self, root, joined, left_features=None):
        self._left_feature_group = root
        self.joins = [_Join(fg) for fg in joined]
        # What the view selects from the root, which is what the backend recognises.
        self._left_features = (
            left_features if left_features is not None else list(root.features)
        )

    @property
    def featuregroups(self):
        return [self._left_feature_group] + [
            j.query._left_feature_group for j in self.joins
        ]


class _FeatureView:
    def __init__(self, root, joined, serving_keys, left_features=None):
        self.name = "air_quality_fv"
        self.version = 1
        self.featurestore_id = 67
        self.query = _Query(root, joined, left_features)
        self.serving_keys = serving_keys
        # The view's output schema: every selected feature under its output name, which for a
        # joined feature group is its prefix and the feature's name. A root column the view does
        # not select is not in it.
        self.features = list(self.query._left_features) + [
            f for fg in joined for f in fg.features
        ]


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


SPINE_DF = [{"country": "SE", "city": "Stockholm", "street": "Sveavagen"}]


def _times(count=3):
    return PredictionTimes.every(
        "daily", offset="08:00", start=date(2026, 9, 14), count=count
    )


def _crossed(entities, count=3):
    """The frame a caller now passes: entities crossed with a schedule, times in `date`."""
    return _times(count).cross(entities, event_time="date")


class TestConstruction:
    def test_cross_product_size_and_order(self, feature_view):
        spine_df = pd.DataFrame(
            [
                {"country": "SE", "city": "Stockholm", "street": "B"},
                {"country": "SE", "city": "Stockholm", "street": "A"},
            ]
        )
        spine = InferenceSpine(feature_view, _crossed(spine_df, 3))
        df = spine.dataframe
        assert len(df) == 6
        # spine_df order is preserved, then ascending prediction time within each entry.
        assert list(df["street"]) == ["B", "B", "B", "A", "A", "A"]
        assert list(df[ROW_ID_COLUMN]) == [0, 1, 2, 3, 4, 5]
        assert list(df["date"].dt.day) == [14, 15, 16, 14, 15, 16]

    def test_duplicate_entries_are_preserved_with_distinct_row_ids(self, feature_view):
        spine_df = pd.DataFrame(SPINE_DF * 2)
        spine = InferenceSpine(feature_view, _crossed(spine_df, 1))
        assert len(spine.dataframe) == 2
        assert list(spine.dataframe[ROW_ID_COLUMN]) == [0, 1]

    def test_entries_may_carry_the_prediction_time_itself(self, feature_view):
        spine_df = pd.DataFrame(
            [{**SPINE_DF[0], "date": pd.Timestamp("2026-09-14 08:00")}]
        )
        spine = InferenceSpine(feature_view, spine_df)
        assert len(spine.dataframe) == 1
        assert spine.event_time == "date"

    def test_a_list_of_dicts_is_accepted(self, feature_view):
        spine = InferenceSpine(feature_view, _crossed(SPINE_DF, 2))
        assert len(spine.dataframe) == 2

    def test_row_id_is_absent_from_the_supplied_columns(self, feature_view):
        spine = InferenceSpine(feature_view, _crossed(SPINE_DF, 1))
        assert ROW_ID_COLUMN not in spine.supplied_columns
        assert set(spine.supplied_columns) == {"country", "city", "street", "date"}


class TestValidation:
    @pytest.mark.parametrize(
        "spine_df, message",
        [
            (pd.DataFrame([]), "at least one row"),
            (
                [{"citty": "Stockholm", "date": pd.Timestamp("2026-09-14")}],
                "match nothing",
            ),
            ([{ROW_ID_COLUMN: 1, "date": pd.Timestamp("2026-09-14")}], "match nothing"),
            # Without a time per row there is nothing to look features up as of.
            (SPINE_DF, "must carry the prediction time"),
        ],
    )
    def test_rejects(self, feature_view, spine_df, message):
        with pytest.raises(FeatureStoreException, match=message):
            InferenceSpine(feature_view, spine_df)

    def test_a_frame_of_only_the_event_time_has_nothing_to_bind(self, feature_view):
        with pytest.raises(FeatureStoreException, match="no serving key"):
            InferenceSpine(feature_view, [{"date": pd.Timestamp("2026-09-14")}])

    def test_an_absent_serving_key_warns_rather_than_failing(self, feature_view):
        with pytest.warns(
            UserWarning, match=r"Serving key\(s\) \['country', 'street'\]"
        ):
            spine = InferenceSpine(feature_view, _crossed([{"city": "Stockholm"}], 2))
        assert len(spine.dataframe) == 2

    def test_a_root_feature_may_be_passed_in(self, feature_view):
        spine = InferenceSpine(
            feature_view, _crossed([{**SPINE_DF[0], "pm25": 9.5}], 1)
        )
        assert "pm25" in spine.supplied_columns


class TestWireForm:
    def test_carries_the_schema_never_the_rows(self, feature_view):
        spine = InferenceSpine(feature_view, _crossed(SPINE_DF, 3))
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
        spine = InferenceSpine(feature_view, _crossed(SPINE_DF, 1))
        assert "parquetBasename" not in spine.to_dict()
        spine.parquet_staged = True
        assert spine.to_dict()["parquetBasename"] == spine.basename

    def test_max_event_time_is_the_last_prediction_time(self, feature_view):
        spine = InferenceSpine(feature_view, _crossed(SPINE_DF, 3))
        last = pd.Timestamp("2026-09-16 08:00", tz="UTC")
        assert spine.to_dict()["maxEventTime"] == int(last.timestamp() * 1000)

    def test_the_feature_view_rides_on_the_wire_instead_of_a_bound(self, feature_view):
        # The backend looks the staleness bound up from this identity rather than being told it.
        payload = InferenceSpine(feature_view, _crossed(SPINE_DF, 1)).to_dict()
        assert payload["featureViewName"] == feature_view.name
        assert payload["featureViewVersion"] == feature_view.version
        assert payload["featureViewFeaturestoreId"] == feature_view.featurestore_id


class TestArrowTable:
    def test_types_follow_the_feature_view_schema(self, feature_view):
        import pyarrow as pa

        spine = InferenceSpine(
            feature_view, _crossed([{**SPINE_DF[0], "pm25": 9.5}], 2)
        )
        table = spine._arrow_table()
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
            feature_view, _crossed([{**SPINE_DF[0], "pm25": "not-a-number"}], 1)
        )
        with pytest.raises(FeatureStoreException, match="does not convert"):
            spine._arrow_table()

    def test_writes_a_parquet_file_named_by_the_basename(self, feature_view, tmp_path):
        spine = InferenceSpine(feature_view, _crossed(SPINE_DF, 2))
        path = spine._write_parquet(str(tmp_path))
        assert path.endswith(spine.basename)
        import pyarrow.parquet as pq

        assert pq.read_table(path).num_rows == 2


class TestPassthroughColumns:
    """Training data is built from a labels frame, so the spine has to carry the labels."""

    def _frame(self):
        return pd.DataFrame(
            [
                {
                    "country": "SE",
                    "city": "Stockholm",
                    "street": "Sveavagen",
                    "date": datetime(2026, 3, 1),
                    "label": 1.0,
                }
            ]
        )

    def test_a_column_the_view_does_not_define_is_refused_by_default(
        self, feature_view
    ):
        # Inference has no labels, so an unrecognised column is still a mistake worth catching.
        with pytest.raises(FeatureStoreException, match="match nothing"):
            InferenceSpine(feature_view, self._frame())

    def test_passthrough_carries_it_and_marks_it_on_the_wire(self, feature_view):
        spine = InferenceSpine(feature_view, self._frame(), allow_passthrough=True)
        columns = {c["name"]: c for c in spine.to_dict()["columns"]}
        assert columns["label"]["passthrough"] is True
        assert columns["label"]["type"] == "double"
        # Columns the view does define are untouched by the flag.
        assert "passthrough" not in columns["city"]

    def test_passthrough_types_come_from_the_frame(self, feature_view):
        frame = self._frame()
        frame["fold"] = pd.Series([3], dtype="int64")
        frame["note"] = "a"
        spine = InferenceSpine(feature_view, frame, allow_passthrough=True)
        columns = {c["name"]: c for c in spine.to_dict()["columns"]}
        assert columns["fold"]["type"] == "bigint"
        assert columns["note"]["type"] == "string"

    def test_the_row_id_is_still_refused_even_with_passthrough(self, feature_view):
        frame = self._frame()
        frame[ROW_ID_COLUMN] = 0
        with pytest.raises(FeatureStoreException, match="match nothing"):
            InferenceSpine(feature_view, frame, allow_passthrough=True)

    def test_the_parquet_file_and_the_wire_agree_on_passthrough_types(
        self, feature_view
    ):
        # A file typed differently from its declaration fails at the CAST in the query service,
        # which is how this was found: the schema defaulted passthrough columns to string.
        frame = self._frame()
        frame["fold"] = pd.Series([3], dtype="int64")
        spine = InferenceSpine(feature_view, frame, allow_passthrough=True)
        declared = {c["name"]: c["type"] for c in spine.to_dict()["columns"]}
        written = {f.name: f.type for f in spine._arrow_table().schema}
        assert declared["label"] == "double" and str(written["label"]) == "double"
        assert declared["fold"] == "bigint" and str(written["fold"]) == "int64"


class TestViewLevelFeatureAge:
    """The bound is the view's, so the wire form names the view instead of carrying a bound."""

    def test_the_spine_names_its_feature_view(self, feature_view):
        payload = InferenceSpine(feature_view, _crossed(SPINE_DF, 1)).to_dict()
        assert payload["featureViewName"] == "air_quality_fv"
        assert payload["featureViewVersion"] == 1
        assert payload["featureViewFeaturestoreId"] == 67

    def test_no_bound_is_ever_sent(self, feature_view):
        """No bound is sent at all.

        A bound on the request is a bound the caller picked, which is what the column exists to
        stop. The backend reads it from the view's own row instead.
        """
        payload = InferenceSpine(feature_view, _crossed(SPINE_DF, 1)).to_dict()
        assert not [k for k in payload if "FeatureAge" in k]


class TestSpineCeilings:
    """Refused before the frame is typed, written and uploaded, not after a round trip."""

    def test_too_many_rows(self, feature_view, monkeypatch):
        monkeypatch.setattr(inference_spine, "MAX_SPINE_ROWS", 3)
        with pytest.raises(FeatureStoreException, match="more than 3 rows"):
            InferenceSpine(feature_view, _crossed(SPINE_DF, 4))

    def test_too_many_columns(self, feature_view, monkeypatch):
        monkeypatch.setattr(inference_spine, "MAX_SPINE_COLUMNS", 2)
        with pytest.raises(FeatureStoreException, match="columns; the limit is 2"):
            InferenceSpine(feature_view, _crossed(SPINE_DF, 1))

    def test_within_the_ceilings(self, feature_view):
        assert InferenceSpine(feature_view, _crossed(SPINE_DF, 1)).row_count == 1


def _feature_view_with_event_time_type(hive_type):
    root = _FeatureGroup(
        "air_quality",
        [
            _Feature("city", "string"),
            _Feature("date", hive_type),
            _Feature("pm25", "double"),
        ],
        "date",
    )
    weather = _FeatureGroup(
        "weather", [_Feature("city", "string"), _Feature("date", "timestamp")], "date"
    )
    keys = [_ServingKey("city", root, True), _ServingKey("city", weather, False)]
    return _FeatureView(root, [weather], keys)


class TestEventTimeType:
    """The spine's event time is written in the root feature group's own type.

    The backend declares the column in that type and the query service casts the file to it, so a
    TIMESTAMP column under a BIGINT root failed at the cast, and an epoch given as an integer was
    parsed as nanoseconds and landed in 1970.
    """

    def test_an_epoch_root_gets_epoch_milliseconds(self):
        fv = _feature_view_with_event_time_type("bigint")
        moment = datetime(2026, 9, 16, tzinfo=timezone.utc)
        spine = InferenceSpine(fv, [{"city": "Stockholm", "date": moment}])

        assert spine.to_dict()["columns"][2] == {"name": "date", "type": "bigint"}
        assert list(spine.dataframe["date"]) == [int(moment.timestamp() * 1000)]
        assert str(spine._arrow_table().schema.field("date").type) == "int64"

    def test_an_integer_prediction_time_is_epoch_milliseconds(self):
        fv = _feature_view_with_event_time_type("timestamp")
        spine = InferenceSpine(fv, [{"city": "Stockholm", "date": 1789516800000}])

        assert spine.to_dict()["maxEventTime"] == 1789516800000
        assert spine.dataframe["date"][0] == pd.Timestamp(
            1789516800000, unit="ms", tz="UTC"
        )

    def test_an_integer_prediction_time_under_an_epoch_root_round_trips(self):
        fv = _feature_view_with_event_time_type("bigint")
        spine = InferenceSpine(fv, [{"city": "Stockholm", "date": 1789516800000}])

        assert list(spine.dataframe["date"]) == [1789516800000]
        assert spine.to_dict()["maxEventTime"] == 1789516800000

    def test_a_date_root_gets_dates(self):
        fv = _feature_view_with_event_time_type("date")
        spine = InferenceSpine(fv, [{"city": "Stockholm", "date": "2026-09-16"}])

        assert spine.to_dict()["columns"][2] == {"name": "date", "type": "date"}
        assert list(spine.dataframe["date"]) == [date(2026, 9, 16)]
        assert str(spine._arrow_table().schema.field("date").type) == "date32[day]"

    def test_an_unsupported_event_time_type_is_refused_up_front(self):
        fv = _feature_view_with_event_time_type("string")
        with pytest.raises(
            FeatureStoreException, match="`string`, which a prediction time"
        ):
            InferenceSpine(fv, [{"city": "Stockholm", "date": "2026-09-16"}])

    def test_a_value_that_is_neither_a_timestamp_nor_an_epoch_is_refused(self):
        fv = _feature_view_with_event_time_type("timestamp")
        with pytest.raises(FeatureStoreException, match="not a timestamp or an epoch"):
            InferenceSpine(fv, [{"city": "Stockholm", "date": "yesterday-ish"}])


class TestUnselectedRootColumns:
    """The recognised set is what the view selects, which is what the backend recognises.

    Accepting every column of the root feature group here let a frame past the client and then
    failed in the resolver, whose accepted-columns list is built from the stored view and so
    named a different set.
    """

    def _view(self):
        root = _FeatureGroup(
            "air_quality",
            [
                _Feature("city", "string"),
                _Feature("date", "timestamp"),
                _Feature("pm25", "double"),
                _Feature("internal_note", "string"),
            ],
            "date",
            primary_key=["city"],
        )
        keys = [_ServingKey("city", root, True)]
        # The view selects the event time and pm25; internal_note is a column of the feature
        # group that the view does not carry.
        return _FeatureView(
            root,
            [],
            keys,
            left_features=[_Feature("date", "timestamp"), _Feature("pm25", "double")],
        )

    def test_a_selected_root_feature_is_still_a_passed_feature(self):
        fv = self._view()
        spine = InferenceSpine(
            fv, [{"city": "Stockholm", "date": datetime(2026, 9, 16), "pm25": 7.0}]
        )
        assert "pm25" in spine.supplied_columns

    def test_an_unselected_root_column_is_refused_here_rather_than_by_the_backend(self):
        fv = self._view()
        with pytest.raises(FeatureStoreException, match=r"\['internal_note'\]"):
            InferenceSpine(
                fv,
                [
                    {
                        "city": "Stockholm",
                        "date": datetime(2026, 9, 16),
                        "internal_note": "x",
                    }
                ],
            )

    def test_it_is_a_label_when_training_data_allows_passthrough(self):
        fv = self._view()
        spine = InferenceSpine(
            fv,
            [
                {
                    "city": "Stockholm",
                    "date": datetime(2026, 9, 16),
                    "internal_note": "x",
                }
            ],
            allow_passthrough=True,
        )
        columns = {c["name"]: c for c in spine.to_dict()["columns"]}
        assert columns["internal_note"]["passthrough"] is True


class TestPassthroughShadowingAJoinedFeature:
    """A frame column named like a joined feature is not a label; it is the feature.

    Root features in the frame are passed features and bind the root lookup. A joined feature
    group's feature has no such meaning: carrying the frame's copy through would put the looked
    up column and the caller's column in the result under one name.
    """

    def test_is_refused(self, feature_view):
        frame = _crossed(SPINE_DF, 1)
        frame["temp"] = 21.5
        with pytest.raises(FeatureStoreException, match=r"\['temp'\] are features"):
            InferenceSpine(feature_view, frame, allow_passthrough=True)

    def test_a_root_feature_is_still_a_passed_feature(self, feature_view):
        frame = _crossed(SPINE_DF, 1)
        frame["pm25"] = 12.0
        spine = InferenceSpine(feature_view, frame, allow_passthrough=True)
        assert "pm25" not in [
            c["name"] for c in spine.to_dict()["columns"] if c.get("passthrough")
        ]


class TestPolarsCeiling:
    def test_an_oversized_polars_frame_is_refused_before_conversion(
        self, feature_view, monkeypatch
    ):
        pl = pytest.importorskip("polars")
        monkeypatch.setattr(inference_spine, "MAX_SPINE_ROWS", 2)
        frame = pl.from_pandas(_crossed(SPINE_DF, 3))
        with pytest.raises(FeatureStoreException, match="3 rows; the limit is 2"):
            InferenceSpine(feature_view, frame)


class TestSparkSpineDataFrame:
    """A Spark DataFrame is accepted as `spine_df`, but only under the Spark engine."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        # pyspark refuses to build a DataFrame from rows on pandas < 2.2, and the
        # Pandas 1.x matrix job runs this file. Skipping is right rather than pinning: the
        # behaviour under test is the collect, which needs a session that can be created.
        pandas_version = tuple(int(p) for p in pd.__version__.split(".")[:2])
        if pandas_version < (2, 2):
            pytest.skip(
                f"pyspark needs pandas >= 2.2 to create a DataFrame; have {pd.__version__}"
            )
        from hsfs.engine import spark as spark_engine_mod

        engine = spark_engine_mod.Engine()
        engine._spark_session.conf.set("spark.sql.shuffle.partitions", "1")
        yield engine._spark_session

    def test_a_spark_dataframe_is_collected_under_the_spark_engine(
        self, mocker, feature_view, spark_session
    ):
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        sdf = spark_session.createDataFrame(
            [("stockholm", datetime(2026, 3, 1)), ("gothenburg", datetime(2026, 3, 2))],
            ["city", "date"],
        )

        spine = InferenceSpine(feature_view, sdf)

        assert spine.row_count == 2
        assert list(spine.dataframe["city"]) == ["stockholm", "gothenburg"]
        assert list(spine.dataframe[ROW_ID_COLUMN]) == [0, 1]

    def test_a_spark_dataframe_is_refused_under_the_python_engine(
        self, mocker, feature_view, spark_session
    ):
        # There is no Spark session to evaluate it, so accepting it would fail later and
        # further away from the call that got it wrong.
        mocker.patch("hsfs.engine._get_type", return_value="python")
        sdf = spark_session.createDataFrame(
            [("stockholm", datetime(2026, 3, 1))], ["city", "date"]
        )

        with pytest.raises(FeatureStoreException, match="Python engine"):
            InferenceSpine(feature_view, sdf)

    def test_the_event_time_survives_the_collect(
        self, mocker, feature_view, spark_session
    ):
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        moment = datetime(2026, 3, 1, 12, 30, tzinfo=timezone.utc)
        sdf = spark_session.createDataFrame([("stockholm", moment)], ["city", "date"])

        spine = InferenceSpine(feature_view, sdf)

        assert spine.to_dict()["maxEventTime"] == int(moment.timestamp() * 1000)

    def test_the_session_timezone_is_utc(self, spark_session):
        # The collect returns tz-naive timestamps in the session timezone, and the spine reads
        # them as UTC. If this stops being UTC, every Spark spine's event times shift silently.
        assert spark_session.conf.get("spark.sql.session.timeZone") == "UTC"

    def test_a_label_column_still_rides_along(
        self, mocker, feature_view, spark_session
    ):
        # Training data from a Spark frame: the label is not a view column, so it is carried
        # through rather than refused.
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        sdf = spark_session.createDataFrame(
            [("stockholm", datetime(2026, 3, 1), 1.5)], ["city", "date", "label"]
        )

        spine = InferenceSpine(feature_view, sdf, allow_passthrough=True)

        columns = {c["name"]: c for c in spine.to_dict()["columns"]}
        assert columns["label"]["passthrough"] is True
        assert columns["label"]["type"] == "double"

    def test_only_one_row_over_the_ceiling_is_collected(
        self, mocker, feature_view, spark_session, monkeypatch
    ):
        # The ceiling used to be checked after toPandas had brought the whole frame to the
        # driver, so a frame large enough to matter was one large enough to exhaust it first.
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        monkeypatch.setattr(inference_spine, "MAX_SPINE_ROWS", 2)
        sdf = spark_session.createDataFrame(
            [
                ("a", datetime(2026, 3, 1)),
                ("b", datetime(2026, 3, 1)),
                ("c", datetime(2026, 3, 1)),
            ],
            ["city", "date"],
        )
        limited = mocker.spy(sdf, "limit")

        with pytest.raises(FeatureStoreException, match="more than 2 rows"):
            InferenceSpine(feature_view, sdf)

        limited.assert_called_once_with(3)

    def test_the_registered_view_is_typed_from_the_schema_not_inferred(
        self, mocker, feature_view, spark_session
    ):
        # A key column that is entirely NULL is a legitimate spine whose lookups come back NULL.
        # Spark cannot infer a type for it, so registering the view from the pandas frame refused
        # it; the typed Arrow table the Python engine writes is what is registered instead.
        from hsfs.engine import spark as spark_engine_mod

        mocker.patch("hsfs.engine._get_type", return_value="spark")
        frame = pd.DataFrame(
            {
                "country": ["SE"],
                "city": [None],
                "street": ["Sveavagen"],
                "date": [datetime(2026, 3, 1, tzinfo=timezone.utc)],
                "label": pd.Series([None], dtype="Int64"),
            }
        )
        spine = InferenceSpine(feature_view, frame, allow_passthrough=True)
        engine = spark_engine_mod.Engine()

        engine._register_spine_temporary_view(spine, spine.table_name)
        try:
            schema = spark_session.table(spine.table_name).schema
            row = spark_session.sql(
                f"SELECT unix_millis(date) AS ms, city, label FROM {spine.table_name}"
            ).collect()[0]
        finally:
            engine._drop_spine_temporary_view(spine.table_name)

        assert schema["city"].dataType.simpleString() == "string"
        assert schema["label"].dataType.simpleString() == "bigint"
        assert row.ms == int(
            datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp() * 1000
        )
        assert row.city is None and row.label is None

    def test_an_unrecognised_column_is_still_refused(
        self, mocker, feature_view, spark_session
    ):
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        sdf = spark_session.createDataFrame(
            [("stockholm", datetime(2026, 3, 1))], ["citty", "date"]
        )

        with pytest.raises(FeatureStoreException, match="match nothing"):
            InferenceSpine(feature_view, sdf)
