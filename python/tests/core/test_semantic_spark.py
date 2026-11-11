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

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import engine
from hsfs.core import delta_engine, semantic_spark


ANSWER = {
    "sql": "SELECT SUM(`amount`) `revenue` FROM `sl_1a2b3c4d_11` JOIN `sl_1a2b3c4d_12`",
    "views": [
        {
            "alias": "sl_1a2b3c4d_11",
            "featureGroupId": 11,
            "name": "orders",
            "version": 1,
            "featureStore": "shop_featurestore",
        },
        {
            "alias": "sl_1a2b3c4d_12",
            "featureGroupId": 12,
            "name": "time_spine_day",
            "version": 1,
            "featureStore": "shop_featurestore",
        },
    ],
}


class FakeSession:
    def __init__(self, time_zone="UTC"):
        self.conf = MagicMock()
        self.conf.get.return_value = time_zone
        self.catalog = MagicMock()
        self.sql = MagicMock(side_effect=lambda q: SimpleNamespace(sparkSession=self))


class FakeEngine:
    """The table registration of hsfs's Spark engine, recording what it is asked to register."""

    def __init__(self, session, connect=False):
        self._spark_session = session
        self._is_connect = connect
        self.delta = []
        self.hudi = []

    def _register_delta_temporary_table(
        self, delta_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        self.delta.append(
            (delta_fg_alias, feature_store_id, feature_store_name, read_options)
        )

    def _register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        self.hudi.append(
            (hudi_fg_alias, feature_store_id, feature_store_name, read_options)
        )


def _feature_group(fmt, name="orders"):
    fg = MagicMock(time_travel_format=fmt, version=1)
    fg.name = name
    fg.read.side_effect = AssertionError("fg.read() applies HOPS_START_TIME")
    fg.as_of.side_effect = AssertionError("never time travels")
    return fg


@pytest.fixture
def session():
    return FakeSession()


@pytest.fixture
def spark_engine(mocker, session):
    fake = FakeEngine(session)
    mocker.patch.object(engine, "_get_type", return_value="spark")
    mocker.patch.object(engine, "_engine", fake)
    mocker.patch.object(engine, "_get_instance", return_value=fake)
    mocker.patch(
        "hsfs.core.feature_store_api.FeatureStoreApi._get",
        return_value=SimpleNamespace(id=5, name="shop_featurestore"),
    )
    return fake


@pytest.fixture
def groups(mocker):
    by_id = {11: _feature_group("DELTA"), 12: _feature_group("DELTA", "time_spine_day")}
    mocker.patch(
        "hsfs.core.feature_group_api.FeatureGroupApi._get_by_id",
        side_effect=lambda fs_id, fg_id: by_id[fg_id],
    )
    return by_id


class TestRegistration:
    def test_each_view_is_registered_raw_then_the_sql_runs(
        self, spark_engine, groups, session
    ):
        df = semantic_spark._query(ANSWER, session)

        assert [(r[0].alias, r[1], r[2], r[3]) for r in spark_engine.delta] == [
            ("sl_1a2b3c4d_11", 5, "shop_featurestore", None),
            ("sl_1a2b3c4d_12", 5, "shop_featurestore", None),
        ]
        assert spark_engine.delta[0][0].feature_group is groups[11]
        session.sql.assert_called_once_with(ANSWER["sql"])
        assert df._hopsworks_semantic_views == ["sl_1a2b3c4d_11", "sl_1a2b3c4d_12"]

    def test_the_scheduler_window_does_not_filter_the_views(
        self, spark_engine, groups, session, monkeypatch
    ):
        monkeypatch.setenv("HOPS_START_TIME", "2026-06-01T10:00:00Z")
        monkeypatch.setenv("HOPS_END_TIME", "2026-06-01T11:00:00Z")

        semantic_spark._query(ANSWER, session)

        for raw, *_ in spark_engine.delta:
            assert raw.left_feature_group_start_timestamp is None
            assert raw.left_feature_group_end_timestamp is None
            # The latest snapshot, as the Delta engine reads an alias without a window.
            reader = SimpleNamespace(
                _feature_group=raw.feature_group, DELTA_DOT_PREFIX="delta."
            )
            assert delta_engine.DeltaEngine._setup_delta_read_opts(reader, raw) == {}

    def test_hudi_is_read_on_classic_spark_only(self, spark_engine, groups, session):
        groups[11].time_travel_format = "HUDI"
        semantic_spark._query(ANSWER, session)
        assert [r[0].alias for r in spark_engine.hudi] == ["sl_1a2b3c4d_11"]

        spark_engine._is_connect = True
        with pytest.raises(FeatureStoreException, match="HUDI on Spark Connect"):
            semantic_spark._query(ANSWER, session)

    def test_another_format_is_refused_and_the_views_already_registered_dropped(
        self, spark_engine, groups, session
    ):
        groups[12].time_travel_format = None

        with pytest.raises(
            FeatureStoreException, match="time_spine_day version 1 is NONE"
        ):
            semantic_spark._query(ANSWER, session)

        session.catalog.dropTempView.assert_called_once_with("sl_1a2b3c4d_11")
        session.sql.assert_not_called()

    def test_drop_views_drops_the_views_of_the_call(
        self, spark_engine, groups, session
    ):
        df = semantic_spark._query(ANSWER, session)

        semantic_spark._drop_views(df)

        assert [c.args[0] for c in session.catalog.dropTempView.call_args_list] == [
            "sl_1a2b3c4d_11",
            "sl_1a2b3c4d_12",
        ]
        with pytest.raises(ValueError, match="engine='spark'"):
            semantic_spark._drop_views(SimpleNamespace(sparkSession=session))


class TestSession:
    def test_the_time_zone_is_checked_before_the_engine_starts_and_never_set(
        self, mocker
    ):
        session = FakeSession(time_zone="Europe/Stockholm")
        mocker.patch.object(engine, "_get_type", return_value="spark")
        mocker.patch.object(engine, "_engine", None)
        get_instance = mocker.patch.object(engine, "_get_instance")

        with pytest.raises(FeatureStoreException, match="'Europe/Stockholm'"):
            semantic_spark._query(ANSWER, session)

        get_instance.assert_not_called()
        session.conf.set.assert_not_called()

    def test_a_session_other_than_the_engines_is_refused(self, spark_engine, groups):
        with pytest.raises(
            FeatureStoreException, match="not the one the hsfs engine holds"
        ):
            semantic_spark._query(ANSWER, FakeSession())

    def test_the_active_session_is_the_default(
        self, spark_engine, groups, session, mocker
    ):
        mocker.patch("pyspark.sql.SparkSession.getActiveSession", return_value=session)

        semantic_spark._query(ANSWER, None)

        session.sql.assert_called_once()

    def test_the_python_engine_is_refused(self, mocker, session):
        mocker.patch.object(engine, "_get_type", return_value="python")

        with pytest.raises(FeatureStoreException, match="Spark engine"):
            semantic_spark._query(ANSWER, session)


class TestSemanticProject:
    def test_engine_spark_asks_sql_for_spark_and_runs_it(self, mocker):
        from hopsworks_common.semantic_layer import SemanticProject

        api = MagicMock()
        api._sql.return_value = ANSWER
        sp = SemanticProject(name="sales", id=7, semantic_layer_api=api)
        run = mocker.patch("hsfs.core.semantic_spark._query", return_value="df")
        session = FakeSession()

        assert sp.query(["revenue"], engine="spark", spark=session) == "df"

        assert api._sql.call_args.args[1] == {"metrics": ["revenue"], "engine": "SPARK"}
        run.assert_called_once_with(ANSWER, session)
        api._query.assert_not_called()

    def test_an_unknown_engine_is_refused(self):
        from hopsworks_common.semantic_layer import SemanticProject

        with pytest.raises(ValueError, match="engine 'duckdb'"):
            SemanticProject(name="sales", id=7).query(["revenue"], engine="duckdb")

    def test_sql_keeps_the_views_in_the_private_answer(self, mocker):
        from hopsworks_common.core.semantic_layer_api import SemanticLayerApi

        api = SemanticLayerApi.__new__(SemanticLayerApi)
        mocker.patch.object(api, "_gateway_json", return_value=ANSWER)

        assert json.dumps(api._sql(None, {"engine": "SPARK"})) == json.dumps(ANSWER)
