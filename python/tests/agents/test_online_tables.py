from datetime import datetime, timezone

import pytest
from hopsworks_agents.eval import online_tables as ot


pd = pytest.importorskip("pandas")
sqlalchemy = pytest.importorskip("sqlalchemy")


@pytest.fixture
def engine(tmp_path):
    engine = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'online.db'}")
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE agent_feedback_clusters_1 ("
            " deployment_id BIGINT NOT NULL,"
            " cluster_id VARCHAR(64) NOT NULL,"
            " label TEXT,"
            " size INT,"
            " ungradable TINYINT,"
            " first_seen TIMESTAMP,"
            " PRIMARY KEY (deployment_id, cluster_id))"
        )
    yield engine
    engine.dispose()


def rows(engine):
    with engine.connect() as conn:
        return [
            tuple(r)
            for r in conn.exec_driver_sql(
                "SELECT deployment_id, cluster_id, label, size, ungradable, first_seen"
                " FROM agent_feedback_clusters_1 ORDER BY cluster_id"
            )
        ]


class Group:
    name = "agent_feedback_clusters"
    version = 1


class TestUpsert:
    def test_rows_are_written_with_the_tables_columns_only(self, engine):
        frame = pd.DataFrame(
            [{"deployment_id": 3, "cluster_id": "c1", "label": "late", "size": 2}]
        )
        # a column the table does not have is left out, not refused
        frame["not_a_column"] = "x"
        assert ot._upsert(engine, "agent_feedback_clusters_1", frame) == 1
        assert rows(engine) == [(3, "c1", "late", 2, None, None)]

    def test_a_row_written_again_replaces_the_one_with_its_key(self, engine):
        first = pd.DataFrame(
            [{"deployment_id": 3, "cluster_id": "c1", "label": "late", "size": 2}]
        )
        ot._upsert(engine, "agent_feedback_clusters_1", first)
        again = pd.DataFrame(
            [
                {"deployment_id": 3, "cluster_id": "c1", "label": "later", "size": 5},
                {"deployment_id": 3, "cluster_id": "c2", "label": "new", "size": 1},
            ]
        )
        ot._upsert(engine, "agent_feedback_clusters_1", again)
        assert [(r[1], r[2], r[3]) for r in rows(engine)] == [
            ("c1", "later", 5),
            ("c2", "new", 1),
        ]

    def test_missing_values_are_null_and_timestamps_are_utc(self, engine):
        frame = pd.DataFrame(
            [
                {
                    "deployment_id": 3,
                    "cluster_id": "c1",
                    "label": None,
                    "size": float("nan"),
                    "ungradable": True,
                    "first_seen": pd.Timestamp("2026-10-27T10:00:00+02:00"),
                },
                {
                    "deployment_id": 3,
                    "cluster_id": "c2",
                    "label": "x",
                    "size": 1,
                    "ungradable": False,
                    "first_seen": pd.NaT,
                },
            ]
        )
        ot._upsert(engine, "agent_feedback_clusters_1", frame)
        first, second = rows(engine)
        assert first[2] is None and first[3] is None and first[4] == 1
        # stored as the UTC wall time, as the online feature store stores it
        assert str(first[5]).startswith("2026-10-27 08:00:00")
        assert second[5] is None and second[4] == 0

    def test_nothing_to_write_writes_nothing(self, engine):
        empty = pd.DataFrame(columns=["deployment_id", "cluster_id"])
        assert ot._upsert(engine, "agent_feedback_clusters_1", empty) == 0
        assert rows(engine) == []


class TestValues:
    def test_numpy_and_pandas_scalars_become_python_values(self):
        np = pytest.importorskip("numpy")
        assert ot._sql_value(np.int64(4)) == 4
        assert type(ot._sql_value(np.int64(4))) is int
        assert ot._sql_value(pd.NA) is None
        aware = datetime(2026, 10, 27, 10, tzinfo=timezone.utc)
        assert ot._sql_value(aware) == datetime(2026, 10, 27, 10)


class TestWriteOnline:
    def test_the_feature_groups_online_table_is_written_and_the_engine_closed(
        self, engine, monkeypatch
    ):
        disposed = []
        monkeypatch.setattr(ot, "_online_engine", lambda feature_store: engine)
        monkeypatch.setattr(engine, "dispose", lambda: disposed.append(True))
        frame = pd.DataFrame([{"deployment_id": 3, "cluster_id": "c1"}])
        assert ot._write_online(object(), Group(), frame) == 1
        assert rows(engine)[0][:2] == (3, "c1")
        assert disposed == [True]

    def test_the_engine_is_the_job_users_own(self, monkeypatch):
        """Built from the online storage connector, which carries the user the job runs as."""
        made = {}

        class Connector:
            connection_string = "jdbc:mysql://mysql.service:3306/proj"
            arguments = [
                {"name": "user", "value": "proj_alice"},
                {"name": "password", "value": "pw"},
            ]

        class Store:
            def get_online_storage_connector(self):
                return Connector()

        def create_engine(url, **kwargs):
            made["url"] = url
            made["kwargs"] = kwargs
            return object()

        monkeypatch.setattr(sqlalchemy, "create_engine", create_engine)
        ot._online_engine(Store())
        assert made["url"] == "mysql+pymysql://proj_alice:pw@mysql.service:3306/proj"
        assert "time_zone" in made["kwargs"]["connect_args"]["init_command"]
