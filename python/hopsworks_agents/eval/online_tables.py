"""Writing the analysis jobs' feature groups over SQL, as the user the job runs as.

The failure analysis and the trace featurization write their rows to the online
tables of feature groups the platform provisions. They used to do it with
``fg.insert(storage="online")``, which goes through the project's Kafka topic --
and the broker lets only a data owner produce to it, so a job a data scientist
started failed at its last step, after the model calls were paid for. Over SQL
the job writes with the online database user of whoever started it, and
Hopsworks grants every member who is not a data owner INSERT, UPDATE and DELETE
on exactly these tables.

Online only, as the inserts were: nothing here touches the offline side.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


#: Rows per statement: a review run writes tens of rows, a featurization run
#: up to a few thousand, and one statement per row was the slow part.
BATCH_SIZE = 500


def _online_engine(feature_store: Any) -> Engine:
    """An engine on the project's online database, as the user this job runs as.

    The online storage connector carries that user's credentials; the MYSQL_*
    variables an agent pod gets are not given to jobs.
    """
    from hopsworks_agents.protocol.memory import _url_from_connector  # noqa: PLC0415
    from sqlalchemy import create_engine  # noqa: PLC0415

    url = _url_from_connector(feature_store.get_online_storage_connector())
    # UTC, as the online feature store writes timestamps: the server's own zone
    # would shift every naive datetime written here by its offset.
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"init_command": "SET time_zone = '+00:00'"},
    )


def _sql_value(value: Any) -> Any:
    """A value the driver can bind: no pandas or numpy scalars, no NaN, timestamps naive UTC."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        import pandas as pd  # noqa: PLC0415

        if value is pd.NaT or value is pd.NA:
            return None
        if isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
    except ImportError:
        pass
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    item = getattr(value, "item", None)
    if callable(item):  # a numpy scalar
        return _sql_value(item())
    return value


def _records(frame: Any, columns: set[str]) -> list[dict[str, Any]]:
    """The frame's rows as dicts of the table's columns only, every row with the same keys."""
    names = [c for c in frame.columns if c in columns]
    return [
        {name: _sql_value(value) for name, value in zip(names, row, strict=True)}
        for row in frame[names].itertuples(index=False, name=None)
    ]


def _upsert(engine: Engine, table_name: str, frame: Any) -> int:
    """Insert the frame's rows, replacing a row whose primary key is already there.

    Replacing is what the feature group insert did: the clusters are written
    again each run under the same cluster_id, and a featurized trace is written
    again when it is featurized again.
    """
    from sqlalchemy import MetaData, Table  # noqa: PLC0415

    table = Table(table_name, MetaData(), autoload_with=engine)
    rows = _records(frame, set(table.c.keys()))
    if not rows:
        return 0
    keys = {c.name for c in table.primary_key.columns}
    updated = [name for name in rows[0] if name not in keys]
    # Rows of key columns only have nothing to replace: one already there stays.
    if engine.dialect.name == "mysql":
        from sqlalchemy.dialects.mysql import insert  # noqa: PLC0415

        stmt = insert(table)
        if updated:
            stmt = stmt.on_duplicate_key_update(
                {name: stmt.inserted[name] for name in updated}
            )
        else:
            stmt = stmt.prefix_with("IGNORE")
    else:  # sqlite, for the tests
        from sqlalchemy.dialects.sqlite import insert  # noqa: PLC0415

        stmt = insert(table)
        if updated:
            stmt = stmt.on_conflict_do_update(
                index_elements=sorted(keys),
                set_={name: stmt.excluded[name] for name in updated},
            )
        else:
            stmt = stmt.on_conflict_do_nothing()
    with engine.begin() as conn:
        for start in range(0, len(rows), BATCH_SIZE):
            conn.execute(stmt, rows[start : start + BATCH_SIZE])
    return len(rows)


def _write_online(feature_store: Any, group: Any, frame: Any) -> int:
    """Write the frame to the feature group's online table; the number of rows written."""
    engine = _online_engine(feature_store)
    try:
        return _upsert(engine, f"{group.name}_{group.version}", frame)
    finally:
        engine.dispose()
