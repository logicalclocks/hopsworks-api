"""Creates the feature groups and feature views the locust users read from.

locust_fg: `ip` primary key plus 10 features.
locust_ip_meta_fg: `ip` primary key plus `region` and `risk_score`.
locust_fv reads locust_fg; locust_join_fv joins both on `ip`.
Re-running upserts the same rows.
Called by locustfile.py at startup unless skip-setup is set, or on its own: `python setup_data.py <rows>`.
"""

import sys

import numpy as np
import pandas as pd
from common import (
    FG_NAME,
    FV_NAME,
    JOIN_FV_NAME,
    META_FG_NAME,
    VERSION,
    login,
)


def features_df(rows):
    rng = np.random.default_rng(0)
    now = pd.Timestamp("2026-09-29")
    return pd.DataFrame(
        {
            "ip": range(rows),
            "ts_1": now,
            "ts_2": now + pd.Timedelta(hours=1),
            "int_1": rng.integers(0, 100_000, rows),
            "int_2": rng.integers(0, 100_000, rows),
            "float_1": rng.random(rows),
            "float_2": rng.random(rows),
            "string_1": [f"a{i}" for i in range(rows)],
            "string_2": [f"b{i}" for i in range(rows)],
            "string_3": [f"c{i}" for i in range(rows)],
            "string_4": [f"d{i}" for i in range(rows)],
        }
    )


def meta_df(rows):
    return pd.DataFrame(
        {
            "ip": range(rows),
            "region": [["eu", "us", "apac"][i % 3] for i in range(rows)],
            "risk_score": [i / rows for i in range(rows)],
        }
    )


def get_or_create_fg(fs, name, df):
    fg = fs.get_or_create_feature_group(
        name=name,
        version=VERSION,
        primary_key=["ip"],
        online_enabled=True,
        statistics_config=False,
    )
    fg.insert(df, write_options={"wait_for_job": True, "wait_for_online_ingestion": True})
    return fg


def setup(fs, rows):
    fg = get_or_create_fg(fs, FG_NAME, features_df(rows))
    meta_fg = get_or_create_fg(fs, META_FG_NAME, meta_df(rows))
    fs.get_or_create_feature_view(name=FV_NAME, version=VERSION, query=fg.select_all())
    fs.get_or_create_feature_view(
        name=JOIN_FV_NAME,
        version=VERSION,
        query=fg.select_all().join(meta_fg.select(["region", "risk_score"]), on=["ip"]),
    )
    print(f"{FG_NAME}, {META_FG_NAME}: {rows} rows; views {FV_NAME}, {JOIN_FV_NAME}")


if __name__ == "__main__":
    setup(login().get_feature_store(), int(sys.argv[1]))
