# ruff: noqa: INP001
"""The dbt models against DuckDB's own TPC-H Q8, without a cluster: python -m pytest tests in the pipeline's directory.

Needs DuckDB, dbt-core and dbt-duckdb, which the dbt-pipeline environment ships.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


_spec = importlib.util.spec_from_file_location(
    "tpch_pipeline", Path(__file__).resolve().parents[1] / "tpch_pipeline.py"
)
pipeline = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pipeline)

SF = 0.1


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    scratch = tmp_path_factory.mktemp("tpch")
    path = scratch / "tpch.duckdb"
    counts = pipeline.generate(path, SF, "300MB", 1)
    assert pipeline.dbt_build(path, scratch / "target", "300MB", 1, None)
    return path, counts, pipeline.read_outputs(path)


def test_dbgen_writes_the_eight_tpch_tables(built):
    _, counts, _ = built
    assert set(counts) == {
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    }
    assert counts["nation"] == 25 and counts["region"] == 5
    assert counts["orders"] == int(1_500_000 * SF)


def test_the_dag_computes_tpch_q8(built):
    import duckdb

    path, _, outputs = built
    con = duckdb.connect(str(path), read_only=True)
    con.execute("LOAD tpch")
    expected = con.execute("PRAGMA tpch(8)").fetch_df()
    con.close()
    got = outputs["brazil_volume_by_year"]
    assert got["o_year"].tolist() == expected["o_year"].tolist() == [1995, 1996]
    assert got["mkt_share"].round(2).tolist() == expected["mkt_share"].round(2).tolist()
    assert (got["mkt_share"] - expected["mkt_share"]).abs().max() < 1e-9


def test_the_nations_add_up_to_the_american_market(built):
    _, _, outputs = built
    by_nation = outputs["america_volume_by_nation_year"]
    total = by_nation.groupby("o_year")["volume"].sum()
    market = outputs["brazil_volume_by_year"].set_index("o_year")["america_volume"]
    assert ((total - market).abs() < 0.01).all()
    brazil = by_nation[by_nation["supplier_nation"] == "BRAZIL"].set_index("o_year")[
        "volume"
    ]
    shares = outputs["brazil_volume_by_year"].set_index("o_year")["mkt_share"]
    assert ((brazil / market - shares).abs() < 1e-9).all()


def test_every_output_has_its_primary_key_and_event_time(built):
    _, _, outputs = built
    for model, (_, primary_key) in pipeline.OUTPUTS.items():
        frame = outputs[model]
        assert not frame.duplicated(primary_key).any(), model
        assert frame["year_start"].notna().all(), model
