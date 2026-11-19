"""Unit tests for the join-spec parser shared by ``fv create`` and ``fg derive``."""

from __future__ import annotations

import pytest
from hopsworks.cli import joinspec


def test_parses_full_spec():
    spec = joinspec.parse("products:2 LEFT product_id=id p_")
    assert spec.fg_name == "products"
    assert spec.version == 2
    assert spec.join_type == "LEFT"
    assert spec.on == ["product_id"]
    assert spec.right_on == ["id"]
    assert spec.prefix == "p_"


def test_parses_minimal_spec():
    spec = joinspec.parse("orders INNER order_id")
    assert spec.fg_name == "orders"
    assert spec.version is None
    assert spec.join_type == "INNER"
    assert spec.on == ["order_id"]
    assert spec.right_on is None
    assert spec.prefix is None


def test_parses_composite_key():
    spec = joinspec.parse("snapshots LEFT deal_id,snapshot_date s_")
    assert spec.on == ["deal_id", "snapshot_date"]
    assert spec.right_on is None
    assert spec.join_kwargs() == {
        "on": ["deal_id", "snapshot_date"],
        "left_on": None,
        "right_on": None,
    }
    assert spec.prefix == "s_"


def test_composite_key_with_renamed_column():
    spec = joinspec.parse("snapshots INNER deal_id,snapshot_date=day")
    assert spec.join_kwargs() == {
        "on": None,
        "left_on": ["deal_id", "snapshot_date"],
        "right_on": ["deal_id", "day"],
    }


def test_join_type_is_case_insensitive():
    assert joinspec.parse("a left id").join_type == "LEFT"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "orders",  # no join type
        "orders INNER",  # no column
        "orders MAYBE id",  # bad type
        "123bad INNER id",  # bad identifier
        "orders INNER id,",  # trailing comma
    ],
)
def test_rejects_malformed(bad):
    with pytest.raises(joinspec.JoinSpecError):
        joinspec.parse(bad)
