"""Unit tests for the join-spec parser shared by ``fv create`` and ``fg derive``."""

from __future__ import annotations

import pytest
from hopsworks.cli import joinspec


def test_parses_full_spec():
    spec = joinspec.parse("products:2 LEFT product_id=id p_")
    assert spec.fg_name == "products"
    assert spec.version == 2
    assert spec.join_type == "LEFT"
    assert spec.on == "product_id"
    assert spec.right_on == "id"
    assert spec.prefix == "p_"


def test_parses_minimal_spec():
    spec = joinspec.parse("orders INNER order_id")
    assert spec.fg_name == "orders"
    assert spec.version is None
    assert spec.join_type == "INNER"
    assert spec.on == "order_id"
    assert spec.right_on is None
    assert spec.prefix is None


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
    ],
)
def test_rejects_malformed(bad):
    with pytest.raises(joinspec.JoinSpecError):
        joinspec.parse(bad)
