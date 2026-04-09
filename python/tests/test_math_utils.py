import pytest

from hopsworks_common.math_utils import clamp, normalize


def test_clamp_below_min():
    assert clamp(1, 5, 10) == 5


def test_clamp_above_max():
    assert clamp(15, 5, 10) == 10


def test_clamp_within_range():
    assert clamp(7, 5, 10) == 7


def test_normalize_midpoint():
    assert normalize(5, 0, 10) == 0.5


def test_normalize_min():
    assert normalize(0, 0, 10) == 0.0


def test_normalize_max():
    assert normalize(10, 0, 10) == 1.0


def test_normalize_raises_when_equal():
    with pytest.raises(ValueError):
        normalize(5, 5, 5)

    # percentage() is intentionally not tested here
