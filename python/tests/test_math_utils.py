from hopsworks_common.math_utils import clamp


def test_clamp_below_min():
    assert clamp(1, 5, 10) == 5


def test_clamp_above_max():
    assert clamp(15, 5, 10) == 10


def test_clamp_within_range():
    assert clamp(7, 5, 10) == 7
