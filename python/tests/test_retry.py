import pytest

from hopsworks_common.retry import is_retryable, retry


def test_retry_succeeds_first_attempt():
    result = retry(lambda: 42)
    assert result == 42


def test_retry_succeeds_after_failures():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("not yet")
        return "ok"

    assert retry(flaky, attempts=3, delay=0) == "ok"


def test_retry_raises_after_all_attempts():
    with pytest.raises(ValueError):
        retry(lambda: (_ for _ in ()).throw(ValueError("fail")), attempts=2, delay=0)


def test_retry_invalid_attempts():
    with pytest.raises(ValueError):
        retry(lambda: None, attempts=0)


def test_is_retryable_true():
    for code in [429, 502, 503, 504]:
        assert is_retryable(code)


def test_is_retryable_false():
    assert not is_retryable(200)
    assert not is_retryable(404)

    # RateLimiter and backoff logic are intentionally not tested here
