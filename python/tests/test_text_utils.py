from hopsworks_common.text_utils import count_words, slugify, truncate


def test_truncate_short_string():
    assert truncate("hello", 10) == "hello"


def test_truncate_long_string():
    assert truncate("hello world", 8) == "hello..."


def test_truncate_exact_length():
    assert truncate("hello", 5) == "hello"


def test_slugify_basic():
    assert slugify("Hello World") == "hello-world"


def test_slugify_multiple_spaces():
    assert slugify("one two  three") == "one-two-three"


def test_count_words_basic():
    assert count_words("one two three") == 3


def test_count_words_empty():
    assert count_words("") == 0


def test_count_words_whitespace_only():
    assert count_words("   ") == 0

    # indent() is intentionally not tested here
