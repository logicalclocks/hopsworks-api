from hopsworks_common.color import colorize, truncate


class TestColorize:
    def test_known_color(self):
        result = colorize("hello", "red")
        assert result == "\033[31mhello\033[0m"

    def test_unknown_color_returns_original(self):
        result = colorize("hello", "purple")
        assert result == "hello"

    def test_all_colors(self):
        for color in ("red", "green", "yellow", "blue"):
            result = colorize("x", color)
            assert result.startswith("\033[")
            assert result.endswith("x\033[0m")


class TestTruncate:
    def test_short_text_unchanged(self):
        assert truncate("hi", 10) == "hi"

    def test_exact_length_unchanged(self):
        assert truncate("hello", 5) == "hello"

    def test_long_text_truncated(self):
        assert truncate("hello world", 8) == "hello..."

    def test_custom_ellipsis(self):
        assert truncate("hello world", 8, "…") == "hello w…"
