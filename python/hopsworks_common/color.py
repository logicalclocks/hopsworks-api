"""Utilities for terminal color formatting."""


def colorize(text: str, color: str) -> str:
    """Wrap text in ANSI color codes.

    Args:
        text: The text to colorize.
        color: One of 'red', 'green', 'yellow', 'blue'.

    Returns:
        The text wrapped in ANSI escape codes, or the original text if the
        color is not recognized.
    """
    codes = {
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
    }
    reset = "\033[0m"
    code = codes.get(color)
    if code is None:
        return text
    return f"{code}{text}{reset}"


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from a string.

    Args:
        text: The text to strip.

    Returns:
        The text with all ANSI escape codes removed.
    """
    import re

    return re.sub(r"\033\[[0-9;]*m", "", text)


def truncate(text: str, max_length: int, ellipsis: str = "...") -> str:
    """Truncate text to a maximum length.

    Args:
        text: The text to truncate.
        max_length: The maximum number of characters.
        ellipsis: The string appended when truncation occurs.

    Returns:
        The original text if it fits, otherwise the truncated text with the
        ellipsis appended.
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(ellipsis)] + ellipsis
