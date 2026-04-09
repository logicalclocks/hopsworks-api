#
#   Copyright 2024 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from __future__ import annotations


def truncate(text: str, max_length: int, suffix: str = "...") -> str:
    """Return text truncated to max_length, appending suffix if truncated.

    Args:
        text: The string to truncate.
        max_length: Maximum allowed length including suffix.
        suffix: String appended when truncation occurs.

    Returns:
        The original string if short enough, otherwise a truncated version.
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


def slugify(text: str) -> str:
    """Convert text to a lowercase slug with words joined by hyphens.

    Args:
        text: The string to slugify.

    Returns:
        A lowercase hyphen-separated slug.
    """
    return "-".join(text.lower().split())


def count_words(text: str) -> int:
    """Return the number of words in text.

    Args:
        text: The string to count words in.

    Returns:
        Word count as an integer.
    """
    if not text or not text.strip():
        return 0
    return len(text.split())


def indent(text: str, spaces: int = 4) -> str:
    """Indent every line of text by a given number of spaces.

    Args:
        text: The multiline string to indent.
        spaces: Number of spaces to prepend to each line.

    Returns:
        The indented string.
    """
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.splitlines())
