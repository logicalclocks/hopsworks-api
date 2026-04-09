def clamp(value: float, min_value: float, max_value: float) -> float:
    """Return value clamped to the range [min_value, max_value].

    Args:
        value: The value to clamp.
        min_value: The lower bound.
        max_value: The upper bound.

    Returns:
        The clamped value.
    """
    return max(min_value, min(value, max_value))


def percentage(part: float, total: float) -> float:
    """Return what percentage part is of total.

    Args:
        part: The partial value.
        total: The total value.

    Returns:
        The percentage as a float between 0 and 100.
    """
    if total == 0:
        raise ValueError("total must not be zero")
    return (part / total) * 100
