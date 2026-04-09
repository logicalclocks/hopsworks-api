def clamp(value: float, min_value: float, max_value: float) -> float:
    """Return value clamped to the range [min_value, max_value]."""
    return max(min_value, min(value, max_value))


def percentage(part: float, total: float) -> float:
    """Return what percentage part is of total."""
    if total == 0:
        raise ValueError("total must not be zero")
    return (part / total) * 100
