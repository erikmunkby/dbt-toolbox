"""Module for dict utils."""


def remove_empty_values(data: dict) -> dict:
    """Recursively remove key-value pairs where the value is empty."""
    if isinstance(data, dict):
        return {k: remove_empty_values(v) for k, v in data.items() if v or v == 0}  # type: ignore
    if isinstance(data, list):
        return [remove_empty_values(item) for item in data if (item or item == 0)]
    return data
