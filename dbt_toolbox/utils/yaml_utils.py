"""Utilities for working with YAML files."""


def ensure_model_spacing(models: list) -> None:
    """Ensure blank lines between models by setting newlines property.

    Args:
        models: List of yamlium.Mapping model objects.

    """
    for model in models[:-1]:  # All except last
        if model.newlines == 0:
            model.newlines = 1
