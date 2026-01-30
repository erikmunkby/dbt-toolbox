"""dbt-toolbox: Ultra-fast drop-in replacement for dbt executions.

This module provides the public API for programmatic access to dbt project data.

Functions:
    get_models: Retrieve all parsed dbt models from the project.
    get_sources: Retrieve all parsed dbt sources from the project.
    get_macros: Retrieve all parsed dbt macros from the project.
    get_seeds: Retrieve all parsed dbt seeds from the project.

Classes:
    Model: Data class representing a dbt model.
    Source: Data class representing a dbt source.
    Macro: Data class representing a dbt macro.
    Seed: Data class representing a dbt seed.

Example:
    >>> from dbt_toolbox import get_models
    >>> models = get_models()
    >>> for name, model in models.items():
    ...     print(f"{name}: {len(model.columns)} columns")

"""

from dbt_toolbox._version import __version__
from dbt_toolbox.data_models import Macro, Model, Seed, Source
from dbt_toolbox.dbt_parser import dbtParser


def get_models(target: str | None = None) -> dict[str, Model]:
    """Get all dbt models from the project.

    Args:
        target: Optional dbt target environment to use. If None, uses default target.

    Returns:
        Dictionary mapping model names to Model objects containing parsed model data.

    """
    return dbtParser(target=target).models


def get_sources(target: str | None = None) -> dict[str, Source]:
    """Get all dbt sources from the project.

    Args:
        target: Optional dbt target environment to use. If None, uses default target.

    Returns:
        Dictionary mapping source names to Source objects containing parsed source data.

    """
    return dbtParser(target=target).sources


def get_macros(target: str | None = None) -> dict[str, Macro]:
    """Get all dbt macros from the project.

    Args:
        target: Optional dbt target environment to use. If None, uses default target.

    Returns:
        Dictionary mapping macro names to Macro objects containing parsed macro data.

    """
    return dbtParser(target=target).macros


def get_seeds(target: str | None = None) -> dict[str, Seed]:
    """Get all dbt seeds from the project.

    Args:
        target: Optional dbt target environment to use. If None, uses default target.

    Returns:
        Dictionary mapping macro names to Seed objects containing seeds metadata.

    """
    return dbtParser(target=target).seeds


__all__ = [
    "Macro",
    "Model",
    "Source",
    "__version__",
    "get_macros",
    "get_models",
    "get_seeds",
    "get_sources",
]
