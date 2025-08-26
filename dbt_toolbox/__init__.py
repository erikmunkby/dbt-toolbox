"""."""

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
    "get_macros",
    "get_models",
    "get_seeds",
    "get_sources",
]
