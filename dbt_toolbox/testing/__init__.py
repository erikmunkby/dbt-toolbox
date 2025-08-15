"""Testing utilities for dbt toolbox."""

from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.testing.column_tests import check_column_documentation


def get_all_models(target: str | None = None) -> dict[str, Model]:
    """Fetch a dictionary containing all models in the dbt project.

    Args:
        target: The dbt target, if None the default model will be used.

    """
    return dbtParser(target=target).models


__all__ = ["check_column_documentation", "get_all_models"]
