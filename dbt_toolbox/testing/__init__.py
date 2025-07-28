"""Testing utilities for dbt toolbox."""

from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser import dbt_parser
from dbt_toolbox.testing.column_tests import check_column_documentation


def get_all_models() -> dict[str, Model]:
    """Fetch a dictionary containing all models in the dbt project."""
    return dbt_parser.models


__all__ = ["check_column_documentation", "get_all_models"]
