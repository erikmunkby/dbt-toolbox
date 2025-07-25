"""Test dbt parser."""

from dbt_toolbox.dbt_parser.dbt_parser import dbtParser


def test_load_models() -> None:
    """."""
    dbt = dbtParser()
    assert dbt.models["customers"].name == "customers"
    assert dbt.models["customers"].compiled_columns == ["customer_id", "full_name"]
