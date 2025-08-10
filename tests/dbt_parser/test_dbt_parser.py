"""Test dbt parser."""

from dbt_toolbox.dbt_parser.dbt_parser import dbtParser


def test_load_models() -> None:
    """."""
    dbt = dbtParser()
    assert dbt.models["customers"].name == "customers"
    assert dbt.models["customers"].final_columns == ["customer_id", "full_name"]


def test_macro_changed() -> None:
    """Change a macro, and check that the "macro changed" flag is true."""
    dbtParser()
    # TODO: Implement
