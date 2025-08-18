"""Test dbt parser."""

from pathlib import Path

from dbt_toolbox.dbt_parser import dbtParser


def test_load_models() -> None:
    """."""
    dbt = dbtParser()
    assert dbt.models["customers"].name == "customers"
    assert dbt.models["customers"].final_columns == ["customer_id", "full_name"]


def test_macro_changed() -> None:
    """Change a macro, and check that the "macro changed" flag is true."""
    dbtParser()
    # TODO: Implement


def test_code_changes_instant_reflect(temp_model_path: tuple[str, Path]) -> None:
    """Test that code changes are reflected as soon as read."""
    name, path = temp_model_path
    code1 = "select 1"
    path.write_text(code1)
    m = dbtParser().get_model(name)
    assert m is not None
    assert m.raw_code == code1
    assert not m.code_changed

    code2 = "select 2"
    path.write_text(code2)
    m = dbtParser().get_model(name)
    assert m is not None
    assert m.raw_code == code2
    assert m.code_changed  # Now the code should be flagged as changed


def test_materialized_config() -> None:
    """Make sure the materialized config is properly picked up."""
    m = dbtParser().get_model("customer_orders")
    assert m is not None
    assert m.config["materialized"] == "ephemeral"
