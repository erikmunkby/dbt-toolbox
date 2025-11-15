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
    assert m.config["materialized"] == "table"


def test_parse_selection_query_upstream() -> None:
    """Test that +model correctly includes upstream models."""
    parser = dbtParser()

    # Test +customer_orders should include customers and orders (upstream models)
    result = parser.parse_selection_query("+customer_orders")
    assert "customer_orders" in result.model_names
    assert "customers" in result.model_names
    assert "orders" in result.model_names
    assert len(result.model_names) == 3


def test_parse_selection_query_downstream() -> None:
    """Test that model+ correctly includes downstream models."""
    parser = dbtParser()

    # Test customers+ should include customer_orders (downstream model)
    result = parser.parse_selection_query("customers+")
    assert "customers" in result.model_names
    assert "customer_orders" in result.model_names


def test_parse_selection_query_both() -> None:
    """Test that +model+ correctly includes both upstream and downstream models."""
    parser = dbtParser()

    # Test +orders+ should include orders, customer_orders (downstream), but not customers
    result = parser.parse_selection_query("+orders+")
    assert "orders" in result.model_names
    assert "customer_orders" in result.model_names


def test_parse_selection_query_with_unparseable_upstream(
    temp_model_path: tuple[str, Path],
) -> None:
    """Test that +model warns about unparseable upstream models instead of silently ignoring them.

    This test reproduces the bug where upstream models that fail to parse are silently
    excluded from the selection, causing +my_model to only return [my_model] instead of
    [my_model, upstream1, upstream2, ...].
    """
    # Create a model with invalid SQL that will fail to parse
    broken_name, broken_path = temp_model_path
    broken_path.write_text(
        "SELECT * FROM customers WHERE this is totally broken SQL syntax @@@ ### !!!"
    )

    # Create another temp model that depends on the broken one
    from dbt_toolbox.settings import settings

    models_dir = Path(settings.dbt_project_dir) / "models"
    dependent_path = models_dir / "temp_depends_on_broken.sql"
    dependent_path.write_text(f"SELECT * FROM {{{{ ref('{broken_name}') }}}}")  # noqa: S608

    try:
        parser = dbtParser()

        # The broken model should not be in parser.models (it failed to parse)
        assert broken_name not in parser.models

        # The dependent model should exist and know about its upstream dependency
        assert "temp_depends_on_broken" in parser.models
        dependent_model = parser.models["temp_depends_on_broken"]
        assert broken_name in dependent_model.upstream.models

        # BUG: When we query +temp_depends_on_broken, it should include the broken upstream
        # or at least warn about it, but currently it silently excludes it
        result = parser.parse_selection_query("+temp_depends_on_broken")

        # This assertion will FAIL with the current buggy code because broken_name is excluded
        # After the fix, this should either:
        # 1. Include broken_name in the result (preferred), OR
        # 2. Raise a warning/error about the missing upstream dependency
        assert broken_name in result.model_names, (
            f"Expected {broken_name} to be included in +temp_depends_on_broken selection, "
            f"but got: {result.model_names}. Upstream models that fail to parse should not be "
            "silently ignored."
        )
        assert "temp_depends_on_broken" in result.model_names

    finally:
        # Clean up
        if dependent_path.exists():
            dependent_path.unlink()
