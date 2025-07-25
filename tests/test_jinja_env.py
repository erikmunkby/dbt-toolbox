"""Module for testing jinja rendering."""

from dbt_toolbox.dbt_parser._jinja_handler import jinja


def test_jinja_simple_render() -> None:
    """Test a very simple jinja render."""
    assert jinja.render("pytest {{ simple_macro() }}") == "pytest \n'A simple macro'\n"
