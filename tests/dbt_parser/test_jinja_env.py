"""Module for testing jinja rendering."""

from unittest.mock import patch

from dbt_toolbox.dbt_parser._cache import Cache
from dbt_toolbox.dbt_parser._jinja_handler import Jinja
from dbt_toolbox.warnings_collector import warnings_collector


def test_jinja_simple_render() -> None:
    """Test a very simple jinja render."""
    cache = Cache(dbt_target="dev")
    assert (
        Jinja(cache=cache).render("pytest {{ macro_used_for_pytest() }}")
        == "pytest \n'THIS STRING MUST NOT CHANGE'\n"
    )


def test_jinja_unknown_macro_warning() -> None:
    """Test that unknown macros generate warnings instead of errors."""
    # Test rendering with an unknown macro
    cache = Cache(dbt_target="dev")
    result = Jinja(cache=cache).render("SELECT * FROM {{ unknown_macro() }}")
    warnings = warnings_collector.get_warnings()

    # Check that we have a warning with the expected message and category
    assert any(
        "Unknown macro 'unknown_macro' encountered in template." in msg
        and category == "unknown_jinja_macro"
        for msg, category in warnings.items()
    )

    # Verify the result contains the macro call as-is
    assert "{{ unknown_macro() }}" in result
    assert result == "SELECT * FROM {{ unknown_macro() }}"


def test_warnings_ignored_functionality() -> None:
    """Test that warnings can be ignored via settings."""
    # Test that warnings can be ignored via settings
    # This test needs to be isolated, so we'll clear the specific warned macros cache
    cache = Cache(dbt_target="dev")

    # Clear only the warned macros cache for this test
    warned_cache = cache.get_warned_macros_cache()
    original_warned = warned_cache.read()

    try:
        # Clear the warned macros cache for clean test
        warned_cache.write(set())

        # Clear warnings collector for clean test
        warnings_collector.clear()

        # Mock settings to ignore warnings
        with patch("dbt_toolbox.settings.settings.warnings_ignored", ["unknown_jinja_macro"]):
            with patch("dbt_toolbox.utils.log") as mock_log:
                result = Jinja(cache=cache).render("SELECT * FROM {{ ignored_test_macro() }}")

                # Verify no warning was logged
                warning_calls = [
                    call
                    for call in mock_log.call_args_list
                    if len(call.args) > 0 and "Warning: Unknown macro" in call.args[0]
                ]
                assert len(warning_calls) == 0

                # Verify no warning was added to warnings collector
                warnings = warnings_collector.get_warnings()
                assert len(warnings) == 0, "No warnings should be collected when ignored"

                # Verify the result still contains the macro
                assert "{{ ignored_test_macro() }}" in result

    finally:
        # Restore original warned macros
        warned_cache.write(original_warned)


def test_python_modules_datetime() -> None:
    """Test that Python datetime module is available in Jinja."""
    cache = Cache(dbt_target="dev")
    result = Jinja(cache=cache).render(
        '{{ modules.datetime.datetime.strptime("2025-05-01", "%Y-%m-%d").strftime("%B %d, %Y") }}'
    )
    assert result == "May 01, 2025"


def test_python_modules_re() -> None:
    """Test that Python re module is available in Jinja."""
    cache = Cache(dbt_target="dev")
    result = Jinja(cache=cache).render('{{ modules.re.sub("[0-9]+", "X", "abc123def456") }}')
    assert result == "abcXdefX"


def test_python_modules_pytz() -> None:
    """Test that Python pytz module is available in Jinja."""
    cache = Cache(dbt_target="dev")
    result = Jinja(cache=cache).render('{{ modules.pytz.timezone("UTC").zone }}')
    assert result == "UTC"


def test_this_object() -> None:
    """Test that {{ this }} object is available in Jinja."""
    cache = Cache(dbt_target="dev")
    result = Jinja(cache=cache).render("{{ this.database }}.{{ this.schema }}.{{ this.table }}")
    assert result == "__database__.__schema__.__table__"


def test_this_object_with_adapter() -> None:
    """Test that {{ this }} works with adapter.get_relation()."""
    cache = Cache(dbt_target="dev")
    result = Jinja(cache=cache).render(
        """{% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ relation }}"""
    )
    assert "__get_relation__" in result
