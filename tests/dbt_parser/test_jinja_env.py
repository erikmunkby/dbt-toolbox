"""Module for testing jinja rendering."""

from unittest.mock import patch

from dbt_toolbox.dbt_parser._jinja_handler import Jinja


def test_jinja_simple_render() -> None:
    """Test a very simple jinja render."""
    assert (
        Jinja().render("pytest {{ macro_used_for_pytest() }}")
        == "pytest \n'THIS STRING MUST NOT CHANGE'\n"
    )


def test_jinja_unknown_macro_warning() -> None:
    """Test that unknown macros generate warnings instead of errors."""
    with patch("dbt_toolbox.utils.log") as mock_log:
        # Test rendering with an unknown macro
        result = Jinja().render("SELECT * FROM {{ unknown_macro() }}")

        # Verify that a warning was logged
        mock_log.assert_called_with(
            "Warning: Unknown macro 'unknown_macro' encountered in template", level="WARN"
        )

        # Verify the result contains the macro call as-is
        assert "{{ unknown_macro() }}" in result
        assert result == "SELECT * FROM {{ unknown_macro() }}"


def test_jinja_unknown_macro_no_duplicate_warnings() -> None:
    """Test that the same unknown macro doesn't generate duplicate warnings."""
    from dbt_toolbox.dbt_parser._cache import cache

    # Clear the warned macros cache to start fresh for this test
    warned_cache = cache.get_warned_macros_cache()
    original_warned = warned_cache.read()

    try:
        # Start with clean cache for this test
        warned_cache.write(set())

        with patch("dbt_toolbox.utils.log") as mock_log:
            jinja = Jinja()

            # Render the same unknown macro twice
            jinja.render("{{ fresh_unknown_macro() }}")
            jinja.render("{{ fresh_unknown_macro() }} and {{ fresh_unknown_macro() }}")

            # Check that the warning was called exactly once
            warning_calls = [
                call
                for call in mock_log.call_args_list
                if len(call.args) > 0 and "Warning: Unknown macro" in call.args[0]
            ]
            assert len(warning_calls) == 1

            # Verify the warning content
            warning_call = warning_calls[0]
            assert (
                warning_call.args[0]
                == "Warning: Unknown macro 'fresh_unknown_macro' encountered in template"
            )
            assert warning_call.kwargs.get("level") == "WARN"

    finally:
        # Restore original warned macros
        warned_cache.write(original_warned)


def test_warnings_ignored_functionality() -> None:
    """Test that warnings can be ignored via settings."""
    from unittest.mock import patch

    # Test that warnings can be ignored via settings
    # This test needs to be isolated, so we'll clear the specific warned macros cache
    from dbt_toolbox.dbt_parser._cache import cache

    # Clear only the warned macros cache for this test
    warned_cache = cache.get_warned_macros_cache()
    original_warned = warned_cache.read()

    try:
        # Clear the warned macros cache for clean test
        warned_cache.write(set())

        # Mock settings to ignore warnings
        with patch("dbt_toolbox.settings.settings.warnings_ignored", ["unknown_jinja_macro"]):
            with patch("dbt_toolbox.utils.log") as mock_log:
                result = Jinja().render("SELECT * FROM {{ ignored_test_macro() }}")

                # Verify no warning was logged
                warning_calls = [
                    call
                    for call in mock_log.call_args_list
                    if len(call.args) > 0 and "Warning: Unknown macro" in call.args[0]
                ]
                assert len(warning_calls) == 0

                # Verify the result still contains the macro
                assert "{{ ignored_test_macro() }}" in result

    finally:
        # Restore original warned macros
        warned_cache.write(original_warned)
