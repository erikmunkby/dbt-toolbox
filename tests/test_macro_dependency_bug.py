"""Test for macro dependency bug where models don't get flagged when dependent macros change."""

import time

from dbt_toolbox.cli._analyze_models import ExecutionReason, analyze_model_statuses
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.settings import settings


def test_macro_dependency_detection() -> None:
    """Test that models are flagged as needing execution when their dependent macros change.

    This test reproduces the bug where:
    1. We run dt build
    2. Then modify a macro
    3. Run dt build again
    4. Models that depend on the macro should be flagged as outdated but currently are not
    """
    # Setup: Get the orders model which depends on simple_macro
    dbt_parser = dbtParser()

    # First, ensure the model is built and cached (simulate first dt build)
    orders_model = dbt_parser.get_model("orders")
    assert orders_model is not None, "orders model should exist"
    assert "simple_macro" in orders_model.upstream.macros, "orders should depend on simple_macro"

    # Get initial state before macro change
    initial_analysis = analyze_model_statuses(dbt_parser, "orders")
    assert "orders" in initial_analysis
    initial_needs_execution = initial_analysis["orders"].needs_execution
    _initial_reason = initial_analysis["orders"].reason

    # Now simulate modifying the macro (wait a moment to ensure timestamp difference)
    time.sleep(0.1)
    macro_path = settings.dbt_project_dir / "macros" / "simple_macro.sql"
    original_content = macro_path.read_text()

    try:
        # Modify the macro content to simulate user editing it
        modified_content = original_content.replace(
            "'A super modified simple macro'", "'A newly modified simple macro'"
        )
        macro_path.write_text(modified_content)

        # Create a new parser instance to simulate dt build being run again
        dbt_parser_after_change = dbtParser()

        # Check if the macro change was detected
        assert dbt_parser_after_change.macro_changed("simple_macro"), (
            "simple_macro should be detected as changed"
        )

        # Now analyze models - orders should be flagged as needing execution due to macro change
        analysis_after_change = analyze_model_statuses(dbt_parser_after_change, "orders")
        assert "orders" in analysis_after_change
        after_change_needs_execution = analysis_after_change["orders"].needs_execution
        after_change_reason = analysis_after_change["orders"].reason

        # The key test: if the model was not needing execution initially due to macro dependency,
        # it should now need execution because of the macro change
        # OR if it was already needing execution, the reason should be UPSTREAM_MACRO_CHANGED

        if not initial_needs_execution:
            # If it didn't need execution before, it should now
            assert after_change_needs_execution, (
                "orders should need execution because simple_macro changed"
            )
            assert after_change_reason == ExecutionReason.UPSTREAM_MACRO_CHANGED, (
                f"orders should be flagged with UPSTREAM_MACRO_CHANGED, got {after_change_reason}"
            )
        else:
            # If it was already needing execution, check that macro change is now the reason
            # (higher priority reasons might still apply)
            # At minimum, the macro should be detected as changed
            orders_model_after = dbt_parser_after_change.get_model("orders")
            assert orders_model_after.upstream_macros_changed, (
                "orders model should have upstream_macros_changed=True after macro change"
            )

    finally:
        # Restore original content
        macro_path.write_text(original_content)


def test_macro_dependency_detection_multiple_models() -> None:
    """Test that multiple models depending on the same macro are all flagged when it changes."""
    dbt_parser = dbtParser()

    # Get models and check their macro dependencies
    orders_model = dbt_parser.get_model("orders")
    assert orders_model is not None
    assert "simple_macro" in orders_model.upstream.macros

    # Check if any other models also use simple_macro
    models_using_simple_macro = []
    for model_name, model in dbt_parser.models.items():
        if "simple_macro" in model.upstream.macros:
            models_using_simple_macro.append(model_name)

    # Get initial analysis state
    initial_analysis = analyze_model_statuses(dbt_parser, None)  # All models
    _initial_states = {
        name: (initial_analysis[name].needs_execution, initial_analysis[name].reason)
        for name in models_using_simple_macro
    }

    # Modify the macro
    time.sleep(0.1)
    macro_path = settings.dbt_project_dir / "macros" / "simple_macro.sql"
    original_content = macro_path.read_text()

    try:
        modified_content = original_content.replace(
            "'A super modified simple macro'", "'A yet another modified simple macro'"
        )
        macro_path.write_text(modified_content)

        # New parser instance
        dbt_parser_after_change = dbtParser()

        # All models using simple_macro should now be flagged due to macro change
        analysis_after_change = analyze_model_statuses(dbt_parser_after_change, None)
        _final_states = {
            name: (analysis_after_change[name].needs_execution, analysis_after_change[name].reason)
            for name in models_using_simple_macro
        }

        # Key test: All models should have upstream_macros_changed=True in the model itself
        for model_name in models_using_simple_macro:
            model_after = dbt_parser_after_change.get_model(model_name)
            assert model_after.upstream_macros_changed, (
                f"{model_name} should have upstream_macros_changed=True"
            )

    finally:
        # Restore original content
        macro_path.write_text(original_content)
