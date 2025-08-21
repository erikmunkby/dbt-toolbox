"""Test for macro dependency bug where models don't get flagged when dependent macros change."""

import time

from dbt_toolbox.actions.analyze_models import ExecutionReason, analyze_model_statuses
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
    # Setup: Get the dedicated test model which depends on macro_to_modify_for_pytest
    dbt_parser = dbtParser()

    # First, ensure the model is built and cached (simulate first dt build)
    test_model = dbt_parser.get_model("macro_change_detection_model")
    assert test_model is not None, "macro_change_detection_model should exist"
    assert "macro_to_modify_for_pytest" in test_model.upstream.macros, (
        "macro_change_detection_model should depend on macro_to_modify_for_pytest"
    )

    # Get initial state before macro change
    initial_analysis_list = analyze_model_statuses(dbt_parser, "macro_change_detection_model")
    initial_analysis = {result.model.name: result for result in initial_analysis_list}
    assert "macro_change_detection_model" in initial_analysis
    initial_needs_execution = initial_analysis["macro_change_detection_model"].needs_execution

    # Now simulate modifying the macro (wait a moment to ensure timestamp difference)
    time.sleep(0.1)
    macro_path = settings.dbt_project_dir / "macros" / "macro_to_modify_for_pytest.sql"
    original_content = macro_path.read_text()

    try:
        # Modify the macro content to simulate user editing it
        modified_content = original_content.replace(
            "'Tests will modify this macro. DO NOT EDIT MANUALLY'",
            "'Modified by test - macro dependency detection'",
        )
        macro_path.write_text(modified_content)

        # Create a new parser instance to simulate dt build being run again
        dbt_parser_after_change = dbtParser()

        # Check if the macro change was detected
        assert dbt_parser_after_change.macro_changed("macro_to_modify_for_pytest"), (
            "macro_to_modify_for_pytest should be detected as changed"
        )

        # Now analyze models - the test model should be flagged as needing execution due to
        # macro change
        analysis_after_change_list = analyze_model_statuses(
            dbt_parser_after_change, "macro_change_detection_model"
        )
        analysis_after_change = {
            result.model.name: result for result in analysis_after_change_list
        }
        assert "macro_change_detection_model" in analysis_after_change
        after_change_needs_execution = analysis_after_change[
            "macro_change_detection_model"
        ].needs_execution
        after_change_reason = analysis_after_change["macro_change_detection_model"].reason

        # The key test: if the model was not needing execution initially due to macro dependency,
        # it should now need execution because of the macro change
        # OR if it was already needing execution, the reason should be UPSTREAM_MACRO_CHANGED

        if not initial_needs_execution:
            # If it didn't need execution before, it should now
            assert after_change_needs_execution, (
                "macro_change_detection_model should need execution because "
                "macro_to_modify_for_pytest changed"
            )
            assert after_change_reason == ExecutionReason.UPSTREAM_MACRO_CHANGED, (
                f"macro_change_detection_model should be flagged with "
                f"UPSTREAM_MACRO_CHANGED, got {after_change_reason}"
            )
        else:
            # If it was already needing execution, check that macro change is now the reason
            # (higher priority reasons might still apply)
            # At minimum, the macro should be detected as changed
            test_model_after = dbt_parser_after_change.get_model("macro_change_detection_model")
            assert test_model_after is not None, (
                "macro_change_detection_model should exist after change"
            )
            assert test_model_after.upstream_macros_changed, (
                "macro_change_detection_model should have upstream_macros_changed=True "
                "after macro change"
            )

    finally:
        # Restore original content
        macro_path.write_text(original_content)


def test_analyze_command_consistency_after_macro_change() -> None:
    """Test that dt analyze gives consistent results when run twice after a macro change.

    This ensures that the analyze command itself doesn't affect cache state and gives
    repeatable results for the same codebase state.
    """
    import time

    # Setup: Get the dedicated test model which depends on macro_to_modify_for_pytest
    dbt_parser = dbtParser()

    # First, ensure we have a clean baseline
    test_model = dbt_parser.get_model("macro_change_detection_model")
    assert test_model is not None, "macro_change_detection_model should exist"

    # Modify the macro to create a detectable change
    time.sleep(0.1)
    macro_path = settings.dbt_project_dir / "macros" / "macro_to_modify_for_pytest.sql"
    original_content = macro_path.read_text()

    try:
        # Modify the macro content
        modified_content = original_content.replace(
            "'Tests will modify this macro. DO NOT EDIT MANUALLY'",
            "'Modified by consistency test - macro change'",
        )
        macro_path.write_text(modified_content)

        # Create a fresh parser to detect the change
        dbt_parser_fresh = dbtParser()

        # Run analyze twice with the same parser and verify results are identical
        first_analysis_list = analyze_model_statuses(
            dbt_parser_fresh, "macro_change_detection_model"
        )
        second_analysis_list = analyze_model_statuses(
            dbt_parser_fresh, "macro_change_detection_model"
        )

        first_analysis = {result.model.name: result for result in first_analysis_list}
        second_analysis = {result.model.name: result for result in second_analysis_list}

        # Verify we have results for our test model
        assert "macro_change_detection_model" in first_analysis
        assert "macro_change_detection_model" in second_analysis

        # Extract results for comparison
        first_result = first_analysis["macro_change_detection_model"]
        second_result = second_analysis["macro_change_detection_model"]

        # Key consistency checks
        assert first_result.needs_execution == second_result.needs_execution, (
            f"needs_execution should be consistent: first={first_result.needs_execution}, "
            f"second={second_result.needs_execution}"
        )

        if first_result.reason is not None:
            assert first_result.reason == second_result.reason, (
                f"execution reason should be consistent: first={first_result.reason}, "
                f"second={second_result.reason}"
            )

        assert first_result.reason_description == second_result.reason_description, (
            f"reason description should be consistent: first='{first_result.reason_description}', "
            f"second='{second_result.reason_description}'"
        )

        # Model properties should be identical
        assert first_result.model.name == second_result.model.name
        assert first_result.model.last_built == second_result.model.last_built
        assert (
            first_result.model.upstream_macros_changed
            == second_result.model.upstream_macros_changed
        )

        # Now test with a completely fresh parser for the second analysis
        # This simulates running "dt analyze" twice from the command line
        dbt_parser_second_fresh = dbtParser()
        third_analysis_list = analyze_model_statuses(
            dbt_parser_second_fresh, "macro_change_detection_model"
        )
        third_analysis = {result.model.name: result for result in third_analysis_list}

        assert "macro_change_detection_model" in third_analysis
        third_result = third_analysis["macro_change_detection_model"]

        # Results should still be consistent even with a fresh parser instance
        assert first_result.needs_execution == third_result.needs_execution, (
            f"needs_execution should be consistent across parser instances: "
            f"first={first_result.needs_execution}, third={third_result.needs_execution}"
        )

        if first_result.reason is not None:
            assert first_result.reason == third_result.reason, (
                f"execution reason should be consistent across parser instances: "
                f"first={first_result.reason}, third={third_result.reason}"
            )

    finally:
        # Restore original content
        macro_path.write_text(original_content)


def test_macro_dependency_detection_multiple_models() -> None:
    """Test that multiple models depending on the same macro are all flagged when it changes."""
    dbt_parser = dbtParser()

    # Check if any models use our test macro
    models_using_test_macro = []
    for model_name, model in dbt_parser.models.items():
        if "macro_to_modify_for_pytest" in model.upstream.macros:
            models_using_test_macro.append(model_name)

    # Ensure we have at least one model using the test macro
    assert len(models_using_test_macro) > 0, (
        "At least one model should use macro_to_modify_for_pytest"
    )
    assert "macro_change_detection_model" in models_using_test_macro

    # Modify the macro
    time.sleep(0.1)
    macro_path = settings.dbt_project_dir / "macros" / "macro_to_modify_for_pytest.sql"
    original_content = macro_path.read_text()

    try:
        modified_content = original_content.replace(
            "'Tests will modify this macro. DO NOT EDIT MANUALLY'",
            "'Modified by multiple models test'",
        )
        macro_path.write_text(modified_content)

        # New parser instance
        dbt_parser_after_change = dbtParser()

        # Key test: All models should have upstream_macros_changed=True in the model itself
        for model_name in models_using_test_macro:
            model_after = dbt_parser_after_change.get_model(model_name)
            assert model_after is not None, f"{model_name} should exist after change"
            assert model_after.upstream_macros_changed, (
                f"{model_name} should have upstream_macros_changed=True"
            )

    finally:
        # Restore original content
        macro_path.write_text(original_content)
