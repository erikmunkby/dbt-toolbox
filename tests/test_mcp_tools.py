"""Tests for MCP tools to ensure they return valid JSON and handle common issues."""

import json

import pytest

from dbt_toolbox.actions.all_settings import get_all_settings
from dbt_toolbox.mcp import (
    tool_analyze_models,
    tool_build_model,
    tool_generate_docs,
    tool_list_dbt_objects,
    tool_show_docs,
)
from dbt_toolbox.mcp._utils import mcp_json_response


def list_settings_wrapper(target: str | None = None) -> str:
    """Wrapper for list_settings functionality to test it directly."""
    try:
        all_settings = get_all_settings(target=target)
        return mcp_json_response(
            {name: setting._asdict() for name, setting in all_settings.items()}
        )
    except Exception as e:  # noqa: BLE001
        return mcp_json_response({"status": "error", "message": f"Failed to get settings: {e!s}"})


@pytest.mark.parametrize(
    ("tool_name", "tool_func", "expected_keys"),
    [
        ("analyze_models", lambda: tool_analyze_models.analyze_models(), ["status"]),
        (
            "analyze_models_with_params",
            lambda: tool_analyze_models.analyze_models(target="dev", model="customer_orders"),
            ["status"],
        ),
        (
            "list_dbt_objects",
            lambda: tool_list_dbt_objects.list_dbt_objects(),
            ["status", "count", "items"],
        ),
        (
            "show_docs",
            lambda: tool_show_docs.show_docs(model_name="customer_orders"),
            [],
        ),
        (
            "show_docs_with_params",
            lambda: tool_show_docs.show_docs(
                model_name="customer_orders", model_type="model", target="dev"
            ),
            [],
        ),
        (
            "generate_docs",
            lambda: tool_generate_docs.generate_docs(model="customer_orders"),
            [],
        ),
        (
            "generate_docs_with_params",
            lambda: tool_generate_docs.generate_docs(
                model="customer_orders", target="dev", fix_inplace=False
            ),
            [],
        ),
        (
            "build_models",
            lambda: tool_build_model.build_models(),
            [],
        ),
        (
            "build_models_with_params",
            lambda: tool_build_model.build_models(
                model="customer_orders",
                full_refresh=False,
                target="dev",
                force=False,
            ),
            [],
        ),
        ("list_settings", lambda: list_settings_wrapper(), []),
        ("list_settings_with_target", lambda: list_settings_wrapper(target="dev"), []),
    ],
)
class TestMcpToolsJsonSerialization:
    """Test that all MCP tools return valid JSON and don't have serialization issues."""

    def test_tool_returns_valid_json(
        self, tool_name: str, tool_func: callable, expected_keys: list[str]
    ) -> None:
        """Test that MCP tool returns valid JSON with expected keys."""
        result = tool_func()

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict), f"{tool_name} did not return a dict"

        # Check for expected keys if specified
        for key in expected_keys:
            assert key in parsed, f"{tool_name} missing expected key: {key}"


class TestListDbtObjectsSpecificTests:
    """Specific tests for list_dbt_objects that require special validation."""

    def test_list_dbt_objects_path_serialization(self) -> None:
        """Test that list_dbt_objects properly serializes file paths as strings."""
        result = tool_list_dbt_objects.list_dbt_objects()

        # Should be valid JSON (this would fail with PosixPath serialization issue)
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "items" in parsed

        # Ensure all paths are strings, not PosixPath objects
        for item in parsed["items"]:
            if "sql_path" in item:
                assert isinstance(item["sql_path"], str)
            if "yaml_path" in item and item["yaml_path"] is not None:
                assert isinstance(item["yaml_path"], str)

    @pytest.mark.parametrize(
        "filter_args",
        [
            {"pattern": "customer"},
            {"type": "model"},
            {"type": "source"},
            {"pattern": "^staging_.*"},
        ],
    )
    def test_list_dbt_objects_with_filters(self, filter_args: dict) -> None:
        """Test that list_dbt_objects with various filters returns valid JSON."""
        result = tool_list_dbt_objects.list_dbt_objects(**filter_args)
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "status" in parsed


class TestMcpToolsErrorHandling:
    """Test that MCP tools handle errors gracefully."""

    def test_analyze_models_handles_invalid_model_selection(self) -> None:
        """Test that analyze_models handles invalid model selection gracefully."""
        result = tool_analyze_models.analyze_models(model="non_existent_model")

        # Should still return valid JSON even if model doesn't exist
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_list_dbt_objects_handles_invalid_regex(self) -> None:
        """Test that list_dbt_objects handles invalid regex patterns."""
        result = tool_list_dbt_objects.list_dbt_objects(pattern="[invalid_regex")

        # Should return valid JSON with error message
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert parsed.get("status") == "error"
        assert "Invalid regex pattern" in parsed.get("message", "")

    def test_show_docs_handles_non_existent_model(self) -> None:
        """Test that show_docs handles non-existent models gracefully."""
        result = tool_show_docs.show_docs(model_name="completely_fake_model")

        # Should return valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_generate_docs_handles_non_existent_model(self) -> None:
        """Test that generate_docs handles non-existent models gracefully."""
        result = tool_generate_docs.generate_docs(model="completely_fake_model")

        # Should return valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)


class TestMcpToolsDataStructure:
    """Test that MCP tools return expected data structures."""

    def test_analyze_models_returns_expected_structure(self) -> None:
        """Test that analyze_models returns the expected improved structure."""
        result = tool_analyze_models.analyze_models()
        parsed = json.loads(result)

        # Check for new improved structure
        assert "status" in parsed
        assert "summary" in parsed
        # Note: models_with_issues may be removed if empty by remove_empty_values utility
        assert "analysis_complete" in parsed

        # Check summary structure
        summary = parsed["summary"]
        assert "total_models_analyzed" in summary
        assert "models_with_issues" in summary
        assert "total_issues_found" in summary
        assert "issue_breakdown" in summary

        # Test with a model that should have issues to ensure structure is correct
        result_with_issues = tool_analyze_models.analyze_models(
            model="model_with_nonexistant_macro"
        )
        parsed_with_issues = json.loads(result_with_issues)

        if parsed_with_issues.get("status") == "HAS_ISSUES":
            # When there are issues, models_with_issues should be present
            assert "models_with_issues" in parsed_with_issues
            assert isinstance(parsed_with_issues["models_with_issues"], list)

    def test_list_dbt_objects_returns_expected_structure(self) -> None:
        """Test that list_dbt_objects returns expected structure."""
        result = tool_list_dbt_objects.list_dbt_objects()
        parsed = json.loads(result)

        assert "status" in parsed
        assert "count" in parsed
        assert "items" in parsed
        assert isinstance(parsed["items"], list)

        # Check item structure if any items exist
        if parsed["items"]:
            item = parsed["items"][0]
            assert "object_type" in item
            assert item["object_type"] in ["model", "source"]

            if item["object_type"] == "model":
                assert "model_name" in item
                assert "sql_path" in item
            elif item["object_type"] == "source":
                assert "source_name" in item
                assert "table_name" in item
                assert "full_name" in item


class TestMcpToolsValidationFeedback:
    """Test that MCP tools provide proper validation feedback."""

    def test_analyze_models_distinguishes_source_yaml_errors(self) -> None:
        """Test that analyze_models provides clear feedback for source YAML errors."""
        # Create a test model that references a source column not in YAML
        from pathlib import Path

        from dbt_toolbox.settings import settings

        # Create a temporary model that references missing source column
        models_dir = Path(settings.dbt_project_dir) / "models"
        test_model_path = models_dir / "test_source_yaml_error.sql"

        try:
            # Create a model that references a non-existent column in a source
            test_model_path.write_text(
                """
                select
                    id,
                    nonexistent_column  -- This column is not in sources.yml
                from {{ source('raw_data', 'raw_orders') }}
                """
            )

            # Run analysis
            result = tool_analyze_models.analyze_models(model="test_source_yaml_error")
            parsed = json.loads(result)

            # If there are issues, check the feedback
            if parsed.get("status") == "HAS_ISSUES":
                models_with_issues = parsed.get("models_with_issues", [])
                if models_with_issues:
                    # Find the test model
                    test_model_result = next(
                        (
                            m
                            for m in models_with_issues
                            if m["model_name"] == "test_source_yaml_error"
                        ),
                        None,
                    )
                    if test_model_result:
                        issues = test_model_result.get("issues", [])
                        # Look for missing column issues
                        missing_col_issues = [i for i in issues if i["type"] == "missing_columns"]
                        if missing_col_issues:
                            issue = missing_col_issues[0]
                            # Check that it identifies this as a source
                            assert issue["details"].get("is_source") is True, (
                                "Should identify raw_orders as a source"
                            )
                            # Check the description mentions YAML
                            assert "YAML" in issue["description"], (
                                "Should mention YAML for source errors"
                            )
                            assert "schema.yml" in issue["recommendation"], (
                                "Should recommend updating schema.yml"
                            )
        finally:
            # Clean up test model
            if test_model_path.exists():
                test_model_path.unlink()

    def test_build_models_surfaces_validation_errors(self) -> None:
        """Test that build_models returns proper error message when validation fails."""
        # Create a test model with validation error
        from pathlib import Path

        from dbt_toolbox.settings import settings

        models_dir = Path(settings.dbt_project_dir) / "models"
        test_model_path = models_dir / "test_validation_error.sql"

        try:
            # Create a model that references a non-existent model
            test_model_path.write_text(
                """
                select * from {{ ref('completely_nonexistent_model') }}
                """
            )

            # Try to build (validation enabled by default)
            result = tool_build_model.build_models(model="test_validation_error", force=False)
            parsed = json.loads(result)

            # Should get validation_failed status with clear message
            if parsed.get("status") == "validation_failed":
                assert "validation_passed" in parsed
                assert parsed["validation_passed"] is False
                assert "message" in parsed
                # Message should guide user to analyze_models or force=True
                assert (
                    "analyze_models" in parsed["message"]
                    or "validation failed" in parsed["message"].lower()
                )
        finally:
            # Clean up test model
            if test_model_path.exists():
                test_model_path.unlink()


class TestMcpToolsPerformance:
    """Basic performance and integration tests."""

    def test_all_tools_respond_within_reasonable_time(self) -> None:
        """Test that all MCP tools respond within a reasonable time."""
        import time

        tools_to_test = [
            ("analyze_models", lambda: tool_analyze_models.analyze_models()),
            ("list_dbt_objects", lambda: tool_list_dbt_objects.list_dbt_objects()),
            ("show_docs", lambda: tool_show_docs.show_docs("customer_orders")),
            ("generate_docs", lambda: tool_generate_docs.generate_docs("customer_orders")),
            ("build_models", lambda: tool_build_model.build_models()),
            ("list_settings", lambda: list_settings_wrapper()),
        ]

        for tool_name, tool_func in tools_to_test:
            start_time = time.time()
            result = tool_func()
            end_time = time.time()

            # Should complete within 30 seconds (reasonable for dbt operations)
            duration = end_time - start_time
            assert duration < 30, f"{tool_name} took {duration:.2f}s, which is too long"

            # Should return valid JSON
            parsed = json.loads(result)
            assert isinstance(parsed, dict), f"{tool_name} did not return a valid JSON dict"
