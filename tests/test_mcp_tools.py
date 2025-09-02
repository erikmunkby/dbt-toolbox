"""Tests for MCP tools to ensure they return valid JSON and handle common issues."""

import json

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


class TestMcpToolsJsonSerialization:
    """Test that all MCP tools return valid JSON and don't have serialization issues."""

    def test_analyze_models_returns_valid_json(self) -> None:
        """Test that analyze_models returns valid JSON."""
        result = tool_analyze_models.analyze_models()

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "status" in parsed

    def test_analyze_models_with_params_returns_valid_json(self) -> None:
        """Test that analyze_models with parameters returns valid JSON."""
        result = tool_analyze_models.analyze_models(target="dev", model="customer_orders")

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "status" in parsed

    def test_list_dbt_objects_returns_valid_json(self) -> None:
        """Test that list_dbt_objects returns valid JSON without PosixPath issues."""
        result = tool_list_dbt_objects.list_dbt_objects()

        # Should be valid JSON (this would fail with PosixPath serialization issue)
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "status" in parsed
        assert "count" in parsed
        assert "items" in parsed

        # Ensure all paths are strings, not PosixPath objects
        for item in parsed["items"]:
            if "sql_path" in item:
                assert isinstance(item["sql_path"], str)
            if "yaml_path" in item and item["yaml_path"] is not None:
                assert isinstance(item["yaml_path"], str)

    def test_list_dbt_objects_with_filters_returns_valid_json(self) -> None:
        """Test that list_dbt_objects with various filters returns valid JSON."""
        test_cases = [
            {"pattern": "customer"},
            {"type": "model"},
            {"type": "source"},
            {"pattern": "^staging_.*"},
        ]

        for case in test_cases:
            result = tool_list_dbt_objects.list_dbt_objects(**case)  # type: ignore
            parsed = json.loads(result)
            assert isinstance(parsed, dict)
            assert "status" in parsed

    def test_show_docs_returns_valid_json(self) -> None:
        """Test that show_docs returns valid JSON."""
        result = tool_show_docs.show_docs(model_name="customer_orders")

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_show_docs_with_params_returns_valid_json(self) -> None:
        """Test that show_docs with different parameters returns valid JSON."""
        result = tool_show_docs.show_docs(
            model_name="customer_orders", model_type="model", target="dev"
        )

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_generate_docs_returns_valid_json(self) -> None:
        """Test that generate_docs returns valid JSON."""
        result = tool_generate_docs.generate_docs(model="customer_orders")

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_generate_docs_with_params_returns_valid_json(self) -> None:
        """Test that generate_docs with parameters returns valid JSON."""
        result = tool_generate_docs.generate_docs(
            model="customer_orders", target="dev", fix_inplace=False
        )

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_build_models_returns_valid_json(self) -> None:
        """Test that build_models returns valid JSON."""
        result = tool_build_model.build_models(analyze_only=True)

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_build_models_with_params_returns_valid_json(self) -> None:
        """Test that build_models with various parameters returns valid JSON."""
        result = tool_build_model.build_models(
            model="customer_orders",
            full_refresh=False,
            threads=2,
            target="dev",
            analyze_only=True,
            disable_smart=False,
        )

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_list_settings_returns_valid_json(self) -> None:
        """Test that list_settings returns valid JSON."""
        result = list_settings_wrapper()

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_list_settings_with_target_returns_valid_json(self) -> None:
        """Test that list_settings with target returns valid JSON."""
        result = list_settings_wrapper(target="dev")

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, dict)


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

    def test_list_settings_returns_expected_structure(self) -> None:
        """Test that list_settings returns expected structure."""
        result = list_settings_wrapper()
        parsed = json.loads(result)

        # Should contain settings data
        assert isinstance(parsed, dict)

        # Check that we have some expected settings
        # (assuming the structure from the docstring)
        if "status" not in parsed:  # Not an error response
            # Should have setting entries with source at minimum
            # Note: "value" may be removed if empty by remove_empty_values utility
            for setting_data in parsed.values():
                if isinstance(setting_data, dict):
                    # Expected structure for settings - source is always present
                    assert "source" in setting_data
                    # May also have "value" and "location" fields


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
            ("build_models", lambda: tool_build_model.build_models(analyze_only=True)),
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
