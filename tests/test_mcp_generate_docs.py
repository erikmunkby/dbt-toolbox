"""Tests for the MCP generate_docs tool."""

import json
from unittest.mock import patch

import pytest

from dbt_toolbox.actions.build_docs import DocsResult, YamlBuilder
from dbt_toolbox.data_models import ColumnChanges
from dbt_toolbox.dbt_parser import dbtParser


class TestMCPGenerateDocs:
    """Test the MCP generate_docs tool functionality."""

    @pytest.fixture(autouse=True)
    def setup_test(self) -> None:
        """Import the MCP function for testing."""
        # Import here to avoid FastMCP dependency issues in CI
        try:
            from dbt_toolbox.mcp.mcp import generate_docs

            # Access the actual function from the FunctionTool wrapper
            self.generate_docs = generate_docs.fn
        except ImportError:
            pytest.skip("FastMCP not available for testing")
        except AttributeError:
            # If generate_docs is not wrapped (e.g., in older FastMCP versions)
            from dbt_toolbox.mcp.mcp import generate_docs

            self.generate_docs = generate_docs

    def test_generate_docs_preview_mode_success(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test generate_docs in preview mode (fix_inplace=False)."""
        with patch("dbt_toolbox.mcp.mcp.dbtParser") as mock_parser_class:
            mock_parser_class.return_value = dbt_parser

            with patch.object(YamlBuilder, "build") as mock_build:
                mock_build.return_value = DocsResult(
                    model_name="customers",
                    model_path="/path/to/customers.sql",
                    success=True,
                    changes=ColumnChanges(added=["new_col"], removed=[], reordered=False),
                    nbr_columns_with_placeholders=1,
                    yaml_content="models:\n  - name: customers\n    columns: []",
                    error_message=None,
                )

                result_json = self.generate_docs("customers", fix_inplace=False)
                result = json.loads(result_json)

                assert result["success"] is True
                assert result["model_name"] == "customers"
                assert result["yaml_content"] is not None
                assert "models:" in result["yaml_content"]
                assert result["changes"]["added"] == ["new_col"]
                assert result["nbr_columns_with_placeholders"] == 1
                # error_message is not present when None (removed by remove_empty_values)

    def test_generate_docs_update_mode_success(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test generate_docs in update mode (fix_inplace=True)."""
        with patch("dbt_toolbox.mcp.mcp.dbtParser") as mock_parser_class:
            mock_parser_class.return_value = dbt_parser

            with patch.object(YamlBuilder, "build") as mock_build:
                mock_build.return_value = DocsResult(
                    model_name="customers",
                    model_path="/path/to/customers.sql",
                    success=True,
                    changes=ColumnChanges(added=[], removed=["old_col"], reordered=True),
                    nbr_columns_with_placeholders=0,
                    yaml_content=None,
                    error_message=None,
                )

                result_json = self.generate_docs("customers", fix_inplace=True)
                result = json.loads(result_json)

                assert result["success"] is True
                assert result["model_name"] == "customers"
                # yaml_content is not present when None (removed by remove_empty_values)
                assert "yaml_content" not in result
                assert result["changes"]["removed"] == ["old_col"]
                assert result["changes"]["reordered"] is True
                assert result["nbr_columns_with_placeholders"] == 0

    def test_generate_docs_model_not_found(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test generate_docs with non-existent model."""
        with patch("dbt_toolbox.mcp.mcp.dbtParser") as mock_parser_class:
            mock_parser_class.return_value = dbt_parser

            result_json = self.generate_docs("nonexistent_model")
            result = json.loads(result_json)

            assert result["status"] == "error"
            assert "not found" in result["message"]
            assert "Available models include" in result["message"]

    def test_generate_docs_parser_initialization_error(self) -> None:
        """Test generate_docs when dbt parser initialization fails."""
        with patch("dbt_toolbox.mcp.mcp.dbtParser", side_effect=Exception("Parser failed")):
            result_json = self.generate_docs("customers")
            result = json.loads(result_json)

            assert result["status"] == "error"
            assert "Failed to initialize dbt parser" in result["message"]
            assert "Parser failed" in result["message"]

    def test_generate_docs_build_error(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test generate_docs when build operation fails."""
        with patch("dbt_toolbox.mcp.mcp.dbtParser") as mock_parser_class:
            mock_parser_class.return_value = dbt_parser

            with patch.object(YamlBuilder, "build") as mock_build:
                mock_build.return_value = DocsResult(
                    model_name="customers",
                    model_path="/path/to/customers.sql",
                    success=False,
                    changes=ColumnChanges(added=[], removed=[], reordered=False),
                    nbr_columns_with_placeholders=0,
                    yaml_content=None,
                    error_message="Permission denied when writing to schema file",
                )

                result_json = self.generate_docs("customers")
                result = json.loads(result_json)

                assert result["success"] is False
                assert result["error_message"] == "Permission denied when writing to schema file"
                assert result["model_name"] == "customers"

    def test_generate_docs_unexpected_error(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test generate_docs when unexpected error occurs."""
        with patch("dbt_toolbox.mcp.mcp.dbtParser") as mock_parser_class:
            mock_parser_class.return_value = dbt_parser

            with patch.object(YamlBuilder, "build", side_effect=Exception("Unexpected error")):
                result_json = self.generate_docs("customers")
                result = json.loads(result_json)

                assert result["status"] == "error"
                assert "Unexpected error while generating docs" in result["message"]
                assert "Unexpected error" in result["message"]

    def test_generate_docs_with_target_parameter(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test generate_docs with target parameter."""
        with patch("dbt_toolbox.mcp.mcp.dbtParser") as mock_parser_class:
            mock_parser_class.return_value = dbt_parser

            with patch.object(YamlBuilder, "build") as mock_build:
                mock_build.return_value = DocsResult(
                    model_name="customers",
                    model_path="/path/to/customers.sql",
                    success=True,
                    changes=ColumnChanges(added=[], removed=[], reordered=False),
                    nbr_columns_with_placeholders=0,
                    yaml_content="test yaml",
                    error_message=None,
                )

                result_json = self.generate_docs("customers", target="prod", fix_inplace=False)
                result = json.loads(result_json)

                # Verify dbt parser was called with target
                mock_parser_class.assert_called_once_with(target="prod")
                assert result["success"] is True
                assert result["yaml_content"] == "test yaml"
