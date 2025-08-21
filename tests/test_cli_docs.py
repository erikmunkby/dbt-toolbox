"""Tests for the CLI docs command."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest
import typer.testing
import yamlium

from dbt_toolbox.actions.build_docs import DocsResult, YamlBuilder
from dbt_toolbox.cli.main import app
from dbt_toolbox.data_models import ColumnChanges
from dbt_toolbox.dbt_parser import dbtParser


@pytest.fixture
def cli_runner() -> typer.testing.CliRunner:
    """Create a Typer test client."""
    return typer.testing.CliRunner()


class TestYamlBuilder:
    """Test YamlBuilder class functionality."""

    def test_yaml_builder_init_existing_model(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test YamlBuilder initialization with existing model."""
        builder = YamlBuilder("customers", dbt_parser)

        assert builder.model.name == "customers"
        assert isinstance(builder.yml, yamlium.Mapping)
        assert "columns" in builder.yml
        assert isinstance(builder.yaml_docs, dict)

    def test_yaml_builder_init_nonexistent_model(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test YamlBuilder initialization with nonexistent model raises error."""
        with pytest.raises(KeyError):
            YamlBuilder("nonexistent_model", dbt_parser)

    def test_get_column_description_existing_docs(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test getting column description from existing YAML docs."""
        builder = YamlBuilder("customers", dbt_parser)

        # customers model should have existing column docs in schema.yml
        desc = builder._get_column_description("customer_id")

        assert desc is not None
        assert desc["name"] == "customer_id"
        assert "description" in desc

    def test_get_column_description_placeholder(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test getting placeholder description for undocumented column."""
        builder = YamlBuilder("customers", dbt_parser)

        # Test with a column that likely doesn't have docs
        desc = builder._get_column_description("nonexistent_column")

        assert desc is not None
        assert desc["name"] == "nonexistent_column"
        assert "description" in desc

    def test_detect_column_changes_no_changes(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test column change detection when no changes exist."""
        builder = YamlBuilder("customers", dbt_parser)

        # Get current columns
        existing_columns = [{"name": c["name"]} for c in builder.yml.get("columns", [])]

        changes = builder._detect_column_changes(existing_columns)

        assert changes.added == []
        assert changes.removed == []
        assert changes.reordered is False

    def test_detect_column_changes_with_additions(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test column change detection with new columns."""
        builder = YamlBuilder("customers", dbt_parser)

        # Add a new column
        existing_columns = [{"name": c["name"]} for c in builder.yml.get("columns", [])]
        new_columns = [*existing_columns, {"name": "new_column"}]

        changes = builder._detect_column_changes(new_columns)

        assert "new_column" in changes.added
        assert changes.removed == []

    def test_detect_column_changes_with_removals(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test column change detection with removed columns."""
        builder = YamlBuilder("customers", dbt_parser)

        # Remove a column (take all but first)
        existing_columns = [{"name": c["name"]} for c in builder.yml.get("columns", [])]
        if existing_columns:
            removed_column = existing_columns[0]["name"]
            new_columns = existing_columns[1:]

            changes = builder._detect_column_changes(new_columns)

            assert removed_column in changes.removed
            assert changes.added == []

    def test_detect_column_changes_reordered(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test column change detection with reordered columns."""
        builder = YamlBuilder("customers", dbt_parser)

        # Reverse the order of columns
        existing_columns = [{"name": c["name"]} for c in builder.yml.get("columns", [])]
        if len(existing_columns) > 1:
            reordered_columns = list(reversed(existing_columns))

            changes = builder._detect_column_changes(reordered_columns)

            assert changes.reordered is True
            assert changes.added == []
            assert changes.removed == []


class TestDocsCommand:
    """Test the CLI docs command functionality."""

    def test_docs_command_missing_model(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test docs command fails when model parameter is missing."""
        result = cli_runner.invoke(app, ["docs"])

        assert result.exit_code != 0
        # Check stderr for error messages as Typer might output there
        error_output = result.stdout + (result.stderr or "")
        assert (
            "Missing option" in error_output
            or "required" in error_output.lower()
            or result.exit_code == 2
        )

    def test_docs_command_nonexistent_model(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test docs command fails with nonexistent model."""
        result = cli_runner.invoke(app, ["docs", "--model", "nonexistent_model"])

        assert result.exit_code != 0

    def test_docs_command_valid_model_no_clipboard(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test docs command with valid model without clipboard option."""
        with patch.object(YamlBuilder, "build") as mock_build:
            # Mock the return value to be a proper DocsResult for file update mode
            mock_build.return_value = DocsResult(
                model_name="customers",
                model_path="/path/to/customers.sql",
                success=True,
                changes=ColumnChanges(added=[], removed=[], reordered=False),
                nbr_columns_with_placeholders=0,
                yaml_content=None,  # No YAML content when fix_inplace=True
                error_message=None,
                yaml_path=None,
                mode=None,
            )

            result = cli_runner.invoke(app, ["docs", "--model", "customers"])

            assert result.exit_code == 0
            mock_build.assert_called_once_with(fix_inplace=True)

    def test_docs_command_valid_model_with_clipboard(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test docs command with valid model and clipboard option."""
        with (
            patch.object(YamlBuilder, "build") as mock_build,
            patch("subprocess.Popen") as mock_popen,
        ):
            # Mock the return value to be a proper DocsResult
            mock_build.return_value = DocsResult(
                model_name="customers",
                model_path="/path/to/customers.sql",
                success=True,
                changes=ColumnChanges(added=[], removed=[], reordered=False),
                nbr_columns_with_placeholders=0,
                yaml_content="models:\n  - name: customers\n    columns: []",
                error_message=None,
                yaml_path=None,
                mode=None,
            )

            # Mock subprocess.Popen for clipboard functionality
            mock_process = MagicMock()
            mock_popen.return_value = mock_process

            result = cli_runner.invoke(app, ["docs", "--model", "customers", "--clipboard"])

            assert result.exit_code == 0
            mock_build.assert_called_once_with(fix_inplace=False)
            mock_popen.assert_called_once_with(args="pbcopy", stdin=subprocess.PIPE)

    def test_docs_command_short_options(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test docs command with short option flags."""
        with (
            patch.object(YamlBuilder, "build") as mock_build,
            patch("subprocess.Popen") as mock_popen,
        ):
            # Mock the return value to be a proper DocsResult
            mock_build.return_value = DocsResult(
                model_name="customers",
                model_path="/path/to/customers.sql",
                success=True,
                changes=ColumnChanges(added=[], removed=[], reordered=False),
                nbr_columns_with_placeholders=0,
                yaml_content="models:\n  - name: customers\n    columns: []",
                error_message=None,
                yaml_path=None,
                mode=None,
            )

            # Mock subprocess.Popen for clipboard functionality
            mock_process = MagicMock()
            mock_popen.return_value = mock_process

            result = cli_runner.invoke(app, ["docs", "-m", "customers", "-c"])

            assert result.exit_code == 0
            mock_build.assert_called_once_with(fix_inplace=False)
            mock_popen.assert_called_once_with(args="pbcopy", stdin=subprocess.PIPE)

    def test_build_clipboard_mode_returns_yaml_content(
        self,
        dbt_project_setup: None,
        dbt_parser: dbtParser,
    ) -> None:
        """Test YamlBuilder.build in clipboard mode (fix_inplace=False)."""
        builder = YamlBuilder("customers", dbt_parser)
        result = builder.build(fix_inplace=False)

        # Should return YAML content when fix_inplace=False
        assert result.yaml_content is not None
        assert result.success is True
        assert "models:" in result.yaml_content

    def test_build_update_mode_no_changes(
        self,
        dbt_project_setup: None,
        dbt_parser: dbtParser,
    ) -> None:
        """Test YamlBuilder.build in update mode with no changes."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock the _detect_column_changes to return no changes
        with patch.object(builder, "_detect_column_changes") as mock_detect:
            mock_detect.return_value = ColumnChanges(
                added=[],
                removed=[],
                reordered=False,
            )

            result = builder.build(fix_inplace=True)

            # Should return successful result with no changes
            assert result.success is True
            assert result.yaml_content is None  # No YAML content when fix_inplace=True
            assert not result.changes.added
            assert not result.changes.removed
            assert not result.changes.reordered

    def test_build_update_mode_with_changes(
        self,
        dbt_project_setup: None,
        dbt_parser: dbtParser,
    ) -> None:
        """Test YamlBuilder.build in update mode with changes."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock the model's update_model_yaml method
        with patch.object(builder.model, "update_model_yaml") as mock_update:
            # Mock detect changes to return some changes
            with patch.object(builder, "_detect_column_changes") as mock_detect:
                mock_detect.return_value = ColumnChanges(
                    added=["new_column"],
                    removed=[],
                    reordered=False,
                )

                result = builder.build(fix_inplace=True)

                # Should call update_model_yaml
                mock_update.assert_called_once()

                # Should return successful result with changes
                assert result.success is True
                assert result.yaml_content is None  # No YAML content when fix_inplace=True
                assert "new_column" in result.changes.added

    def test_error_handling_with_detailed_message(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test that detailed error messages are displayed when build fails."""
        with patch.object(YamlBuilder, "build") as mock_build:
            # Mock the return value to be a failed DocsResult with error message
            mock_build.return_value = DocsResult(
                model_name="customers",
                model_path="/path/to/customers.sql",
                success=False,
                changes=ColumnChanges(added=[], removed=[], reordered=False),
                nbr_columns_with_placeholders=0,
                yaml_content=None,
                error_message=(
                    "Permission denied when writing to schema file: "
                    "[Errno 13] Permission denied: 'schema.yml'"
                ),
                yaml_path=None,
                mode=None,
            )

            result = cli_runner.invoke(app, ["docs", "--model", "customers"])

            assert result.exit_code == 1
            assert "Failed to update model" in result.stdout
            assert "Permission denied when writing to schema file" in result.stdout

    def test_error_handling_clipboard_mode_with_detailed_message(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test that detailed error messages are displayed when clipboard mode fails."""
        with patch.object(YamlBuilder, "build") as mock_build:
            # Mock the return value to be a failed DocsResult with error message
            mock_build.return_value = DocsResult(
                model_name="customers",
                model_path="/path/to/customers.sql",
                success=False,
                changes=ColumnChanges(added=[], removed=[], reordered=False),
                nbr_columns_with_placeholders=0,
                yaml_content=None,
                error_message="Failed to generate YAML content: Invalid YAML structure",
                yaml_path=None,
                mode=None,
            )

            result = cli_runner.invoke(app, ["docs", "--model", "customers", "--clipboard"])

            assert result.exit_code == 1
            assert "Failed to generate YAML for model" in result.stdout
            assert "Failed to generate YAML content" in result.stdout


class TestCLIIntegration:
    """Integration tests for the CLI command."""

    def test_cli_app_has_docs_command(self) -> None:
        """Test that the main CLI app has the docs command registered."""
        # Check if docs command is registered by inspecting the app
        # Typer stores registered commands differently, so we check the app structure
        assert hasattr(app, "registered_commands")
        # Alternative: check if we can invoke docs command
        runner = typer.testing.CliRunner()
        result = runner.invoke(app, ["docs", "--help"])
        assert result.exit_code == 0

    def test_cli_main_function_exists(self) -> None:
        """Test that main function exists and is callable."""
        from dbt_toolbox.cli.main import main

        assert callable(main)

    def test_full_cli_workflow_clipboard(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test complete CLI workflow with clipboard output."""
        with (
            patch("subprocess.Popen") as mock_popen,
            patch("dbt_toolbox.utils._printers.cprint") as mock_cprint,
        ):
            mock_process = MagicMock()
            mock_popen.return_value = mock_process

            result = cli_runner.invoke(app, ["docs", "--model", "customers", "--clipboard"])

            assert result.exit_code == 0
            mock_popen.assert_called_once()
            assert mock_cprint.call_count >= 1

    def test_error_handling_invalid_model(
        self,
        cli_runner: typer.testing.CliRunner,
        dbt_project_setup: None,
    ) -> None:
        """Test error handling for invalid model names."""
        result = cli_runner.invoke(app, ["docs", "--model", "invalid_model_name"])

        # Command should fail gracefully
        assert result.exit_code != 0
        # The error might be captured in the exception rather than stdout/stderr
        # We just need to verify the command exits with non-zero code
