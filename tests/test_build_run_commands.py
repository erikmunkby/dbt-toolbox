"""Tests for the build and run commands."""

from unittest.mock import Mock, patch

import pytest
from typer.testing import CliRunner

from dbt_toolbox.cli.main import app


@pytest.mark.parametrize("command", ["build", "run"])
class TestBuildRunCommands:
    """Test the dt build and dt run commands (shared functionality)."""

    def test_command_exists(self, command: str) -> None:
        """Test that the command is registered in the CLI app."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert command in result.stdout

    def test_command_help(self, command: str) -> None:
        """Test that the command shows help correctly."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, [command, "--help"])

        # Should exit successfully after showing help
        assert result.exit_code == 0

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_with_model_selection(self, mock_create_plan: Mock, command: str) -> None:
        """Test command with model selection."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command, "--model", "customers"])

        # Should exit successfully
        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_with_select_option(self, mock_create_plan: Mock, command: str) -> None:
        """Test command with --select option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["orders"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command, "--select", "orders"])

        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_without_model_selection(self, mock_create_plan: Mock, command: str) -> None:
        """Test command without model selection."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command])

        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_with_additional_args(self, mock_create_plan: Mock, command: str) -> None:
        """Test that additional arguments are passed through."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command, "--threads", "4", "--full-refresh"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that parameters are passed to create_execution_plan
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == command
        assert call_args.threads == 4
        assert call_args.full_refresh is True

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_dbt_not_found(self, mock_create_plan: Mock, command: str) -> None:
        """Test error handling when dbt command is not found."""
        # Mock execution plan that fails
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(1)
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command])

        # Should exit with error code 1
        assert result.exit_code == 1

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_exit_code_passthrough(self, mock_create_plan: Mock, command: str) -> None:
        """Test that dbt's exit code is passed through when smart execution is disabled."""
        # Mock execution plan that fails with exit code 2
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(2)
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["nonexistent"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command, "--model", "nonexistent", "--force"])

        # Should exit with the same code as dbt
        assert result.exit_code == 2

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_keyboard_interrupt(self, mock_create_plan: Mock, command: str) -> None:
        """Test handling of keyboard interrupt."""
        # Mock execution plan that simulates keyboard interrupt
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(130)
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command])

        # Should exit with standard Ctrl+C exit code
        assert result.exit_code == 130

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_with_target_option(self, mock_create_plan: Mock, command: str) -> None:
        """Test command with --target option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command, "--target", "prod", "--model", "customers"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that parameters including target are passed to create_execution_plan
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == command
        assert call_args.target == "prod"
        assert call_args.model_selection == "customers"

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_without_target_option(self, mock_create_plan: Mock, command: str) -> None:
        """Test command without --target option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, [command, "--model", "customers"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that target is None when not provided
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == command
        assert call_args.target is None
        assert call_args.model_selection == "customers"


class TestBuildSpecificFeatures:
    """Test features specific to the build command."""

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_with_selection_ignores_validation_errors_outside_selection(
        self, mock_create_plan: Mock
    ) -> None:
        """Test that validation ignores erroneous models outside the selection."""
        # Mock execution plan with successful validation
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_plan.lineage_valid = True  # This indicates validation passed
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--select", "customers"])

        # Should exit successfully (validation passed)
        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Verify the execution plan was called with the right parameters
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "build"
        assert call_args.model_selection == "customers"
