"""Tests for the build command."""

from unittest.mock import Mock, patch

from typer.testing import CliRunner

from dbt_toolbox.cli.main import app


class TestBuildCommand:
    """Test the dt build command."""

    def test_build_command_exists(self) -> None:
        """Test that the build command is registered in the CLI app."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "build" in result.stdout

    def test_build_command_help(self) -> None:
        """Test that the build command shows help correctly."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["build", "--help"])

        # Should exit successfully after showing help
        assert result.exit_code == 0
        # Should contain build command documentation
        assert "Build dbt models with intelligent cache-based execution" in result.stdout

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_with_model_selection(self, mock_execute: Mock) -> None:
        """Test build command with model selection."""
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "customers"])

        # Should exit successfully
        assert result.exit_code == 0

        # Should call execute_dbt_command with the right command
        mock_execute.assert_called_once()
        called_args = mock_execute.call_args[0][0]
        assert called_args[:2] == ["dbt", "build"]
        assert "--select" in called_args
        assert "customers" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_with_select_option(self, mock_execute: Mock) -> None:
        """Test build command with --select option."""
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--select", "orders"])

        assert result.exit_code == 0

        # Should call execute_dbt_command with the right command
        mock_execute.assert_called_once()
        called_args = mock_execute.call_args[0][0]
        assert called_args[:2] == ["dbt", "build"]
        assert "--select" in called_args
        assert "orders" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_without_model_selection(self, mock_execute: Mock) -> None:
        """Test build command without model selection."""
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        assert result.exit_code == 0

        # Should call dbt build without model selection but with project and profiles dirs
        mock_execute.assert_called_once()
        called_args = mock_execute.call_args[0][0]
        assert called_args[:2] == ["dbt", "build"]

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_with_additional_args(self, mock_execute: Mock) -> None:
        """Test that additional arguments are passed through."""
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--threads", "4", "--full-refresh"])

        assert result.exit_code == 0
        mock_execute.assert_called_once()

        # Check that both --threads and --full-refresh are passed through
        called_args = mock_execute.call_args[0][0]
        assert called_args[:2] == ["dbt", "build"]
        assert "--threads" in called_args
        assert "4" in called_args
        assert "--full-refresh" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_dbt_not_found(self, mock_execute: Mock) -> None:
        """Test error handling when dbt command is not found."""
        mock_execute.side_effect = SystemExit(1)
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        # Should exit with error code 1
        assert result.exit_code == 1

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_exit_code_passthrough(self, mock_execute: Mock) -> None:
        """Test that dbt's exit code is passed through when smart execution is disabled."""
        mock_execute.side_effect = SystemExit(2)
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "nonexistent", "--disable-smart"])

        # Should exit with the same code as dbt
        assert result.exit_code == 2

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_keyboard_interrupt(self, mock_execute: Mock) -> None:
        """Test handling of keyboard interrupt."""
        mock_execute.side_effect = SystemExit(130)
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        # Should exit with standard Ctrl+C exit code
        assert result.exit_code == 130

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_with_target_option(self, mock_execute: Mock) -> None:
        """Test build command with --target option."""
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--target", "prod", "--model", "customers"])

        assert result.exit_code == 0
        mock_execute.assert_called_once()

        # Check that --target is passed through to dbt command
        called_args = mock_execute.call_args[0][0]
        assert called_args[:2] == ["dbt", "build"]
        assert "--target" in called_args
        assert "prod" in called_args
        assert "--select" in called_args
        assert "customers" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_build_without_target_option(self, mock_execute: Mock) -> None:
        """Test build command without --target option."""
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "customers"])

        assert result.exit_code == 0
        mock_execute.assert_called_once()

        # Check that --target is NOT in the command when not provided
        called_args = mock_execute.call_args[0][0]
        assert called_args[:2] == ["dbt", "build"]
        assert "--target" not in called_args
        assert "--select" in called_args
        assert "customers" in called_args
