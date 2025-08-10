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

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    def test_build_with_model_selection(self, mock_validate: Mock, mock_execute: Mock) -> None:
        """Test build command with model selection."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "customers"])

        # Should exit successfully
        assert result.exit_code == 0

        # Should call execute_dbt_command with the right command
        mock_execute.assert_called_once()
        # Function now takes dbt_parser and base_command as keyword arguments
        args, kwargs = mock_execute.call_args
        assert "base_command" in kwargs
        called_args = kwargs["base_command"]
        assert called_args[:2] == ["dbt", "build"]
        assert "--select" in called_args
        assert "customers" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    def test_build_with_select_option(self, mock_validate: Mock, mock_execute: Mock) -> None:
        """Test build command with --select option."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--select", "orders"])

        assert result.exit_code == 0

        # Should call execute_dbt_command with the right command
        mock_execute.assert_called_once()
        # Function now takes dbt_parser and base_command as keyword arguments
        args, kwargs = mock_execute.call_args
        assert "base_command" in kwargs
        called_args = kwargs["base_command"]
        assert called_args[:2] == ["dbt", "build"]
        assert "--select" in called_args
        assert "orders" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    def test_build_without_model_selection(self, mock_validate: Mock, mock_execute: Mock) -> None:
        """Test build command without model selection."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        assert result.exit_code == 0

        # Should call dbt build without model selection but with project and profiles dirs
        mock_execute.assert_called_once()
        # Function now takes dbt_parser and base_command as keyword arguments
        args, kwargs = mock_execute.call_args
        assert "base_command" in kwargs
        called_args = kwargs["base_command"]
        assert called_args[:2] == ["dbt", "build"]

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    def test_build_with_additional_args(self, mock_validate: Mock, mock_execute: Mock) -> None:
        """Test that additional arguments are passed through."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--threads", "4", "--full-refresh"])

        assert result.exit_code == 0
        mock_execute.assert_called_once()

        # Check that both --threads and --full-refresh are passed through
        args, kwargs = mock_execute.call_args
        assert "base_command" in kwargs
        called_args = kwargs["base_command"]
        assert called_args[:2] == ["dbt", "build"]
        assert "--threads" in called_args
        assert "4" in called_args
        assert "--full-refresh" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    def test_build_dbt_not_found(self, mock_validate: Mock, mock_execute: Mock) -> None:
        """Test error handling when dbt command is not found."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
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
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    def test_build_keyboard_interrupt(self, mock_validate: Mock, mock_execute: Mock) -> None:
        """Test handling of keyboard interrupt."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
        mock_execute.side_effect = SystemExit(130)
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        # Should exit with standard Ctrl+C exit code
        assert result.exit_code == 130

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    @patch("dbt_toolbox.cli._dbt_executor.dbtParser")
    @patch("dbt_toolbox.cli._dbt_executor.analyze_model_statuses")
    def test_build_with_target_option(
        self, mock_analyze: Mock, mock_dbt_parser: Mock, mock_validate: Mock, mock_execute: Mock
    ) -> None:
        """Test build command with --target option."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock execute_dbt_command to simulate successful execution
        # Mock analyze_model_statuses to return a model that needs execution
        from datetime import datetime, timezone

        from dbt_toolbox.cli._analyze_models import AnalysisResult, ExecutionReason

        mock_model = Mock(spec=Mock)
        mock_model.name = "customers"
        mock_model.last_built = datetime.now(tz=timezone.utc)

        mock_analyze.return_value = {
            "customers": AnalysisResult(
                model=mock_model,
                reason=ExecutionReason.OUTDATED_MODEL,
            ),
        }

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--target", "prod", "--model", "customers"])

        assert result.exit_code == 0
        mock_execute.assert_called_once()

        # Check that --target is passed through to dbt command
        args, kwargs = mock_execute.call_args
        assert "base_command" in kwargs
        called_args = kwargs["base_command"]
        assert called_args[:2] == ["dbt", "build"]
        assert "--target" in called_args
        assert "prod" in called_args
        assert "--select" in called_args
        assert "customers" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    def test_build_without_target_option(self, mock_validate: Mock, mock_execute: Mock) -> None:
        """Test build command without --target option."""
        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock execute_dbt_command to simulate successful execution
        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "customers"])

        assert result.exit_code == 0
        mock_execute.assert_called_once()

        # Check that --target is NOT in the command when not provided
        args, kwargs = mock_execute.call_args
        assert "base_command" in kwargs
        called_args = kwargs["base_command"]
        assert called_args[:2] == ["dbt", "build"]
        assert "--target" not in called_args
        assert "--select" in called_args
        assert "customers" in called_args
