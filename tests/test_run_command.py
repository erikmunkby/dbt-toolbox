"""Tests for the run command."""

from unittest.mock import Mock, patch

from typer.testing import CliRunner

from dbt_toolbox.cli.main import app


class TestRunCommand:
    """Test the dt run command."""

    def test_run_command_exists(self) -> None:
        """Test that the run command is registered in the CLI app."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "run" in result.stdout

    def test_run_command_help(self) -> None:
        """Test that the run command shows help correctly."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["run", "--help"])

        # Should exit successfully after showing help
        assert result.exit_code == 0

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_with_model_selection(self, mock_create_plan: Mock) -> None:
        """Test run command with model selection."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run", "--model", "customers"])

        # Should exit successfully
        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_with_select_option(self, mock_create_plan: Mock) -> None:
        """Test run command with --select option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["orders"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run", "--select", "orders"])

        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_without_model_selection(self, mock_create_plan: Mock) -> None:
        """Test run command without model selection."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run"])

        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_with_additional_args(self, mock_create_plan: Mock) -> None:
        """Test that additional arguments are passed through."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run", "--threads", "4", "--full-refresh"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that parameters are passed to create_execution_plan
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "run"
        assert call_args.threads == 4
        assert call_args.full_refresh is True

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_with_target_option(self, mock_create_plan: Mock) -> None:
        """Test run command with --target option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run", "--target", "prod", "--model", "customers"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that parameters including target are passed to create_execution_plan
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "run"
        assert call_args.target == "prod"
        assert call_args.model == "customers"

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_without_target_option(self, mock_create_plan: Mock) -> None:
        """Test run command without --target option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run", "--model", "customers"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that target is None when not provided
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "run"
        assert call_args.target is None
        assert call_args.model == "customers"

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_dbt_not_found(self, mock_create_plan: Mock) -> None:
        """Test error handling when dbt command is not found."""
        # Mock execution plan that fails
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(1)
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run"])

        # Should exit with error code 1
        assert result.exit_code == 1

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_run_exit_code_passthrough(self, mock_create_plan: Mock) -> None:
        """Test that dbt's exit code is passed through when smart execution is disabled."""
        # Mock execution plan that fails with exit code 2
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(2)
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["nonexistent"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["run", "--model", "nonexistent", "--disable-smart"])

        # Should exit with the same code as dbt
        assert result.exit_code == 2
