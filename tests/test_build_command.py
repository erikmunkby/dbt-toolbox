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

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_with_model_selection(self, mock_create_plan: Mock) -> None:
        """Test build command with model selection."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "customers"])

        # Should exit successfully
        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_with_select_option(self, mock_create_plan: Mock) -> None:
        """Test build command with --select option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["orders"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--select", "orders"])

        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_without_model_selection(self, mock_create_plan: Mock) -> None:
        """Test build command without model selection."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        assert result.exit_code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_with_additional_args(self, mock_create_plan: Mock) -> None:
        """Test that additional arguments are passed through."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--threads", "4", "--full-refresh"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that parameters are passed to create_execution_plan
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "build"
        assert call_args.threads == 4
        assert call_args.full_refresh is True

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_dbt_not_found(self, mock_create_plan: Mock) -> None:
        """Test error handling when dbt command is not found."""
        # Mock execution plan that fails
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(1)
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        # Should exit with error code 1
        assert result.exit_code == 1

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_exit_code_passthrough(self, mock_create_plan: Mock) -> None:
        """Test that dbt's exit code is passed through when smart execution is disabled."""
        # Mock execution plan that fails with exit code 2
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(2)
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["nonexistent"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "nonexistent", "--disable-smart"])

        # Should exit with the same code as dbt
        assert result.exit_code == 2

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_keyboard_interrupt(self, mock_create_plan: Mock) -> None:
        """Test handling of keyboard interrupt."""
        # Mock execution plan that simulates keyboard interrupt
        mock_plan = Mock()
        mock_plan.run.side_effect = SystemExit(130)
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build"])

        # Should exit with standard Ctrl+C exit code
        assert result.exit_code == 130

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_with_target_option(self, mock_create_plan: Mock) -> None:
        """Test build command with --target option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--target", "prod", "--model", "customers"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that parameters including target are passed to create_execution_plan
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "build"
        assert call_args.target == "prod"
        assert call_args.model == "customers"

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_build_without_target_option(self, mock_create_plan: Mock) -> None:
        """Test build command without --target option."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.analyses = []
        mock_plan.models_to_execute = ["customers"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--model", "customers"])

        assert result.exit_code == 0
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that target is None when not provided
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "build"
        assert call_args.target is None
        assert call_args.model == "customers"

    @patch("dbt_toolbox.actions.dbt_executor.analyze_column_references")
    @patch("dbt_toolbox.actions.dbt_executor.analyze_model_statuses")
    @patch("dbt_toolbox.dbt_parser.dbtParser")
    def test_build_with_selection_ignores_validation_errors_outside_selection(
        self,
        mock_dbt_parser_class: Mock,
        mock_analyze_model_statuses: Mock,
        mock_analyze_column_references: Mock,
    ) -> None:
        """Test that validation ignores erroneous models outside the selection."""
        # Mock dbtParser instance
        mock_dbt_parser = Mock()
        mock_dbt_parser_class.return_value = mock_dbt_parser

        # Mock parse_selection_query_return_models to return selected models only
        mock_selected_model = Mock()
        mock_selected_model.name = "customers"
        mock_dbt_parser.parse_selection_query_return_models.return_value = {
            "customers": mock_selected_model
        }

        # Mock column analysis to return no issues (validation passes for selected models)
        mock_column_analysis = Mock()
        mock_column_analysis.non_existent_columns = {}
        mock_column_analysis.referenced_non_existent_models = {}
        mock_analyze_column_references.return_value = mock_column_analysis

        # Mock model status analysis
        mock_analysis = Mock()
        mock_analysis.needs_execution = True
        mock_analysis.model = mock_selected_model
        mock_analyze_model_statuses.return_value = [mock_analysis]

        cli_runner = CliRunner()

        result = cli_runner.invoke(app, ["build", "--select", "customers"])

        # Should exit successfully (validation passed)
        assert result.exit_code == 0

        # Verify that column analysis was called with only the selected models
        mock_analyze_column_references.assert_called_once()
        call_args = mock_analyze_column_references.call_args[1]
        target_models = call_args["target_models"]
        assert len(target_models) == 1
        assert target_models[0].name == "customers"
