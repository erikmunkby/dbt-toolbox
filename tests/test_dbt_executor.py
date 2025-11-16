"""Tests for the shared dbt executor."""

from unittest.mock import Mock, patch

import pytest

from dbt_toolbox.actions.dbt_executor import _execute_dbt_raw
from dbt_toolbox.cli._build_run_command_factory import execute_dbt_with_smart_selection
from dbt_toolbox.data_models import DbtExecutionParams


class TestDbtExecutor:
    """Test the shared dbt execution engine."""

    @patch("dbt_toolbox.actions.dbt_executor._stream_process_output")
    @patch("dbt_toolbox.utils._printers")
    @patch("dbt_toolbox.actions.dbt_executor.settings")
    @patch("dbt_toolbox.actions.dbt_executor.parse_dbt_output")
    @patch("subprocess.Popen")
    def test_execute_dbt_command_success(
        self,
        mock_popen: Mock,
        mock_parser: Mock,
        mock_settings: Mock,
        mock_printers: Mock,
        mock_stream: Mock,
    ) -> None:
        """Test successful execution of a dbt command."""
        # Mock settings
        mock_settings.dbt_project_dir = "/test/project"
        mock_settings.dbt_profiles_dir = "/test/profiles"

        # Mock the streaming function to return some output
        mock_stream.return_value = ["Success\n"]

        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        # Create a mock dbtParser instance
        mock_dbt_parser = Mock()
        mock_dbt_parser.models = {}
        mock_dbt_parser.cache = Mock()

        # Mock the parse_dbt_output to return empty logs
        mock_logs = Mock()
        mock_logs.failed_models = []
        mock_parser.return_value = mock_logs

        result = _execute_dbt_raw(mock_dbt_parser, ["dbt", "run", "--model", "test"])

        assert result.return_code == 0
        mock_popen.assert_called_once()

        # Check that project-dir and profiles-dir are added
        called_args = mock_popen.call_args[0][0]
        assert called_args[:4] == ["dbt", "run", "--model", "test"]
        assert "--project-dir" in called_args
        assert "/test/project" in called_args
        assert "--profiles-dir" in called_args
        assert "/test/profiles" in called_args

    @patch("dbt_toolbox.utils._printers")
    @patch("dbt_toolbox.actions.dbt_executor.settings")
    @patch("dbt_toolbox.actions.dbt_executor.parse_dbt_output")
    @patch("subprocess.Popen")
    def test_execute_dbt_command_failure(
        self,
        mock_popen: Mock,
        mock_parser: Mock,
        mock_settings: Mock,
        mock_printers: Mock,
    ) -> None:
        """Test handling of dbt command failure."""
        # Mock settings
        mock_settings.dbt_project_dir = "/test/project"
        mock_settings.dbt_profiles_dir = "/test/profiles"

        mock_process = Mock()
        mock_process.stdout.readline.side_effect = ["ERROR\n", ""]
        mock_process.poll.side_effect = [None, 1]
        mock_process.wait.return_value = 1
        mock_popen.return_value = mock_process

        # Mock the output parser to return failed models
        mock_execution_result = Mock()
        mock_execution_result.failed_models = ["test_model"]
        mock_parser.parse_output.return_value = mock_execution_result

        # Create a mock dbtParser instance
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_instance.models = {}
        mock_dbt_parser_instance.cache = Mock()

        result = _execute_dbt_raw(
            mock_dbt_parser_instance, ["dbt", "run", "--model", "nonexistent"]
        )

        assert result.return_code == 1
        mock_popen.assert_called_once()

    @patch("subprocess.Popen")
    def test_execute_dbt_command_not_found(self, mock_popen: Mock) -> None:
        """Test handling when dbt command is not found."""
        mock_popen.side_effect = FileNotFoundError("dbt not found")

        # Create a mock dbtParser instance
        mock_dbt_parser = Mock()

        result = _execute_dbt_raw(mock_dbt_parser, ["dbt", "run"])

        assert result.return_code == 1

    @patch("subprocess.Popen")
    def test_execute_dbt_command_keyboard_interrupt(self, mock_popen: Mock) -> None:
        """Test handling of keyboard interrupt."""
        mock_popen.side_effect = KeyboardInterrupt()

        # Create a mock dbtParser instance
        mock_dbt_parser = Mock()

        result = _execute_dbt_raw(mock_dbt_parser, ["dbt", "run"])

        assert result.return_code == 130

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_execute_dbt_with_smart_selection_build(
        self,
        mock_create_plan: Mock,
    ) -> None:
        """Test smart execution for build command."""
        # Mock execution plan with some models needing execution
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["orders"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        params = DbtExecutionParams(
            command_name="build",
            model_selection="customers+",
            force=False,
        )

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_with_smart_selection(params)
        assert exc_info.value.code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_execute_dbt_with_smart_selection_run(
        self,
        mock_create_plan: Mock,
    ) -> None:
        """Test smart execution for run command."""
        # Mock execution plan with all models needing execution
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["customers", "orders"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        params = DbtExecutionParams(
            command_name="run",
            model_selection="customers+",
            force=False,
        )

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_with_smart_selection(params)
        assert exc_info.value.code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_execute_dbt_with_smart_selection_all_cached(
        self,
        mock_create_plan: Mock,
    ) -> None:
        """Test smart execution when all models are cached."""
        # Mock execution plan with no models needing execution
        mock_plan = Mock()
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list (models were analyzed)
        mock_plan.models_to_execute = []  # But no models need execution (all cached)
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        params = DbtExecutionParams(
            command_name="build",
            model_selection="customers+",
            force=False,
        )

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_with_smart_selection(params)
        assert exc_info.value.code == 0

        # Should create execution plan but not run anything
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_not_called()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_execute_dbt_with_force_mode(
        self,
        mock_create_plan: Mock,
    ) -> None:
        """Test execution with force mode (skip validation and cache analysis)."""
        # Mock execution plan for force mode
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        params = DbtExecutionParams(
            command_name="build",
            model_selection="customers+",
            force=True,
        )

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_with_smart_selection(params)
        assert exc_info.value.code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

    @patch("dbt_toolbox.cli._build_run_command_factory.create_execution_plan")
    def test_execute_dbt_with_options(self, mock_create_plan: Mock) -> None:
        """Test that all options are properly passed through."""
        # Mock execution plan
        mock_plan = Mock()
        mock_plan.run.return_value.return_code = 0
        mock_plan.lineage_valid = True
        mock_plan.analyses = [Mock()]  # Non-empty analyses list
        mock_plan.models_to_execute = ["all"]
        mock_plan.models_to_skip = []
        mock_create_plan.return_value = mock_plan

        params = DbtExecutionParams(
            command_name="run",
            model_selection="customers",
            full_refresh=True,
            threads=4,
            vars='{"key": "value"}',
            target=None,
            force=True,
        )

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_with_smart_selection(params)
        assert exc_info.value.code == 0

        # Should create execution plan and run it
        mock_create_plan.assert_called_once()
        mock_plan.run.assert_called_once()

        # Check that parameters are passed to create_execution_plan
        call_args = mock_create_plan.call_args[0][0]
        assert call_args.command_name == "run"
        assert call_args.model_selection == "customers"
        assert call_args.full_refresh is True
        assert call_args.threads == 4
        assert call_args.vars == '{"key": "value"}'
        assert call_args.target is None
        assert call_args.force is True
