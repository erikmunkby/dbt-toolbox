"""Tests for the shared dbt executor."""

from unittest.mock import Mock, patch

import pytest

from dbt_toolbox.cli._dbt_executor import execute_dbt_command, execute_dbt_with_smart_selection


class TestDbtExecutor:
    """Test the shared dbt execution engine."""

    @patch("dbt_toolbox.cli._dbt_executor._stream_process_output")
    @patch("dbt_toolbox.cli._dbt_executor._printers")
    @patch("dbt_toolbox.cli._dbt_executor.settings")
    @patch("dbt_toolbox.cli._dbt_executor.dbt_output_parser")
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

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(mock_dbt_parser, ["dbt", "run", "--model", "test"])

        assert exc_info.value.code == 0
        mock_popen.assert_called_once()

        # Check that project-dir and profiles-dir are added
        called_args = mock_popen.call_args[0][0]
        assert called_args[:4] == ["dbt", "run", "--model", "test"]
        assert "--project-dir" in called_args
        assert "/test/project" in called_args
        assert "--profiles-dir" in called_args
        assert "/test/profiles" in called_args

    @patch("dbt_toolbox.cli._dbt_executor._printers")
    @patch("dbt_toolbox.cli._dbt_executor.settings")
    @patch("dbt_toolbox.cli._dbt_executor.dbt_output_parser")
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

        # Mock removed - dbt_parser not patched anymore

        # Create a mock dbtParser instance
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_instance.models = {}
        mock_dbt_parser_instance.cache = Mock()

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(mock_dbt_parser_instance, ["dbt", "run", "--model", "nonexistent"])

        assert exc_info.value.code == 1
        mock_popen.assert_called_once()

    @patch("subprocess.Popen")
    def test_execute_dbt_command_not_found(self, mock_popen: Mock) -> None:
        """Test handling when dbt command is not found."""
        mock_popen.side_effect = FileNotFoundError("dbt not found")

        # Create a mock dbtParser instance
        mock_dbt_parser = Mock()

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(mock_dbt_parser, ["dbt", "run"])

        assert exc_info.value.code == 1

    @patch("subprocess.Popen")
    def test_execute_dbt_command_keyboard_interrupt(self, mock_popen: Mock) -> None:
        """Test handling of keyboard interrupt."""
        mock_popen.side_effect = KeyboardInterrupt()

        # Create a mock dbtParser instance
        mock_dbt_parser = Mock()

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(mock_dbt_parser, ["dbt", "run"])

        assert exc_info.value.code == 130

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor.analyze_model_statuses")
    @patch("dbt_toolbox.cli._dbt_executor.print_execution_analysis")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    @patch("dbt_toolbox.cli._dbt_executor.dbtParser")
    def test_execute_dbt_with_smart_selection_build(
        self,
        mock_dbt_parser_class: Mock,
        mock_validate: Mock,
        mock_print_analysis: Mock,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test smart execution for build command."""
        # Mock dbtParser constructor
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_class.return_value = mock_dbt_parser_instance

        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock analysis results showing some models need execution
        from dbt_toolbox.cli._analyze_models import AnalysisResult, ExecutionReason

        mock_analysis = {
            "customers": AnalysisResult(model=Mock(name="customers")),  # No reason = no execution
            "orders": AnalysisResult(
                model=Mock(name="orders"), reason=ExecutionReason.CODE_CHANGED
            ),
        }
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers+",
            disable_smart=False,
        )

        # Should analyze, print results, and execute with filtered selection
        mock_analyze.assert_called_once()
        mock_print_analysis.assert_called_once()
        mock_execute.assert_called_once()

        # Check that the command was filtered to only needed models
        # execute_dbt_command now takes dbt_parser and base_command as keyword arguments
        executed_args = mock_execute.call_args
        executed_command = executed_args.kwargs["base_command"]
        assert executed_command[:2] == ["dbt", "build"]
        assert "--select" in executed_command
        assert "orders" in executed_command

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor.analyze_model_statuses")
    @patch("dbt_toolbox.cli._dbt_executor.print_execution_analysis")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    @patch("dbt_toolbox.cli._dbt_executor.dbtParser")
    def test_execute_dbt_with_smart_selection_run(
        self,
        mock_dbt_parser_class: Mock,
        mock_validate: Mock,
        mock_print_analysis: Mock,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test smart execution for run command."""
        # Mock dbtParser constructor
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_class.return_value = mock_dbt_parser_instance

        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock analysis results showing all models need execution
        from dbt_toolbox.cli._analyze_models import AnalysisResult, ExecutionReason

        mock_analysis = {
            "customers": AnalysisResult(
                model=Mock(name="customers"), reason=ExecutionReason.CODE_CHANGED
            ),
            "orders": AnalysisResult(
                model=Mock(name="orders"), reason=ExecutionReason.CODE_CHANGED
            ),
        }
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="run",
            model="customers+",
            disable_smart=False,
        )

        # Should analyze, print results, and execute
        mock_analyze.assert_called_once()
        mock_print_analysis.assert_called_once()
        mock_execute.assert_called_once()

        # Check that the command uses run
        # execute_dbt_command now takes dbt_parser and base_command as keyword arguments
        executed_args = mock_execute.call_args
        executed_command = executed_args.kwargs["base_command"]
        assert executed_command[:2] == ["dbt", "run"]

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor.analyze_model_statuses")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    @patch("dbt_toolbox.cli._dbt_executor.dbtParser")
    def test_execute_dbt_with_smart_selection_all_cached(
        self,
        mock_dbt_parser_class: Mock,
        mock_validate: Mock,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test smart execution when all models are cached."""
        # Mock dbtParser constructor
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_class.return_value = mock_dbt_parser_instance

        # Mock lineage validation to pass
        mock_validate.return_value = True
        # Mock analysis results showing no models need execution
        from dbt_toolbox.cli._analyze_models import AnalysisResult

        mock_analysis = {
            "customers": AnalysisResult(model=Mock(name="customers")),  # No reason = no execution
            "orders": AnalysisResult(model=Mock(name="orders")),  # No reason = no execution
        }
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers+",
            disable_smart=False,
        )

        # Should analyze but not execute anything
        mock_analyze.assert_called_once()
        mock_execute.assert_not_called()

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor.analyze_model_statuses")
    @patch("dbt_toolbox.cli._dbt_executor.dbtParser")
    def test_execute_dbt_with_smart_selection_disabled(
        self,
        mock_dbt_parser_class: Mock,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test execution with smart selection disabled."""
        # Mock dbtParser constructor
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_class.return_value = mock_dbt_parser_instance

        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers+",
            disable_smart=True,
        )

        # Should not analyze and execute directly
        mock_analyze.assert_not_called()
        mock_execute.assert_called_once()

        # Check that the original selection is preserved
        # execute_dbt_command now takes dbt_parser and base_command as keyword arguments
        executed_args = mock_execute.call_args
        executed_command = executed_args.kwargs["base_command"]
        assert executed_command[:2] == ["dbt", "build"]
        assert "--select" in executed_command
        assert "customers+" in executed_command

    @patch("dbt_toolbox.cli._dbt_executor.analyze_model_statuses")
    @patch("dbt_toolbox.cli._dbt_executor.print_execution_analysis")
    @patch("dbt_toolbox.cli._dbt_executor._validate_lineage_references")
    @patch("dbt_toolbox.cli._dbt_executor.dbtParser")
    def test_execute_dbt_with_smart_selection_analyze_only(
        self,
        mock_dbt_parser_class: Mock,
        mock_validate: Mock,
        mock_print_analysis: Mock,
        mock_analyze: Mock,
    ) -> None:
        """Test analyze-only mode."""
        # Mock dbtParser constructor
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_class.return_value = mock_dbt_parser_instance

        # Mock lineage validation to pass
        mock_validate.return_value = True
        from dbt_toolbox.cli._analyze_models import AnalysisResult, ExecutionReason

        mock_analysis = {
            "customers": AnalysisResult(
                model=Mock(name="customers"), reason=ExecutionReason.CODE_CHANGED
            )
        }
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers",
            analyze_only=True,
            disable_smart=False,
        )

        # Should analyze and print but not execute
        mock_analyze.assert_called_once()
        mock_print_analysis.assert_called_once()

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._dbt_executor.dbtParser")
    def test_execute_dbt_with_options(
        self, mock_dbt_parser_class: Mock, mock_execute: Mock
    ) -> None:
        """Test that all options are properly passed through."""
        # Mock dbtParser constructor
        mock_dbt_parser_instance = Mock()
        mock_dbt_parser_class.return_value = mock_dbt_parser_instance

        execute_dbt_with_smart_selection(
            command_name="run",
            model="customers",
            full_refresh=True,
            threads=4,
            vars='{"key": "value"}',
            target=None,  # Changed from "prod" to avoid profile errors
            disable_smart=True,
        )

        mock_execute.assert_called_once()
        # execute_dbt_command now takes dbt_parser and base_command as keyword arguments
        executed_args = mock_execute.call_args
        executed_command = executed_args.kwargs["base_command"]

        assert executed_command[:2] == ["dbt", "run"]
        assert "--select" in executed_command
        assert "customers" in executed_command
        assert "--full-refresh" in executed_command
        assert "--threads" in executed_command
        assert "4" in executed_command
        assert "--vars" in executed_command
        assert '{"key": "value"}' in executed_command
