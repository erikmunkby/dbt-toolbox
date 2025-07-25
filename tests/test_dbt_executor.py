"""Tests for the shared dbt executor."""

from unittest.mock import Mock, patch

import pytest

from dbt_toolbox.cli._dbt_executor import execute_dbt_command, execute_dbt_with_smart_selection


class TestDbtExecutor:
    """Test the shared dbt execution engine."""

    @patch("dbt_toolbox.cli._dbt_executor._stream_process_output")
    @patch("dbt_toolbox.cli._dbt_executor.printer")
    @patch("dbt_toolbox.cli._dbt_executor.settings")
    @patch("dbt_toolbox.cli._dbt_executor.dbt_output_parser")
    @patch("subprocess.Popen")
    def test_execute_dbt_command_success(
        self,
        mock_popen: Mock,
        mock_parser: Mock,
        mock_settings: Mock,
        mock_printer: Mock,
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

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(["dbt", "run", "--model", "test"])

        assert exc_info.value.code == 0
        mock_popen.assert_called_once()

        # Check that project-dir and profiles-dir are added
        called_args = mock_popen.call_args[0][0]
        assert called_args[:4] == ["dbt", "run", "--model", "test"]
        assert "--project-dir" in called_args
        assert "/test/project" in called_args
        assert "--profiles-dir" in called_args
        assert "/test/profiles" in called_args

    @patch("dbt_toolbox.cli._dbt_executor.printer")
    @patch("dbt_toolbox.cli._dbt_executor.settings")
    @patch("dbt_toolbox.cli._dbt_executor.dbt_parser")
    @patch("dbt_toolbox.cli._dbt_executor.dbt_output_parser")
    @patch("subprocess.Popen")
    def test_execute_dbt_command_failure(
        self,
        mock_popen: Mock,
        mock_parser: Mock,
        mock_dbt_parser: Mock,
        mock_settings: Mock,
        mock_printer: Mock,
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

        # Mock the dbt parser cache
        mock_dbt_parser.cache.clear_models_cache.return_value = ["test_model"]

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(["dbt", "run", "--model", "nonexistent"])

        assert exc_info.value.code == 1
        mock_popen.assert_called_once()

    @patch("subprocess.Popen")
    def test_execute_dbt_command_not_found(self, mock_popen: Mock) -> None:
        """Test handling when dbt command is not found."""
        mock_popen.side_effect = FileNotFoundError("dbt not found")

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(["dbt", "run"])

        assert exc_info.value.code == 1

    @patch("subprocess.Popen")
    def test_execute_dbt_command_keyboard_interrupt(self, mock_popen: Mock) -> None:
        """Test handling of keyboard interrupt."""
        mock_popen.side_effect = KeyboardInterrupt()

        with pytest.raises(SystemExit) as exc_info:
            execute_dbt_command(["dbt", "run"])

        assert exc_info.value.code == 130

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.analyze_build_execution")
    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.print_execution_analysis")
    def test_execute_dbt_with_smart_selection_build(
        self,
        mock_print_analysis: Mock,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test smart execution for build command."""
        # Mock analysis results showing some models need execution
        mock_analysis = {
            "customers": Mock(needs_execution=False),
            "orders": Mock(needs_execution=True),
        }
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers+",
            disable_smart=False,
        )

        # Should analyze, print results, and execute with filtered selection
        mock_analyze.assert_called_once_with("customers+")
        mock_print_analysis.assert_called_once()
        mock_execute.assert_called_once()

        # Check that the command was filtered to only needed models
        executed_command = mock_execute.call_args[0][0]
        assert executed_command[:2] == ["dbt", "build"]
        assert "--select" in executed_command
        assert "orders" in executed_command

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.analyze_build_execution")
    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.print_execution_analysis")
    def test_execute_dbt_with_smart_selection_run(
        self,
        mock_print_analysis: Mock,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test smart execution for run command."""
        # Mock analysis results showing all models need execution
        mock_analysis = {
            "customers": Mock(needs_execution=True),
            "orders": Mock(needs_execution=True),
        }
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="run",
            model="customers+",
            disable_smart=False,
        )

        # Should analyze, print results, and execute
        mock_analyze.assert_called_once_with("customers+")
        mock_print_analysis.assert_called_once()
        mock_execute.assert_called_once()

        # Check that the command uses run
        executed_command = mock_execute.call_args[0][0]
        assert executed_command[:2] == ["dbt", "run"]

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.analyze_build_execution")
    def test_execute_dbt_with_smart_selection_all_cached(
        self,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test smart execution when all models are cached."""
        # Mock analysis results showing no models need execution
        mock_analysis = {
            "customers": Mock(needs_execution=False),
            "orders": Mock(needs_execution=False),
        }
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers+",
            disable_smart=False,
        )

        # Should analyze but not execute anything
        mock_analyze.assert_called_once_with("customers+")
        mock_execute.assert_not_called()

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.analyze_build_execution")
    def test_execute_dbt_with_smart_selection_disabled(
        self,
        mock_analyze: Mock,
        mock_execute: Mock,
    ) -> None:
        """Test execution with smart selection disabled."""
        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers+",
            disable_smart=True,
        )

        # Should not analyze and execute directly
        mock_analyze.assert_not_called()
        mock_execute.assert_called_once()

        # Check that the original selection is preserved
        executed_command = mock_execute.call_args[0][0]
        assert executed_command[:2] == ["dbt", "build"]
        assert "--select" in executed_command
        assert "customers+" in executed_command

    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.analyze_build_execution")
    @patch("dbt_toolbox.cli._build_analysis.build_analyzer.print_execution_analysis")
    def test_execute_dbt_with_smart_selection_analyze_only(
        self,
        mock_print_analysis: Mock,
        mock_analyze: Mock,
    ) -> None:
        """Test analyze-only mode."""
        mock_analysis = {"customers": Mock(needs_execution=True)}
        mock_analyze.return_value = mock_analysis

        execute_dbt_with_smart_selection(
            command_name="build",
            model="customers",
            analyze_only=True,
            disable_smart=False,
        )

        # Should analyze and print but not execute
        mock_analyze.assert_called_once_with("customers")
        mock_print_analysis.assert_called_once()

    @patch("dbt_toolbox.cli._dbt_executor.execute_dbt_command")
    def test_execute_dbt_with_options(self, mock_execute: Mock) -> None:
        """Test that all options are properly passed through."""
        execute_dbt_with_smart_selection(
            command_name="run",
            model="customers",
            full_refresh=True,
            threads=4,
            vars='{"key": "value"}',
            target="prod",
            disable_smart=True,
        )

        mock_execute.assert_called_once()
        executed_command = mock_execute.call_args[0][0]

        assert executed_command[:2] == ["dbt", "run"]
        assert "--select" in executed_command
        assert "customers" in executed_command
        assert "--full-refresh" in executed_command
        assert "--threads" in executed_command
        assert "4" in executed_command
        assert "--vars" in executed_command
        assert '{"key": "value"}' in executed_command
        assert "--target" in executed_command
        assert "prod" in executed_command
