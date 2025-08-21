"""Tests for the analyze command."""

from unittest.mock import Mock, patch

from typer.testing import CliRunner

from dbt_toolbox.actions.analyze_models import AnalysisResult, ExecutionReason
from dbt_toolbox.cli.main import app
from dbt_toolbox.data_models import Model


class TestAnalyzeCommand:
    """Test the dt analyze command."""

    def test_analyze_command_exists(self) -> None:
        """Test that the analyze command is registered in the CLI app."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "analyze" in result.stdout

    @patch("dbt_toolbox.cli.analyze.print_column_analysis_results")
    @patch("dbt_toolbox.cli.analyze.analyze_model_statuses")
    def test_analyze_with_model_selection(
        self, mock_analyze: Mock, mock_column_analysis: Mock
    ) -> None:
        """Test analyze command with model selection."""
        # Mock analysis results - two valid models
        from datetime import datetime, timezone

        mock_model1 = Mock(spec=Model)
        mock_model1.name = "customers"
        mock_model1.last_built = datetime.now(tz=timezone.utc)
        mock_model2 = Mock(spec=Model)
        mock_model2.name = "orders"
        mock_model2.last_built = datetime.now(tz=timezone.utc)

        mock_analyze.return_value = [
            AnalysisResult(model=mock_model1, needs_execution=False),
            AnalysisResult(model=mock_model2, needs_execution=False),
        ]

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze", "--model", "customers+"])

        assert result.exit_code == 0
        # Verify analyze was called with dbt_parser and selection
        args, kwargs = mock_analyze.call_args
        assert len(args) == 0  # No positional args
        assert "dbt_selection" in kwargs
        assert kwargs["dbt_selection"] == "customers+"
        assert "dbt_parser" in kwargs
        assert "Cache Analysis Results" in result.stdout
        assert "All models have valid cache!" in result.stdout

    @patch("dbt_toolbox.cli.analyze.print_column_analysis_results")
    @patch("dbt_toolbox.cli.analyze.analyze_model_statuses")
    def test_analyze_with_failed_models(
        self, mock_analyze: Mock, mock_column_analysis: Mock
    ) -> None:
        """Test analyze command with failed models."""
        # Mock analysis results with failed models
        from datetime import datetime, timezone

        mock_failed_model = Mock(spec=Model)
        mock_failed_model.name = "failed_model"
        mock_failed_model.last_built = None
        mock_model1 = Mock(spec=Model)
        mock_model1.name = "customers"
        mock_model1.last_built = datetime.now(tz=timezone.utc)
        mock_model2 = Mock(spec=Model)
        mock_model2.name = "orders"
        mock_model2.last_built = datetime.now(tz=timezone.utc)

        mock_analyze.return_value = [
            AnalysisResult(
                model=mock_failed_model,
                reason=ExecutionReason.LAST_EXECUTION_FAILED,
            ),
            AnalysisResult(model=mock_model1, needs_execution=False),
            AnalysisResult(model=mock_model2, needs_execution=False),
        ]

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        # Verify analyze was called with dbt_parser and None selection
        args, kwargs = mock_analyze.call_args
        assert len(args) == 0  # No positional args
        assert "dbt_selection" in kwargs
        assert kwargs["dbt_selection"] is None
        assert "dbt_parser" in kwargs
        assert "Models needing execution: 1" in result.stdout

    @patch("dbt_toolbox.cli.analyze.print_column_analysis_results")
    @patch("dbt_toolbox.cli.analyze.analyze_model_statuses")
    def test_analyze_with_upstream_macro_changes(
        self, mock_analyze: Mock, mock_column_analysis: Mock
    ) -> None:
        """Test analyze command detecting upstream macro changes."""
        # Mock analysis results with upstream changes
        from datetime import datetime, timezone

        mock_affected_model = Mock(spec=Model)
        mock_affected_model.name = "affected_model"
        mock_affected_model.last_built = datetime.now(tz=timezone.utc)
        mock_other_model = Mock(spec=Model)
        mock_other_model.name = "other_model"
        mock_other_model.last_built = datetime.now(tz=timezone.utc)

        mock_analyze.return_value = [
            AnalysisResult(
                model=mock_affected_model,
                reason=ExecutionReason.UPSTREAM_MACRO_CHANGED,
            ),
            AnalysisResult(model=mock_other_model, needs_execution=False),
        ]

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        # Verify analyze was called with dbt_parser and None selection
        args, kwargs = mock_analyze.call_args
        assert len(args) == 0  # No positional args
        assert "dbt_selection" in kwargs
        assert kwargs["dbt_selection"] is None
        assert "dbt_parser" in kwargs
        assert "Models needing execution: 1" in result.stdout


class TestCacheAnalyzer:
    """Test the cache analyzer functionality."""

    @patch("dbt_toolbox.cli.analyze.print_column_analysis_results")
    @patch("dbt_toolbox.cli.analyze.analyze_model_statuses")
    def test_analyze_with_no_models(self, mock_analyze: Mock, mock_column_analysis: Mock) -> None:
        """Test analyzing when no models are available."""
        mock_analyze.return_value = []

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        assert "Total models analyzed: 0" in result.stdout

    def test_format_time_delta(self) -> None:
        """Test time delta formatting."""
        from datetime import timedelta

        from dbt_toolbox.cli.analyze import _format_time_delta

        # Test seconds
        delta = timedelta(seconds=30)
        result = _format_time_delta(delta)
        assert result == "30 seconds"

        # Test minutes
        delta = timedelta(minutes=5, seconds=30)
        result = _format_time_delta(delta)
        assert result == "5 minutes"

        # Test hours
        delta = timedelta(hours=2, minutes=30)
        result = _format_time_delta(delta)
        assert result == "2 hours"

        # Test days
        delta = timedelta(days=3, hours=5)
        result = _format_time_delta(delta)
        assert result == "3 days"
