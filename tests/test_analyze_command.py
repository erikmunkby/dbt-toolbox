"""Tests for the analyze command."""

from unittest.mock import Mock, patch

from typer.testing import CliRunner

from dbt_toolbox.analysees import (
    AnalysisResult,
    AnalysisResults,
    ColumnAnalysis,
    DocsAnalysis,
    ExecutionReason,
)
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

    @patch("dbt_toolbox.cli.analyze.analyze")
    @patch("dbt_toolbox.cli.analyze.print_analysis_results")
    def test_analyze_with_model_selection(
        self, mock_print_analysis: Mock, mock_analyze: Mock
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

        mock_analyze.return_value = AnalysisResults(
            model_analysis=[
                AnalysisResult(model=mock_model1, needs_execution=False),
                AnalysisResult(model=mock_model2, needs_execution=False),
            ],
            column_analysis=ColumnAnalysis(model_results=[], overall_status="OK"),
            docs_analysis=DocsAnalysis(
                overall_status="OK", duplicate_issues=[], total_docs_macros=0, unique_docs_macros=0
            ),
        )

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze", "--model", "customers+"])

        assert result.exit_code == 0
        # Verify analyze was called with target and model
        args, kwargs = mock_analyze.call_args
        assert len(args) == 0  # No positional args
        assert "model" in kwargs
        assert kwargs["model"] == "customers+"
        assert "target" in kwargs
        # Verify print_analysis_results was called with the mocked results
        mock_print_analysis.assert_called_once()

    @patch("dbt_toolbox.cli.analyze.analyze")
    @patch("dbt_toolbox.cli.analyze.print_analysis_results")
    def test_analyze_with_failed_models(
        self, mock_print_analysis: Mock, mock_analyze: Mock
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

        mock_analyze.return_value = AnalysisResults(
            model_analysis=[
                AnalysisResult(
                    model=mock_failed_model,
                    reason=ExecutionReason.LAST_EXECUTION_FAILED,
                ),
                AnalysisResult(model=mock_model1, needs_execution=False),
                AnalysisResult(model=mock_model2, needs_execution=False),
            ],
            column_analysis=ColumnAnalysis(model_results=[], overall_status="OK"),
            docs_analysis=DocsAnalysis(
                overall_status="OK", duplicate_issues=[], total_docs_macros=0, unique_docs_macros=0
            ),
        )

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        # Verify analyze was called with dbt_parser and None selection
        args, kwargs = mock_analyze.call_args
        assert len(args) == 0  # No positional args
        assert "model" in kwargs
        assert kwargs["model"] is None
        assert "target" in kwargs
        # Verify print_analysis_results was called with the mocked results
        mock_print_analysis.assert_called_once()

    @patch("dbt_toolbox.cli.analyze.analyze")
    @patch("dbt_toolbox.cli.analyze.print_analysis_results")
    def test_analyze_with_upstream_macro_changes(
        self, mock_print_analysis: Mock, mock_analyze: Mock
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

        mock_analyze.return_value = AnalysisResults(
            model_analysis=[
                AnalysisResult(
                    model=mock_affected_model,
                    reason=ExecutionReason.UPSTREAM_MACRO_CHANGED,
                ),
                AnalysisResult(model=mock_other_model, needs_execution=False),
            ],
            column_analysis=ColumnAnalysis(model_results=[], overall_status="OK"),
            docs_analysis=DocsAnalysis(
                overall_status="OK", duplicate_issues=[], total_docs_macros=0, unique_docs_macros=0
            ),
        )

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        # Verify analyze was called with dbt_parser and None selection
        args, kwargs = mock_analyze.call_args
        assert len(args) == 0  # No positional args
        assert "model" in kwargs
        assert kwargs["model"] is None
        assert "target" in kwargs
        # Verify print_analysis_results was called with the mocked results
        mock_print_analysis.assert_called_once()


class TestCacheAnalyzer:
    """Test the cache analyzer functionality."""

    @patch("dbt_toolbox.cli.analyze.analyze")
    @patch("dbt_toolbox.cli.analyze.print_analysis_results")
    def test_analyze_with_no_models(self, mock_print_analysis: Mock, mock_analyze: Mock) -> None:
        """Test analyzing when no models are available."""
        mock_analyze.return_value = AnalysisResults(
            model_analysis=[],
            column_analysis=ColumnAnalysis(model_results=[], overall_status="OK"),
            docs_analysis=DocsAnalysis(
                overall_status="OK", duplicate_issues=[], total_docs_macros=0, unique_docs_macros=0
            ),
        )

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        # Verify print_analysis_results was called with the mocked results
        mock_print_analysis.assert_called_once()
