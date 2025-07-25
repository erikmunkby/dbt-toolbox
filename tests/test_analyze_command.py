"""Tests for the analyze command."""

from unittest.mock import Mock, patch

from typer.testing import CliRunner

from dbt_toolbox.cli.analyze import CacheAnalysisResult, cache_analyzer
from dbt_toolbox.cli.main import app


class TestAnalyzeCommand:
    """Test the dt analyze command."""

    def test_analyze_command_exists(self) -> None:
        """Test that the analyze command is registered in the CLI app."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "analyze" in result.stdout

    def test_analyze_command_help(self) -> None:
        """Test that the analyze command shows help correctly."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze", "--help"])

        # Should exit successfully after showing help
        assert result.exit_code == 0
        # Should contain analyze command documentation
        assert "Analyze cache state without manipulating it" in result.stdout

    @patch("dbt_toolbox.cli.analyze.cache_analyzer")
    def test_analyze_with_model_selection(self, mock_analyzer: Mock) -> None:
        """Test analyze command with model selection."""
        # Mock analysis results
        mock_analysis = Mock()
        mock_analysis.total_models = 2
        mock_analysis.models_needing_execution = []
        mock_analysis.failed_models = []
        mock_analysis.id_mismatch_models = []
        mock_analysis.outdated_models = []
        mock_analysis.upstream_changed_models = []
        mock_analysis.valid_models = [
            CacheAnalysisResult("customers", "valid", "Cache is up to date", "2 minutes ago"),
            CacheAnalysisResult("orders", "valid", "Cache is up to date", "3 minutes ago"),
        ]
        mock_analyzer.analyze_all_models.return_value = mock_analysis

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze", "--model", "customers+"])

        assert result.exit_code == 0
        mock_analyzer.analyze_all_models.assert_called_once_with("customers+")
        assert "Cache Analysis Results" in result.stdout
        assert "All models have valid cache!" in result.stdout

    @patch("dbt_toolbox.cli.analyze.cache_analyzer")
    def test_analyze_with_failed_models(self, mock_analyzer: Mock) -> None:
        """Test analyze command with failed models."""
        # Mock analysis results with failed models
        mock_analysis = Mock()
        mock_analysis.total_models = 3
        mock_analysis.models_needing_execution = [
            CacheAnalysisResult("failed_model", "failed", "Model failed in last execution", None),
        ]
        mock_analysis.failed_models = [
            CacheAnalysisResult("failed_model", "failed", "Model failed in last execution", None),
        ]
        mock_analysis.id_mismatch_models = []
        mock_analysis.outdated_models = []
        mock_analysis.upstream_changed_models = []
        mock_analysis.valid_models = [
            CacheAnalysisResult("customers", "valid", "Cache is up to date", "2 minutes ago"),
            CacheAnalysisResult("orders", "valid", "Cache is up to date", "3 minutes ago"),
        ]
        mock_analyzer.analyze_all_models.return_value = mock_analysis

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        mock_analyzer.analyze_all_models.assert_called_once_with(None)
        assert "Failed Models (1)" in result.stdout
        assert "Models needing execution: 1" in result.stdout

    @patch("dbt_toolbox.cli.analyze.cache_analyzer")
    def test_analyze_with_upstream_macro_changes(self, mock_analyzer: Mock) -> None:
        """Test analyze command detecting upstream macro changes."""
        # Mock analysis results with upstream changes
        mock_analysis = Mock()
        mock_analysis.total_models = 2
        mock_analysis.models_needing_execution = [
            CacheAnalysisResult(
                "affected_model",
                "upstream_changed",
                "Upstream macros changed: simple_macro",
                None,
            ),
        ]
        mock_analysis.failed_models = []
        mock_analysis.id_mismatch_models = []
        mock_analysis.outdated_models = []
        mock_analysis.upstream_changed_models = [
            CacheAnalysisResult(
                "affected_model",
                "upstream_changed",
                "Upstream macros changed: simple_macro",
                None,
            ),
        ]
        mock_analysis.valid_models = [
            CacheAnalysisResult("other_model", "valid", "Cache is up to date", "2 minutes ago"),
        ]
        mock_analyzer.analyze_all_models.return_value = mock_analysis

        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["analyze"])

        assert result.exit_code == 0
        mock_analyzer.analyze_all_models.assert_called_once_with(None)
        assert "Upstream Dependencies Changed (1)" in result.stdout
        assert "Models needing execution: 1" in result.stdout


class TestCacheAnalyzer:
    """Test the cache analyzer functionality."""

    def test_cache_analysis_result_structure(self) -> None:
        """Test CacheAnalysisResult structure."""
        result = CacheAnalysisResult(
            model_name="test_model",
            status="valid",
            issue_description="Cache is up to date",
            timestamp_info="5 minutes ago",
        )

        assert result.model_name == "test_model"
        assert result.status == "valid"
        assert result.issue_description == "Cache is up to date"
        assert result.timestamp_info == "5 minutes ago"

    @patch("dbt_toolbox.cli.analyze.dbt_parser")
    def test_analyze_with_no_models(self, mock_dbt_parser: Mock) -> None:
        """Test analyzing when no models are available."""
        mock_dbt_parser.models = {}

        analysis = cache_analyzer.analyze_all_models()

        assert analysis.total_models == 0
        assert len(analysis.models_needing_execution) == 0

    @patch("dbt_toolbox.cli.analyze.dbt_parser")
    def test_analyze_with_failed_model_tracking(self, mock_dbt_parser: Mock) -> None:
        """Test analyzing with failed model tracking."""
        # Mock a model
        mock_model = Mock()
        mock_model.name = "test_model"
        mock_model.hash = "test_hash"
        mock_model.last_checked = Mock()  # Will be mocked as recent

        mock_dbt_parser.models = {"test_model": mock_model}
        mock_dbt_parser.cached_models = {"test_model": mock_model}
        mock_dbt_parser.list_raw_models = {"test_model": mock_model}

        # Mock the cache_failed_models property directly
        mock_cache_failed_models = Mock()
        mock_cache_failed_models.exists.return_value = True
        mock_cache_failed_models.read.return_value = {"test_model"}

        mock_cache = Mock()
        mock_cache.cache_failed_models = mock_cache_failed_models
        mock_dbt_parser.cache = mock_cache

        analysis = cache_analyzer.analyze_all_models()

        # Should detect the failed model
        assert len(analysis.failed_models) == 1
        assert analysis.failed_models[0].model_name == "test_model"
        assert analysis.failed_models[0].status == "failed"

    def test_format_time_delta(self) -> None:
        """Test time delta formatting."""
        from datetime import timedelta

        # Test seconds
        delta = timedelta(seconds=30)
        result = cache_analyzer._format_time_delta(delta)
        assert result == "30 seconds"

        # Test minutes
        delta = timedelta(minutes=5, seconds=30)
        result = cache_analyzer._format_time_delta(delta)
        assert result == "5 minutes"

        # Test hours
        delta = timedelta(hours=2, minutes=30)
        result = cache_analyzer._format_time_delta(delta)
        assert result == "2 hours"

        # Test days
        delta = timedelta(days=3, hours=5)
        result = cache_analyzer._format_time_delta(delta)
        assert result == "3 days"
