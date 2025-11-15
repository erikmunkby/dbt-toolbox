"""Tests for analysees.models module."""

from dbt_toolbox.analysees.data_models import ExecutionReason
from dbt_toolbox.analysees.models import _analyze_model, analyze_model_statuses
from dbt_toolbox.dbt_parser._dbt_parser import dbtParser


class TestAnalyzeModel:
    """Test the _analyze_model function."""

    def test_analyze_model_never_built(self, parser: dbtParser) -> None:
        """Test that models that have never been built are flagged."""
        model_name = "customer_orders"
        model = parser.models[model_name]

        # Set last_built to None (never built)
        model.last_built = None

        result = _analyze_model(model)

        assert result.needs_execution is True
        assert result.reason == ExecutionReason.NEVER_BUILT

    def test_analyze_model_last_build_failed(self, parser: dbtParser) -> None:
        """Test that failed models are flagged for re-execution."""
        from datetime import datetime, timezone

        model_name = "customer_orders"
        model = parser.models[model_name]

        # Mark as built (not never built) but failed
        model.last_built = datetime.now(tz=timezone.utc)
        model.last_build_failed = True

        result = _analyze_model(model)

        assert result.needs_execution is True
        assert result.reason == ExecutionReason.LAST_EXECUTION_FAILED

    def test_analyze_model_code_changed(self, parser: dbtParser) -> None:
        """Test that models with changed code are detected."""
        from datetime import datetime, timezone

        model_name = "customer_orders"
        model = parser.models[model_name]

        # Mark as built (not never built) with code changed
        model.last_built = datetime.now(tz=timezone.utc)
        model.last_build_failed = False
        model.code_changed = True

        result = _analyze_model(model)

        assert result.needs_execution is True
        assert result.reason == ExecutionReason.CODE_CHANGED

    def test_analyze_model_upstream_macros_changed(self, parser: dbtParser) -> None:
        """Test that models with changed upstream macros are detected."""
        from datetime import datetime, timezone

        model_name = "customer_orders"
        model = parser.models[model_name]

        # Mark as built (not never built) with upstream macros changed
        model.last_built = datetime.now(tz=timezone.utc)
        model.last_build_failed = False
        model.code_changed = False
        model.upstream_macros_changed = True

        result = _analyze_model(model)

        assert result.needs_execution is True
        assert result.reason == ExecutionReason.UPSTREAM_MACRO_CHANGED

    def test_analyze_model_returns_result(self, parser: dbtParser) -> None:
        """Test that _analyze_model returns an AnalysisResult."""
        model_name = "customer_orders"
        model = parser.models[model_name]

        result = _analyze_model(model)

        # Should return an AnalysisResult
        assert hasattr(result, "model")
        assert hasattr(result, "needs_execution")
        assert hasattr(result, "reason")


class TestAnalyzeModelStatuses:
    """Test the analyze_model_statuses function."""

    def test_analyze_all_models(self, parser: dbtParser) -> None:
        """Test analyzing all models without selection."""
        selected_models = parser.models
        results = analyze_model_statuses(parser, selected_models)

        assert len(results) > 0
        assert all(hasattr(r, "model") for r in results)
        assert all(hasattr(r, "needs_execution") for r in results)

    def test_analyze_specific_model(self, parser: dbtParser) -> None:
        """Test analyzing a specific model."""
        selected_models = {"customer_orders": parser.models["customer_orders"]}
        results = analyze_model_statuses(parser, selected_models)

        # Should only analyze the selected model
        assert len(results) >= 1
        model_names = [r.model.name for r in results]
        assert "customer_orders" in model_names

    def test_analyze_returns_analysis_results(self, parser: dbtParser) -> None:
        """Test that analyze_model_statuses returns proper AnalysisResult objects."""
        selected_models = parser.models
        results = analyze_model_statuses(parser, selected_models)

        # All results should have required attributes
        assert all(hasattr(r, "model") for r in results)
        assert all(hasattr(r, "needs_execution") for r in results)
        assert all(hasattr(r, "reason") for r in results)
