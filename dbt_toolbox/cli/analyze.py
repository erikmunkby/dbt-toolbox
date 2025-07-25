"""Analyze command for comprehensive cache analysis without manipulation."""

from dataclasses import dataclass
from datetime import timedelta
from typing import NamedTuple

import typer
from rich.console import Console
from rich.table import Table

from dbt_toolbox.cli._build_analysis import BuildAnalyzer, ExecutionReason
from dbt_toolbox.constants import EXECUTION_TIMESTAMP
from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser.dbt_parser import dbt_parser
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import printer


class CacheAnalysisResult(NamedTuple):
    """Result of cache analysis for a model."""

    model_name: str
    status: str  # "outdated", "id_mismatch", "failed", "valid", "upstream_changed"
    issue_description: str
    timestamp_info: str | None = None
    upstream_changes: list[ExecutionReason] | None = None


@dataclass
class CacheAnalysis:
    """Comprehensive cache analysis results."""

    outdated_models: list[CacheAnalysisResult]
    id_mismatch_models: list[CacheAnalysisResult]
    failed_models: list[CacheAnalysisResult]
    upstream_changed_models: list[CacheAnalysisResult]
    valid_models: list[CacheAnalysisResult]

    @property
    def total_models(self) -> int:
        """Total number of models analyzed."""
        return (
            len(self.outdated_models)
            + len(self.id_mismatch_models)
            + len(self.failed_models)
            + len(self.upstream_changed_models)
            + len(self.valid_models)
        )

    @property
    def models_needing_execution(self) -> list[CacheAnalysisResult]:
        """All models that need execution."""
        return (
            self.outdated_models
            + self.id_mismatch_models
            + self.failed_models
            + self.upstream_changed_models
        )


class CacheAnalyzer:
    """Analyzes cache state without manipulating it."""

    def __init__(self) -> None:
        """Initialize the cache analyzer."""
        self.build_analyzer = BuildAnalyzer()

    def analyze_all_models(self, model_selection: str | None = None) -> CacheAnalysis:
        """Analyze all models' cache state.

        Args:
            model_selection: Optional dbt model selection string

        Returns:
            Complete cache analysis results

        """
        # Get cached models (read-only, no cache updates)
        cached_models = dbt_parser.cached_models

        # Get all raw models that exist in the project
        raw_models = dbt_parser.list_raw_models

        # Get target models - include both cached and raw models
        if model_selection:
            target_models = self.build_analyzer.parse_dbt_selection(model_selection)
            # Filter to only models that exist in the project
            target_models = target_models.intersection(raw_models.keys())
        else:
            # Get all models that exist in the project
            target_models = set(raw_models.keys())

        # Initialize result lists
        outdated_models = []
        id_mismatch_models = []
        failed_models = []
        upstream_changed_models = []
        valid_models = []

        for model_name in sorted(target_models):
            # Check if model failed first (even if not in cache)
            if self._is_model_failed(model_name):
                failed_models.append(
                    CacheAnalysisResult(
                        model_name=model_name,
                        status="failed",
                        issue_description="Model failed in last execution and needs re-run",
                        timestamp_info=None,
                    ),
                )
                continue

            # If model is not in cache, it needs execution
            if model_name not in cached_models:
                outdated_models.append(
                    CacheAnalysisResult(
                        model_name=model_name,
                        status="outdated",
                        issue_description="Model not found in cache",
                        timestamp_info=None,
                    ),
                )
                continue

            model = cached_models[model_name]
            analysis = self._analyze_single_model(model)

            if analysis.status == "outdated":
                outdated_models.append(analysis)
            elif analysis.status == "id_mismatch":
                id_mismatch_models.append(analysis)
            elif analysis.status == "failed":
                failed_models.append(analysis)
            elif analysis.status == "upstream_changed":
                upstream_changed_models.append(analysis)
            else:
                valid_models.append(analysis)

        return CacheAnalysis(
            outdated_models=outdated_models,
            id_mismatch_models=id_mismatch_models,
            failed_models=failed_models,
            upstream_changed_models=upstream_changed_models,
            valid_models=valid_models,
        )

    def _analyze_single_model(self, model: Model) -> CacheAnalysisResult:
        """Analyze a single model's cache state.

        Args:
            model: The model to analyze

        Returns:
            Cache analysis result for the model

        """
        # Check if model was marked as failed
        if self._is_model_failed(model.name):
            return CacheAnalysisResult(
                model_name=model.name,
                status="failed",
                issue_description="Model failed in last execution and needs re-run",
                timestamp_info=None,
            )

        # Check for ID mismatch (model has been modified)
        cached_hash = self._get_cached_model_hash(model.name)
        if cached_hash and cached_hash != model.hash:
            return CacheAnalysisResult(
                model_name=model.name,
                status="id_mismatch",
                issue_description="Model code has been modified since last cache",
                timestamp_info=None,
            )

        # Check if cache is outdated
        cache_expiration = EXECUTION_TIMESTAMP - timedelta(minutes=settings.cache_validity_minutes)
        if model.last_checked < cache_expiration:
            age_delta = EXECUTION_TIMESTAMP - model.last_checked
            age_description = self._format_time_delta(age_delta)

            return CacheAnalysisResult(
                model_name=model.name,
                status="outdated",
                issue_description=f"Cache is older than {settings.cache_validity_minutes} minutes",
                timestamp_info=f"Last updated: {age_description} ago",
            )

        # Check for upstream changes (models and macros)
        upstream_changes = []

        # Check upstream models
        changed_upstream_models = self.build_analyzer.upstream_models_changed(model)
        if changed_upstream_models:
            upstream_changes.append(
                ExecutionReason(
                    code="UPSTREAM_MODELS_CHANGED",
                    description=f"Upstream models changed: {', '.join(changed_upstream_models)}",
                ),
            )

        # Check upstream macros
        changed_upstream_macros = self.build_analyzer.upstream_macros_changed(model)
        if changed_upstream_macros:
            upstream_changes.append(
                ExecutionReason(
                    code="UPSTREAM_MACROS_CHANGED",
                    description=f"Upstream macros changed: {', '.join(changed_upstream_macros)}",
                ),
            )

        if upstream_changes:
            change_descriptions = [reason.description for reason in upstream_changes]
            return CacheAnalysisResult(
                model_name=model.name,
                status="upstream_changed",
                issue_description="; ".join(change_descriptions),
                timestamp_info=None,
                upstream_changes=upstream_changes,
            )

        # Model cache is valid
        age_delta = EXECUTION_TIMESTAMP - model.last_checked
        age_description = self._format_time_delta(age_delta)

        return CacheAnalysisResult(
            model_name=model.name,
            status="valid",
            issue_description="Cache is up to date",
            timestamp_info=f"Last updated: {age_description} ago",
        )

    def _is_model_failed(self, model_name: str) -> bool:
        """Check if a model is marked as failed.

        Args:
            model_name: Name of the model to check

        Returns:
            True if model is marked as failed

        """
        # Check if model is in the failed models cache
        if dbt_parser.cache.cache_failed_models.exists():
            return model_name in dbt_parser.cache.cache_failed_models.read()
        return False

    def _get_cached_model_hash(self, model_name: str) -> str | None:
        """Get the cached hash for a model.

        Args:
            model_name: Name of the model

        Returns:
            Cached model hash or None if not found

        """
        if dbt_parser.cache.cache_models.exists():
            cached_models = dbt_parser.cache.cache_models.read()
            model = cached_models.get(model_name)
            if model:
                return model.hash
        return None

    def _format_time_delta(self, delta: timedelta) -> str:
        """Format a time delta in human-readable format.

        Args:
            delta: Time delta to format

        Returns:
            Human-readable time string

        """
        total_seconds = int(delta.total_seconds())

        if total_seconds < 60:  # noqa: PLR2004
            return f"{total_seconds} seconds"
        if total_seconds < 3600:  # noqa: PLR2004
            minutes = total_seconds // 60
            return f"{minutes} minutes"
        if total_seconds < 86400:  # noqa: PLR2004
            hours = total_seconds // 3600
            return f"{hours} hours"
        days = total_seconds // 86400
        return f"{days} days"


def print_analysis_results(analysis: CacheAnalysis) -> None:  # noqa: C901
    """Print cache analysis results in a formatted way.

    Args:
        analysis: Cache analysis results to print

    """
    console = Console()

    # Header
    printer.cprint("🔍 Cache Analysis Results", color="cyan")
    printer.cprint(f"Total models analyzed: {analysis.total_models}", color="nocolor")

    if analysis.models_needing_execution:
        printer.cprint(
            f"Models needing execution: {len(analysis.models_needing_execution)}",
            color="yellow",
        )
    else:
        printer.cprint("✅ All models have valid cache!", color="green")

    print()  # noqa: T201 blankline

    # Failed models section
    if analysis.failed_models:
        printer.cprint(f"❌ Failed Models ({len(analysis.failed_models)}):", color="red")
        table = Table(show_header=True, header_style="bold red")
        table.add_column("Model", style="red")
        table.add_column("Issue", style="white")

        for result in analysis.failed_models:
            table.add_row(result.model_name, result.issue_description)

        console.print(table)
        print()  # noqa: T201 blankline

    # ID mismatch models section
    if analysis.id_mismatch_models:
        printer.cprint(f"🔄 Modified Models ({len(analysis.id_mismatch_models)}):", color="yellow")
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Model", style="yellow")
        table.add_column("Issue", style="white")

        for result in analysis.id_mismatch_models:
            table.add_row(result.model_name, result.issue_description)

        console.print(table)
        print()  # noqa: T201 blankline

    # Upstream changed models section
    if analysis.upstream_changed_models:
        printer.cprint(
            f"🔗 Upstream Dependencies Changed ({len(analysis.upstream_changed_models)}):",
            color="yellow",
        )
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Model", style="magenta")
        table.add_column("Upstream Changes", style="white")

        for result in analysis.upstream_changed_models:
            table.add_row(result.model_name, result.issue_description)

        console.print(table)
        print()  # noqa: T201 blankline

    # Outdated models section
    if analysis.outdated_models:
        printer.cprint(f"⏰ Outdated Models ({len(analysis.outdated_models)}):", color="cyan")
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Model", style="blue")
        table.add_column("Issue", style="white")
        table.add_column("Age", style="cyan")

        for result in analysis.outdated_models:
            table.add_row(
                result.model_name,
                result.issue_description,
                result.timestamp_info or "N/A",
            )

        console.print(table)

    # Valid models section (only show count unless verbose)
    if analysis.valid_models:
        printer.cprint(f"✅ Valid Models ({len(analysis.valid_models)}):", color="green")
        # Just show a summary for valid models to keep output clean
        for result in analysis.valid_models:
            printer.cprint(
                f"   • {result.model_name} - {result.timestamp_info}",
                color="bright_black",
            )


# Global analyzer instance
cache_analyzer = CacheAnalyzer()


def analyze_command(
    model: str | None = typer.Option(
        None,
        "--model",
        "-m",
        "--select",
        "-s",
        help="Analyze specific models (dbt selection syntax)",
    ),
) -> None:
    """Analyze cache state without manipulating it.

    Shows outdated models, ID mismatches, and failed models that need re-execution.
    """
    printer.cprint("🔍 Analyzing model cache state...", color="cyan")

    # Perform analysis
    analysis = cache_analyzer.analyze_all_models(model)

    # Print results
    print_analysis_results(analysis)

    # Summary
    if analysis.models_needing_execution:
        printer.cprint(
            f"\n💡 Tip: Run 'dt build' to execute the {len(analysis.models_needing_execution)} "
            "models that need updates.",
            color="cyan",
        )
