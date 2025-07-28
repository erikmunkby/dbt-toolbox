"""Build analysis logic for intelligent execution."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

from dbt_toolbox.constants import EXECUTION_TIMESTAMP
from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser.dbt_parser import dbt_parser
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import printer


class ExecutionReason(NamedTuple):
    """Reason why a model needs execution."""

    code: str
    description: str


@dataclass
class ModelExecutionAnalysis:
    """Analysis result for a model's execution necessity."""

    model: Model
    needs_execution: bool
    reasons: list[ExecutionReason]

    def __post_init__(self) -> None:
        """Ensure consistency between needs_execution and reasons."""
        self.needs_execution = len(self.reasons) > 0


class BuildAnalyzer:
    """Analyzes which models need execution based on cache and dependencies."""

    def __init__(self) -> None:
        """Initialize the build analyzer."""
        # Don't cache the validity delta to allow for dynamic updates

    @property
    def cache_expiration(self) -> datetime:
        """Get the current cache validity delta."""
        return EXECUTION_TIMESTAMP - timedelta(minutes=settings.cache_validity_minutes)

    def parse_dbt_selection(self, selection: str | None) -> set[str]:
        """Parse dbt model selection syntax to get target models.

        Args:
            selection: dbt selection string (e.g., "my_model+", "+my_model", "my_model")

        Returns:
            Set of model names that would be executed by dbt.

        """
        if not selection:
            # No selection means all models
            return set(dbt_parser.models.keys())

        target_models = set()

        # Handle multiple selections separated by comma or space
        selections = re.split(r"[,\s]+", selection.strip())

        for sel in selections:
            if not sel:
                continue

            # Parse selection patterns
            if sel.endswith("+"):
                # downstream selection: "model+"
                model_name = sel[:-1]
                if model_name in dbt_parser.models:
                    target_models.add(model_name)
                    # Add all downstream models
                    downstream_models = dbt_parser.get_downstream_models(model_name)
                    target_models.update(m.name for m in downstream_models)
            elif sel.startswith("+"):
                # upstream selection: "+model"
                model_name = sel[1:]
                if model_name in dbt_parser.models:
                    target_models.add(model_name)
                    # Add all upstream models
                    upstream_nodes = dbt_parser.dependency_graph.get_upstream_nodes(model_name)
                    # Filter to only models (not macros)
                    upstream_models = [
                        node
                        for node in upstream_nodes
                        if dbt_parser.dependency_graph.get_node_type(node) == "model"
                    ]
                    target_models.update(upstream_models)
            elif "+" in sel:
                # bidirectional selection: "+model+" or complex patterns
                parts = sel.split("+")
                if len(parts) == 3 and parts[0] == "" and parts[2] == "":  # noqa: PLR2004
                    # "+model+" pattern
                    model_name = parts[1]
                    if model_name in dbt_parser.models:
                        target_models.add(model_name)
                        # Add upstream
                        upstream_nodes = dbt_parser.dependency_graph.get_upstream_nodes(model_name)
                        upstream_models = [
                            node
                            for node in upstream_nodes
                            if dbt_parser.dependency_graph.get_node_type(node) == "model"
                        ]
                        target_models.update(upstream_models)
                        # Add downstream
                        downstream_models = dbt_parser.get_downstream_models(model_name)
                        target_models.update(m.name for m in downstream_models)
            # direct model selection
            elif sel in dbt_parser.models:
                target_models.add(sel)

        return target_models

    def upstream_models_changed(self, model: Model) -> list[str]:
        """Get list of upstream models that have changed."""
        return [
            m
            for m in model.upstream.models
            if m in dbt_parser.models and not dbt_parser.models[m].is_fresh
        ]

    def upstream_macros_changed(self, model: Model) -> list[str]:
        """Get list of upstream macros that have changed."""
        return [
            macro_name
            for macro_name in model.upstream.macros
            if dbt_parser.macro_changed(macro_name)
        ]

    def analyze_model_execution(self, model: Model) -> ModelExecutionAnalysis:
        """Analyze if a model needs execution and why.

        Args:
            model: The model to analyze.

        Returns:
            ModelExecutionAnalysis with execution decision and reasons.

        """
        reasons = []

        # Check condition 1: Model itself has changed
        if not model.is_fresh:
            reasons.append(
                ExecutionReason(
                    code="MODEL_STALE",
                    description=f"Model '{model.name}' has changed since last cache or cache "
                    f"timed out ({settings.cache_validity_minutes}min limit)",
                ),
            )

        # Check condition 3: Upstream models changed
        changed_upstream_models = self.upstream_models_changed(model)
        if changed_upstream_models:
            reasons.append(
                ExecutionReason(
                    code="UPSTREAM_MODELS_CHANGED",
                    description=f"Upstream models changed: {', '.join(changed_upstream_models)}",
                ),
            )

        # Check condition 4: Upstream macros changed
        changed_upstream_macros = self.upstream_macros_changed(model)
        if changed_upstream_macros:
            reasons.append(
                ExecutionReason(
                    code="UPSTREAM_MACROS_CHANGED",
                    description=f"Upstream macros changed: {', '.join(changed_upstream_macros)}",
                ),
            )

        return ModelExecutionAnalysis(
            model=model,
            needs_execution=(len(reasons) > 0),
            reasons=reasons,
        )

    def analyze_build_execution(
        self,
        selection: str | None = None,
    ) -> dict[str, ModelExecutionAnalysis]:
        """Analyze which models need execution for a build command.

        Args:
            selection: dbt selection string (e.g., "my_model+")

        Returns:
            Dictionary mapping model names to their execution analysis.

        """
        # Get target models from dbt selection
        target_models = self.parse_dbt_selection(selection)

        # Analyze each target model
        analysis_results = {}
        for model_name in target_models:
            if model_name in dbt_parser.models:
                model = dbt_parser.models[model_name]
                analysis_results[model_name] = self.analyze_model_execution(model)

        return analysis_results

    def print_execution_analysis(
        self,
        analyses: dict[str, ModelExecutionAnalysis],
        verbose: bool = False,
    ) -> None:
        """Print a summary of the execution analysis.

        Args:
            analyses: Dictionary of model execution analyses.
            verbose: Whether to list all models that need execution.

        """
        total_models = len(analyses)
        models_to_execute = sum(1 for a in analyses.values() if a.needs_execution)
        models_to_skip = total_models - models_to_execute

        printer.cprint("🔍 Build Execution Analysis", color="cyan")
        printer.cprint(f"   📊 Total models in selection: {total_models}")
        printer.cprint(f"   ✅ Models to execute: {models_to_execute}")
        printer.cprint(f"   ⏭️  Models to skip: {models_to_skip}")

        if verbose and models_to_execute > 0:
            printer.cprint("\n📋 Models requiring execution:", color="yellow")
            for model_name, analysis in analyses.items():
                if analysis.needs_execution:
                    printer.cprint(f"  • {model_name}")
                    for reason in analysis.reasons:
                        printer.cprint(f"    - {reason.description}", color="bright_black")

        if verbose and models_to_skip > 0:
            printer.cprint("\n⏭️  Models with valid cache (skipping):", color="green")
            for model_name, analysis in analyses.items():
                if not analysis.needs_execution:
                    model_checked = analysis.model.last_built

                    # Skip models that were never built
                    if model_checked is None:
                        printer.cprint(f"  • {model_name} (never built)")
                        continue

                    now = datetime.now(timezone.utc)

                    # Handle timezone-naive datetimes by assuming UTC
                    if model_checked.tzinfo is None:
                        model_checked = model_checked.replace(tzinfo=timezone.utc)

                    age_minutes = (now - model_checked).total_seconds() / 60
                    printer.cprint(f"  • {model_name} (cached {age_minutes:.1f}m ago)")


# Global analyzer instance
build_analyzer = BuildAnalyzer()
