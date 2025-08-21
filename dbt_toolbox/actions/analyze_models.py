"""Module for all model analysees."""

import copy
from dataclasses import dataclass
from enum import Enum

from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.utils import _printers


class ExecutionReason(Enum):
    """Enum defining reasons why a model needs execution."""

    UPSTREAM_MODEL_CHANGED = "upstream_model_changed"
    UPSTREAM_MACRO_CHANGED = "upstream_macro_changed"
    OUTDATED_MODEL = "outdated_model"
    LAST_EXECUTION_FAILED = "last_execution_failed"
    CODE_CHANGED = "code_changed"


@dataclass
class AnalysisResult:
    """Results of the analysis."""

    model: Model
    needs_execution: bool = True
    reason: ExecutionReason | None = None

    @property
    def reason_description(self) -> str:
        """Return a human-readable description of the execution reason."""
        return {
            ExecutionReason.CODE_CHANGED: "Model code changed.",
            ExecutionReason.UPSTREAM_MACRO_CHANGED: "Upstream macro changed.",
            ExecutionReason.UPSTREAM_MODEL_CHANGED: "Upstream model changed.",
            ExecutionReason.OUTDATED_MODEL: "Model build is outdated.",
            ExecutionReason.LAST_EXECUTION_FAILED: "Last model execution failed.",
            None: "",
        }[self.reason]


def _analyze_model(model: Model) -> AnalysisResult:
    """Will analyze the model to see if it needs updating.

    Prio order:
    1. Last build failed
    2. Code changed
    3. Upstream macros changed
    4. Cache outdated
    """
    # Check if the model needs execution
    if model.last_build_failed:
        return AnalysisResult(model=model, reason=ExecutionReason.LAST_EXECUTION_FAILED)
    if model.code_changed:
        return AnalysisResult(model=model, reason=ExecutionReason.CODE_CHANGED)
    if model.upstream_macros_changed:
        return AnalysisResult(model=model, reason=ExecutionReason.UPSTREAM_MACRO_CHANGED)
    if model.cache_outdated:
        return AnalysisResult(model=model, reason=ExecutionReason.OUTDATED_MODEL)
    return AnalysisResult(model=model, needs_execution=False)


def analyze_model_statuses(
    dbt_parser: dbtParser, dbt_selection: str | None = None
) -> list[AnalysisResult]:
    """Analyze the execution status of models based on their dependencies and cache.

    Args:
        dbt_parser: The dbt parser object.
        dbt_selection: Optional dbt selection string to filter models

    Returns:
        A list of AnalysisResult objects representing the analysis of each model's status.

    """
    models_selected = dbt_parser.parse_dbt_selection(selection=dbt_selection)
    # First do a simple analysis of models, freshness and last execution status
    analysees: dict[str, AnalysisResult] = {
        name: _analyze_model(dbt_parser.models[name]) for name in models_selected
    }

    # Then flag all downstream models, if they're not already part of list.
    for model_name, analysis in copy.copy(analysees).items():
        if analysis.needs_execution:
            for downstream_model in dbt_parser.get_downstream_models(model_name):
                if downstream_model.name not in analysees:
                    analysees[downstream_model.name] = AnalysisResult(
                        model=downstream_model, reason=ExecutionReason.UPSTREAM_MODEL_CHANGED
                    )

    # Finally prune any not in selection and return as list
    return [result for model_name, result in analysees.items() if model_name in models_selected]


def print_execution_analysis(
    analyses: list[AnalysisResult],
    verbose: bool = False,
) -> None:
    """Print a summary of the execution analysis.

    Args:
        analyses: List of model execution analyses.
        verbose: Whether to list all models that need execution.

    """
    total_models = len(analyses)
    models_to_execute = sum(1 for a in analyses if a.needs_execution)
    models_to_skip = total_models - models_to_execute

    _printers.cprint("🔍 Build Execution Analysis", color="cyan")
    _printers.cprint(f"   📊 Total models in selection: {total_models}")
    _printers.cprint(f"   ✅ Models to execute: {models_to_execute}")
    _printers.cprint(f"   ⏭️  Models to skip: {models_to_skip}")

    if verbose and models_to_execute > 0:
        _printers.cprint("\n📋 Models requiring execution:", color="yellow")
        for analysis in analyses:
            if analysis.needs_execution:
                _printers.cprint(
                    f"  • {analysis.model.name} ({analysis.reason_description})",
                    color="bright_black",
                )
