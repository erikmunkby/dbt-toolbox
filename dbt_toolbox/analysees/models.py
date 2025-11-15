"""Module for model execution analysis."""

import copy

from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser import dbtParser

from .data_models import AnalysisResult, ExecutionReason


def _analyze_model(model: Model) -> AnalysisResult:
    """Will analyze the model to see if it needs updating.

    Prio order:
    1. Last build failed
    2. Never built
    3. Code changed
    4. Upstream macros changed
    5. Cache outdated
    """
    # Check if the model needs execution
    if model.last_build_failed:
        return AnalysisResult(model=model, reason=ExecutionReason.LAST_EXECUTION_FAILED)
    if model.last_built is None:
        return AnalysisResult(model=model, reason=ExecutionReason.NEVER_BUILT)
    if model.code_changed:
        return AnalysisResult(model=model, reason=ExecutionReason.CODE_CHANGED)
    if model.upstream_macros_changed:
        return AnalysisResult(model=model, reason=ExecutionReason.UPSTREAM_MACRO_CHANGED)
    if model.cache_outdated:
        return AnalysisResult(model=model, reason=ExecutionReason.OUTDATED_MODEL)
    return AnalysisResult(model=model, needs_execution=False)


def analyze_model_statuses(
    dbt_parser: dbtParser, selected_models: dict[str, Model]
) -> list[AnalysisResult]:
    """Analyze the execution status of models based on their dependencies and cache.

    Args:
        dbt_parser: The dbt parser object.
        selected_models: Dictionary of selected models to analyze

    Returns:
        A list of AnalysisResult objects representing the analysis of each model's status.

    """
    # First do a simple analysis of models, freshness and last execution status
    analysees: dict[str, AnalysisResult] = {
        name: _analyze_model(model) for name, model in selected_models.items()
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
    return [result for model_name, result in analysees.items() if model_name in selected_models]
