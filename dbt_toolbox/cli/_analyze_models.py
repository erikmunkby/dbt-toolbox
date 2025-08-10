from dataclasses import dataclass
from enum import Enum

from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.utils import _printers


class ExecutionReason(Enum):
    UPSTREAM_MODEL_CHANGED = "upstream_model_changed"
    UPSTREAM_MACRO_CHANGED = "upstream_macro_changed"
    OUTDATED_MODEL = "outdated_model"
    LAST_EXECUTION_FAILED = "last_execution_failed"
    CODE_CHANGED = "code_changed"


@dataclass
class AnalysisResult:
    """Results of the analysis."""

    model: Model
    reason: ExecutionReason | None = None

    @property
    def needs_execution(self) -> bool:
        return self.reason is not None

    @property
    def reason_description(self) -> str:
        return {
            ExecutionReason.CODE_CHANGED: "Model code changed.",
            ExecutionReason.UPSTREAM_MACRO_CHANGED: "Upstream macro changed.",
            ExecutionReason.UPSTREAM_MODEL_CHANGED: "Upstream model changed.",
            ExecutionReason.OUTDATED_MODEL: "Model build is outdated.",
            ExecutionReason.LAST_EXECUTION_FAILED: "Last model execution failed.",
            None: "",
        }[self.reason]


def _analyze_model(model: Model) -> AnalysisResult | None:
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
    return None


def analyze_model_statuses(
    dbt_parser: dbtParser, dbt_selection: str | None = None
) -> dict[str, AnalysisResult]:
    """Analyze the execution status of models based on their dependencies and cache.

    Args:
        dbt_parser: The dbt parser object.
        dbt_selection: Optional dbt selection string to filter models

    Returns:
        A list of AnalysisResult objects representing the analysis of each model's status.

    """
    # Placeholder implementation
    models_selected = dbt_parser.parse_dbt_selection(selection=dbt_selection)
    results = {}

    # Get all changed macros

    # First do a simple analysis of models, freshness and last execution status
    for model_name in models_selected:
        analysis = _analyze_model(dbt_parser.models[model_name])
        if analysis:
            results[model_name] = analysis

    # Then flag all downstream models, if they're not already part of list.
    for model_name in list(results.keys()):
        for downstream_model in dbt_parser.get_downstream_models(model_name):
            if downstream_model.name not in results:
                results[downstream_model.name] = AnalysisResult(
                    model=downstream_model, reason=ExecutionReason.UPSTREAM_MODEL_CHANGED
                )

    # Finally prune any not in selection
    # Also add any that do not need execution
    return {
        name: results.get(name, AnalysisResult(model=dbt_parser.models[name]))
        for name in models_selected
    }


def print_execution_analysis(
    analyses: dict[str, AnalysisResult],
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

    _printers.cprint("🔍 Build Execution Analysis", color="cyan")
    _printers.cprint(f"   📊 Total models in selection: {total_models}")
    _printers.cprint(f"   ✅ Models to execute: {models_to_execute}")
    _printers.cprint(f"   ⏭️  Models to skip: {models_to_skip}")

    if verbose and models_to_execute > 0:
        _printers.cprint("\n📋 Models requiring execution:", color="yellow")
        for model_name, analysis in analyses.items():
            if analysis.needs_execution:
                _printers.cprint(
                    f"  • {model_name} ({analysis.reason_description})", color="bright_black"
                )
