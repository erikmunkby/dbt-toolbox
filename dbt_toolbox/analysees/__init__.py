"""Unified analysis module for dbt-toolbox.

This module provides a single entry point for all analysis types:
- Model execution analysis
- Column lineage validation
- Docs macros analysis
- Project-level analysis
"""

from dbt_toolbox.dbt_parser import dbtParser

from .columns import analyze_column_references
from .data_models import (
    AnalysisResult,
    AnalysisResults,
    ColumnAnalysis,
    ColumnIssue,
    CTEIssue,
    DocsAnalysis,
    DuplicateDocsIssue,
    ExecutionReason,
    ModelAnalysisResult,
)
from .docs_macros import analyze_docs_macros
from .models import analyze_model_statuses
from .print_analysis import print_analysis_results


def analyze(
    target: str | None = None, model: str | None = None, dbt_parser: dbtParser | None = None
) -> AnalysisResults:
    """Unified analysis function that performs all analysis types.

    Args:
        target: dbt target environment (e.g., 'dev', 'prod')
        model: dbt model selection (--select/--model syntax)
        dbt_parser: Optional dbtParser instance to reuse (avoids re-parsing)

    Returns:
        AnalysisResults containing all analysis results

    """
    # Reuse dbt_parser if provided, otherwise create new one
    if dbt_parser is None:
        dbt_parser = dbtParser(target=target)

    # Parse model selection once at the top level
    if model:
        # Parse selection and get selected models dictionary
        selection_result = dbt_parser.parse_selection_query(model)
        selected_models = selection_result.models_dict
        # Convert to list for column analysis
        target_models = list(selected_models.values())
    else:
        # All models
        selected_models = dbt_parser.models
        target_models = None

    # Perform model execution analysis with selected models
    model_analysis = analyze_model_statuses(dbt_parser=dbt_parser, selected_models=selected_models)

    # Perform column lineage analysis with target models
    column_analysis = analyze_column_references(dbt_parser=dbt_parser, target_models=target_models)

    # Perform docs macros analysis (no model selection needed)
    docs_analysis = analyze_docs_macros(dbt_parser=dbt_parser)

    return AnalysisResults(
        model_analysis=model_analysis,
        column_analysis=column_analysis,
        docs_analysis=docs_analysis,
    )


# Public interface - only expose top-level functions and data models
__all__ = [
    # Data models
    "AnalysisResult",
    "AnalysisResults",
    "CTEIssue",
    "ColumnAnalysis",
    "ColumnIssue",
    "DocsAnalysis",
    "DuplicateDocsIssue",
    "ExecutionReason",
    "ModelAnalysisResult",
    # Top-level functions
    "analyze",
    "print_analysis_results",
]
