"""Printing functions for dbt-toolbox analysis results.

This module contains all functions for displaying analysis results in a formatted way.
"""

from collections import defaultdict
from typing import Literal

from rich import box
from rich.console import Console
from rich.table import Table

from .data_models import (
    AnalysisResult,
    AnalysisResults,
    ColumnAnalysis,
    DocsAnalysis,
    ExecutionReason,
)

PrintModes = Literal["analysis", "validation"]

NAME_STYLE = "cyan"
DETAIL_STYLE = "dim"
MUTED_STYLE = "dim"

TRUNCATE_THRESHOLD = 5


def _print_summary_header(
    title: str, status: str, summary_items: list[tuple[str, str | int]] | None = None
) -> None:
    """Print a summary header with indicator and optional inline stats.

    Args:
        title: Section title
        status: Status text (OK, ISSUES_FOUND, UPDATES_NEEDED)
        summary_items: Optional list of (label, value) tuples for inline stats

    """
    console = Console()
    indicators = {
        "OK": ("green", "[ok]"),
        "ISSUES_FOUND": ("red", "[!]"),
        "UPDATES_NEEDED": ("yellow", "[*]"),
    }
    color, indicator = indicators.get(status, ("white", ""))
    console.print(f"\n{indicator} {title}", style=f"bold {color}")
    if summary_items:
        parts = [f"{label}: {value}" for label, value in summary_items]
        console.print("   " + " | ".join(parts), style=MUTED_STYLE)


def _create_table(columns: list[tuple[str, str]]) -> Table:
    """Create a consistently styled table.

    Args:
        columns: List of (column_name, style) tuples

    """
    table = Table(box=box.SIMPLE_HEAD, show_edge=False, pad_edge=False, header_style="bold")
    for col_name, col_style in columns:
        table.add_column(col_name, style=col_style, no_wrap=False)
    return table


def _truncate_items(items: list[str], threshold: int = TRUNCATE_THRESHOLD) -> str:
    """Join items with truncation when the list is long."""
    if len(items) <= threshold:
        return ", ".join(items)
    shown = ", ".join(items[:threshold])
    return f"{shown} [dim](+{len(items) - threshold} more)[/dim]"


def _print_execution_details(analyses: list[AnalysisResult], console: Console) -> None:
    """Print detailed execution reasons for models that need execution.

    Groups NEVER_BUILT and OUTDATED_MODEL reasons when there are more than 3 models
    with the same reason.

    Args:
        analyses: List of model execution analyses
        console: Rich console instance

    """
    # Group models with same reason if more than this threshold
    group_threshold = 3

    # Filter to models that need execution
    models_to_execute = [a for a in analyses if a.needs_execution]

    if not models_to_execute:
        return

    # Group models by execution reason (skip models without a reason)
    grouped_by_reason: dict[ExecutionReason, list[AnalysisResult]] = defaultdict(list)
    for analysis in models_to_execute:
        if analysis.reason is None:
            continue
        grouped_by_reason[analysis.reason].append(analysis)

    # Determine which reasons to group (more than group_threshold models)
    reasons_to_group = {
        ExecutionReason.NEVER_BUILT,
        ExecutionReason.OUTDATED_MODEL,
        ExecutionReason.TESTS_CHANGED,
    }
    grouped_reasons = {
        reason: models
        for reason, models in grouped_by_reason.items()
        if reason in reasons_to_group and len(models) > group_threshold
    }

    # Build table
    table = _create_table([("Model", NAME_STYLE), ("Reason", "")])

    # Add individual models (excluding those that will be grouped)
    for reason, models in grouped_by_reason.items():
        if reason in grouped_reasons:
            continue
        for analysis in models:
            table.add_row(analysis.model.name, analysis.reason_description)

    # Add grouped summaries at the end
    for reason in [
        ExecutionReason.NEVER_BUILT,
        ExecutionReason.OUTDATED_MODEL,
        ExecutionReason.TESTS_CHANGED,
    ]:
        if reason in grouped_reasons:
            models = grouped_reasons[reason]
            reason_desc = models[0].reason_description
            table.add_row(
                f"[{len(models)} models]",
                reason_desc,
            )

    if table.row_count > 0:
        console.print()
        console.print(table)


def print_execution_analysis(
    analyses: list[AnalysisResult], mode: PrintModes = "analysis"
) -> None:
    """Print model execution analysis in standardized format.

    Args:
        analyses: List of model execution analyses
        mode: Print mode - "analysis" for analyze command, "validation" for build command

    """
    console = Console()
    total_models = len(analyses)
    models_to_execute = sum(1 for a in analyses if a.needs_execution)
    models_to_skip = total_models - models_to_execute

    # Determine status
    status = "OK" if models_to_execute == 0 else "UPDATES_NEEDED"

    # Header with inline summary
    _print_summary_header(
        "Build Execution Analysis",
        status,
        summary_items=[
            ("Execute", models_to_execute),
            ("Skip", models_to_skip),
            ("Total", total_models),
        ],
    )

    if mode == "analysis":
        _print_execution_details(analyses=analyses, console=console)


def print_column_analysis_results(
    analysis: ColumnAnalysis,
    mode: PrintModes = "analysis",
) -> None:
    """Print column reference analysis in standardized format.

    Args:
        analysis: Column analysis results to print
        mode: Print mode - "analysis" for analyze command, "validation" for build command

    """
    console = Console()

    # Check for issues
    has_issues = bool(
        analysis.non_existent_columns
        or analysis.referenced_non_existent_models
        or analysis.cte_column_issues
    )

    # Determine status
    status = "ISSUES_FOUND" if has_issues else "OK"

    # Count issues
    total_issues = 0
    if analysis.non_existent_columns:
        total_issues += sum(len(cols) for cols in analysis.non_existent_columns.values())
    if analysis.cte_column_issues:
        total_issues += sum(
            len(cols)
            for cte_dict in analysis.cte_column_issues.values()
            for cols in cte_dict.values()
        )
    if analysis.referenced_non_existent_models:
        total_issues += sum(
            len(models) for models in analysis.referenced_non_existent_models.values()
        )

    # Header
    title = "Lineage Validation" if mode == "validation" else "Column Reference Analysis"
    summary = [("Issues", total_issues)] if has_issues else None
    _print_summary_header(title, status, summary_items=summary)

    if not has_issues:
        return

    # Non-existent columns table
    if analysis.non_existent_columns:
        table = _create_table(
            [("Model", NAME_STYLE), ("Referenced Model", DETAIL_STYLE), ("Missing Columns", "")]
        )

        for model_name, referenced_models in analysis.non_existent_columns.items():
            for referenced_model, missing_columns in referenced_models.items():
                table.add_row(model_name, referenced_model, _truncate_items(missing_columns))

        console.print()
        console.print(table)

    # CTE column issues table
    if analysis.cte_column_issues:
        table = _create_table(
            [("Model", NAME_STYLE), ("CTE Name", DETAIL_STYLE), ("Missing Columns", "")]
        )

        for model_name, cte_issues in analysis.cte_column_issues.items():
            for cte_name, missing_columns in cte_issues.items():
                table.add_row(model_name, cte_name, _truncate_items(missing_columns))

        console.print()
        console.print(table)

    # Referenced non-existent models table
    if analysis.referenced_non_existent_models:
        table = _create_table([("Model", NAME_STYLE), ("Non-existent Referenced Models", "")])

        for model_name, non_existent_models in analysis.referenced_non_existent_models.items():
            table.add_row(model_name, _truncate_items(sorted(set(non_existent_models))))

        console.print()
        console.print(table)


def print_docs_analysis_results(analysis: DocsAnalysis, mode: PrintModes = "analysis") -> None:
    """Print docs macro analysis in standardized format.

    Args:
        analysis: Docs analysis results
        mode: Print mode - "analysis" for analyze command, "validation" for build command

    """
    console = Console()

    # Header
    title = "Docs Macro Validation" if mode == "validation" else "Docs Macro Analysis"

    total_duplicates = (
        sum(issue.occurrences - 1 for issue in analysis.duplicate_issues)
        if analysis.duplicate_issues
        else 0
    )

    summary_items: list[tuple[str, str | int]] = [("Macros", analysis.total_docs_macros)]
    if total_duplicates:
        summary_items.append(("Duplicates", total_duplicates))

    _print_summary_header(title, analysis.overall_status, summary_items=summary_items)

    # Duplicate docs macros table
    if analysis.duplicate_issues:
        table = _create_table(
            [("Macro Name", NAME_STYLE), ("Occurrences", DETAIL_STYLE), ("File Paths", "")]
        )

        for issue in analysis.duplicate_issues:
            table.add_row(
                issue.macro_name,
                str(issue.occurrences),
                "\n".join(issue.file_paths),
            )

        console.print()
        console.print(table)


def print_analysis_results(
    results: AnalysisResults,
    mode: PrintModes = "analysis",
) -> None:
    """Print all analysis results in a structured format.

    Args:
        results: Analysis results to print
        verbose: Whether to show verbose output
        mode: Print mode - "analysis" for analyze command, "validation" for build command

    """
    # Print each analysis section
    print_execution_analysis(results.model_analysis, mode=mode)
    print()  # noqa: T201
    print_column_analysis_results(results.column_analysis, mode=mode)
    print()  # noqa: T201
    print_docs_analysis_results(results.docs_analysis, mode=mode)
    print()  # noqa: T201
