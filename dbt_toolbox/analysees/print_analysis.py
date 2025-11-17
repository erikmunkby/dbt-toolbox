"""Printing functions for dbt-toolbox analysis results.

This module contains all functions for displaying analysis results in a formatted way.
"""

from collections import defaultdict
from typing import Literal

from rich.console import Console
from rich.table import Table

from dbt_toolbox.utils import _printers

from .data_models import (
    AnalysisResult,
    AnalysisResults,
    ColumnAnalysis,
    DocsAnalysis,
    ExecutionReason,
)

PrintModes = Literal["analysis", "validation"]


def _print_section_header(title: str, status: str) -> None:
    """Print a standardized section header with title and status.

    Args:
        title: Section title
        status: Status text (OK, ISSUES_FOUND, etc.)

    """
    console = Console()

    if status == "OK":
        status_icon = "✅"
        status_color = "green"
    elif status == "ISSUES_FOUND":
        status_icon = "❌"
        status_color = "red"
    else:
        status_icon = "🔶"
        status_color = "yellow"

    console.print(f"{title} ", style="cyan", end="")
    console.print(f"({status_icon} {status})", style=status_color)


def _print_metadata(items: list[tuple[str, str, str]]) -> None:
    """Print metadata items in a consistent format.

    Args:
        items: List of (icon, label, value) tuples

    """
    for icon, label, value in items:
        _printers.cprint(f"   {icon} {label}: {value}")


def _print_table_section(title: str, table: Table, console: Console) -> None:
    """Print a table section with consistent formatting.

    Args:
        title: Section title
        table: Rich table to display
        console: Rich console instance

    """
    print()  # noqa: T201
    _printers.cprint(title, color="red")
    console.print(table)


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
    reasons_to_group = {ExecutionReason.NEVER_BUILT, ExecutionReason.OUTDATED_MODEL}
    grouped_reasons = {
        reason: models
        for reason, models in grouped_by_reason.items()
        if reason in reasons_to_group and len(models) > group_threshold
    }

    # Build table with individual models (not grouped)
    table = Table(show_header=True, header_style="bold yellow")
    table.add_column("Model", style="yellow")
    table.add_column("Execution Reason", style="white")

    # Add individual models (excluding those that will be grouped)
    for reason, models in grouped_by_reason.items():
        if reason in grouped_reasons:
            # Skip - will be shown as grouped summary
            continue
        for analysis in models:
            table.add_row(analysis.model.name, analysis.reason_description)

    # Add grouped summaries at the end
    for reason in [ExecutionReason.NEVER_BUILT, ExecutionReason.OUTDATED_MODEL]:
        if reason in grouped_reasons:
            models = grouped_reasons[reason]
            # Use the first model's reason_description for consistency
            reason_desc = models[0].reason_description
            table.add_row(
                f"({len(models)} models)",
                f"{reason_desc}",
            )

    if table.row_count > 0:
        _print_table_section("Models Requiring Execution:", table, console)


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

    # Header
    _print_section_header("Build Execution Analysis", status)

    # Main focus: Models to execute
    console.print(f"   ✅ Models to execute: {models_to_execute} (of {total_models} total)")
    if models_to_skip > 0:
        console.print(f"   ⏭️  Models to skip: {models_to_skip}")

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

    # Header
    title = "Lineage Validation" if mode == "validation" else "Column Reference Analysis"

    _print_section_header(title, status)

    # Metadata
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

    if not has_issues:
        return

    metadata = [("🔍", "Total issues found", str(total_issues))]
    _print_metadata(metadata)
    # Non-existent columns table
    if analysis.non_existent_columns:
        table = Table(show_header=True, header_style="bold red")
        table.add_column("Model", style="red")
        table.add_column("Referenced Model", style="yellow")
        table.add_column("Missing Columns", style="white")

        for model_name, referenced_models in analysis.non_existent_columns.items():
            for referenced_model, missing_columns in referenced_models.items():
                table.add_row(model_name, referenced_model, ", ".join(missing_columns))

        missing_count = sum(len(cols) for cols in analysis.non_existent_columns.values())
        _print_table_section(f"Non-existent Columns ({missing_count}):", table, console)

    # CTE column issues table
    if analysis.cte_column_issues:
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Model", style="yellow")
        table.add_column("CTE Name", style="blue")
        table.add_column("Missing Columns", style="white")

        for model_name, cte_issues in analysis.cte_column_issues.items():
            for cte_name, missing_columns in cte_issues.items():
                table.add_row(model_name, cte_name, ", ".join(missing_columns))

        cte_count = sum(
            len(cols)
            for cte_dict in analysis.cte_column_issues.values()
            for cols in cte_dict.values()
        )
        _print_table_section(f"CTE Column Issues ({cte_count}):", table, console)

    # Referenced non-existent models table
    if analysis.referenced_non_existent_models:
        table = Table(show_header=True, header_style="bold red")
        table.add_column("Model", style="red")
        table.add_column("Non-existent Referenced Models", style="white")

        for model_name, non_existent_models in analysis.referenced_non_existent_models.items():
            table.add_row(model_name, ", ".join(set(non_existent_models)))

        model_count = sum(
            len(models) for models in analysis.referenced_non_existent_models.values()
        )
        _print_table_section(f"Referenced Non-existent Models ({model_count}):", table, console)


def print_docs_analysis_results(analysis: DocsAnalysis, mode: PrintModes = "analysis") -> None:
    """Print docs macro analysis in standardized format.

    Args:
        analysis: Docs analysis results
        mode: Print mode - "analysis" for analyze command, "validation" for build command

    """
    console = Console()

    # Header
    title = "Docs Macro Analysis"
    if mode == "validation":
        title = "Docs Macro Validation"

    _print_section_header(title, analysis.overall_status)

    # Metadata
    total_duplicates = (
        sum(issue.occurrences - 1 for issue in analysis.duplicate_issues)
        if analysis.duplicate_issues
        else 0
    )
    metadata = [
        ("📊", "Total docs macros", str(analysis.total_docs_macros)),
    ]
    if total_duplicates:
        metadata += [("❌", "Duplicate issues", str(total_duplicates))]
    _print_metadata(metadata)

    # Duplicate docs macros table
    if analysis.duplicate_issues:
        table = Table(show_header=True, header_style="bold red")
        table.add_column("Macro Name", style="red")
        table.add_column("Occurrences", style="yellow")
        table.add_column("File Paths", style="white")

        for issue in analysis.duplicate_issues:
            table.add_row(
                issue.macro_name,
                str(issue.occurrences),
                "\n".join(issue.file_paths),
            )

        _print_table_section(
            f"Duplicate Docs Macros ({total_duplicates} duplicates):", table, console
        )


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
