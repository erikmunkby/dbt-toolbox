"""Analyze command for comprehensive cache analysis without manipulation."""

from datetime import timedelta

import typer
from rich.console import Console
from rich.table import Table

from dbt_toolbox.actions.analyze_columns_references import analyze_column_references
from dbt_toolbox.actions.analyze_models import (
    AnalysisResult,
    ExecutionReason,
    analyze_model_statuses,
)
from dbt_toolbox.cli._common_options import Target
from dbt_toolbox.constants import EXECUTION_TIMESTAMP
from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import _printers


def _format_time_delta(delta: timedelta) -> str:
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


def _get_timestamp_info(analysis_result: AnalysisResult) -> str:
    """Get timestamp info for display purposes.

    Args:
        analysis_result: The AnalysisResult from analyze_model_statuses

    Returns:
        Human-readable timestamp information

    """
    model = analysis_result.model

    if not model.last_built:
        return "Never built"

    age_delta = EXECUTION_TIMESTAMP - model.last_built
    age_description = _format_time_delta(age_delta)
    return f"Last updated: {age_description} ago"


def print_column_analysis_results(dbt_parser: dbtParser, target_models: list[Model]) -> None:
    """Print column reference analysis results.

    Args:
        dbt_parser: The dbt parser.
        target_models: A list of targeted models.

    """
    console = Console()
    analysis = analyze_column_references(dbt_parser=dbt_parser, target_models=target_models)

    # Check if there are any issues to report
    if (
        not analysis.non_existent_columns
        and not analysis.referenced_non_existent_models
        and not analysis.cte_column_issues
    ):
        _printers.cprint("✅ All column references are valid!", color="green")
        return

    _printers.cprint("📊 Column Reference Analysis", color="cyan")
    print()  # noqa: T201 blankline

    # Non-existent columns section
    if analysis.non_existent_columns:
        total_missing_cols = sum(len(cols) for cols in analysis.non_existent_columns.values())
        _printers.cprint(
            f"❌ Non-existent Columns ({total_missing_cols}):",
            color="red",
        )
        table = Table(show_header=True, header_style="bold red")
        table.add_column("Model", style="red")
        table.add_column("Referenced Model", style="yellow")
        table.add_column("Missing Columns", style="white")

        for model_name, referenced_models in analysis.non_existent_columns.items():
            for referenced_model, missing_columns in referenced_models.items():
                table.add_row(
                    model_name,
                    referenced_model,
                    ", ".join(missing_columns),
                )

        console.print(table)
        print()  # noqa: T201 blankline

    # CTE column issues section
    if analysis.cte_column_issues:
        total_cte_issues = sum(
            len(cols)
            for cte_dict in analysis.cte_column_issues.values()
            for cols in cte_dict.values()
        )
        _printers.cprint(
            f"🔶 CTE Column Issues ({total_cte_issues}):",
            color="yellow",
        )
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Model", style="yellow")
        table.add_column("CTE Name", style="blue")
        table.add_column("Missing Columns", style="white")

        for model_name, cte_issues in analysis.cte_column_issues.items():
            for cte_name, missing_columns in cte_issues.items():
                table.add_row(
                    model_name,
                    cte_name,
                    ", ".join(missing_columns),
                )

        console.print(table)
        print()  # noqa: T201 blankline

    # Referenced non-existent models section
    if analysis.referenced_non_existent_models:
        total_missing_models = sum(
            len(models) for models in analysis.referenced_non_existent_models.values()
        )
        _printers.cprint(
            f"❌ Referenced Non-existent Models ({total_missing_models}):",
            color="red",
        )
        table = Table(show_header=True, header_style="bold red")
        table.add_column("Model", style="red")
        table.add_column("Non-existent Referenced Models", style="white")

        for model_name, non_existent_models in analysis.referenced_non_existent_models.items():
            table.add_row(
                model_name,
                ", ".join(set(non_existent_models)),
            )

        console.print(table)
        print()  # noqa: T201 blankline


def print_analysis_results(analysis_results: list[AnalysisResult]) -> None:
    """Print cache analysis results in a formatted way.

    Args:
        analysis_results: Dictionary of model analysis results from analyze_model_statuses

    """
    console = Console()

    # Separate into categories
    models_needing_execution = [result for result in analysis_results if result.needs_execution]
    valid_models = [result for result in analysis_results if not result.needs_execution]

    # Header
    _printers.cprint("🔍 Cache Analysis Results", color="cyan")
    _printers.cprint(f"Total models analyzed: {len(analysis_results)}", color="nocolor")

    if models_needing_execution:
        _printers.cprint(
            f"Models needing execution: {len(models_needing_execution)}",
            color="yellow",
        )
    else:
        _printers.cprint("✅ All models have valid cache!", color="green")

    print()  # noqa: T201 blankline

    # Models needing execution section (combined table)
    if models_needing_execution:
        _printers.cprint(
            f"🔧 Models Needing Execution ({len(models_needing_execution)}):",
            color="yellow",
        )
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Model", style="white")
        table.add_column("Issue", style="white")
        table.add_column("Info", style="cyan")

        for result in models_needing_execution:
            # Color code the model name based on issue type
            if result.reason == ExecutionReason.LAST_EXECUTION_FAILED:
                model_style = "red"
            elif result.reason == ExecutionReason.CODE_CHANGED:
                model_style = "yellow"
            elif result.reason in [
                ExecutionReason.UPSTREAM_MODEL_CHANGED,
                ExecutionReason.UPSTREAM_MACRO_CHANGED,
            ]:
                model_style = "magenta"
            else:  # outdated
                model_style = "blue"

            # Get issue description with special handling for outdated models
            if result.reason == ExecutionReason.OUTDATED_MODEL:
                issue_description = (
                    f"Cache is older than {settings.cache_validity_minutes} minutes"
                )
            else:
                issue_description = result.reason_description

            table.add_row(
                f"[{model_style}]{result.model.name}[/{model_style}]",
                issue_description,
                _get_timestamp_info(result),
            )

        console.print(table)
        print()  # noqa: T201 blankline

    # Valid models section (only show count unless verbose)
    if valid_models:
        _printers.cprint(f"✅ Valid Models ({len(valid_models)}):", color="green")
        # Just show a summary for valid models to keep output clean
        for result in valid_models:
            _printers.cprint(
                f"   • {result.model.name} - {_get_timestamp_info(result)}",
                color="bright_black",
            )


def analyze_command(
    target: str | None = Target,
    model: str | None = typer.Option(
        None,
        "--model",
        "-m",
        "--select",
        "-s",
        help="Analyze specific models (dbt selection syntax)",
    ),
) -> None:
    """Analyze cache state and column references without manipulating them.

    Shows outdated models, ID mismatches, failed models that need re-execution,
    and column reference issues.
    """
    _printers.cprint("🔍 Analyzing model cache state and column references...", color="cyan")
    dbt_parser = dbtParser(target=target)

    # Perform cache analysis using the new analyze_model_statuses function
    analysis_results = analyze_model_statuses(dbt_parser=dbt_parser, dbt_selection=model)

    # Print cache analysis results
    print_analysis_results(analysis_results)

    # Perform column analysis on available models, sources, and seeds

    # Filter models if selection is provided
    if model:
        target_models = dbt_parser.parse_dbt_selection(model)
        models = [m for m in dbt_parser.models.values() if m.name in target_models]
    else:
        models = list(dbt_parser.models.values())

    # Print column analysis results
    print_column_analysis_results(dbt_parser=dbt_parser, target_models=models)

    # Summary
    models_needing_execution = sum(1 for result in analysis_results if result.needs_execution)
    if models_needing_execution:
        _printers.cprint(
            f"\n💡 Tip: Run 'dt build' to execute the {models_needing_execution} "
            "models that need updates.",
            color="cyan",
        )
