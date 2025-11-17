"""Factory for build and run command creation with smart execution."""

from collections.abc import Callable
from typing import Annotated

import typer

from dbt_toolbox.actions.dbt_executor import create_execution_plan
from dbt_toolbox.analysees.print_analysis import print_execution_analysis
from dbt_toolbox.cli._common_options import (
    ArgumentModelSelection,
    OptionModelSelection,
    OptionTarget,
)
from dbt_toolbox.cli._exit_handler import exit_run
from dbt_toolbox.data_models import DbtExecutionParams, Model
from dbt_toolbox.utils import _printers


def execute_dbt_with_smart_selection(params: DbtExecutionParams) -> None:
    """Execute a dbt command with validation and intelligent model selection.

    Args:
        params: DbtExecutionParams object containing all execution parameters

    """
    # Create execution plan (always validates unless force=True)
    plan = create_execution_plan(params)

    # If force mode, run all models without analysis
    if params.force:
        execution_results = plan.run()
        if execution_results.return_code == 0:
            exit_run(0, f"{params.command_name.title()} completed successfully")
        else:
            exit_run(1, f"{params.command_name.title()} failed")

    # Check if validation failed
    if not plan.lineage_valid:
        exit_run(1, "Validation failed - use --force to skip validation")

    # Check if selection matched no models
    if not plan.analyses:
        _printers.cprint(
            "⚠️  Selection matched no models",
            color="yellow",
        )
        exit_run(0)

    # Print analysis results
    print_execution_analysis(plan.analyses, mode="validation")

    # Check if all models are cached
    if not plan.models_to_execute:
        _printers.cprint(
            "✅ All models have valid cache - nothing to execute!",
            color="green",
        )
        _print_compute_time(skipped_models=plan.models_to_skip)
        exit_run(0)

    # Print optimized selection if different from original
    if len(plan.models_to_execute) != len(plan.analyses):
        new_selection = " ".join(plan.models_to_execute)
        _printers.cprint(f"🎯 Optimized selection: {new_selection}", color="cyan")

    # Execute the plan
    execution_results = plan.run()

    # Print compute time saved if execution was successful
    if plan.analyses and not execution_results.parsed_logs.failed_models:
        _print_compute_time(skipped_models=plan.models_to_skip)

    # Exit with appropriate message based on results
    if execution_results.return_code == 0:
        exit_run(0, f"{params.command_name.title()} completed successfully")
    else:
        exit_run(1, f"{params.command_name.title()} failed")


def _format_time(time_seconds: float) -> str:
    """Format compute time in seconds to human-readable format.

    Args:
        time_seconds: Time in seconds

    Returns:
        Human-readable time string

    """
    if time_seconds < 60:  # noqa: PLR2004
        return f"{time_seconds:.1f}s"
    if time_seconds < 3600:  # noqa: PLR2004
        minutes = int(time_seconds // 60)
        seconds = time_seconds % 60
        return f"{minutes}m {seconds:.1f}s"
    hours = int(time_seconds // 3600)
    remaining_seconds = time_seconds % 3600
    minutes = int(remaining_seconds // 60)
    return f"{hours}h {minutes}m"


def _print_compute_time(skipped_models: list[Model]) -> None:
    """Print the compute time saved in console."""
    time_seconds = sum(
        [m.compute_time_seconds if m.compute_time_seconds else 0 for m in skipped_models]
    )

    if skipped_models:
        time_display = _format_time(time_seconds)
        _printers.cprint(
            f"⚡ Skipped {len(skipped_models)} "
            f"model{'s' if len(skipped_models) != 1 else ''}, "
            f"saved ~{time_display} of compute time",
            color="green",
        )


def create_dbt_command_function(command_name: str, help_text: str) -> Callable:
    """Create a dbt command function with standardized options.

    Args:
        dbt_parser: The dbt parser object.
        command_name: The dbt command name (e.g., 'build', 'run')
        help_text: Help text for the command

    Returns:
        A function that can be used as a typer command.

    """

    def dbt_command(  # noqa: PLR0913
        models: ArgumentModelSelection = None,
        target: OptionTarget = None,
        model: OptionModelSelection = None,
        full_refresh: Annotated[
            bool,
            typer.Option("--full-refresh", help="Drop incremental models and rebuild"),
        ] = False,
        threads: Annotated[
            int | None,
            typer.Option("--threads", help="Number of threads to use"),
        ] = None,
        vars: Annotated[  # noqa: A002
            str | None,
            typer.Option("--vars", help="Supply variables to the project (YAML string)"),
        ] = None,
        force: Annotated[
            bool,
            typer.Option(
                "--force",
                help="Skip validation and cache analysis, run all selected models",
            ),
        ] = False,
    ) -> None:
        """Dynamically created dbt command with intelligent execution."""
        # Merge positional models argument with --model option
        # Positional argument takes precedence if both are provided
        final_model_selection = models or model

        params = DbtExecutionParams(
            command_name=command_name,
            model_selection=final_model_selection,
            full_refresh=full_refresh,
            threads=threads,
            vars=vars,
            target=target,
            force=force,
        )
        execute_dbt_with_smart_selection(params)

    # Set the docstring and name dynamically
    dbt_command.__doc__ = help_text
    dbt_command.__name__ = command_name
    return dbt_command
