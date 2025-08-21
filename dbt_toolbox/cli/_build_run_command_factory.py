"""Factory for build and run command creation with smart execution."""

import sys
from collections.abc import Callable
from typing import Annotated

import typer

from dbt_toolbox.actions.analyze_models import print_execution_analysis
from dbt_toolbox.actions.dbt_executor import create_execution_plan
from dbt_toolbox.cli._common_options import Target
from dbt_toolbox.data_models import DbtExecutionParams, Model
from dbt_toolbox.utils import _printers


def execute_dbt_with_smart_selection(params: DbtExecutionParams) -> None:
    """Execute a dbt command with intelligent model selection using plan->run->status flow.

    Args:
        params: DbtExecutionParams object containing all execution parameters

    """
    # Handle printing based on what happened
    action = "Building" if params.command_name == "build" else "Running"
    if params.model:
        _printers.cprint(
            f"🔨 {action} models:",
            params.model,
            highlight_idx=1,
            color="cyan",
        )
    else:
        _printers.cprint(f"🔨 {action} all models", color="cyan")

    # Create execution plan
    plan = create_execution_plan(params)

    # Handle analyze-only mode printing
    if params.analyze_only and plan.analyses:
        print_execution_analysis(plan.analyses, verbose=True)
        sys.exit(0)

    if params.disable_smart:
        execution_results = plan.run()
        sys.exit(execution_results.return_code)

    # Handle regular execution with analysis

    print_execution_analysis(plan.analyses)
    if not plan.models_to_execute:
        _printers.cprint(
            "✅ All models have valid cache - nothing to execute!",
            color="green",
        )
        _print_compute_time(skipped_models=plan.models_to_skip)
        sys.exit(0)

    # Print execution status
    if len(plan.models_to_execute) == len(plan.analyses):
        _printers.cprint("🔥 All selected models need execution", color="yellow")
    else:
        new_selection = " ".join(plan.models_to_execute)
        _printers.cprint(f"🎯 Optimized selection: {new_selection}", color="cyan")

    # Execute the plan
    execution_results = plan.run()

    # Print compute time saved if execution was successful
    if plan.analyses and not params.disable_smart and not execution_results.logs.failed_models:
        _print_compute_time(skipped_models=plan.models_to_skip)

    sys.exit(execution_results.return_code)


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
        target: str | None = Target,
        model: Annotated[
            str | None,
            typer.Option(
                "--model",
                "-m",
                "--select",
                "-s",
                "--models",
                help=f"Select models to {command_name} (same as dbt --select/--model)",
            ),
        ] = None,
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
        analyze_only: Annotated[
            bool,
            typer.Option(
                "--analyze",
                help="Only analyze which models need execution, don't run dbt",
            ),
        ] = False,
        disable_smart: Annotated[
            bool,
            typer.Option(
                "--disable-smart",
                help="Disable intelligent execution and run all selected models",
            ),
        ] = False,
    ) -> None:
        """Dynamically created dbt command with intelligent execution."""
        params = DbtExecutionParams(
            command_name=command_name,
            model=model,
            full_refresh=full_refresh,
            threads=threads,
            vars=vars,
            target=target,
            analyze_only=analyze_only,
            disable_smart=disable_smart,
        )
        execute_dbt_with_smart_selection(params)

    # Set the docstring and name dynamically
    dbt_command.__doc__ = help_text
    dbt_command.__name__ = command_name
    return dbt_command
