"""Shared dbt execution engine for build and run commands."""

import subprocess
import sys
from collections.abc import Callable
from typing import Annotated

import typer

from dbt_toolbox.cli._analyze_columns import analyze_column_references
from dbt_toolbox.cli._build_analysis import build_analyzer
from dbt_toolbox.cli._dbt_output_parser import dbt_output_parser
from dbt_toolbox.constants import EXECUTION_TIMESTAMP
from dbt_toolbox.dbt_parser import dbt_parser
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import printer


def _validate_lineage_references(models_to_check: list[str] | None = None) -> bool:
    """Validate lineage references for models before execution.

    Args:
        models_to_check: List of model names to validate. If None, validates all models.

    Returns:
        True if all lineage references are valid, False otherwise.

    """
    if not settings.enforce_lineage_validation:
        return True

    printer.cprint("🔍 Validating lineage references...", color="cyan")

    # Get all models, sources, and seeds
    models = dbt_parser.models
    sources = dbt_parser.sources
    seeds = dbt_parser.seeds

    # Filter models if specific models are requested
    if models_to_check:
        models = {name: model for name, model in models.items() if name in models_to_check}

    # Perform column analysis
    analysis = analyze_column_references(models, sources, seeds)

    # Check if there are any issues
    if not analysis.non_existent_columns and not analysis.referenced_non_existent_models:
        return True

    # Print validation errors
    printer.cprint("❌ Lineage validation failed!", color="red")
    print()  # noqa: T201

    # Show non-existent columns
    if analysis.non_existent_columns:
        total_missing_cols = sum(len(cols) for cols in analysis.non_existent_columns.values())
        printer.cprint(f"Missing columns ({total_missing_cols}):", color="red")
        for model_name, referenced_models in analysis.non_existent_columns.items():
            for referenced_model, missing_columns in referenced_models.items():
                printer.cprint(
                    f"  • {model_name} → {referenced_model}: {', '.join(missing_columns)}",
                    color="yellow",
                )

    # Show non-existent referenced models/sources
    if analysis.referenced_non_existent_models:
        total_missing_models = sum(
            len(models) for models in analysis.referenced_non_existent_models.values()
        )
        printer.cprint(f"Non-existent references ({total_missing_models}):", color="red")
        for model_name, non_existent_models in analysis.referenced_non_existent_models.items():
            printer.cprint(
                f"  • {model_name} → {', '.join(set(non_existent_models))}",
                color="yellow",
            )

    print()  # noqa: T201
    printer.cprint(
        "💡 Tip: You can disable lineage validation by setting "
        "'enforce_lineage_validation = false' in your configuration",
        color="cyan",
    )
    return False


def _stream_process_output(process: subprocess.Popen) -> list[str]:
    """Stream process output in real-time and capture for parsing.

    Args:
        process: The subprocess.Popen object

    Returns:
        List of captured output lines

    """
    captured_output = []
    if process.stdout:
        while True:
            output = process.stdout.readline()
            if output == "" and process.poll() is not None:
                break
            if output:
                # Print to stdout immediately
                sys.stdout.write(output)
                sys.stdout.flush()
                # Capture for later parsing
                captured_output.append(output)
    return captured_output


def execute_dbt_command(base_command: list[str]) -> None:  # noqa: PLR0912
    """Execute a dbt command with standard project and profiles directories.

    Args:
        base_command: Base dbt command as list of strings (e.g., ["dbt", "build"]).

    """
    # Always add project-dir and profiles-dir to dbt commands
    command = base_command.copy()
    command.extend(["--project-dir", str(settings.dbt_project_dir)])
    command.extend(["--profiles-dir", str(settings.dbt_profiles_dir)])

    printer.cprint("🚀 Executing:", " ".join(command), highlight_idx=1, color="green")

    try:
        # Execute the dbt command with real-time output streaming
        process = subprocess.Popen(  # noqa: S603
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Combine stderr with stdout
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True,
        )

        # Stream output in real-time and capture for parsing
        captured_output = _stream_process_output(process)

        # Wait for process to complete and get return code
        return_code = process.wait()

        # Parse dbt output to identify model results (only for build/run commands)
        command_name = base_command[1] if len(base_command) > 1 else ""
        if command_name in ["build", "run"]:
            # Use captured output for parsing
            combined_output = "".join(captured_output)
            execution_result = dbt_output_parser.parse_output(combined_output)

            # Update build success flags for all models based on execution results
            all_models = dbt_parser.models  # Get all models to update their build status

            # Mark successful models as built successfully
            if execution_result.successful_models:
                for model_name in execution_result.successful_models:
                    if model_name in all_models:
                        model = all_models[model_name]
                        model.last_build_failed = False
                        model.last_built = EXECUTION_TIMESTAMP
                        dbt_parser.cache.cache_model(model)

            # Handle failed models - mark as failed and clear from cache
            if execution_result.failed_models and return_code != 0:
                printer.cprint(
                    f"🧹 Marking {len(execution_result.failed_models)} models as failed...",
                    color="yellow",
                )

                removed_models = []
                for model_name in execution_result.failed_models:
                    if model_name in all_models:
                        model = all_models[model_name]
                        model.last_build_failed = True
                        model.last_built = EXECUTION_TIMESTAMP
                        dbt_parser.cache.cache_model(model)

                    # Also clear the cache to force rebuild next time
                    if dbt_parser.cache.clear_model_cache(model_name):
                        removed_models.append(model_name)

                if removed_models:
                    printer.cprint("   Removed from cache:", color="cyan")
                    for model in removed_models:
                        printer.cprint(f"   • {model}", color="bright_black")
                else:
                    printer.cprint("   No failed models found in cache", color="bright_black")

        # Exit with the same code as dbt
        sys.exit(return_code)

    except KeyboardInterrupt:
        printer.cprint("❌ Command interrupted by user", color="red")
        sys.exit(130)  # Standard exit code for Ctrl+C
    except FileNotFoundError:
        printer.cprint(
            "❌ Error: 'dbt' command not found.",
            "Please ensure dbt is installed and available in your PATH.",
            highlight_idx=1,
            color="red",
        )
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        printer.cprint("❌ Unexpected error:", str(e), highlight_idx=1, color="red")
        sys.exit(1)


def execute_dbt_with_smart_selection(  # noqa: PLR0912, PLR0913
    command_name: str,
    model: str | None = None,
    full_refresh: bool = False,
    threads: int | None = None,
    vars: str | None = None,  # noqa: A002
    target: str | None = None,
    analyze_only: bool = False,
    disable_smart: bool = False,
) -> None:
    """Execute a dbt command with intelligent model selection.

    Args:
        command_name: The dbt command to run ('build' or 'run')
        model: Model selection string
        full_refresh: Whether to do a full refresh
        threads: Number of threads to use
        vars: Variables to pass to dbt
        target: Target to use
        analyze_only: Only show analysis without executing
        disable_smart: Disable smart execution and run all selected models

    """
    if not disable_smart and not _validate_lineage_references():
        sys.exit(1)
    # Start building the dbt command
    dbt_command = ["dbt", command_name]

    # Display what we're doing
    action = "Building" if command_name == "build" else "Running"
    if model:
        printer.cprint(
            f"🔨 {action} models:",
            model,
            highlight_idx=1,
            color="cyan",
        )
    else:
        printer.cprint(f"🔨 {action} all models", color="cyan")

    # Add model selection if provided
    if model:
        dbt_command.extend(["--select", model])

    # Add other common options
    if full_refresh:
        dbt_command.append("--full-refresh")

    if threads:
        dbt_command.extend(["--threads", str(threads)])

    # Add target if provided
    if target:
        dbt_command.extend(["--target", target])

    if vars:
        dbt_command.extend(["--vars", vars])

    # Perform intelligent execution analysis (enabled by default)
    if not disable_smart:
        # Analyze which models need execution
        analyses = build_analyzer.analyze_build_execution(model)
        build_analyzer.print_execution_analysis(analyses)

        if analyze_only:
            # Just show analysis and exit
            return

        # Filter models to only those that need execution (smart execution)
        models_to_execute = [
            name for name, analysis in analyses.items() if analysis.needs_execution
        ]

        if not models_to_execute:
            printer.cprint(
                "✅ All models have valid cache - nothing to execute!",
                color="green",
            )
            return

        # Update dbt command with filtered model selection
        if len(models_to_execute) == len(analyses):
            # All models need execution, keep original selection
            printer.cprint("🔥 All selected models need execution", color="yellow")
        else:
            # Create new selection with only models that need execution
            new_selection = " ".join(models_to_execute)
            printer.cprint(f"🎯 Optimized selection: {new_selection}", color="cyan")

            # Update the dbt command to use the optimized selection
            # Find and replace the --select argument
            for i, arg in enumerate(dbt_command):
                if arg == "--select":
                    dbt_command[i + 1] = new_selection
                    break
            else:
                # If --select wasn't found, add it
                dbt_command.extend(["--select", new_selection])
    elif analyze_only:
        # If smart execution is disabled but analyze_only is requested
        analyses = build_analyzer.analyze_build_execution(model)
        build_analyzer.print_execution_analysis(analyses, verbose=True)
        return

    execute_dbt_command(dbt_command)


def create_dbt_command_function(command_name: str, help_text: str) -> Callable:
    """Create a dbt command function with standardized options.

    Args:
        command_name: The dbt command name (e.g., 'build', 'run')
        help_text: Help text for the command

    Returns:
        A function that can be used as a typer command.

    """

    def dbt_command(  # noqa: PLR0913
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
        target: Annotated[
            str | None,
            typer.Option("--target", "-t", help="Which target to load for the given profile"),
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
        execute_dbt_with_smart_selection(
            command_name=command_name,
            model=model,
            full_refresh=full_refresh,
            threads=threads,
            vars=vars,
            target=target,
            analyze_only=analyze_only,
            disable_smart=disable_smart,
        )

    # Set the docstring and name dynamically
    dbt_command.__doc__ = help_text
    dbt_command.__name__ = command_name
    return dbt_command
