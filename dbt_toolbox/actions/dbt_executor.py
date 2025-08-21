"""Shared dbt execution engine for build and run commands."""

import subprocess
import sys
from dataclasses import dataclass

from dbt_toolbox.actions.analyze_columns_references import analyze_column_references
from dbt_toolbox.actions.analyze_models import AnalysisResult, analyze_model_statuses
from dbt_toolbox.cli._dbt_output_parser import DbtParsedLogs, parse_dbt_output
from dbt_toolbox.data_models import DbtExecutionParams, Model
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import _printers


@dataclass
class DbtExecutionResults:
    """Results from executing dbt commands."""

    return_code: int
    logs: DbtParsedLogs


@dataclass
class DbtCommandResult:
    """Combined result of dbt command execution with analysis."""

    analyses: list[AnalysisResult] | None
    execution: DbtExecutionResults


@dataclass
class ExecutionPlan:
    """Execution plan containing analysis and execution strategy."""

    analyses: list[AnalysisResult]
    models_to_execute: list[str]
    models_to_skip: list[Model]
    dbt_command: list[str]
    lineage_valid: bool
    params: DbtExecutionParams
    _dbt_parser: dbtParser

    @property
    def compute_time_saved_seconds(self) -> float:
        """Get the total compute time saved due to skipping models."""
        return sum(
            [m.compute_time_seconds if m.compute_time_seconds else 0 for m in self.models_to_skip]
        )

    def run(self) -> DbtExecutionResults:
        """Execute the planned dbt command.

        Returns:
            DbtExecutionResults with return code and parsed logs.

        """
        if not self.lineage_valid:
            return DbtExecutionResults(return_code=1, logs=DbtParsedLogs(models={}))

        if not self.models_to_execute:
            return DbtExecutionResults(return_code=0, logs=DbtParsedLogs(models={}))

        return _execute_dbt_raw(dbt_parser=self._dbt_parser, dbt_command=self.dbt_command)


def _validate_lineage_references(dbt_parser: dbtParser) -> bool:
    """Validate lineage references for models before execution.

    Args:
        dbt_parser: The dbt parser object.
        models_to_check: List of model names to validate. If None, validates all models.

    Returns:
        True if all lineage references are valid, False otherwise.

    """
    if not settings.enforce_lineage_validation:
        return True

    # Perform column analysis
    analysis = analyze_column_references(dbt_parser=dbt_parser)

    # Check if there are any issues
    if not analysis.non_existent_columns and not analysis.referenced_non_existent_models:
        return True

    # Print validation errors
    _printers.cprint("❌ Lineage validation failed!", color="red")
    print()  # noqa: T201

    # Show non-existent columns
    if analysis.non_existent_columns:
        total_missing_cols = sum(len(cols) for cols in analysis.non_existent_columns.values())
        _printers.cprint(f"Missing columns ({total_missing_cols}):", color="red")
        for model_name, referenced_models in analysis.non_existent_columns.items():
            for referenced_model, missing_columns in referenced_models.items():
                _printers.cprint(
                    f"  • {model_name} → {referenced_model}: {', '.join(missing_columns)}",
                    color="yellow",
                )

    # Show non-existent referenced models/sources
    if analysis.referenced_non_existent_models:
        total_missing_models = sum(
            len(models) for models in analysis.referenced_non_existent_models.values()
        )
        _printers.cprint(f"Non-existent references ({total_missing_models}):", color="red")
        for model_name, non_existent_models in analysis.referenced_non_existent_models.items():
            _printers.cprint(
                f"  • {model_name} → {', '.join(set(non_existent_models))}",
                color="yellow",
            )

    print()  # noqa: T201
    _printers.cprint(
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


def _execute_dbt_raw(dbt_parser: dbtParser, dbt_command: list[str]) -> DbtExecutionResults:
    """Execute a raw dbt command with standard project and profiles directories.

    Args:
        dbt_parser:     The dbt parser object.
        dbt_command:    Complete dbt command as list of strings
                        (e.g., ["dbt", "build", "--select", "model"]).

    Returns:
        DbtExecutionResults with return code and parsed logs.

    """
    # Always add project-dir and profiles-dir to dbt commands
    command = dbt_command.copy()
    command.extend(["--project-dir", str(settings.dbt_project_dir)])
    command.extend(["--profiles-dir", str(settings.dbt_profiles_dir)])

    _printers.cprint("🚀 Executing:", " ".join(command), highlight_idx=1, color="green")

    # Initialize default values
    dbt_return_code = 1
    dbt_logs = DbtParsedLogs(models={})

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
        dbt_return_code = process.wait()

        # Parse dbt output to identify model results (only for build/run commands)
        command_name = dbt_command[1] if len(dbt_command) > 1 else ""
        if command_name in ["build", "run"]:
            # Use captured output for parsing
            combined_output = "".join(captured_output)
            dbt_logs = parse_dbt_output(combined_output)

            # Mark successful models as built successfully
            for model_name, model in dbt_parser.models.items():
                model_results = dbt_logs.get_model(model_name)
                if not model_results:
                    continue
                if model_results.status == "OK":
                    exec_time = model_results.execution_time_seconds
                    model.set_build_successful(compute_time_seconds=exec_time if exec_time else 0)
                elif model_results.status == "ERROR":
                    model.set_build_failed()
                # Finally, cache the model with its results.
                dbt_parser.cache.cache_model(model=model)

            # Handle failed models - mark as failed and clear from cache
            if dbt_logs.failed_models and dbt_return_code != 0:
                _printers.cprint(
                    f"🧹 Marking {len(dbt_logs.failed_models)} models as failed...",
                    color="yellow",
                )

    except KeyboardInterrupt:
        _printers.cprint("❌ Command interrupted by user", color="red")
        dbt_return_code = 130  # Standard exit code for Ctrl+C
    except FileNotFoundError:
        _printers.cprint(
            "❌ Error: 'dbt' command not found.",
            "Please ensure dbt is installed and available in your PATH.",
            highlight_idx=1,
            color="red",
        )
        dbt_return_code = 1
    except Exception as e:  # noqa: BLE001
        _printers.cprint("❌ Unexpected error:", str(e), highlight_idx=1, color="red")
        dbt_return_code = 1

    return DbtExecutionResults(return_code=dbt_return_code, logs=dbt_logs)


def create_execution_plan(params: DbtExecutionParams) -> ExecutionPlan:
    """Create an execution plan for a dbt command with intelligent model selection.

    Args:
        params: DbtExecutionParams object containing all execution parameters

    Returns:
        ExecutionPlan with analysis results and execution strategy.

    """
    dbt_parser = dbtParser(target=params.target)

    # Validate lineage references if smart execution is enabled
    lineage_valid = True
    if not params.disable_smart:
        lineage_valid = _validate_lineage_references(dbt_parser=dbt_parser)

    # Start building the dbt command
    dbt_command = ["dbt", params.command_name]

    # Add model selection if provided
    if params.model:
        dbt_command.extend(["--select", params.model])

    # Add other common options
    if params.full_refresh:
        dbt_command.append("--full-refresh")

    if params.threads:
        dbt_command.extend(["--threads", str(params.threads)])

    # Add target if provided
    if params.target:
        dbt_command.extend(["--target", params.target])

    if params.vars:
        dbt_command.extend(["--vars", params.vars])

    # Handle disabled smart execution or analyze-only mode
    if params.disable_smart:
        return ExecutionPlan(
            analyses=[],
            models_to_execute=["all"],  # Placeholder for full execution
            models_to_skip=[],
            dbt_command=dbt_command,
            lineage_valid=lineage_valid,
            params=params,
            _dbt_parser=dbt_parser,
        )

    # Perform intelligent execution analysis (enabled by default)
    analyses = analyze_model_statuses(dbt_parser=dbt_parser, dbt_selection=params.model)

    # Filter models to only those that need execution (smart execution)
    models_to_execute: list[str] = []
    models_to_skip: list[Model] = []
    for analysis in analyses:
        if analysis.needs_execution:
            models_to_execute.append(analysis.model.name)
        else:
            models_to_skip.append(analysis.model)

    # Update dbt command with filtered model selection
    if models_to_execute and len(models_to_execute) < len(analyses):
        # Create new selection with only models that need execution
        new_selection = " ".join(models_to_execute)

        # Update the dbt command to use the optimized selection
        # Find and replace the -s argument
        for i, arg in enumerate(dbt_command):
            if arg in ["-s", "--select", "-m", "--models", "--model"]:
                dbt_command[i + 1] = new_selection
                break
        else:
            # If -s wasn't found, add it
            dbt_command.extend(["-s", new_selection])

    return ExecutionPlan(
        analyses=analyses,
        models_to_execute=models_to_execute,
        models_to_skip=models_to_skip,
        dbt_command=dbt_command,
        lineage_valid=lineage_valid,
        params=params,
        _dbt_parser=dbt_parser,
    )
