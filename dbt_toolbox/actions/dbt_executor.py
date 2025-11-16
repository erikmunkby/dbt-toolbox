"""Shared dbt execution engine for build and run commands."""

import subprocess
import sys
from dataclasses import dataclass

from dbt_toolbox.analysees import AnalysisResult, analyze
from dbt_toolbox.analysees.print_analysis import print_column_analysis_results
from dbt_toolbox.cli._dbt_output_parser import DbtParsedLogs, _extract_model_info, parse_dbt_output
from dbt_toolbox.data_models import DbtExecutionParams, Model
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import cprint, log


@dataclass
class DbtExecutionResults:
    """Results from executing dbt commands."""

    return_code: int
    parsed_logs: DbtParsedLogs
    raw_logs: str


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
            return DbtExecutionResults(
                return_code=1, parsed_logs=DbtParsedLogs(models={}), raw_logs=""
            )

        if not self.models_to_execute:
            return DbtExecutionResults(
                return_code=0, parsed_logs=DbtParsedLogs(models={}), raw_logs=""
            )

        return _execute_dbt_raw(dbt_parser=self._dbt_parser, dbt_command=self.dbt_command)


def _validate_lineage_references(
    target: str | None, selection_query: str | None, dbt_parser: dbtParser
) -> bool:
    """Validate lineage references for models before execution.

    Args:
        target: dbt target environment
        selection_query: A dbt selection query e.g. customers+
        dbt_parser: dbtParser instance to reuse

    Returns:
        True if all lineage references are valid, False otherwise.

    """
    if not settings.enforce_validation:
        return True

    # Perform analysis using the unified analyze function
    # Pass dbt_parser to avoid creating a new instance and re-parsing selection
    results = analyze(target=target, model=selection_query, dbt_parser=dbt_parser)
    analysis = results.column_analysis

    # Check if there are any issues
    if not analysis.non_existent_columns and not analysis.referenced_non_existent_models:
        return True

    # Print validation failure results using the unified printing function
    print_column_analysis_results(analysis=analysis, mode="validation")

    return False


def _stream_process_output(process: subprocess.Popen, dbt_parser: dbtParser) -> list[str]:
    """Stream process output in real-time and capture for parsing.

    Optionally cache models as they complete if dbt_parser is provided.

    Args:
        process: The subprocess.Popen object
        dbt_parser: Optional dbt parser to cache models as they complete

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

                # If dbt_parser provided, try to parse and cache completed models
                model_info = _extract_model_info(output.strip())
                if model_info and model_info.status in ["OK", "ERROR"]:
                    # Find the model in dbt_parser and update its status
                    model = dbt_parser.models.get(model_info.name)
                    if model:
                        if model_info.status == "OK":
                            model.set_build_successful(
                                compute_time_seconds=model_info.execution_time_seconds or 0
                            )
                        elif model_info.status == "ERROR":
                            model.set_build_failed()

                        # Cache the model immediately
                        dbt_parser.cache.cache_model(model=model)
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

    log.debug("Executing: %s", " ".join(command))

    # Initialize default values
    dbt_return_code = 1
    dbt_logs = DbtParsedLogs(models={})
    dbt_raw_output = ""

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

        # Determine if we should enable streaming cache for build/run commands
        command_name = dbt_command[1] if len(dbt_command) > 1 else ""

        # Stream output in real-time and capture for parsing
        # Pass dbt_parser only for build/run commands to enable streaming cache
        captured_output = _stream_process_output(process, dbt_parser)

        # Wait for process to complete and get return code
        dbt_return_code = process.wait()
        dbt_raw_output = "".join(captured_output)

        # Parse dbt output to identify model results (only for build/run commands)
        if command_name in ["build", "run"]:
            # Use captured output for parsing
            dbt_logs = parse_dbt_output(dbt_raw_output)

            # Handle failed models - mark as failed and clear from cache
            if dbt_logs.failed_models and dbt_return_code != 0:
                cprint(
                    f"🧹 Marking {len(dbt_logs.failed_models)} models as failed...",
                    color="yellow",
                )

    except KeyboardInterrupt:
        cprint("❌ Command interrupted by user", color="red")
        dbt_return_code = 130  # Standard exit code for Ctrl+C
    except FileNotFoundError:
        cprint(
            "❌ Error: 'dbt' command not found.",
            "Please ensure dbt is installed and available in your PATH.",
            highlight_idx=1,
            color="red",
        )
        dbt_return_code = 1
    except Exception as e:  # noqa: BLE001
        cprint("❌ Unexpected error:", str(e), highlight_idx=1, color="red")
        dbt_return_code = 1

    return DbtExecutionResults(
        return_code=dbt_return_code, parsed_logs=dbt_logs, raw_logs=dbt_raw_output
    )


def create_execution_plan(params: DbtExecutionParams) -> ExecutionPlan:
    """Create an execution plan for a dbt command with validation and intelligent model selection.

    Args:
        params: DbtExecutionParams object containing all execution parameters

    Returns:
        ExecutionPlan with analysis results and execution strategy.

    """
    dbt_parser = dbtParser(target=params.target)

    # Start building the dbt command
    dbt_command = ["dbt", params.command_name]

    # Add model selection if provided
    if params.model_selection:
        dbt_command.extend(["--select", params.model_selection])

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

    # If force mode, skip validation and analysis
    if params.force:
        return ExecutionPlan(
            analyses=[],
            models_to_execute=["all"],  # Placeholder for full execution
            models_to_skip=[],
            dbt_command=dbt_command,
            lineage_valid=True,  # Skip validation in force mode
            params=params,
            _dbt_parser=dbt_parser,
        )

    # Validate lineage references (unless force=True)
    lineage_valid = _validate_lineage_references(
        target=params.target, selection_query=params.model_selection, dbt_parser=dbt_parser
    )

    # Perform intelligent execution analysis
    # Pass dbt_parser to avoid creating a new instance and re-parsing selection
    results = analyze(target=params.target, model=params.model_selection, dbt_parser=dbt_parser)
    analyses = results.model_analysis

    # Filter models to only those that need execution
    models_to_execute: list[str] = []
    models_to_skip: list[Model] = []
    for analysis in analyses:
        if analysis.needs_execution:
            models_to_execute.append(analysis.model.name)
        else:
            models_to_skip.append(analysis.model)

    # Replace selection with explicit model names
    # This ensures we use the resolved model names (e.g., fuzzy-matched or path-resolved)
    # instead of the original query string that may not be valid dbt syntax
    if models_to_execute:
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
