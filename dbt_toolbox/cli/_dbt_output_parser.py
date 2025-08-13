"""Parser for dbt command output to identify failed models."""

import re
from dataclasses import dataclass
from typing import NamedTuple


class ModelResult(NamedTuple):
    """Result of a model execution from dbt output."""

    name: str
    status: str  # OK, ERROR, SKIP, etc.
    execution_time_seconds: float | None = None
    error_message: str | None = None


@dataclass
class DbtExecutionResult:
    """Result of parsing dbt execution output."""

    successful_models: list[str]
    failed_models: list[str]
    skipped_models: list[str]
    all_results: list[ModelResult]


def parse_dbt_output(output: str) -> DbtExecutionResult:
    """Parse dbt command output to extract model execution results.

    Args:
        output: Raw output from dbt command execution.

    Returns:
        DbtExecutionResult with categorized model results.

    """
    all_results = []
    lines = output.split("\n")

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        # Skip RUN status lines
        if "[RUN]" in line:
            continue

        # Try to extract model information from the line
        model_info = _extract_model_info(line)
        if model_info:
            all_results.append(model_info)

    # Categorize results
    successful_models = [r.name for r in all_results if r.status == "OK"]
    failed_models = [r.name for r in all_results if r.status == "ERROR"]
    skipped_models = [r.name for r in all_results if r.status == "SKIP"]

    return DbtExecutionResult(
        successful_models=successful_models,
        failed_models=failed_models,
        skipped_models=skipped_models,
        all_results=all_results,
    )


def _extract_model_info(line: str) -> ModelResult | None:
    """Extract model information from a dbt output line.

    Args:
        line: A line from dbt output

    Returns:
        ModelResult if model information is found, None otherwise

    """
    # Look for pattern: [NUMBER of NUMBER] STATUS [created/creating] [sql]
    # [table/view/incremental] model [schema.model_name]

    # Extract model name from various patterns
    model_name = None

    # Pattern 1: "model schema.model_name" or "model target.model_name"
    model_match = re.search(r"model\s+\w+\.(\w+)", line)
    if model_match:
        model_name = model_match.group(1)

    # Pattern 2: "relation schema.model_name" (for SKIP relations)
    if not model_name:
        relation_match = re.search(r"relation\s+\w+\.(\w+)", line)
        if relation_match:
            model_name = relation_match.group(1)

    if not model_name:
        return None

    # Determine status based on keywords in the line
    status = None
    execution_time = None
    error_message = None

    if "OK created" in line or "OK loaded" in line:
        status = "OK"
        # Extract execution time from patterns like "[OK in 0.29s]" or "[SELECT 123 in 0.45s]"
        time_match = re.search(r"\[(?:[A-Z]+\s+\d+\s+in\s+([\d.]+)s|OK\s+in\s+([\d.]+)s)\]", line)
        if time_match:
            try:
                execution_time = float(time_match.group(1) or time_match.group(2))
            except (ValueError, TypeError, AttributeError):
                execution_time = None

    elif "ERROR creating" in line:
        status = "ERROR"
        error_message = _extract_error_message(line)
        # Extract execution time from patterns like "[ERROR in 0.02s]"
        time_match = re.search(r"\[ERROR\s+in\s+([\d.]+)s\]", line)
        if time_match:
            try:
                execution_time = float(time_match.group(1))
            except (ValueError, TypeError):
                execution_time = None

    elif "SKIP relation" in line:
        status = "SKIP"

    if status:
        return ModelResult(
            name=model_name,
            status=status,
            execution_time_seconds=execution_time,
            error_message=error_message,
        )

    return None


def _extract_error_message(line: str) -> str | None:
    """Extract error message from a dbt error line.

    Args:
        line: Line containing the error.

    Returns:
        Extracted error message or None if not found.

    """
    if "ERROR" in line:
        # Try to get everything after "ERROR creating"
        parts = line.split("ERROR creating")
        if len(parts) > 1:
            return parts[1].strip()
    return None
