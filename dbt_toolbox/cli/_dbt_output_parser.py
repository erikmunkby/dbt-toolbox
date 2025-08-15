"""Parser for dbt command output to identify failed models."""

import re
from dataclasses import dataclass
from typing import Literal, NamedTuple

StatusTypes = Literal["OK", "ERROR", "SKIP"]  # OK, ERROR, SKIP, etc.


class ModelResult(NamedTuple):
    """Result of a model execution from dbt output."""

    name: str
    status: StatusTypes
    execution_time_seconds: float | None = None
    error_message: str | None = None


@dataclass
class DbtParsedLogs:
    """Result of parsing dbt execution output."""

    models: dict[str, ModelResult]

    def _filter(self, status: StatusTypes, /) -> list[str]:
        return [name for name, m in self.models.items() if m.status == status]

    @property
    def successful_models(self) -> list[str]:
        return self._filter("OK")

    @property
    def failed_models(self) -> list[str]:
        return self._filter("ERROR")

    @property
    def skipped_models(self) -> list[str]:
        return self._filter("SKIP")

    def get_model(self, name: str, /) -> ModelResult | None:
        return self.models.get(name)


def parse_dbt_output(output: str) -> DbtParsedLogs:
    """Parse dbt command output to extract model execution results.

    Args:
        output: Raw output from dbt command execution.

    Returns:
        DbtExecutionResult with categorized model results.

    """
    lines = output.split("\n")

    results = {}
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
            results[model_info.name] = model_info
    return DbtParsedLogs(models=results)


def _extract_model_info(line: str) -> ModelResult | None:
    """Extract model information from a dbt output line.

    Args:
        line: A line from dbt output

    Returns:
        ModelResult if model information is found, None otherwise

    """
    # Look for pattern: [NUMBER of NUMBER] STATUS [created/creating] [sql]
    # [table/view/incremental] model [schema.model_name]

    # Strip ANSI escape codes that interfere with pattern matching
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    clean_line = ansi_escape.sub("", line)

    # Extract model name from various patterns
    model_name = None

    # Pattern 1: "model schema.model_name" or "model target.model_name"
    model_match = re.search(r"model\s+\w+\.(\w+)", clean_line)
    if model_match:
        model_name = model_match.group(1)

    # Pattern 2: "relation schema.model_name" (for SKIP relations)
    if not model_name:
        relation_match = re.search(r"relation\s+\w+\.(\w+)", clean_line)
        if relation_match:
            model_name = relation_match.group(1)

    if not model_name:
        return None

    # Determine status based on keywords in the line
    status = None
    execution_time = None
    error_message = None

    if "OK created" in clean_line or "OK loaded" in clean_line or "OK creating" in clean_line:
        status = "OK"
        # Extract execution time from patterns like "[OK in 0.29s]" or "[SELECT 123 in 0.45s]"
        time_pattern = r"\[(?:[A-Z]+\s+\d+\s+in\s+([\d.]+)s|OK\s+in\s+([\d.]+)s)\]"
        time_match = re.search(time_pattern, clean_line)
        if time_match:
            execution_time = float(time_match.group(1) or time_match.group(2))

    if "ERROR creating" in clean_line:
        status = "ERROR"
        error_message = _extract_error_message(clean_line)
        # Extract execution time from patterns like "[ERROR in 0.02s]"
        time_match = re.search(r"\[ERROR\s+in\s+([\d.]+)s\]", clean_line)
        if time_match:
            try:
                execution_time = float(time_match.group(1))
            except (ValueError, TypeError):
                execution_time = None

    elif "SKIP relation" in clean_line:
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
