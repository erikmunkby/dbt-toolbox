"""Parser for dbt command output to identify failed models and test results."""

import re
from dataclasses import dataclass, field
from typing import Literal, NamedTuple

StatusTypes = Literal["OK", "ERROR", "SKIP"]  # OK, ERROR, SKIP, etc.
TestStatusTypes = Literal["PASS", "FAIL", "WARN", "ERROR"]


class ModelResult(NamedTuple):
    """Result of a model execution from dbt output."""

    name: str
    status: StatusTypes
    execution_time_seconds: float | None = None
    error_message: str | None = None


class TestResult(NamedTuple):
    """Result of a test execution from dbt output."""

    name: str
    status: TestStatusTypes
    execution_time_seconds: float | None = None


@dataclass
class DbtParsedLogs:
    """Result of parsing dbt execution output."""

    models: dict[str, ModelResult]
    tests: dict[str, TestResult] = field(default_factory=dict)

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

    def _filter_tests(self, status: TestStatusTypes, /) -> list[str]:
        return [name for name, t in self.tests.items() if t.status == status]

    @property
    def passed_tests(self) -> list[str]:
        return self._filter_tests("PASS")

    @property
    def failed_tests(self) -> list[str]:
        return self._filter_tests("FAIL")


def parse_dbt_output(output: str) -> DbtParsedLogs:
    """Parse dbt command output to extract model and test execution results.

    Args:
        output: Raw output from dbt command execution.

    Returns:
        DbtParsedLogs with categorized model and test results.

    """
    lines = output.split("\n")

    model_results: dict[str, ModelResult] = {}
    test_results: dict[str, TestResult] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        # Skip RUN status lines
        if "[RUN]" in line:
            continue

        # Try to extract model information first
        model_info = _extract_model_info(line)
        if model_info:
            model_results[model_info.name] = model_info
            continue

        # Try to extract test information
        test_info = _extract_test_info(line)
        if test_info:
            test_results[test_info.name] = test_info

    return DbtParsedLogs(models=model_results, tests=test_results)


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


def _extract_test_info(line: str) -> TestResult | None:
    """Extract test information from a dbt output line.

    Args:
        line: A line from dbt output.

    Returns:
        TestResult if test information is found, None otherwise.

    """
    # Strip ANSI escape codes
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    clean_line = ansi_escape.sub("", line)

    # Test lines look like:
    #   PASS not_null_orders_order_id ... [PASS in 0.03s]
    #   FAIL 1 not_null_orders_order_id ... [FAIL 1 in 0.03s]
    #   WARN 1 not_null_orders_order_id ... [WARN 1 in 0.03s]
    #   ERROR not_null_orders_order_id ... [ERROR in 0.03s]
    # They do NOT contain "model schema.name" pattern

    # Skip lines that look like model lines (contain "model <word>.<word>")
    if re.search(r"model\s+\w+\.\w+", clean_line):
        return None

    test_name = None
    status: TestStatusTypes | None = None

    # Match PASS test_name
    pass_match = re.search(r"\bPASS\s+(\w+)", clean_line)
    if pass_match:
        test_name = pass_match.group(1)
        status = "PASS"

    # Match FAIL N test_name
    if not test_name:
        fail_match = re.search(r"\bFAIL\s+\d+\s+(\w+)", clean_line)
        if fail_match:
            test_name = fail_match.group(1)
            status = "FAIL"

    # Match WARN N test_name
    if not test_name:
        warn_match = re.search(r"\bWARN\s+\d+\s+(\w+)", clean_line)
        if warn_match:
            test_name = warn_match.group(1)
            status = "WARN"

    # Match ERROR test_name (test errors without count)
    if not test_name:
        error_match = re.search(r"\bERROR\s+(\w+)", clean_line)
        if error_match:
            test_name = error_match.group(1)
            status = "ERROR"

    if not test_name or not status:
        return None

    # Extract execution time from [PASS in X.XXs], [FAIL N in X.XXs], etc.
    execution_time = None
    time_match = re.search(
        r"\[(?:PASS|FAIL\s+\d+|WARN\s+\d+|ERROR)\s+in\s+([\d.]+)s\]", clean_line
    )
    if time_match:
        execution_time = float(time_match.group(1))

    return TestResult(name=test_name, status=status, execution_time_seconds=execution_time)


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
