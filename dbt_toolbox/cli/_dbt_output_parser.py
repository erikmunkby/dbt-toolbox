"""Parser for dbt command output to identify failed models."""

import re
from dataclasses import dataclass, field
from typing import Literal, NamedTuple

StatusTypes = Literal["OK", "ERROR", "SKIP"]  # OK, ERROR, SKIP, etc.
TestStatusTypes = Literal["PASS", "FAIL", "WARN", "ERROR", "SKIP"]


class ModelResult(NamedTuple):
    """Result of a model execution from dbt output."""

    name: str
    status: StatusTypes
    execution_time_seconds: float | None = None
    error_message: str | None = None


class TestResult(NamedTuple):
    """Result of a test execution from dbt output.

    Test naming convention: <namespace>_<test_name>_<model_name>_<metadata>
    - no namespace: "not_null_customers_customer_id"
    - with namespace: "dbt_utils_expression_is_true_customers_1_2"
    """

    name: str
    status: TestStatusTypes
    model_name: str  # Model that owns this test
    execution_time_seconds: float | None = None


@dataclass
class DbtParsedLogs:
    """Result of parsing dbt execution output."""

    models: dict[str, ModelResult] = field(default_factory=dict)
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

    def get_test(self, name: str, /) -> TestResult | None:
        return self.tests.get(name)


def parse_dbt_line(line: str) -> ModelResult | TestResult | None:
    """Parse a single line of dbt output to extract model or test execution results.

    This is the primary public function for parsing dbt output line-by-line.

    Args:
        line: A single line from dbt output (will be stripped internally).

    Returns:
        ModelResult if a model execution is found.
        TestResult if a test execution is found.
        None if the line doesn't contain model or test results.

    """
    clean_line = line.strip()
    if not clean_line or "[RUN]" in clean_line:
        return None

    # Try to extract test information first (more specific patterns)
    test_info = _extract_test_info(clean_line)
    if test_info:
        return test_info

    # Try to extract model information from the line
    model_info = _extract_model_info(clean_line)
    if model_info:
        return model_info

    return None


def parse_dbt_output(output: str) -> DbtParsedLogs:
    """Parse dbt command output to extract model and test execution results.

    Args:
        output: Raw output from dbt command execution.

    Returns:
        DbtParsedLogs with categorized model and test results.

    """
    lines = output.split("\n")

    model_results = {}
    test_results = {}

    for line in lines:
        result = parse_dbt_line(line)
        if isinstance(result, TestResult):
            test_results[result.name] = result
        elif isinstance(result, ModelResult):
            model_results[result.name] = result

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


def _extract_model_name_from_test(test_name: str) -> str:
    """Extract model name from test name.

    Test naming convention: <namespace>_<test_name>_<model_name>_<metadata>
    Examples:
        - "not_null_customers_customer_id" -> "customers"
        - "unique_orders_order_id" -> "orders"
        - "dbt_utils_expression_is_true_customers_1_2" -> "customers"

    Strategy: Find the first part that looks like a model name (not a test keyword,
    namespace, or numeric metadata). Model names typically come before numeric suffixes.

    Args:
        test_name: Full test name from dbt output

    Returns:
        Model name extracted from test name

    """
    # Common test keywords and namespaces to skip
    skip_words = {
        "not",
        "null",
        "unique",
        "accepted",
        "values",
        "relationships",
        "dbt",
        "utils",
        "expectations",
        "expression",
        "is",
        "true",
        "false",
        "test",
    }

    parts = test_name.split("_")

    # Find the first part that's not a skip word and not purely numeric
    for part in parts:
        # Skip common test keywords/namespaces
        if part.lower() in skip_words:
            continue
        # Skip purely numeric parts (metadata)
        if part.isdigit():
            continue
        # This is likely the model name
        return part

    # Fallback: return full test name if parsing fails
    return test_name


def _extract_test_info(line: str) -> TestResult | None:
    """Extract test information from a dbt output line.

    Test lines follow this pattern:
    - START: "7 of 12 START test dbt_utils_expression_is_true_customers_1_2 ... [RUN]"
    - PASS:  "8 of 12 PASS not_null_customers_customer_id ... [PASS in 0.05s]"
    - FAIL:  "7 of 12 FAIL 930 dbt_utils_expression_is_true_customers_1_2 ... [FAIL in 0.05s]"

    Test naming convention: <namespace>_<test_name>_<model_name>_<metadata>
    - no namespace: "not_null_customers_customer_id"
    - with namespace: "dbt_utils_expression_is_true_customers_1_2"

    Args:
        line: A line from dbt output

    Returns:
        TestResult if test information is found, None otherwise

    """
    # Strip ANSI escape codes
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    clean_line = ansi_escape.sub("", line)

    # Test lines contain "START test" or status (PASS/FAIL/WARN) followed by test name
    if " test " not in clean_line and not re.search(
        r"\s+(PASS|FAIL|WARN)\s+(\d+\s+)?[\w_]+", clean_line
    ):
        return None

    # Match test execution results (PASS/FAIL/WARN)
    result_pattern = r"\d+\s+of\s+\d+\s+(PASS|FAIL|WARN)(?:\s+\d+)?\s+([\w_]+)"
    match = re.search(result_pattern, clean_line)

    if match:
        status = match.group(1)
        test_name = match.group(2)

        # Extract model name from test name
        model_name = _extract_model_name_from_test(test_name)

        # Extract execution time from patterns like "[PASS in 0.05s]" or "[FAIL 930 in 0.05s]"
        execution_time = None
        time_pattern = r"\[(?:PASS|FAIL|WARN)(?:\s+\d+)?\s+in\s+([\d.]+)s\]"
        time_match = re.search(time_pattern, clean_line)
        if time_match:
            try:
                execution_time = float(time_match.group(1))
            except (ValueError, TypeError):
                execution_time = None

        return TestResult(
            name=test_name,
            status=status,
            model_name=model_name,
            execution_time_seconds=execution_time,
        )  # type: ignore

    return None
