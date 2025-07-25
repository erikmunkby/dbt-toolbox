"""Parser for dbt command output to identify failed models."""

import re
from dataclasses import dataclass
from typing import NamedTuple


class ModelResult(NamedTuple):
    """Result of a model execution from dbt output."""

    name: str
    status: str  # OK, ERROR, SKIP, etc.
    error_message: str | None = None


@dataclass
class DbtExecutionResult:
    """Result of parsing dbt execution output."""

    successful_models: list[str]
    failed_models: list[str]
    skipped_models: list[str]
    all_results: list[ModelResult]


class DbtOutputParser:
    """Parser for dbt command output to extract model execution results."""

    # Regex patterns for different dbt output formats
    PATTERNS = {
        # Matches both numbered and non-numbered success patterns
        # "OK created table model test_db.my_model" or "1 of 5 OK created ..."
        # Also handles "sql" prefix: "OK created sql view model test_db.my_model"
        "success": re.compile(
            r"(?:\d+\s+of\s+\d+\s+)?OK\s+created\s+(?:sql\s+)?(?:table|view|incremental)\s+model\s+\w+\.(\w+)",
        ),
        # Matches both numbered and non-numbered error patterns
        # "ERROR creating table model test_db.my_model" or "2 of 5 ERROR ..."
        # Also handles "sql" prefix: "ERROR creating sql view model test_db.my_model"
        "error": re.compile(
            r"(?:\d+\s+of\s+\d+\s+)?ERROR\s+creating\s+(?:sql\s+)?(?:table|view|incremental)\s+model\s+\w+\.(\w+)",
        ),
        # Matches both numbered and non-numbered skip patterns
        # "SKIP relation test_db.my_model" or "3 of 5 SKIP relation test_db.my_model"
        "skip": re.compile(
            r"(?:\d+\s+of\s+\d+\s+)?SKIP\s+relation\s+\w+\.(\w+)",
        ),
    }

    def parse_output(self, output: str) -> DbtExecutionResult:
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

            # Try to match patterns
            for pattern_name, pattern in self.PATTERNS.items():
                match = pattern.search(line)
                if match:
                    model_name = match.group(1)

                    if pattern_name == "success":
                        status = "OK"
                        error_msg = None
                    elif pattern_name == "error":
                        status = "ERROR"
                        error_msg = self._extract_error_message(line)
                    elif pattern_name == "skip":
                        status = "SKIP"
                        error_msg = None
                    else:
                        continue

                    all_results.append(
                        ModelResult(
                            name=model_name,
                            status=status,
                            error_message=error_msg,
                        ),
                    )
                    break

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

    def _extract_error_message(self, line: str) -> str | None:
        """Extract error message from a dbt error line.

        Args:
            line: Line containing the error.

        Returns:
            Extracted error message or None if not found.

        """
        # This is a simple implementation - could be enhanced based on dbt output format
        if "ERROR" in line:
            # Try to get everything after the model name
            parts = line.split("ERROR")
            if len(parts) > 1:
                return parts[1].strip()
        return None


# Global parser instance
dbt_output_parser = DbtOutputParser()
