"""Data models for dbt-toolbox analysis results.

This module contains all dataclasses and enums used across the analysis system.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from dbt_toolbox.data_models import Model


class ExecutionReason(Enum):
    """Enum defining reasons why a model needs execution."""

    NEVER_BUILT = "never_built"
    UPSTREAM_MODEL_CHANGED = "upstream_model_changed"
    UPSTREAM_MACRO_CHANGED = "upstream_macro_changed"
    OUTDATED_MODEL = "outdated_model"
    LAST_EXECUTION_FAILED = "last_execution_failed"
    CODE_CHANGED = "code_changed"


@dataclass
class AnalysisResult:
    """Results of the analysis."""

    model: Model
    needs_execution: bool = True
    reason: ExecutionReason | None = None

    @property
    def reason_description(self) -> str:
        """Return a human-readable description of the execution reason."""
        return {
            ExecutionReason.NEVER_BUILT: "Model has never been built.",
            ExecutionReason.CODE_CHANGED: "Model code changed.",
            ExecutionReason.UPSTREAM_MACRO_CHANGED: "Upstream macro changed.",
            ExecutionReason.UPSTREAM_MODEL_CHANGED: "Upstream model changed.",
            ExecutionReason.OUTDATED_MODEL: "Model build is outdated.",
            ExecutionReason.LAST_EXECUTION_FAILED: "Last model execution failed.",
            None: "",
        }[self.reason]


@dataclass
class ColumnIssue:
    """A specific column issue for a referenced object."""

    referenced_table: str
    missing_columns: list[str]


@dataclass
class CTEIssue:
    """A CTE-specific column issue."""

    cte_name: str
    missing_columns: list[str]


@dataclass
class ModelAnalysisResult:
    """Results of column reference analysis for a single model."""

    model_name: str
    model_path: str
    column_issues: list[ColumnIssue]
    non_existant_model_references: list[str]
    cte_issues: list[CTEIssue]


@dataclass
class ColumnAnalysis:
    """Results of column reference analysis for all models."""

    overall_status: Literal["OK", "ISSUES_FOUND"]
    model_results: list[ModelAnalysisResult]

    @property
    def non_existent_columns(self) -> dict[str, dict[str, list[str]]]:
        """Legacy property for backward compatibility."""
        result = {}
        for model_result in self.model_results:
            if model_result.column_issues:
                result[model_result.model_name] = {
                    issue.referenced_table: issue.missing_columns
                    for issue in model_result.column_issues
                }
        return result

    @property
    def referenced_non_existent_models(self) -> dict[str, list[str]]:
        """Legacy property for backward compatibility."""
        result = {}
        for model_result in self.model_results:
            if model_result.non_existant_model_references:
                result[model_result.model_name] = model_result.non_existant_model_references
        return result

    @property
    def cte_column_issues(self) -> dict[str, dict[str, list[str]]]:
        """Legacy property for backward compatibility."""
        result = {}
        for model_result in self.model_results:
            if model_result.cte_issues:
                result[model_result.model_name] = {
                    issue.cte_name: issue.missing_columns for issue in model_result.cte_issues
                }
        return result


@dataclass
class DuplicateDocsIssue:
    """A duplicate docs macro issue."""

    macro_name: str
    occurrences: int
    file_paths: list[str]


@dataclass
class DocsAnalysis:
    """Results of docs macro analysis."""

    overall_status: Literal["OK", "ISSUES_FOUND"]
    duplicate_issues: list[DuplicateDocsIssue]
    total_docs_macros: int
    unique_docs_macros: int


@dataclass
class AnalysisResults:
    """Consolidated results from all analysis types."""

    model_analysis: list[AnalysisResult]
    column_analysis: ColumnAnalysis
    docs_analysis: DocsAnalysis
