"""Module for analyzing column references in models."""

from dataclasses import dataclass
from typing import Literal

from dbt_toolbox.data_models import Model, Seed, Source
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.dbt_parser._column_resolver import ColumnReference, TableType
from dbt_toolbox.settings import settings


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


def _analyze_model_column_references(
    model: Model,
    dbt_parser: dbtParser,
) -> ModelAnalysisResult:
    """Analyze column references for a single model.

    Args:
        model: Model to analyze
        dbt_parser: The dbt parser.

    Returns:
        ModelAnalysisResult containing the analysis results

    """
    column_issues = []
    non_existent_references = []
    cte_issues = []

    if model.column_references is None or len(model.column_references) == 0:
        # Column resolver failed or returned empty results (e.g., due to SELECT *)
        # For now, skip analysis for these models
        # TODO: Enhance column resolver to handle SELECT * and complex CTE chains
        return ModelAnalysisResult(
            model_name=model.name,
            model_path=str(model.path),
            column_issues=column_issues,
            non_existant_model_references=non_existent_references,
            cte_issues=cte_issues,
        )

    available_objects = {
        "models": dbt_parser.models,
        "sources": dbt_parser.sources,
        "seeds": dbt_parser.seeds,
    }

    for col_ref in model.column_references:
        # Only analyze references that have a table
        if col_ref.table is None:
            continue

        referenced_model = col_ref.table

        # Handle CTE references
        if col_ref.reference_type == TableType.CTE:
            handled_as_cte = _handle_cte_reference(
                col_ref, referenced_model, cte_issues, available_objects
            )
            if handled_as_cte:
                continue
            # If not handled as CTE, fall through to external reference handling

        # Handle external references (models, sources, seeds)
        _handle_external_reference(
            col_ref,
            referenced_model,
            available_objects,
            column_issues,
            non_existent_references,
        )

    return ModelAnalysisResult(
        model_name=model.name,
        model_path=str(model.path),
        column_issues=column_issues,
        non_existant_model_references=non_existent_references,
        cte_issues=cte_issues,
    )


def _handle_cte_reference(
    col_ref: ColumnReference,
    referenced_model: str,
    cte_issues: list[CTEIssue],
    available_objects: dict,
) -> bool:
    """Handle CTE column reference validation.

    Returns:
        True if handled as CTE issue, False if should be handled as external reference

    """
    if col_ref.resolved is False:
        # Check if this might be a SELECT * CTE that should validate externally
        models = available_objects["models"]
        sources = available_objects["sources"]
        seeds = available_objects["seeds"]

        # If the referenced_model exists as an external model/source/seed,
        # then this might be a SELECT * CTE that should be validated externally
        if referenced_model in models or referenced_model in sources or referenced_model in seeds:
            return False  # Let it be handled as external reference

        # Otherwise, this is a genuine CTE column issue
        existing_cte_issue = next(
            (issue for issue in cte_issues if issue.cte_name == referenced_model), None
        )
        if existing_cte_issue is None:
            cte_issues.append(CTEIssue(cte_name=referenced_model, missing_columns=[col_ref.name]))
        elif col_ref.name not in existing_cte_issue.missing_columns:
            existing_cte_issue.missing_columns.append(col_ref.name)

    return True  # Handled as CTE issue


def _handle_external_reference(
    col_ref: ColumnReference,
    referenced_model: str,
    available_objects: dict,
    column_issues: list[ColumnIssue],
    non_existent_references: list[str],
) -> None:
    """Handle external model/source/seed reference validation."""
    models = available_objects["models"]
    sources = available_objects["sources"]
    seeds = available_objects["seeds"]

    # Check if referenced model exists
    if (
        referenced_model not in models
        and referenced_model not in sources
        and referenced_model not in seeds
    ):
        if referenced_model not in non_existent_references:
            non_existent_references.append(referenced_model)
        return

    # Check if column exists in referenced model
    column_exists = _check_column_exists(col_ref.name, referenced_model, models, sources, seeds)

    if not column_exists:
        existing_column_issue = next(
            (issue for issue in column_issues if issue.referenced_table == referenced_model), None
        )
        if existing_column_issue is None:
            column_issues.append(
                ColumnIssue(referenced_table=referenced_model, missing_columns=[col_ref.name])
            )
        elif col_ref.name not in existing_column_issue.missing_columns:
            existing_column_issue.missing_columns.append(col_ref.name)


def _check_column_exists(
    column_name: str,
    referenced_model: str,
    models: dict[str, Model],
    sources: dict[str, Source],
    seeds: dict[str, Seed],
) -> bool:
    """Check if a column exists in the referenced model/source/seed."""
    if referenced_model in seeds:
        # For seeds, we can't validate columns since we don't parse CSV headers
        return True
    if referenced_model in models:
        return column_name in models[referenced_model].final_columns
    if referenced_model in sources:
        return column_name in sources[referenced_model].compiled_columns
    return False


def analyze_column_references(
    dbt_parser: dbtParser, target_models: list[Model] | None = None
) -> ColumnAnalysis:
    """Analyze all models and find columns that don't exist in their referenced objects.

    Args:
        dbt_parser: The dbt parser.
        target_models:    A list of models to target for analysis.

    Returns:
        ColumnAnalysis containing model analysis results

    """
    model_results = []
    if target_models is None:
        target_models = list(dbt_parser.models.values())

    for model in target_models:
        # Skip validation for models in the ignore list
        if model.name in settings.models_ignore_validation:
            continue

        model_result = _analyze_model_column_references(model=model, dbt_parser=dbt_parser)

        # Only add results that have issues
        if (
            model_result.column_issues
            or model_result.non_existant_model_references
            or model_result.cte_issues
        ):
            model_results.append(model_result)

    return ColumnAnalysis(
        model_results=model_results,
        overall_status="OK" if len(model_results) == 0 else "ISSUES_FOUND",
    )
