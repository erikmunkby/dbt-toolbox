"""Module for analyzing column references in models."""

from dataclasses import dataclass

from dbt_toolbox.data_models import Model, Seed, Source
from dbt_toolbox.dbt_parser._column_resolver import ColumnReference, TableType
from dbt_toolbox.settings import settings


@dataclass
class ColumnAnalysis:
    """Results of column reference analysis."""

    non_existent_columns: dict[str, dict[str, list[str]]]
    referenced_non_existent_models: dict[str, list[str]]
    cte_column_issues: dict[str, dict[str, list[str]]]  # model -> cte -> missing columns


def _analyze_model_column_references(
    model: Model,
    models: dict[str, Model],
    sources: dict[str, Source],
    seeds: dict[str, Seed],
) -> tuple[dict[str, list[str]], list[str], dict[str, list[str]]]:
    """Analyze column references for a single model.

    Args:
        model: Model to analyze
        models: Dictionary of model name to Model objects
        sources: Dictionary of source full_name to Source objects
        seeds: Dictionary of seed name to Seed objects

    Returns:
        Tuple of (non_existent_columns, non_existent_references, cte_issues)

    """
    model_non_existent_cols = {}
    model_non_existent_refs = []
    model_cte_issues = {}

    if model.column_references is None or len(model.column_references) == 0:
        # Column resolver failed or returned empty results (e.g., due to SELECT *)
        # For now, skip analysis for these models
        # TODO: Enhance column resolver to handle SELECT * and complex CTE chains
        return model_non_existent_cols, model_non_existent_refs, model_cte_issues

    for col_ref in model.column_references:
        # Only analyze references that have a table
        if col_ref.table is None:
            continue

        referenced_model = col_ref.table

        # Handle CTE references
        if col_ref.reference_type == TableType.CTE:
            available_objects = {"models": models, "sources": sources, "seeds": seeds}
            handled_as_cte = _handle_cte_reference(
                col_ref, referenced_model, model_cte_issues, available_objects
            )
            if handled_as_cte:
                continue
            # If not handled as CTE, fall through to external reference handling

        # Handle external references (models, sources, seeds)
        available_objects = {"models": models, "sources": sources, "seeds": seeds}
        _handle_external_reference(
            col_ref,
            referenced_model,
            available_objects,
            model_non_existent_cols,
            model_non_existent_refs,
        )

    return model_non_existent_cols, model_non_existent_refs, model_cte_issues


def _handle_cte_reference(
    col_ref: ColumnReference,
    referenced_model: str,
    model_cte_issues: dict[str, list[str]],
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
        if referenced_model not in model_cte_issues:
            model_cte_issues[referenced_model] = []
        if col_ref.name not in model_cte_issues[referenced_model]:
            model_cte_issues[referenced_model].append(col_ref.name)

    return True  # Handled as CTE issue


def _handle_external_reference(
    col_ref: ColumnReference,
    referenced_model: str,
    available_objects: dict,
    model_non_existent_cols: dict[str, list[str]],
    model_non_existent_refs: list[str],
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
        if referenced_model not in model_non_existent_refs:
            model_non_existent_refs.append(referenced_model)
        return

    # Check if column exists in referenced model
    column_exists = _check_column_exists(col_ref.name, referenced_model, models, sources, seeds)

    if not column_exists:
        if referenced_model not in model_non_existent_cols:
            model_non_existent_cols[referenced_model] = []
        if col_ref.name not in model_non_existent_cols[referenced_model]:
            model_non_existent_cols[referenced_model].append(col_ref.name)


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
    models: dict[str, Model],
    sources: dict[str, Source],
    seeds: dict[str, Seed],
) -> ColumnAnalysis:
    """Analyze all models and find columns that don't exist in their referenced objects.

    Args:
        models: Dictionary of model name to Model objects
        sources: Dictionary of source full_name to Source objects
        seeds: Dictionary of seed name to Seed objects

    Returns:
        ColumnAnalysis containing:
        - non_existent_columns: {model_name: {referenced_model: [missing_columns]}}
        - referenced_non_existent_models: {model_name: [non_existent_model_names]}
        - cte_column_issues: {model_name: {cte_name: [missing_columns]}}

    """
    non_existent_columns = {}
    referenced_non_existent_models = {}
    cte_column_issues = {}

    for model_name, model in models.items():
        # Skip validation for models in the ignore list
        if model_name in settings.models_ignore_validation:
            continue

        (model_non_existent_cols, model_non_existent_refs, model_cte_issues) = (
            _analyze_model_column_references(
                model,
                models,
                sources,
                seeds,
            )
        )

        if model_non_existent_cols:
            non_existent_columns[model_name] = model_non_existent_cols

        if model_non_existent_refs:
            referenced_non_existent_models[model_name] = model_non_existent_refs

        if model_cte_issues:
            cte_column_issues[model_name] = model_cte_issues

    return ColumnAnalysis(
        non_existent_columns=non_existent_columns,
        referenced_non_existent_models=referenced_non_existent_models,
        cte_column_issues=cte_column_issues,
    )
