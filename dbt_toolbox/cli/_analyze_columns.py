"""Module for analyzing column references in models."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_toolbox.data_models import Model, Seed, Source


@dataclass
class ColumnAnalysis:
    """Results of column reference analysis."""

    non_existent_columns: dict[str, dict[str, list[str]]]
    referenced_non_existent_models: dict[str, list[str]]


def _check_column_in_reference(
    column_name: str,
    referenced_model: str,
    models: dict[str, "Model"],
    sources: dict[str, "Source"],
    seeds: dict[str, "Seed"],
) -> bool:
    """Check if a column exists in the referenced model, source, or seed.

    Args:
        column_name: Name of the column to check
        referenced_model: Name of the referenced model, source, or seed
        models: Dictionary of model name to Model objects
        sources: Dictionary of source full_name to Source objects
        seeds: Dictionary of seed name to Seed objects

    Returns:
        True if column exists in referenced model/source or if referenced model is a seed,
        False otherwise

    """
    if referenced_model in seeds:
        # For seeds, we can't validate columns since we don't parse CSV headers
        # We just assume the reference is valid if the seed exists
        return True
    if referenced_model in models:
        return column_name in models[referenced_model].compiled_columns
    if referenced_model in sources:
        return column_name in sources[referenced_model].compiled_columns
    return False


def _analyze_model_column_references(
    model: "Model",
    models: dict[str, "Model"],
    sources: dict[str, "Source"],
    seeds: dict[str, "Seed"],
) -> tuple[dict[str, list[str]], list[str]]:
    """Analyze column references for a single model.

    Args:
        model: Model to analyze
        models: Dictionary of model name to Model objects
        sources: Dictionary of source full_name to Source objects
        seeds: Dictionary of seed name to Seed objects

    Returns:
        Tuple of (non_existent_columns, non_existent_references)

    """
    model_non_existent_cols = {}
    model_non_existent_refs = []

    for column_name, referenced_model in model.selected_columns.items():
        if referenced_model is None:
            continue

        if (
            referenced_model not in models
            and referenced_model not in sources
            and referenced_model not in seeds
        ):
            model_non_existent_refs.append(referenced_model)
        elif not _check_column_in_reference(column_name, referenced_model, models, sources, seeds):
            if referenced_model not in model_non_existent_cols:
                model_non_existent_cols[referenced_model] = []
            model_non_existent_cols[referenced_model].append(column_name)

    return model_non_existent_cols, model_non_existent_refs


def analyze_column_references(
    models: dict[str, "Model"],
    sources: dict[str, "Source"],
    seeds: dict[str, "Seed"],
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

    """
    non_existent_columns = {}
    referenced_non_existent_models = {}

    for model_name, model in models.items():
        model_non_existent_cols, model_non_existent_refs = _analyze_model_column_references(
            model,
            models,
            sources,
            seeds,
        )

        if model_non_existent_cols:
            non_existent_columns[model_name] = model_non_existent_cols

        if model_non_existent_refs:
            referenced_non_existent_models[model_name] = model_non_existent_refs

    return ColumnAnalysis(
        non_existent_columns=non_existent_columns,
        referenced_non_existent_models=referenced_non_existent_models,
    )
