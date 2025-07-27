"""Module for analyzing column references in models."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_toolbox.data_models import Model


@dataclass
class ColumnAnalysis:
    """Results of column reference analysis."""

    non_existent_columns: dict[str, dict[str, list[str]]]
    referenced_non_existent_models: dict[str, list[str]]


def analyze_column_references(models: dict[str, "Model"]) -> ColumnAnalysis:
    """Analyze all models and find columns that don't exist in their referenced models.

    Args:
        models: Dictionary of model name to Model objects

    Returns:
        ColumnAnalysis containing:
        - non_existent_columns: {model_name: {referenced_model: [missing_columns]}}
        - referenced_non_existent_models: {model_name: [non_existent_model_names]}

    """
    non_existent_columns = {}
    referenced_non_existent_models = {}

    for model_name, model in models.items():
        model_non_existent_cols = {}
        model_non_existent_refs = []

        for column_name, referenced_model in model.selected_columns.items():
            if referenced_model is None:
                continue

            if referenced_model not in models:
                model_non_existent_refs.append(referenced_model)
            else:
                referenced_model_obj = models[referenced_model]
                if column_name not in referenced_model_obj.compiled_columns:
                    if referenced_model not in model_non_existent_cols:
                        model_non_existent_cols[referenced_model] = []
                    model_non_existent_cols[referenced_model].append(column_name)

        if model_non_existent_cols:
            non_existent_columns[model_name] = model_non_existent_cols

        if model_non_existent_refs:
            referenced_non_existent_models[model_name] = model_non_existent_refs

    return ColumnAnalysis(
        non_existent_columns=non_existent_columns,
        referenced_non_existent_models=referenced_non_existent_models,
    )
