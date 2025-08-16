"""Column documentation testing utilities."""

from typing import TypedDict

from dbt_toolbox.dbt_parser import dbtParser


class ColumnDocumentationResult(TypedDict):
    """Result structure for column documentation validation."""

    missing_descriptions: list[str]
    superfluous_descriptions: list[str]


def check_column_documentation() -> dict[str, ColumnDocumentationResult]:
    """Check column documentation coverage across all models.

    Returns a dictionary mapping model names to their documentation issues.
    Models that are sufficiently documented (no missing or superfluous
    column descriptions) are omitted from the results.

    Returns:
        Dictionary where keys are model names and values contain:
        - missing_descriptions: List of column names missing descriptions
        - superfluous_descriptions: List of documented columns not in model

    """
    results = {}

    for model_name, model in dbtParser().models.items():
        # Only include models that have documentation issues
        if model.columns_missing_description or model.superfluent_column_descriptions:
            results[model_name] = ColumnDocumentationResult(
                missing_descriptions=model.columns_missing_description,
                superfluous_descriptions=model.superfluent_column_descriptions,
            )

    return results
