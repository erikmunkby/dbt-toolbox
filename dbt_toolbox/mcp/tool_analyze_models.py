"""MCP tool: Analyze models."""

from dataclasses import asdict

from dbt_toolbox.actions.analyze_columns_references import (
    ColumnAnalysis,
    analyze_column_references,
)
from dbt_toolbox.dbt_parser._dbt_parser import dbtParser
from dbt_toolbox.mcp._utils import mcp_json_response


def analyze_models(target: str | None = None, model: str | None = None) -> str:
    """Analyze and validate all models in the dbt project.

    This will analyze and make sure all model references, column references and CTE references
    are valid. Use this tool frequently in order to verify that no incorrect selections are made.

    If there are models with a large amount of errors, you can ask the user if they want the model
    to be ignored. This can be configured in the pyproject.toml settings via:

    [tool.dbt_toolbox]
    models_ignore_validation = ["my_model"]

    Args:
        target: Specify dbt target environment
        model: Select models to analyze (same as dbt --select/--model)

    Returns:
        JSON string with enhanced AI-readable analysis results including:
        - Clear summary statistics
        - Structured issue descriptions
        - Actionable recommendations
        - Consistent field naming

    Example output structure:
    {
        "status": "SUCCESS" | "HAS_ISSUES",
        "summary": {
            "total_models_analyzed": 15,
            "models_with_issues": 3,
            "total_issues_found": 8,
            "issue_breakdown": {
                "missing_columns": 5,
                "invalid_model_references": 2,
                "cte_issues": 1
            }
        },
        "models_with_issues": [
            {
                "model_name": "customer_orders",
                "model_path": "models/marts/customer_orders.sql",
                "issues": [
                    {
                        "type": "missing_columns",
                        "description": "Referenced columns not found in source table",
                        "details": {
                            "source_table": "raw_customers",
                            "missing_columns": ["customer_tier", "signup_date"]
                        },
                        "recommendation": "Check if columns exist in raw_customers or update refs"
                    },
                    {
                        "type": "invalid_model_reference",
                        "description": "Model reference points to non-existent model",
                        "details": {
                            "invalid_references": ["staging_products_v2"]
                        },
                        "recommendation": "Verify model name exists or update reference"
                    },
                    {
                        "type": "cte_column_issue",
                        "description": "CTE references columns that don't exist",
                        "details": {
                            "cte_name": "customer_metrics",
                            "missing_columns": ["avg_order_value"]
                        },
                        "recommendation": "Ensure CTE selects required columns or update refs"
                    }
                ]
            }
        ],
        "warnings": [
            {
                "type": "unknown_jinja_macro",
                "message": "Unknown macro 'custom_macro' encountered during parsing",
                "source": "jinja_handler",
                "recommendation": "Define the macro or check if it's available in your dbt project"
            }
        ]
    }

    """
    dbt_parser = dbtParser(target=target)

    # Filter models if selection is provided
    if model:
        target_model_names = dbt_parser.parse_selection_query(model)
        target_models = [m for m in dbt_parser.models.values() if m.name in target_model_names]
    else:
        target_models = None

    result = analyze_column_references(dbt_parser=dbt_parser, target_models=target_models)

    # Transform the raw result into a more AI-friendly format
    transformed_result = _transform_analysis_result_for_ai(
        result, target_models or list(dbt_parser.models.values())
    )

    return mcp_json_response(transformed_result)


def _transform_analysis_result_for_ai(result: ColumnAnalysis, analyzed_models: list) -> dict:
    """Transform the raw analysis result into a more AI-readable format.

    Args:
        result: ColumnAnalysis result from analyze_column_references
        analyzed_models: List of models that were analyzed

    Returns:
        Transformed result with enhanced structure and metadata

    """
    # Convert dataclass to dict for easier processing
    result_dict = asdict(result)

    # Calculate summary statistics
    total_models_analyzed = len(analyzed_models)
    models_with_issues = len(result_dict.get("model_results", []))

    # Count different types of issues
    missing_columns_count = 0
    invalid_model_references_count = 0
    cte_issues_count = 0

    for model_result in result_dict.get("model_results", []):
        missing_columns_count += len(model_result.get("column_issues", []))
        if model_result.get("non_existant_model_references"):
            invalid_model_references_count += len(
                model_result.get("non_existant_model_references", [])
            )
        cte_issues_count += len(model_result.get("cte_issues", []))

    total_issues = missing_columns_count + invalid_model_references_count + cte_issues_count

    # Transform model results to be more descriptive
    transformed_models = []
    for model_result in result_dict.get("model_results", []):
        issues = []

        # Transform column issues
        for column_issue in model_result.get("column_issues", []):
            issues.append(  # noqa: PERF401
                {
                    "type": "missing_columns",
                    "description": "Referenced columns not found in source table/model",
                    "details": {
                        "source_table": column_issue.get("referenced_table", "unknown"),
                        "missing_columns": column_issue.get("missing_columns", []),
                    },
                    "recommendation": (
                        f"Check if columns exist in "
                        f"'{column_issue.get('referenced_table', 'unknown')}' or update references"
                    ),
                }
            )

        # Transform model reference issues
        if model_result.get("non_existant_model_references"):
            issues.append(
                {
                    "type": "invalid_model_reference",
                    "description": "Model references point to non-existent models/sources",
                    "details": {
                        "invalid_references": model_result.get("non_existant_model_references", [])
                    },
                    "recommendation": (
                        "Verify these model/source names exist in your dbt project "
                        "or update references"
                    ),
                }
            )

        # Transform CTE issues
        for cte_issue in model_result.get("cte_issues", []):
            issues.append(  # noqa: PERF401
                {
                    "type": "cte_column_issue",
                    "description": "CTE references columns that don't exist in the CTE definition",
                    "details": {
                        "cte_name": cte_issue.get("cte_name", "unknown"),
                        "missing_columns": cte_issue.get("missing_columns", []),
                    },
                    "recommendation": (
                        f"Ensure CTE '{cte_issue.get('cte_name', 'unknown')}' selects required "
                        "columns or update downstream references"
                    ),
                }
            )

        if issues:  # Only add models that have issues
            transformed_models.append(
                {
                    "model_name": model_result.get("model_name", "unknown"),
                    "model_path": model_result.get("model_path", "unknown"),
                    "issue_count": len(issues),
                    "issues": issues,
                }
            )

    # Create the final transformed result
    return {
        "status": "SUCCESS" if total_issues == 0 else "HAS_ISSUES",
        "summary": {
            "total_models_analyzed": total_models_analyzed,
            "models_with_issues": models_with_issues,
            "total_issues_found": total_issues,
            "issue_breakdown": {
                "missing_columns": missing_columns_count,
                "invalid_model_references": invalid_model_references_count,
                "cte_issues": cte_issues_count,
            },
        },
        "models_with_issues": transformed_models,  # Always include, even if empty list
        "analysis_complete": True,
    }
