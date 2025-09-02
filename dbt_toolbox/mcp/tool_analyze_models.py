"""MCP tool: Analyze models."""

from dataclasses import asdict

from dbt_toolbox.actions.analyze_columns_references import analyze_column_references
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

    Example output with descriptions:
    {
        "overall_status": "ISSUES_FOUND", # One of "OK" or "ISSUES_FOUND"
        "model_results": [ # List of all dbt models with issues
            {
                "model_name": "my_model", # Name of the dbt model
                "model_path": "/some/path/models/my_model.sql", # Path to the model
                "column_issues": [{ # All referenced columns not found
                    # The model or source the column was referenced from
                    "referenced_object": "other_model",
                    "missing_columns": ["my_column"] # The column that is missing
                }],
                "non_existent_references": ["my_table"], # A table that is not found
                "cte_issues": [{ # Issues found in CTE references
                    "cte_name": "my_cte", # The CTE in question
                    "missing_columns": ["my_column"] # Any columns not found within CTE
                }]
            }
        ],
        "warnings": [ # Any warnings encountered during analysis
            {
                "type": "unknown_jinja_macro", # Type of warning
                "message": "Unknown macro 'my_macro' encountered...", # Warning message
                "source": "jinja_handler" # Source of the warning
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

    return mcp_json_response(asdict(result))
