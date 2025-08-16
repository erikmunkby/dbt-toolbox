"""Module for mcp server."""

import json
from dataclasses import asdict

from fastmcp import FastMCP

from dbt_toolbox.analysees.analyze_columns_references import analyze_column_references
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.utils import dict_utils

app = FastMCP("dbt-toolbox")


@app.tool()
def analyze_models() -> str:
    """Analyze and validate all models in the dbt project.

    This will analyze and make sure all model references, column references and CTE references
    are valid. Use this tool frequently in order to verify that no incorrect selections are made.

    If there are models with a large amount of errors, you can ask the user if they want the model
    to be ignored. This can be configured in the pyproject.toml settings via:

    [tool.dbt_toolbox]
    models_ignore_validation = ["my_model"]

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
        ]
    }
    """
    result = analyze_column_references(dbt_parser=dbtParser())
    return json.dumps(dict_utils.remove_empty_values(asdict(result)))


if __name__ == "__main__":
    app.run()
