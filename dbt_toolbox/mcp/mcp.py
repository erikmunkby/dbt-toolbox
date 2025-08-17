"""Module for mcp server."""

import json
from dataclasses import asdict

from fastmcp import FastMCP

from dbt_toolbox.analysees.analyze_columns_references import analyze_column_references
from dbt_toolbox.analysees.dbt_executor import create_execution_plan
from dbt_toolbox.data_models import DbtExecutionParams
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.utils import dict_utils

mcp_server = FastMCP("dbt-toolbox")


@mcp_server.tool()
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


@mcp_server.tool()
def build_models(  # noqa: PLR0913
    model: str | None = None,
    full_refresh: bool = False,
    threads: int | None = None,
    vars: str | None = None,  # noqa: A002
    target: str | None = None,
    analyze_only: bool = False,
    disable_smart: bool = False,
) -> str:
    """Build dbt models with intelligent cache-based execution.

    This command provides the same functionality as 'dbt build' with smart execution
    by default - it analyzes which models need execution based on cache validity
    and dependency changes, validates lineage references, and only runs those models
    that actually need updating.

    Args:
        model: Select models to build (same as dbt --select/--model)
        full_refresh: Incremental models only: Will rebuild an incremental model
        threads: Number of threads to use
        vars: Supply variables to the project (YAML string)
        target: Specify dbt target environment
        analyze_only: Only analyze which models need execution, don't run dbt
        disable_smart: Disable the intelligent caching and force a rebuild

    Smart Execution Features:
        • Cache Analysis: Only rebuilds models with outdated cache or dependency changes
        • Lineage Validation: Validates column and model references before execution
        • Optimized Selection: Automatically filters to models that need execution

    Returns:
        JSON string with execution results and model status information.

    Examples:
        build_models()                               # Smart execution (default)
        build_models(model="customers")              # Only run customers if needed
        build_models(model="customers+", analyze_only=True)  # Show what would be executed
        build_models(model="customers", disable_smart=True)  # Force run customers
        build_models(threads=4, target="prod")       # Smart execution with options

    Instructions:
        -   When applicable try to run e.g. "+my_model+" in order to apply changes
            both up and downstream.
        -   After tool use, if status=success, highlight nbr models skipped and time saved.

    """
    # Create parameters object
    params = DbtExecutionParams(
        command_name="build",
        model=model,
        full_refresh=full_refresh,
        threads=threads,
        vars=vars,
        target=target,
        analyze_only=analyze_only,
        disable_smart=disable_smart,
    )

    try:
        # Execute using the existing CLI infrastructure
        plan = create_execution_plan(params)
        result = plan.run()
        return json.dumps(
            {
                "status": "success" if result.return_code == 0 else "error",
                "models_executed": plan.models_to_execute,
                "models_skipped": [m.name for m in plan.models_to_skip],
                "nbr_models_skipped": len(plan.models_to_skip),
                "seconds_saved_by_skipping_models": plan.compute_time_saved_seconds,
                **dict_utils.remove_empty_values(asdict(result.logs)),
            }
        )
    except Exception as e:  # noqa: BLE001
        return json.dumps({"status": "error", "message": f"Build failed: {e!s}"})
