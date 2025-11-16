"""MCP tool build models."""

from dataclasses import asdict

from dbt_toolbox.actions.dbt_executor import create_execution_plan
from dbt_toolbox.data_models import DbtExecutionParams
from dbt_toolbox.mcp._utils import mcp_json_response


def build_models(
    model: str | None = None,
    full_refresh: bool = False,
    vars: str | None = None,  # noqa: A002
    target: str | None = None,
    force: bool = False,
) -> str:
    """Build dbt models with validation and intelligent cache-based execution.

    This command provides the same functionality as 'dbt build' - it validates
    lineage references, analyzes which models need execution based on cache validity
    and dependency changes, and only runs those models that actually need updating.

    Args:
        model: Select models to build (same as dbt --select/--model)
        full_refresh: Incremental models only: Will rebuild an incremental model
        vars: Supply variables to the project (YAML string)
        target: Specify dbt target environment
        force: Skip validation and cache analysis, run all selected models

    Features:
        • Validation: Validates column and model references before execution
        • Cache Analysis: Only rebuilds models with outdated cache or dependency changes
        • Optimized Selection: Automatically filters to models that need execution

    Returns:
        JSON string with execution results, model status information, and any warnings.

    Examples:
        build_models()                               # Validate and run models that need updating
        build_models(model="customers")              # Only run customers if needed
        build_models(model="customers", force=True)  # Force run (skip validation/cache)
        build_models(target="prod")                  # Run with target option

    Instructions:
        -   When applicable try to run e.g. "+my_model+" in order to apply changes
            both up and downstream.
        -   After tool use, if status=success, highlight nbr models skipped and time saved.

    """
    # Create parameters object
    params = DbtExecutionParams(
        command_name="build",
        model_selection=model,
        full_refresh=full_refresh,
        vars=vars,
        target=target,
        force=force,
    )

    try:
        # Execute using the existing CLI infrastructure
        plan = create_execution_plan(params)

        # Check if lineage validation failed
        if not plan.lineage_valid:
            return mcp_json_response(
                {
                    "status": "validation_failed",
                    "message": (
                        "Validation failed. Column or model references are invalid. "
                        "Run analyze_models() to see detailed validation errors, or use "
                        "force=True to skip validation."
                    ),
                    "models_analyzed": [a.model.name for a in plan.analyses],
                    "validation_passed": False,
                }
            )

        result = plan.run()
        output = {
            "status": "success" if result.return_code == 0 else "error",
            "models_executed": plan.models_to_execute,
            "models_skipped": [m.name for m in plan.models_to_skip],
            "nbr_models_skipped": len(plan.models_to_skip),
            "seconds_saved_by_skipping_models": plan.compute_time_saved_seconds,
            **asdict(result.parsed_logs),
        }
        if result.return_code != 0:
            output["dbt_logs"] = result.raw_logs

        return mcp_json_response(output)
    except Exception as e:  # noqa: BLE001
        # Include warnings even in error cases
        error_output = {"status": "error", "message": f"Build failed: {e!s}"}
        return mcp_json_response(error_output)
