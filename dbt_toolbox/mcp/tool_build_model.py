"""MCP tool build models."""

from dataclasses import asdict

from dbt_toolbox.actions.dbt_executor import create_execution_plan
from dbt_toolbox.data_models import DbtExecutionParams
from dbt_toolbox.mcp._utils import mcp_json_response


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
        JSON string with execution results, model status information, and any warnings.

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
