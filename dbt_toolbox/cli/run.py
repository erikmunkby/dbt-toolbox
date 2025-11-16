"""Run command that shadows dbt run with custom behavior."""

from dbt_toolbox.cli._build_run_command_factory import create_dbt_command_function

# Create the run command using the shared function factory
run = create_dbt_command_function(
    command_name="run",
    help_text="""Run dbt models with validation and intelligent cache-based execution.

This command shadows 'dbt run' - it validates lineage references, analyzes
which models need execution based on cache validity and dependency changes,
and only runs those models that actually need updating.

Features:
    • Validation:          Validates column and model references before execution
    • Cache Analysis:      Only rebuilds models with outdated cache or dependency changes
    • Optimized Selection: Automatically filters to models that need execution

Options:
    --force:    Skip validation and cache analysis, run all selected models

Usage:
    dt run [OPTIONS]                     # Validate and run only models that need updating
    dt run --model customers             # Only run customers if needed
    dt run --force --model customers     # Force run customers (skip validation/cache)
    dt run --threads 4 --target prod     # Run with target option
""",
)
