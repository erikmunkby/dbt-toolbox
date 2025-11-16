"""Build command that shadows dbt build with custom behavior."""

from dbt_toolbox.cli._build_run_command_factory import create_dbt_command_function

# Create the build command using the shared function factory
build = create_dbt_command_function(
    command_name="build",
    help_text="""Build dbt models with validation and intelligent cache-based execution.

This command shadows 'dbt build' - it validates lineage references, analyzes
which models need execution based on cache validity and dependency changes,
and only runs those models that actually need updating.

Features:
    • Validation:          Validates column and model references before execution
    • Cache Analysis:      Only rebuilds models with outdated cache or dependency changes
    • Optimized Selection: Automatically filters to models that need execution

Options:
    --force:    Skip validation and cache analysis, run all selected models

Usage:
    dt build [OPTIONS]                    # Validate and run only models that need updating
    dt build --model customers            # Only run customers if needed
    dt build --force --model customers    # Force run customers (skip validation/cache)
    dt build --threads 4 --target prod    # Run with target option
""",
)
