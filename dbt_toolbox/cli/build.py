"""Build command that shadows dbt build with custom behavior."""

from dbt_toolbox.cli._build_run_command_factory import create_dbt_command_function

# Create the build command using the shared function factory
build = create_dbt_command_function(
    command_name="build",
    help_text="""Build dbt models with intelligent cache-based execution.

This command shadows 'dbt build' with smart execution by default - it analyzes
which models need execution based on cache validity and dependency changes,
validates lineage references, and only runs those models that actually need updating.

Smart Execution Features:
    • Cache Analysis:      Only rebuilds models with outdated cache or dependency changes
    • Lineage Validation:  Validates column and model references before execution
    • Optimized Selection: Automatically filters to models that need execution

Options:
    --analyze:          Show which models need execution without running dbt
    --disable-smart:    Disable smart execution, lineage validation, and run
                        all selected models (original dbt behavior)

Usage:
    dt build [OPTIONS]                    # Smart execution (default)
    dt build --model customers           # Only run customers if needed
    dt build --select customers+ --analyze  # Show what would be executed
    dt build --disable-smart --model customers  # Force run customers (bypass all smart features)
    dt build --threads 4 --target prod   # Smart execution with target option
""",
)
