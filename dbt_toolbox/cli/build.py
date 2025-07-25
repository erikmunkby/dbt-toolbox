"""Build command that shadows dbt build with custom behavior."""

from dbt_toolbox.cli._dbt_executor import create_dbt_command_function

# Create the build command using the shared function factory
build = create_dbt_command_function(
    command_name="build",
    help_text="""Build dbt models with intelligent cache-based execution.

This command shadows 'dbt build' with smart execution by default - it analyzes
which models need execution based on cache validity and dependency changes,
and only runs those models that actually need updating.

Intelligent Execution Features:
    --analyze:          Show which models need execution without running dbt
    --disable-smart:    Disable smart execution and run
                        all selected models (original dbt behavior)

Usage:
    dt build [OPTIONS]                    # Smart execution (default)
    dt build --model customers           # Only run customers if needed
    dt build --select customers+ --analyze  # Show what would be executed
    dt build --disable-smart --model customers  # Force run customers (bypass cache)
    dt build --threads 4 --target prod   # Smart execution with target option
""",
)
