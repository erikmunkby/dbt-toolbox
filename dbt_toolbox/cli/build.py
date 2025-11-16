"""Build command that shadows dbt build with custom behavior."""

from dbt_toolbox.cli._build_run_command_factory import create_dbt_command_function

# Create the build command using the shared function factory
build = create_dbt_command_function(
    command_name="build",
    help_text=(
        "Build dbt models with validation and intelligent cache-based execution. "
        "Use the positional MODELS argument or --select/-s flag to specify models.\n\n"
        "Example: dt build +my_model -t prod --full-refresh"
    ),
)
