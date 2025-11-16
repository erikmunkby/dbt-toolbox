"""Run command that shadows dbt run with custom behavior."""

from dbt_toolbox.cli._build_run_command_factory import create_dbt_command_function

# Create the run command using the shared function factory
run = create_dbt_command_function(
    command_name="run",
    help_text=(
        "Run dbt models with validation and intelligent cache-based execution. "
        "Use the positional MODELS argument or --select/-s flag to specify models.\n\n"
        "Example: dt run +my_model -t prod --full-refresh"
    ),
)
