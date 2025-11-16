"""Documentation command for yaml."""

import subprocess
from typing import Annotated

import typer

from dbt_toolbox.actions.build_docs import DocsResult, YamlBuilder
from dbt_toolbox.cli._common_options import (
    ArgumentModelSelection,
    OptionModelSelection,
    OptionTarget,
)
from dbt_toolbox.cli._exit_handler import exit_run
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.utils import _printers


def _handle_clipboard_mode(result: DocsResult) -> None:
    """Handle clipboard mode output and errors."""
    if not result.success:
        error_msg = f"Failed to generate YAML for model {result.model_name}"
        if result.error_message:
            error_msg += f": {result.error_message}"
        exit_run(1, error_msg)

    if result.yaml_content:
        process = subprocess.Popen(args="pbcopy", stdin=subprocess.PIPE)
        process.communicate(input=result.yaml_content.encode())
        _printers.cprint(result.yaml_content)
        exit_run(0, "Documentation copied to clipboard")


def _handle_update_mode(result: DocsResult) -> None:
    """Handle file update mode output and errors."""
    if not result.success:
        error_msg = f"Failed to update model {result.model_name}"
        if result.error_message:
            error_msg += f": {result.error_message}"
        exit_run(1, error_msg)

    has_changes = result.changes.added or result.changes.removed or result.changes.reordered

    if not has_changes:
        _printers.cprint(
            f"ℹ️  No column changes detected for model {result.model_name}",  # noqa: RUF001
            color="bright_black",
        )
        exit_run(0, "Documentation is up to date")

    # Print success message with model name - remove highlight to avoid color mixing
    _printers.cprint(f"✅ updated model {result.model_name}", color="green")

    # Print detailed change information in a consistent subdued color
    if result.changes.added:
        _printers.cprint(
            f"   Added columns: {', '.join(result.changes.added)}", color="bright_black"
        )
    if result.changes.removed:
        _printers.cprint(
            f"   Removed columns: {', '.join(result.changes.removed)}", color="bright_black"
        )
    if result.changes.reordered:
        _printers.cprint("   Column order changed", color="bright_black")

    # Display YAML file operation information in subdued color
    if result.yaml_path and result.mode:
        _printers.cprint(f"   Mode: {result.mode}", color="bright_black")
        _printers.cprint(f"   YAML file: {result.yaml_path}", color="bright_black")

    exit_run(0, "Documentation updated successfully")


def docs(
    models: ArgumentModelSelection = None,
    target: OptionTarget = None,
    model: OptionModelSelection = None,
    clipboard: Annotated[
        bool,
        typer.Option(
            "--clipboard",
            "-c",
            help="Copy output to clipboard",
        ),
    ] = False,
) -> None:
    """Generate documentation for a specific dbt model.

    This is a typer command configured in cli/main.py.

    Example: dt docs my_model -t prod
    """
    # Merge positional models argument with --model option
    # Positional argument takes precedence if both are provided
    final_model_selection = models or model

    # Docs command requires a model to be specified
    if not final_model_selection:
        exit_run(1, "Model selection is required. Use: dt docs <model> or dt docs --model <model>")

    try:
        dbt_parser = dbtParser(target=target)
    except Exception as e:  # noqa: BLE001
        exit_run(1, f"Failed to initialize dbt parser: {e}")

    selection_result = dbt_parser.parse_selection_query(final_model_selection)
    if len(selection_result.model_names) != 1:
        exit_run(1, "Selection for docs can only be singular models")
    selected_model = selection_result.model_names[0]

    if selected_model not in dbt_parser.models:
        max_models_to_show = 5
        available_models = list(dbt_parser.models.keys())[:max_models_to_show]
        if available_models:
            models_str = f"Available models include: {', '.join(available_models)}"
            _printers.cprint(models_str, color="bright_black")
            if len(dbt_parser.models) > max_models_to_show:
                remaining = len(dbt_parser.models) - max_models_to_show
                _printers.cprint(f"... and {remaining} more", color="bright_black")
        exit_run(1, f"Model {final_model_selection} not found")

    try:
        # Use fix_inplace=False when clipboard is True (to get YAML content)
        result = YamlBuilder(selected_model, dbt_parser).build(fix_inplace=not clipboard)
    except Exception as e:  # noqa: BLE001
        exit_run(1, f"Unexpected error while building YAML docs: {e}")

    if clipboard:
        _handle_clipboard_mode(result)
    else:
        _handle_update_mode(result)
