"""Documentation command for yaml."""

import subprocess
from typing import Annotated

import typer

from dbt_toolbox.actions.build_docs import DocsResult, YamlBuilder
from dbt_toolbox.cli._common_options import Target
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.utils import _printers


def _handle_clipboard_mode(result: DocsResult) -> None:
    """Handle clipboard mode output and errors."""
    if not result.success:
        _printers.cprint(
            "❌ Failed to generate YAML for model",
            result.model_name,
            highlight_idx=1,
            color="red",
        )
        if result.error_message:
            _printers.cprint(f"   Error: {result.error_message}", color="red")
        raise typer.Exit(1)

    if result.yaml_content:
        process = subprocess.Popen(args="pbcopy", stdin=subprocess.PIPE)
        process.communicate(input=result.yaml_content.encode())
        _printers.cprint(result.yaml_content)
        _printers.cprint("Also exists in your clipboard (cmd+V)", color="green")


def _handle_update_mode(result: DocsResult) -> None:
    """Handle file update mode output and errors."""
    if not result.success:
        _printers.cprint(
            "❌ Failed to update model", result.model_name, highlight_idx=1, color="red"
        )
        if result.error_message:
            _printers.cprint(f"   Error: {result.error_message}", color="red")
        raise typer.Exit(1)

    has_changes = result.changes.added or result.changes.removed or result.changes.reordered

    if not has_changes:
        _printers.cprint(
            "ℹ️  No column changes detected for model",  # noqa: RUF001
            result.model_name,
            highlight_idx=1,
        )
        return

    # Print detailed change information
    change_messages = []
    if result.changes.added:
        change_messages.append(f"Added columns: {', '.join(result.changes.added)}")
    if result.changes.removed:
        change_messages.append(f"Removed columns: {', '.join(result.changes.removed)}")
    if result.changes.reordered:
        change_messages.append("Column order changed")

    _printers.cprint("✅ updated model", result.model_name, highlight_idx=1)
    for msg in change_messages:
        _printers.cprint(f"   {msg}", color="cyan")

    # Display YAML file operation information
    if result.yaml_path and result.mode:
        _printers.cprint(f"   Mode: {result.mode}", color="yellow")
        _printers.cprint(f"   YAML file: {result.yaml_path}", color="bright_black")


def docs(
    model: Annotated[
        str,
        typer.Option(
            "--model",
            "-m",
            help="Model name to generate documentation for",
        ),
    ],
    clipboard: Annotated[
        bool,
        typer.Option(
            "--clipboard",
            "-c",
            help="Copy output to clipboard",
        ),
    ] = False,
    target: str | None = Target,
) -> None:
    """Generate documentation for a specific dbt model.

    This is a typer command configured in cli/main.py.
    """
    try:
        dbt_parser = dbtParser(target=target)
    except Exception as e:  # noqa: BLE001
        _printers.cprint("❌ Failed to initialize dbt parser", color="red")
        _printers.cprint(f"   Error: {e}", color="red")
        raise typer.Exit(1) from None

    if model not in dbt_parser.models:
        _printers.cprint("❌ Model", model, "not found", highlight_idx=1, color="red")
        max_models_to_show = 5
        available_models = list(dbt_parser.models.keys())[:max_models_to_show]
        if available_models:
            models_str = f"   Available models include: {', '.join(available_models)}"
            _printers.cprint(models_str, color="yellow")
            if len(dbt_parser.models) > max_models_to_show:
                remaining = len(dbt_parser.models) - max_models_to_show
                _printers.cprint(f"   ... and {remaining} more", color="yellow")
        raise typer.Exit(1)

    try:
        # Use fix_inplace=False when clipboard is True (to get YAML content)
        result = YamlBuilder(model, dbt_parser).build(fix_inplace=not clipboard)
    except Exception as e:  # noqa: BLE001
        _printers.cprint("❌ Unexpected error while building YAML docs", color="red")
        _printers.cprint(f"   Error: {e}", color="red")
        raise typer.Exit(1) from None

    if clipboard:
        _handle_clipboard_mode(result)
    else:
        _handle_update_mode(result)
