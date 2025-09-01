"""Centralized exit handler with warnings display and colored messaging."""

import sys

from dbt_toolbox import constants
from dbt_toolbox.utils._printers import cprint
from dbt_toolbox.warnings_collector import warnings_collector


def _display_warnings() -> None:
    """Display all collected warnings in a formatted way."""
    warnings = warnings_collector.get_warnings()

    if not warnings:
        return

    # Print warnings section header
    cprint(
        f"⚠️  Warnings: (Anything unexpected? Please raise issue: {constants.GITHUB_ISSUES_LINK})",
        color="yellow",
    )

    # Print each warning
    categories_shown = set()
    for msg, category in warnings.items():
        # Format the warning message
        cprint(f"  • {category}: {msg}", color="yellow")
        categories_shown.add(category)

    # Add tip for ignoring warnings if any were shown
    if categories_shown:
        categories_list = "', '".join(sorted(categories_shown))
        cprint("💡 Tip: To ignore these warnings, add to your pyproject.toml:", color="cyan")
        cprint("   [tool.dbt_toolbox]", color="bright_black")
        cprint(f"   warnings_ignored = ['{categories_list}']", color="bright_black")


def exit_run(exit_code: int = 0, message: str | None = None, /) -> None:
    """Exit with warnings display and optional colored message.

    Args:
        exit_code: The exit code to use (0 = success, 1 = error, >1 = warning)
        message: Optional exit message to display with appropriate colors

    Color coding:
        - Exit code 0: Green success messages
        - Exit code 1: Red error messages
        - Exit code > 1: Yellow warning messages

    """
    # Always display warnings first
    _display_warnings()

    # Display exit message with appropriate color if provided
    if message:
        if exit_code == 0:
            cprint(f"✅ {message}", color="green")
        elif exit_code == 1:
            cprint(f"❌ {message}", color="red")
        else:  # exit_code > 1
            cprint(f"⚠️  {message}", color="yellow")

    # Exit with the specified code
    sys.exit(exit_code)
