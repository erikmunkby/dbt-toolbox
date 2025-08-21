"""Settings CLI command."""

import typer

from dbt_toolbox.actions.all_settings import get_all_settings
from dbt_toolbox.cli._common_options import Target


def settings_cmd(target: str = Target) -> None:
    """Show all found settings and their sources."""
    typer.secho("dbt-toolbox Settings:", fg=typer.colors.BRIGHT_CYAN, bold=True)
    typer.secho("=" * 50, fg=typer.colors.CYAN)

    all_settings = get_all_settings(target=target)

    for setting_name, source_info in all_settings.items():
        typer.echo()

        # Color value based on source
        value_color = (
            typer.colors.BRIGHT_BLACK if source_info.source == "default" else typer.colors.CYAN
        )

        # Setting name and value on same line
        typer.secho(f"{setting_name}: ", fg=typer.colors.BRIGHT_WHITE, bold=True, nl=False)
        typer.secho(f"{source_info.value}", fg=value_color)

        # Color source
        source_color = {
            "environment variable": typer.colors.MAGENTA,
            "TOML file": typer.colors.BLUE,
            "dbt": typer.colors.BRIGHT_RED,
            "default": typer.colors.BRIGHT_BLACK,
        }.get(source_info.source, typer.colors.WHITE)

        typer.secho("  source: ", fg=typer.colors.WHITE, nl=False)
        typer.secho(f"{source_info.source}", fg=source_color)

        if source_info.location:
            typer.secho("  location: ", fg=typer.colors.WHITE, nl=False)
            typer.secho(f"{source_info.location}", fg=typer.colors.BRIGHT_BLACK)
