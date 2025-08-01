"""Main cli module."""

import typer

from dbt_toolbox.cli.analyze import analyze_command
from dbt_toolbox.cli.build import build
from dbt_toolbox.cli.clean import clean
from dbt_toolbox.cli.docs import docs
from dbt_toolbox.cli.run import run
from dbt_toolbox.settings import settings

app = typer.Typer(help="dbt-toolbox CLI - Tools for working with dbt projects")


app.command()(docs)
app.command()(build)
app.command()(run)
app.command()(clean)
app.command(name="analyze")(analyze_command)


@app.command(name="settings")
def settings_cmd() -> None:
    """Show all found settings and their sources."""
    settings_sources = settings.get_all_settings_with_sources()

    typer.secho("dbt-toolbox Settings:", fg=typer.colors.BRIGHT_CYAN, bold=True)
    typer.secho("=" * 50, fg=typer.colors.CYAN)

    for setting_name, source_info in settings_sources.items():
        typer.echo()
        typer.secho(f"{setting_name}:", fg=typer.colors.BRIGHT_WHITE, bold=True)

        # Color value based on source
        value_color = (
            typer.colors.BRIGHT_BLACK if source_info.source == "default" else typer.colors.CYAN
        )

        typer.secho("  value: ", fg=typer.colors.WHITE, nl=False)
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


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
