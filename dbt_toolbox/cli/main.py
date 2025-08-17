"""Main cli module."""

import typer

from dbt_toolbox import utils
from dbt_toolbox.cli._common_options import Target
from dbt_toolbox.cli.analyze import analyze_command
from dbt_toolbox.cli.build import build
from dbt_toolbox.cli.clean import clean
from dbt_toolbox.cli.docs import docs
from dbt_toolbox.cli.run import run
from dbt_toolbox.run_config import RunConfig
from dbt_toolbox.settings import settings
from dbt_toolbox.utils._printers import cprint

app = typer.Typer(help="dbt-toolbox CLI - Tools for working with dbt projects")


app.command()(docs)
app.command()(build)
app.command()(run)
app.command()(clean)
app.command(name="analyze")(analyze_command)


@app.command(name="settings")
def settings_cmd(target: str = Target) -> None:
    """Show all found settings and their sources."""
    typer.secho("dbt-toolbox Settings:", fg=typer.colors.BRIGHT_CYAN, bold=True)
    typer.secho("=" * 50, fg=typer.colors.CYAN)

    all_settings = {
        **settings.get_all_settings_with_sources(),
        **RunConfig(target=target).get_all_config_with_sources(),
    }

    for setting_name, source_info in all_settings.items():
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


@app.command(name="start-mcp-server")
def start_mcp_server() -> None:
    """Start the MCP server."""
    cprint("Starting mcp server...", color="cyan")
    try:
        from dbt_toolbox.mcp.mcp import mcp_server  # noqa: PLC0415

        mcp_server.run()
    except ModuleNotFoundError as e:
        utils.cprint(
            "Module mcp not found. Install using: ",
            'uv add "dbt-toolbox[mcp]"',
            highlight_idx=1,
            color="red",
        )
        raise ModuleNotFoundError(
            'Missing modules, did you install using `uv add "dbt-toolbox[mcp]"` ?'
        ) from e


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
