"""Main cli module."""

from typing import Annotated

import typer

from dbt_toolbox import utils
from dbt_toolbox._context import set_verbose
from dbt_toolbox.cli.analyze import analyze_command
from dbt_toolbox.cli.build import build
from dbt_toolbox.cli.clean import clean
from dbt_toolbox.cli.docs import docs
from dbt_toolbox.cli.run import run
from dbt_toolbox.cli.settings import settings_cmd
from dbt_toolbox.utils._printers import cprint

app = typer.Typer(
    help="dbt-toolbox CLI - Tools for working with dbt projects",
    pretty_exceptions_show_locals=False,
)


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,  # noqa: ARG001
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Enable debug logging.")
    ] = False,
) -> None:
    """Dbt-toolbox CLI - Tools for working with dbt projects."""
    if verbose:
        set_verbose()


app.command()(docs)
app.command()(build)
app.command()(run)
app.command()(clean)
app.command(name="clear", hidden=True)(clean)  # Alias for clean
app.command(name="analyze")(analyze_command)
app.command(name="settings")(settings_cmd)


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
