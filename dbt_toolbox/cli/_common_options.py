"""Common options.

These can be used as:
@app.command()
def my_command(target: Target):
    ...
"""

from typing import Annotated

import typer

OptionTarget = Annotated[str | None, typer.Option("--target", "-t", help="The dbt target.")]
OptionModelSelection = Annotated[
    str | None,
    typer.Option(
        "--model",
        "-m",
        "--select",
        "-s",
        "--models",
        help="Choose specific model (same as dbt --select/--model)",
    ),
]
ArgumentModelSelection = Annotated[
    str | None,
    typer.Argument(
        help="Model selection (same as --select)",
        show_default=False,
    ),
]
