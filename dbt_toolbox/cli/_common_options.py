"""Common options.

These can be used as:
@app.command()
def my_command(target: Target):
    ...
"""

import typer

Target = typer.Option(None, "--target", "-t", help="The dbt target.")
