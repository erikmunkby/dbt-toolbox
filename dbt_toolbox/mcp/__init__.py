"""Module for the dbt toolbox mcp server."""

from dbt_toolbox import utils

try:
    import mcp  # noqa: F401
except ModuleNotFoundError:
    utils.cprint(
        "Module mcp not found. Install using: ",
        'pip install "dbt-toolbox[mcp]"',
        highlight_idx=1,
        color="red",
    )
