"""Module for mcp server."""

from fastmcp import FastMCP

from dbt_toolbox.actions.all_settings import get_all_settings
from dbt_toolbox.mcp import (
    tool_analyze_models,
    tool_build_model,
    tool_generate_docs,
    tool_list_dbt_objects,
    tool_show_docs,
)
from dbt_toolbox.mcp._utils import mcp_json_response

mcp_server = FastMCP("dbt-toolbox")


mcp_server.tool()(tool_analyze_models.analyze_models)
mcp_server.tool()(tool_show_docs.show_docs)
mcp_server.tool()(tool_list_dbt_objects.list_dbt_objects)
mcp_server.tool()(tool_build_model.build_models)
mcp_server.tool()(tool_generate_docs.generate_docs)


@mcp_server.tool()
def list_settings(target: str | None = None) -> str:
    """List all dbt-toolbox settings with their values and sources.

    Shows configuration from environment variables, TOML files, dbt profiles,
    and default values with clear indication of where each setting comes from.

    Args:
        target: Specify dbt target environment to include target-specific settings

    Returns:
        JSON string with all settings, their values, sources, and metadata.

    Example output:
    {
        "cache_path": {
            "value": ".dbt_toolbox",
            "source": "default",
            "description": "Directory for cache storage"
        },
        "debug": {
            "value": false,
            "source": "toml",
            "description": "Enable debug logging"
        }
    }

    """
    try:
        all_settings = get_all_settings(target=target)
        return mcp_json_response(
            {name: setting._asdict() for name, setting in all_settings.items()}
        )
    except Exception as e:  # noqa: BLE001
        return mcp_json_response({"status": "error", "message": f"Failed to get settings: {e!s}"})
