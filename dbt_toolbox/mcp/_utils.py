"""Util functions for mcp server."""

import json

from dbt_toolbox.utils import dict_utils
from dbt_toolbox.warnings_collector import warnings_collector


def mcp_json_response(data: dict) -> str:
    """Create a JSON response for MCP tools with warnings included.

    This function automatically collects any warnings that were generated
    during the operation and includes them in the response.

    Args:
        data: The main response data to serialize

    Returns:
        JSON string with warnings included if any exist

    """
    return json.dumps(
        dict_utils.remove_empty_values({**data, "warnings": warnings_collector.get_warnings()})
    )
