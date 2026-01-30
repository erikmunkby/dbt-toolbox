"""Error handler decorator for MCP tools."""

import traceback
from collections.abc import Callable
from functools import wraps

from dbt_toolbox.mcp._utils import mcp_json_response

MAX_TRACEBACK_FRAMES = 5


def handle_mcp_errors(func: Callable) -> Callable:
    """Wrap MCP tool functions with consistent error handling.

    Catches all unhandled exceptions and returns structured JSON error
    responses for LLM consumption. Includes "smart" tracebacks limited
    to last N frames to avoid huge logs.

    Args:
        func: The MCP tool function to wrap

    Returns:
        Wrapped function with error handling

    """

    @wraps(func)
    def _wrapper(*args, **kwargs) -> str:  # noqa: ANN002, ANN003
        try:
            return func(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            tb_lines = traceback.format_tb(e.__traceback__)
            # Keep last N frames to avoid huge logs
            short_traceback = (
                tb_lines[-MAX_TRACEBACK_FRAMES:]
                if len(tb_lines) > MAX_TRACEBACK_FRAMES
                else tb_lines
            )

            return mcp_json_response(
                {
                    "status": "error",
                    "message": str(e),
                    "error_type": type(e).__name__,
                    "traceback": [line.strip() for line in short_traceback],
                }
            )

    return _wrapper
