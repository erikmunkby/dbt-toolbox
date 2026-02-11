"""Runtime execution context."""

_execution_context = "cli"
_current_command: str | None = None
_verbose = False


def set_mcp_mode() -> None:
    """Set execution context to MCP mode."""
    global _execution_context  # noqa: PLW0603
    _execution_context = "mcp"


def is_mcp_mode() -> bool:
    """Check if running in MCP mode."""
    return _execution_context == "mcp"


def set_command(command: str) -> None:
    """Set the current CLI command."""
    global _current_command  # noqa: PLW0603
    _current_command = command


def get_command() -> str | None:
    """Get the current CLI command."""
    return _current_command


def set_verbose() -> None:
    """Enable verbose (debug) logging."""
    global _verbose  # noqa: PLW0603
    _verbose = True


def is_verbose() -> bool:
    """Check if verbose logging is enabled."""
    return _verbose
