"""Module for warnings collection."""


class WarningsCollector:
    """Collects warnings across different operations for MCP/LLM integration."""

    def __init__(self) -> None:
        """Initialize empty warnings collection."""
        self._warnings: dict[str, dict[str, str]] = {}

    def add_warning(self, warning_type: str, message: str, source: str = "unknown") -> None:
        """Add a warning to the collection.

        Args:
            warning_type: Type of warning (e.g., "unknown_jinja_macro", "column_issue")
            message: Warning message
            source: Source of the warning (e.g., "jinja_handler", "column_resolver")

        """
        self._warnings[message] = {"type": warning_type, "message": message, "source": source}

    def get_warnings_list(self) -> list[dict[str, str]]:
        """Get all collected warnings."""
        return list(self._warnings.copy().values())

    def clear(self) -> None:
        """Clear all warnings."""
        self._warnings.clear()


# Global warnings collector instance
warnings_collector = WarningsCollector()
