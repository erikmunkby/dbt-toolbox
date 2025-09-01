"""Module for warnings collection."""


class WarningsCollector:
    """Collects warnings across different operations for MCP/LLM integration."""

    def __init__(self) -> None:
        """Initialize empty warnings collection."""
        self._warnings: dict[str, str] = {}

    def add_warning(self, category: str, message: str) -> None:
        """Add a warning to the collection.

        Args:
            category: Type of warning (e.g., "unknown_jinja_macro", "column_issue")
            message: Warning message

        """
        self._warnings[message] = category

    def get_warnings(self) -> dict[str, str]:
        """Get all collected warnings."""
        return self._warnings.copy()

    def clear(self) -> None:
        """Clear all warnings."""
        self._warnings.clear()


# Global warnings collector instance
warnings_collector = WarningsCollector()
