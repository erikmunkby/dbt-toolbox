"""Tests for utils._paths module."""

from pathlib import Path

from dbt_toolbox.utils._paths import build_path


class TestBuildPath:
    """Test the build_path function."""

    def test_build_path_with_string(self) -> None:
        """Test building path from string."""
        result = build_path("models")
        assert isinstance(result, Path)
        # Should be relative to dbt project dir
        assert "models" in str(result)

    def test_build_path_with_path(self) -> None:
        """Test building path from Path object."""
        input_path = Path("models")
        result = build_path(input_path)
        assert isinstance(result, Path)
        assert "models" in str(result)

    def test_build_path_with_nested_path(self) -> None:
        """Test building path with nested structure."""
        result = build_path("models/staging")
        assert isinstance(result, Path)
        assert "models" in str(result)
        assert "staging" in str(result)

    def test_build_path_empty_input(self) -> None:
        """Test building path with empty string."""
        result = build_path("")
        assert isinstance(result, Path)
