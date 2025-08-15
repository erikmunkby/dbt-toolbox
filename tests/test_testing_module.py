"""Test the testing module functionality."""

from dbt_toolbox.testing import check_column_documentation, get_all_models


class TestColumnDocumentation:
    """Test column documentation checking functionality."""

    def test_check_column_documentation_returns_dict(self) -> None:
        """Test that check_column_documentation returns a dictionary."""
        result = check_column_documentation()
        assert isinstance(result, dict)

    def test_check_column_documentation_structure(self) -> None:
        """Test that results have the correct structure."""
        result = check_column_documentation()

        for model_name, issues in result.items():
            assert isinstance(model_name, str)
            assert isinstance(issues, dict)
            assert "missing_descriptions" in issues
            assert "superfluous_descriptions" in issues
            assert isinstance(issues["missing_descriptions"], list)
            assert isinstance(issues["superfluous_descriptions"], list)

            # All items in lists should be strings
            for item in issues["missing_descriptions"]:
                assert isinstance(item, str)
            for item in issues["superfluous_descriptions"]:
                assert isinstance(item, str)

    def test_only_problematic_models_included(self) -> None:
        """Test that only models with documentation issues are included."""
        result = check_column_documentation()

        # Each model in results should have at least one issue
        for model_name, issues in result.items():
            has_missing = len(issues["missing_descriptions"]) > 0
            has_superfluous = len(issues["superfluous_descriptions"]) > 0
            assert has_missing or has_superfluous, (
                f"Model {model_name} has no issues but is in results"
            )


def test_get_all_models() -> None:
    """Test the fetching all models function."""
    assert "customers" in get_all_models()
