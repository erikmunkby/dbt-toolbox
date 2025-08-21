"""Test settings functionality."""

import os
import tempfile
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from dbt_toolbox.settings import Settings


def test_models_ignore_validation_default() -> None:
    """Test that models_ignore_validation defaults to empty list."""
    settings = Settings()
    assert settings.models_ignore_validation == []
    assert isinstance(settings.models_ignore_validation, list)


def test_models_ignore_validation_from_environment() -> None:
    """Test that models_ignore_validation can be set via environment variable."""
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["DBT_PROJECT_DIR"] = temp_dir
        os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"] = "model1,model2,model3"

        try:
            settings = Settings()
            assert settings.models_ignore_validation == ["model1", "model2", "model3"]
        finally:
            del os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"]
            del os.environ["DBT_PROJECT_DIR"]


def test_models_ignore_validation_from_environment_with_spaces() -> None:
    """Test that models_ignore_validation handles spaces correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["DBT_PROJECT_DIR"] = temp_dir
        os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"] = "model1, model2 , model3"

        try:
            settings = Settings()
            assert settings.models_ignore_validation == ["model1", "model2", "model3"]
        finally:
            del os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"]
            del os.environ["DBT_PROJECT_DIR"]


def test_models_ignore_validation_from_environment_empty() -> None:
    """Test that empty environment variable returns empty list."""
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["DBT_PROJECT_DIR"] = temp_dir
        os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"] = ""

        try:
            settings = Settings()
            assert settings.models_ignore_validation == []
        finally:
            del os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"]
            del os.environ["DBT_PROJECT_DIR"]


def test_models_ignore_validation_from_toml(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Test that models_ignore_validation can be set via TOML file."""
    # Create a temporary TOML file
    toml_content = """
[tool.dbt_toolbox]
models_ignore_validation = ["toml_model1", "toml_model2"]
"""
    toml_file = tmp_path / "pyproject.toml"
    toml_file.write_text(toml_content)

    # Create a minimal dbt_project.yml
    dbt_project_file = tmp_path / "dbt_project.yml"
    dbt_project_file.write_text("name: test\nversion: '1.0.0'")

    # Import the settings module to access internal functions
    from dbt_toolbox import settings as settings_module

    # Mock the TOML loading to return our test data
    def mock_find_toml_settings() -> tuple[dict[str, list[str]], Path]:
        return {"models_ignore_validation": ["toml_model1", "toml_model2"]}, toml_file

    # Patch the TOML loading function and the module-level variables
    monkeypatch.setattr(settings_module, "_find_toml_settings", mock_find_toml_settings)
    # Reload the TOML data
    toml_data, toml_path = mock_find_toml_settings()
    monkeypatch.setattr(settings_module, "toml", toml_data)
    monkeypatch.setattr(settings_module, "toml_file_path", toml_path)

    # Now create a new Settings instance
    settings = Settings()
    assert settings.models_ignore_validation == ["toml_model1", "toml_model2"]


def test_models_ignore_validation_in_all_settings() -> None:
    """Test that models_ignore_validation appears in get_all_settings_with_sources."""
    settings = Settings()
    all_settings = settings.get_all_settings_with_sources()
    assert "models_ignore_validation" in all_settings
    assert all_settings["models_ignore_validation"].value == []
    assert all_settings["models_ignore_validation"].source == "default"


def test_setting_precedence_env_over_default() -> None:
    """Test that environment variable takes precedence over default."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save original environment state
        original_dbt_project = os.environ.get("DBT_PROJECT_DIR")

        os.environ["DBT_PROJECT_DIR"] = temp_dir
        os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"] = "env_model"

        try:
            settings = Settings()
            setting_info = settings.get_all_settings_with_sources()["models_ignore_validation"]
            assert setting_info.value == ["env_model"]
            assert setting_info.source == "environment variable"
            assert setting_info.location == "DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"
        finally:
            del os.environ["DBT_TOOLBOX_MODELS_IGNORE_VALIDATION"]
            # Restore original state
            if original_dbt_project is not None:
                os.environ["DBT_PROJECT_DIR"] = original_dbt_project
            elif "DBT_PROJECT_DIR" in os.environ:
                del os.environ["DBT_PROJECT_DIR"]


def test_models_ignore_validation_integration_with_column_analysis() -> None:
    """Test that models_ignore_validation works with column analysis."""
    from dbt_toolbox import settings as settings_module
    from dbt_toolbox.actions.analyze_columns_references import analyze_column_references

    # Use the actual dbt parser to get real models from the test project
    from dbt_toolbox.dbt_parser import dbtParser

    dbt_parser = dbtParser()
    models = dbt_parser.models

    # Skip the test if there are no models to work with
    if not models:
        pytest.skip("No models available in test project")

    # Get a model name to test with
    test_model_name = next(iter(models.keys()))

    # First, test without ignore list
    original_ignore_list = settings_module.settings.models_ignore_validation
    try:
        # Temporarily set empty ignore list
        settings_module.settings._models_ignore_validation = settings_module.Setting(
            value=[], source="test", location="test"
        )

        analyze_column_references(dbt_parser)

        # Now test with ignore list - should skip the model
        settings_module.settings._models_ignore_validation = settings_module.Setting(
            value=[test_model_name], source="test", location="test"
        )

        analysis_with_ignore = analyze_column_references(dbt_parser)

        # The test model should not appear in the analysis results when ignored
        assert test_model_name not in analysis_with_ignore.non_existent_columns
        assert test_model_name not in analysis_with_ignore.referenced_non_existent_models
        assert test_model_name not in analysis_with_ignore.cte_column_issues

    finally:
        # Restore original setting
        settings_module.settings._models_ignore_validation = settings_module.Setting(
            value=original_ignore_list, source="test", location="test"
        )
