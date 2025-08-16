"""Test caching functionality."""

from pathlib import Path

from yamlium import parse

from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.dbt_parser._cache import Cache
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import build_path


def test_dbt_project_validation() -> None:
    """Test dbt project cache validation."""
    # First time we run the check we should fail, second time should work.
    cache = Cache()
    assert not cache._validate_dbt_project_cache()
    assert cache._validate_dbt_project_cache()

    # Reloading it without edits should give true cache
    cache = Cache()  # Reload cache to dump in-memory caching
    assert cache._validate_dbt_project_cache()

    # Make a change to the dbt project
    cache = Cache()  # Reload cache to dump in-memory caching
    yml = parse(settings.dbt_project_yaml_path).to_yaml()
    yml += "\n# Some comment"
    settings.dbt_project_yaml_path.write_text(yml)
    assert not cache._validate_dbt_project_cache()
    assert cache._validate_dbt_project_cache()


def test_macro_cache_validation() -> None:
    """Test validation of macro cache."""
    cache = Cache()
    assert not cache._validate_macro_cache()
    assert cache._validate_macro_cache()
    dbtParser()

    # Add a new macro
    p = Path(settings.dbt_project.macro_paths[0])
    p = build_path(p / "cache_test_macro.sql")
    p.write_text("""
{% macro cache_test_macro() %}
'Test'
{% endmacro %}
    """)
    cache = Cache()
    assert not cache._validate_macro_cache()
    assert cache._validate_macro_cache()
