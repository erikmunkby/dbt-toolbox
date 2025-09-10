"""Test per-target caching functionality."""

from pathlib import Path

from dbt_toolbox.dbt_parser._cache import Cache
from dbt_toolbox.dbt_parser._dbt_parser import dbtParser
from dbt_toolbox.settings import settings


def test_cache_folder_structure_per_target() -> None:
    """Test that cache folders are created per target."""
    # Test different target names create different cache folders
    dev_cache = Cache(dbt_target="dev")
    test_cache = Cache(dbt_target="test_target")
    pr_cache = Cache(dbt_target="pull-request-ci")

    # Check that different cache paths are created
    assert dev_cache.cache_path != test_cache.cache_path
    assert dev_cache.cache_path != pr_cache.cache_path
    assert test_cache.cache_path != pr_cache.cache_path

    # Check that the paths follow the expected pattern
    expected_base = settings.dbt_project_dir / ".dbt_toolbox"
    assert dev_cache.cache_path == expected_base / "dev"
    assert test_cache.cache_path == expected_base / "test_target"
    assert pr_cache.cache_path == expected_base / "pull-request-ci"

    # Check that all cache directories exist
    assert dev_cache.cache_path.exists()
    assert test_cache.cache_path.exists()
    assert pr_cache.cache_path.exists()


def test_cache_isolation_between_targets() -> None:
    """Test that cache data is isolated between different targets."""
    dev_cache = Cache(dbt_target="dev")
    test_cache = Cache(dbt_target="test_target")

    # Clear both caches to start fresh
    dev_cache.clear()
    test_cache.clear()

    # Create test data for each cache
    test_data_dev = {"test": "dev_data"}
    test_data_test = {"test": "test_data"}

    # Write to dev cache
    dev_cache.cache_jinja_env.write(test_data_dev)

    # Write to test cache
    test_cache.cache_jinja_env.write(test_data_test)

    # Verify data isolation - each cache should have its own data
    assert dev_cache.cache_jinja_env.read() == test_data_dev
    assert test_cache.cache_jinja_env.read() == test_data_test

    # Clear one cache shouldn't affect the other
    dev_cache.clear()
    assert not dev_cache.cache_jinja_env.exists()
    assert test_cache.cache_jinja_env.exists()
    assert test_cache.cache_jinja_env.read() == test_data_test


def test_dbt_parser_uses_correct_target_cache() -> None:
    """Test that dbtParser uses the correct cache based on target."""
    # Create parsers with different targets
    dev_parser = dbtParser(target="dev")
    test_parser = dbtParser(target="test_target")

    # Check that they use different cache instances
    assert dev_parser.cache.cache_path != test_parser.cache.cache_path

    # Check target-specific cache paths
    expected_base = settings.dbt_project_dir / ".dbt_toolbox"
    assert dev_parser.cache.cache_path == expected_base / "dev"
    assert test_parser.cache.cache_path == expected_base / "test_target"


def test_model_cache_isolation_between_targets() -> None:
    """Test that model caches are isolated between targets."""
    from sqlglot import parse

    from dbt_toolbox.data_models import DependsOn, Model

    dev_cache = Cache(dbt_target="dev")
    test_cache = Cache(dbt_target="test_target")

    # Create mock dependencies
    empty_deps = DependsOn(sources=[], models=[], macros=[])

    # Create full model objects for each target
    dev_model = Model(
        name="test_model",
        path=Path("models/test_model.sql"),
        raw_code="SELECT 1 as dev_id",
        rendered_code="SELECT 1 as dev_id",
        glot_code=parse("SELECT 1 as dev_id")[0],  # type: ignore
        upstream=empty_deps,
    )

    test_model = Model(
        name="test_model",
        path=Path("models/test_model.sql"),
        raw_code="SELECT 1 as test_id",
        rendered_code="SELECT 1 as test_id",
        glot_code=parse("SELECT 1 as test_id")[0],  # type: ignore
        upstream=empty_deps,
    )

    # Cache models in different targets
    dev_cache.cache_model(dev_model)
    test_cache.cache_model(test_model)

    # Verify each cache has its own model data
    cached_dev_model = dev_cache.get_cached_model("test_model")
    cached_test_model = test_cache.get_cached_model("test_model")

    assert cached_dev_model is not None
    assert cached_test_model is not None
    assert cached_dev_model.raw_code != cached_test_model.raw_code
    assert "dev_id" in cached_dev_model.raw_code
    assert "test_id" in cached_test_model.raw_code
