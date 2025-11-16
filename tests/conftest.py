"""Pytest configuration script."""

import os
from collections.abc import Generator
from pathlib import Path
from shutil import copytree, ignore_patterns, rmtree

import pytest

from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.dbt_parser._cache import Cache
from dbt_toolbox.settings import settings

PROJECT_COPY_PATH = Path("tests/__temporary_copy_dbt_project")


@pytest.fixture(scope="session")
def dbt_project() -> Generator[Path, None, None]:
    """Set up the temporary dbt project directory.

    This fixture runs once per test session and creates a temporary copy
    of the sample dbt project. It sets up the environment and clears the cache.

    Yields:
        Path to the temporary project directory

    Use this when you need the project path but not a parser instance.

    """
    # Copy over the sample project
    if PROJECT_COPY_PATH.exists():
        rmtree(PROJECT_COPY_PATH)
    src_path = Path("tests/dbt_sample_project")
    copytree(
        src_path,
        PROJECT_COPY_PATH,
        ignore=ignore_patterns(".dbt_toolbox", "__pycache__", "target", "logs", "test_folder"),
    )
    os.environ["DBT_PROJECT_DIR"] = str(PROJECT_COPY_PATH)
    os.environ["DBT_TOOLBOX_DEBUG"] = "true"
    # Disable fuzzy matching by default in tests
    # Tests that need it can use patch.object on Settings.fuzzy_model_matching
    os.environ["DBT_TOOLBOX_FUZZY_MODEL_MATCHING"] = "off"
    # Clear the cache
    Cache(dbt_target="dev").clear()
    assert settings.dbt_project_dir == Path().cwd() / PROJECT_COPY_PATH

    yield PROJECT_COPY_PATH

    # Cleanup
    rmtree(PROJECT_COPY_PATH)
    if "DBT_PROJECT_DIR" in os.environ:
        del os.environ["DBT_PROJECT_DIR"]
    if "DBT_TOOLBOX_FUZZY_MODEL_MATCHING" in os.environ:
        del os.environ["DBT_TOOLBOX_FUZZY_MODEL_MATCHING"]


@pytest.fixture(scope="session")
def parser(dbt_project: Path) -> dbtParser:
    """Get a session-scoped dbt parser for read-only tests.

    This parser is created once per test session and shared across all tests.
    It's the fastest option for tests that only read data and don't modify
    files, cache, or parser state.

    Use this for:
    - Selection query tests
    - Graph traversal tests
    - Column resolution tests
    - Any read-only operations

    DO NOT use this for:
    - Tests that modify model files
    - Tests that test cache behavior
    - Tests that need isolated state

    Returns:
        Shared dbtParser instance

    """
    return dbtParser()


@pytest.fixture
def fresh_parser(dbt_project: Path) -> dbtParser:
    """Get a fresh dbt parser instance for tests that need isolation.

    This creates a new parser instance for each test. Use this when your
    test modifies files, clears cache, or needs isolated parser state.

    Use this for:
    - Tests that create/modify/delete model files
    - Tests that test cache invalidation
    - Tests that need a clean parser state

    Returns:
        New dbtParser instance

    """
    return dbtParser()


@pytest.fixture
def dbt_parser(fresh_parser: dbtParser) -> dbtParser:
    """Alias for fresh_parser for backward compatibility.

    Deprecated: Use 'parser' for read-only tests or 'fresh_parser' for
    mutation tests to be explicit about your intent.
    """
    return fresh_parser


@pytest.fixture
def temp_model_path(dbt_project: Path) -> Generator[tuple[str, Path], None, None]:
    """Get a reference to a temporary model file for mutation tests.

    Creates a temporary model path that will be cleaned up after the test.
    Use this when you need to create/modify model files in tests.

    Yields:
        Tuple of (model_name, model_path)

    """
    name = "pytest__temp_model"
    p = PROJECT_COPY_PATH / f"models/{name}.sql"
    yield name, p
    if p.exists():
        p.unlink()
