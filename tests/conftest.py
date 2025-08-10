"""Pytest configuration script."""

import os
from pathlib import Path
from shutil import copytree, ignore_patterns, rmtree

import pytest

from dbt_toolbox.dbt_parser._cache import Cache
from dbt_toolbox.dbt_parser.dbt_parser import dbtParser
from dbt_toolbox.settings import settings


@pytest.fixture(scope="session", autouse=True)
def dbt_project_setup():  # noqa: ANN201
    """Set up the temporary dbt project.

    Happens once per testing session.
    """
    # Copy over the sample project
    destination_path = "tests/__temporary_copy_dbt_project"
    if Path(destination_path).exists():
        rmtree(destination_path)
    src_path = Path("tests/dbt_sample_project")
    copytree(
        src_path,
        destination_path,
        ignore=ignore_patterns(".dbt_toolbox", "__pycache__", "target", "logs", "test_folder"),
    )
    os.environ["DBT_PROJECT_DIR"] = destination_path
    os.environ["DBT_TOOLBOX_DEBUG"] = "true"
    # Clear the cache
    Cache().clear()
    assert settings.dbt_project_dir == Path().cwd() / destination_path
    yield
    rmtree(destination_path)
    if "DBT_PROJECT_DIR" in os.environ:
        del os.environ["DBT_PROJECT_DIR"]


@pytest.fixture
def dbt_parser() -> dbtParser:
    """Get the dbt parser."""
    return dbtParser()
