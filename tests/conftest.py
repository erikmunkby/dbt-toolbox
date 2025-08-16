"""Pytest configuration script."""

import os
from pathlib import Path
from shutil import copytree, ignore_patterns, rmtree

import pytest

from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.dbt_parser._cache import Cache
from dbt_toolbox.settings import settings

PROJECT_COPY_PATH = Path("tests/__temporary_copy_dbt_project")


@pytest.fixture(scope="session", autouse=True)
def dbt_project_setup():  # noqa: ANN201
    """Set up the temporary dbt project.

    Happens once per testing session.
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
    # Clear the cache
    Cache().clear()
    assert settings.dbt_project_dir == Path().cwd() / PROJECT_COPY_PATH
    yield
    rmtree(PROJECT_COPY_PATH)
    if "DBT_PROJECT_DIR" in os.environ:
        del os.environ["DBT_PROJECT_DIR"]


@pytest.fixture
def dbt_parser() -> dbtParser:
    """Get the dbt parser."""
    return dbtParser()


@pytest.fixture
def temp_model_path() -> tuple[str, Path]:  # type: ignore
    """Get a reference to a temporary manipulateable model."""
    name = "pytest__temp_model"
    p = PROJECT_COPY_PATH / f"models/{name}.sql"
    yield name, p  # type: ignore
    p.unlink()
