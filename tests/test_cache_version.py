"""Tests for cache version checking."""

from pathlib import Path

import pytest

from dbt_toolbox._version import __version__
from dbt_toolbox.dbt_parser._cache import Cache
from dbt_toolbox.settings import settings


def test_cache_version_mismatch_cli_mode(dbt_project: Path) -> None:
    """Version mismatch in CLI mode raises SystemExit."""
    cache_root = settings.dbt_project_dir / ".dbt_toolbox"
    cache_root.mkdir(parents=True, exist_ok=True)
    (cache_root / ".version").write_text("0.0.0")

    with pytest.raises(SystemExit):
        Cache("dev")


def test_cache_version_match_succeeds(dbt_project: Path) -> None:
    """Matching version allows Cache to initialize."""
    cache_root = settings.dbt_project_dir / ".dbt_toolbox"
    cache_root.mkdir(parents=True, exist_ok=True)
    (cache_root / ".version").write_text(__version__)

    cache = Cache("dev")
    assert cache.cache_path.exists()
