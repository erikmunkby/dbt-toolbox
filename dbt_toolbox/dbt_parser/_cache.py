"""Cacher."""

import pickle
import shutil
from functools import cached_property
from pathlib import Path
from typing import Any

from loguru import logger

from dbt_toolbox.data_models import MacroBase
from dbt_toolbox.dbt_parser._file_fetcher import fetch_macros
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import utils


class _CacheHolder:
    """Base cache holder for file-based caching."""

    def __init__(self, path: Path, /) -> None:
        """Initialize cache holder with file path.

        Args:
            path: Path to the cache file.

        """
        self.path = path

    def exists(self) -> bool:
        """Check if cache file exists.

        Returns:
            True if cache file exists, False otherwise.

        """
        return self.path.exists()

    def clear(self) -> None:
        """Delete the cache."""
        return self.path.unlink()


class _SetCache(_CacheHolder):
    """Cache handler for set data using pickle serialization."""

    def read(self) -> set:
        """Read set data from cache file.

        Returns:
            Cached set data, or empty set if file doesn't exist.

        """
        if not self.path.exists():
            return set()
        return pickle.loads(self.path.read_bytes())  # noqa: S301

    def write(self, data: set, /) -> None:
        """Write set data to cache file.

        Args:
            data: Set data to cache.

        """
        self.path.write_bytes(pickle.dumps(data))


class _ByteCache(_CacheHolder):
    """Cache handler for arbitrary data using pickle serialization."""

    def read(self) -> Any:  # noqa: ANN401
        """Read data from cache file.

        Returns:
            Cached data, or empty bytes if file doesn't exist.

        """
        if not self.path.exists():
            return b""
        return pickle.loads(self.path.read_bytes())  # noqa: S301

    def write(self, data: Any, /) -> None:  # noqa: ANN401
        """Write data to cache file.

        Args:
            data: Data to cache (can be any pickle-serializable object).

        """
        self.path.write_bytes(pickle.dumps(data))


class Cache:
    """Caching help tool."""

    def clear(self) -> None:
        """Clear the cache."""
        shutil.rmtree(self.cache_path)
        self.cache_path.mkdir()
        if settings.debug:
            logger.debug(f"Cleared cache at {self.cache_path}")

    def clear_model_cache(self, model_name: str) -> bool:
        """Clear cache for a specific model.

        Args:
            model_name: Name of the model to clear from cache.

        Returns:
            True if model was found and removed from cache, False otherwise.

        """
        # Get current cached models
        if not self.cache_models.exists():
            return False

        cached_models = self.cache_models.read()
        if not isinstance(cached_models, dict):
            return False

        # Remove the specific model from cache
        if model_name in cached_models:
            del cached_models[model_name]
            self.cache_models.write(cached_models)
            if settings.debug:
                logger.debug(f"Cleared cache for model: {model_name}")
            return True
        return False

    def clear_models_cache(self, model_names: list[str]) -> list[str]:
        """Clear cache for multiple specific models.

        Args:
            model_names: List of model names to clear from cache.

        Returns:
            List of model names that were actually removed from cache.

        """
        return [model_name for model_name in model_names if self.clear_model_cache(model_name)]

    def track_failed_models(self, failed_models: list[str]) -> None:
        """Track models that failed execution for analyze command.

        Args:
            failed_models: List of model names that failed execution.

        """
        # Load existing failed models
        existing_failed = set()
        if self.cache_failed_models.exists():
            existing_failed = self.cache_failed_models.read()
        # Add new failed models
        existing_failed.update(failed_models)
        # Write updated failed models
        self.cache_failed_models.write(existing_failed)

    def clear_failed_models(self, model_names: list[str] | None = None) -> None:
        """Clear failed model tracking for specific models or all models.

        Args:
            model_names: List of model names to clear from failed tracking,
                        or None to clear all failed models.

        """
        if not self.cache_failed_models.exists():
            return

        if model_names is None:
            # Clear all failed models
            self.cache_failed_models.clear()
            return

        # Load existing failed models
        existing_failed: set = self.cache_failed_models.read()

        # Remove specified models from failed set
        for model_name in model_names:
            existing_failed.discard(model_name)

        # Write updated failed models (or remove file if empty)
        if existing_failed:
            self.cache_failed_models.write(existing_failed)
        else:
            self.cache_failed_models.clear()

    @cached_property
    def cache_path(self) -> Path:
        """Instantiate the cacher, build folder if not exists."""
        p = settings.dbt_project_dir / ".dbt_toolbox"
        if not p.exists():
            p.mkdir()
        return p

    # ------------ Private internal properties ------------
    @cached_property
    def _cache_macro(self) -> _SetCache:
        return _SetCache(self.cache_path / "macro_watcher.cache")

    @cached_property
    def _cache_dbt_project(self) -> _ByteCache:
        return _ByteCache(self.cache_path / "dbt_project.cache")

    @cached_property
    def _cache_dbt_profile(self) -> _ByteCache:
        return _ByteCache(self.cache_path / "dbt_profile.cache")

    @cached_property
    def cache_jinja_env(self) -> _ByteCache:
        """Cache handler for jinja environment."""
        return _ByteCache(self.cache_path / "jinja_env.cache")

    @cached_property
    def cache_failed_models(self) -> _ByteCache:
        """Cache handler for failed models."""
        return _ByteCache(self.cache_path / "failed_models.cache")

    @cached_property
    def cache_models(self) -> _ByteCache:
        """Cache handler for dbt models."""
        return _ByteCache(self.cache_path / "models.cache")

    @cached_property
    def cache_macros(self) -> _ByteCache:
        """Cache handler for dbt macros."""
        return _ByteCache(self.cache_path / "macros.cache")

    def _validate_macro_cache(self) -> bool:
        """Check if any macro has changed since last execution."""
        cached_macros = self._cache_macro.read()
        current_macros = {macro.id for macro in self.macros_list}
        utils.log(
            f"Found {len(cached_macros)} CACHED macros and {len(current_macros)} CURRENT macros.",
        )
        self._cache_macro.write(current_macros)
        return cached_macros == current_macros

    def _validate_dbt_project_cache(self) -> bool:
        """Check if dbt_project yaml changed since last execution."""
        cached_project = self._cache_dbt_project.read()
        self._cache_dbt_project.write(self.dbt_project)
        return cached_project == self.dbt_project

    def _validate_dbt_profiles_cache(self) -> bool:
        """Check if dbt profiles yaml changed since last execution.

        Returns:
            True if profiles haven't changed, False otherwise.

        """
        cached_profiles = self._cache_dbt_profile.read()
        self._cache_dbt_profile.write(self.dbt_profiles)
        return cached_profiles == self.dbt_profiles

    # ---------------------------------------------------------
    # ------------ Public functions and properties ------------
    # ---------------------------------------------------------
    @cached_property
    def dbt_profiles(self) -> bytes:
        """Get dbt profiles yaml as string."""
        return settings.dbt_profiles_yaml_path.read_bytes()

    @cached_property
    def dbt_project(self) -> bytes:
        """Get dbt_project yaml file as string."""
        return settings.dbt_project_yaml_path.read_bytes()

    @cached_property
    def macros_dict(self) -> dict[str, list[MacroBase]]:
        """List all currently available macros."""
        return fetch_macros()

    @cached_property
    def macros_list(self) -> list[MacroBase]:
        """Get all macros as a list."""
        return [macro for macro_list in self.macros_dict.values() for macro in macro_list]

    def validate_jinja_environment(self) -> bool:
        """Validate the cache for everything related to jinja environment."""
        cache_validity = all(
            [
                self._validate_dbt_profiles_cache(),
                self._validate_dbt_project_cache(),
                self._validate_macro_cache(),
            ],
        )
        utils.log(f"Jinja environment cache_valid={cache_validity}")
        return cache_validity


cache = Cache()
