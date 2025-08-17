"""Utility class module."""

import os
from functools import cached_property
from pathlib import Path
from typing import Any, NamedTuple

import tomli
import yamlium
from jinja2 import Environment


class DbtProject:
    """Represents a dbt project configuration."""

    def __init__(self, dbt_project_path: Path) -> None:
        """Initialize by loading and parsing dbt_project.yml."""
        self.text = dbt_project_path.read_text()
        self.parsed: dict = yamlium.parse(self.text).to_dict()  # type: ignore
        self._rendered_parse: yamlium.Mapping | None = None

    def rendered_parse(self, env: Environment) -> yamlium.Mapping:
        """Parse the project file with Jinja rendering.

        Args:
            env: Jinja environment for rendering templates.

        Returns:
            Parsed and rendered project configuration.

        """
        if self._rendered_parse is None:
            self._rendered_parse = yamlium.parse(env.from_string(self.text).render())
        return self._rendered_parse

    @property
    def macro_paths(self) -> list[str]:
        """List of paths for macros."""
        return self.parsed.get("macro-paths", ["macros"])

    @property
    def model_paths(self) -> list[str]:
        """List of paths for models."""
        return self.parsed.get("model-paths", ["models"])

    @property
    def docs_paths(self) -> list[str]:
        """List of paths for documentation macros."""
        return self.parsed.get("docs-paths", ["docs"])

    @property
    def seed_paths(self) -> list[str]:
        """List of paths for seeds."""
        return self.parsed.get("seed-paths", ["seeds"])


class Setting(NamedTuple):
    """Information about where a setting value came from."""

    value: Any
    source: str
    location: str | None = None


def _find_dbt_project_root(start_path: Path | None = None) -> Path | None:
    """Find the dbt project root by searching for dbt_project.yml.

    Searches up the directory tree from the starting path to find
    a dbt_project.yml file, which indicates the dbt project root.

    Args:
        start_path: Path to start searching from. Defaults to current working directory.

    Returns:
        Path to dbt project root, or None if not found.

    """
    current = start_path or Path.cwd()
    for parent in [current, *list(current.parents)]:
        dbt_project_file = parent / "dbt_project.yml"
        if dbt_project_file.exists():
            return parent
    return None


def _find_toml_settings(filename: str = "pyproject.toml") -> tuple[dict, Path | None]:
    """Find and load dbt_toolbox settings from pyproject.toml.

    Searches up the directory tree from current working directory
    to find a pyproject.toml file with dbt_toolbox configuration.

    Args:
        filename: Name of the TOML file to search for.

    Returns:
        Tuple of (dictionary of dbt_toolbox settings, path to toml file).

    """
    current = Path.cwd()
    toml = None
    toml_path = None
    for parent in [current, *list(current.parents)]:
        p = parent / filename
        if p.exists():
            toml = tomli.loads(p.read_text())
            toml_path = p
            break
    if toml:
        return toml.get("tool", {}).get("dbt_toolbox", {}), toml_path
    return {}, None


toml, toml_file_path = _find_toml_settings()


def _get_env_var(name: str) -> str | None:
    """Get environment variable with dbt_toolbox naming convention.

    Args:
        name: Setting name (e.g., 'debug', 'dbt_project_dir').

    Returns:
        Environment variable value, or None if not found.

    """
    # If it's a dbt environment variable, try without prefix first.
    if name.startswith("dbt"):
        env_var = os.environ.get(name.upper())
        if env_var:
            return env_var
    return os.environ.get(f"DBT_TOOLBOX_{name}".upper())


def _get_setting(name: str, default: str | None = None, /) -> Setting:
    """Get setting value with source tracking and precedence: env var > toml > default.

    Args:
        name: Setting name.
        default: Default value if setting not found.

    Returns:
        SettingSource with value, source type, and location info.

    """
    # Check os envs
    env_setting = _get_env_var(name)
    if env_setting:
        env_var_name = name.upper() if name.startswith("dbt") else f"DBT_TOOLBOX_{name}".upper()
        return Setting(
            value=env_setting,
            source="environment variable",
            location=env_var_name,
        )

    toml_setting = toml.get(name)
    if toml_setting:
        return Setting(
            value=toml_setting,
            source="TOML file",
            location=str(toml_file_path) if toml_file_path else "pyproject.toml",
        )

    return Setting(value=default, source="default", location=None)


def _get_bool_setting(name: str, default: str, /) -> Setting:
    """Get boolean setting value with source tracking.

    Args:
        name: Setting name.
        default: Default value as string.

    Returns:
        SettingSource with boolean value and source info.

    """
    source = _get_setting(name, default)
    bool_value = str(source.value).lower() == "true"
    return Setting(value=bool_value, source=source.source, location=source.location)


def _get_list_setting(name: str, default: list[str] | None = None, /) -> Setting:
    """Get list setting value with source tracking.

    Args:
        name: Setting name.
        default: Default value as list.

    Returns:
        SettingSource with list value and source info.

    """
    if default is None:
        default = []

    source = _get_setting(name, None)

    if source.value is None:
        return Setting(value=default, source="default", location=None)

    # Handle different input formats
    if isinstance(source.value, list):
        # Already a list from TOML
        list_value = source.value
    elif isinstance(source.value, str):
        # String from environment variable - split by comma
        list_value = [item.strip() for item in source.value.split(",") if item.strip()]
    else:
        # Fallback to default for unexpected types
        list_value = default

    return Setting(value=list_value, source=source.source, location=source.location)


class Settings:
    """Collection of settings class."""

    @cached_property
    def _debug(self) -> Setting:
        return _get_bool_setting("debug", "false")

    @cached_property
    def debug(self) -> bool:
        """Debug flag."""
        return self._debug.value

    @cached_property
    def _cache_path(self) -> Setting:
        return _get_setting("cache_path", str(self.dbt_project_dir / ".dbt_toolbox"))

    @cached_property
    def cache_path(self) -> Path:
        """Get the path to the cache."""
        return Path(self._cache_path.value)

    @cached_property
    def _dbt_project_dir(self) -> Setting:
        """Get dbt project directory with intelligent path resolution."""
        configured_setting = _get_setting("dbt_project_dir", None)

        if configured_setting.value:
            # If explicitly configured, resolve the path
            configured_path = Path(configured_setting.value)
            if configured_path.is_absolute():
                return configured_setting
            # If relative, resolve from current directory
            resolved_path = Path.cwd() / configured_path
            return Setting(
                value=str(resolved_path.resolve()),
                source=configured_setting.source,
                location=configured_setting.location,
            )

        # If not configured, try to auto-detect dbt project root
        detected_root = _find_dbt_project_root()
        if detected_root:
            return Setting(
                value=str(detected_root),
                source="auto-detected",
                location="dbt_project.yml",
            )

        # Fallback to current directory
        return Setting(value=".", source="default", location=None)

    @cached_property
    def dbt_project_dir(self) -> Path:
        """Get dbt project directory."""
        return Path(self._dbt_project_dir.value)

    @cached_property
    def _dbt_profiles_dir(self) -> Setting:
        """Get dbt profiles directory with default same as dbt_project_dir."""
        return _get_setting("dbt_profiles_dir", str(self.dbt_project_dir))

    @cached_property
    def dbt_profiles_dir(self) -> Path:
        """Get dbt profiles directory."""
        return Path(self._dbt_profiles_dir.value)

    @cached_property
    def _skip_placeholders(self) -> Setting:
        """Whether to skip setting placeholder descriptions."""
        return _get_bool_setting("skip_placeholder", "false")

    @cached_property
    def skip_placeholders(self) -> bool:
        """Whether to skip setting placeholder descriptions."""
        return self._skip_placeholders.value

    @cached_property
    def _placeholder_description(self) -> Setting:
        """Get placeholder description."""
        return _get_setting("placeholder_description", "TODO: PLACEHOLDER")

    @cached_property
    def placeholder_description(self) -> str:
        """Get placeholder description."""
        return self._placeholder_description.value

    @cached_property
    def _dbt_project_yaml_path(self) -> Setting:
        """The path to the dbt project yaml."""
        return Setting(value=self.dbt_project_dir / "dbt_project.yml", source="default")

    @cached_property
    def dbt_project_yaml_path(self) -> Path:
        """The path to the dbt project yaml."""
        return self._dbt_project_yaml_path.value

    @cached_property
    def _dbt_profiles_yaml_path(self) -> Setting:
        """The path to the dbt profiles yaml."""
        return Setting(value=self.dbt_profiles_dir / "profiles.yml", source="default")

    @cached_property
    def dbt_profiles_yaml_path(self) -> Path:
        """The path to the dbt profiles yaml."""
        return self._dbt_profiles_yaml_path.value

    @cached_property
    def dbt_project(self) -> DbtProject:
        """Reference to the dbt project."""
        return DbtProject(self.dbt_project_yaml_path)

    @cached_property
    def _cache_validity_minutes(self) -> Setting:
        return _get_setting("cache_validity_minutes", "1440")

    @cached_property
    def cache_validity_minutes(self) -> int:
        """The cache validity in minutes, default 1440 (one day)."""
        return int(self._cache_validity_minutes.value)

    @cached_property
    def _enforce_lineage_validation(self) -> Setting:
        return _get_bool_setting("enforce_lineage_validation", "true")

    @cached_property
    def enforce_lineage_validation(self) -> bool:
        """Whether to enforce lineage validation before running dbt build/run."""
        return self._enforce_lineage_validation.value

    @cached_property
    def _models_ignore_validation(self) -> Setting:
        return _get_list_setting("models_ignore_validation", [])

    @cached_property
    def models_ignore_validation(self) -> list[str]:
        """List of model names to ignore during validation checks."""
        return self._models_ignore_validation.value

    def get_all_settings_with_sources(self) -> dict[str, Setting]:
        """Get all settings with their source information.

        Returns:
            Dictionary mapping setting names to Setting objects.

        """
        return {
            setting: getattr(self, f"_{setting}")
            for setting in [
                "debug",
                "cache_path",
                "dbt_project_dir",
                "dbt_profiles_dir",
                "skip_placeholders",
                "placeholder_description",
                "cache_validity_minutes",
                "enforce_lineage_validation",
                "models_ignore_validation",
            ]
        }


settings = Settings()
