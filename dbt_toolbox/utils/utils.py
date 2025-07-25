"""Utility functions module."""

from functools import cached_property
from pathlib import Path
from typing import Literal

from jinja2 import Environment
from loguru import logger
from yamlium import Mapping, parse

from dbt_toolbox.settings import settings


class _DbtProject:
    """Represents a dbt project configuration."""

    def __init__(self) -> None:
        """Initialize by loading and parsing dbt_project.yml."""
        self.text = settings.dbt_project_yaml_path.read_text()
        self.parsed: dict = parse(self.text).to_dict()  # type: ignore

    def rendered_parse(self, env: Environment) -> Mapping:
        """Parse the project file with Jinja rendering.

        Args:
            env: Jinja environment for rendering templates.

        Returns:
            Parsed and rendered project configuration.

        """
        return parse(env.from_string(self.text).render())

    @property
    def macro_paths(self) -> list[str]:
        return self.parsed.get("macro-paths", ["macros"])

    @property
    def model_paths(self) -> list[str]:
        return self.parsed.get("model-paths", ["models"])

    @property
    def docs_paths(self) -> list[str]:
        return self.parsed.get("docs-paths", ["docs"])


class _DbtProfile:
    """Represents a dbt profile configuration with dynamic properties."""

    type: str

    def __init__(self) -> None:
        """Build a dynamic property factory for dbt target.

        Loads the profiles.yml file, finds the default target, and
        dynamically sets all target properties as instance attributes.
        """
        default_target = None
        # Find the default target
        for k, v, _ in parse(settings.dbt_profiles_yaml_path).walk_keys():
            if k == "target":
                default_target = str(v)
            if default_target and k == default_target:
                break

        # Set dynamic typing on the profile
        for key, value in v.to_dict().items():  # type: ignore
            setattr(self, key, value)
        self.name = default_target


class Utils:
    """Utility class."""

    @cached_property
    def dbt_project(self) -> _DbtProject:
        """Get dbt project."""
        return _DbtProject()

    @cached_property
    def dbt_profile(self) -> _DbtProfile:
        """Get dbt profile."""
        return _DbtProfile()

    def list_files(self, path: Path | str, file_suffix: str | list[str]) -> list[Path]:
        """Do a glob search of files using file type.

        Args:
            path: Directory path to search in.
            file_suffix: File suffix(es) to match (e.g., '.sql', ['.yml', '.yaml']).

        Returns:
            List of matching file paths.

        """
        if isinstance(path, str):
            path = self.path(path)
        if isinstance(file_suffix, str):
            file_suffix = [file_suffix]
        return [p for suffix in file_suffix for p in path.rglob(f"*{suffix}")]

    @cached_property
    def model_paths(self) -> list[Path]:
        """Get a list of all model paths."""
        return [
            path
            for model_path in self.dbt_project.model_paths
            for path in self.list_files(model_path, [".sql"])
        ]

    @cached_property
    def model_yaml_paths(self) -> list[Path]:
        """Get a list of all model yaml paths."""
        return [
            path
            for model_path in self.dbt_project.model_paths
            for path in self.list_files(model_path, [".yml", ".yaml"])
        ]

    @cached_property
    def docs_macros_paths(self) -> list[Path]:
        """Get a list of all docs macros paths."""
        return [
            path
            for docs_path in self.dbt_project.docs_paths
            for path in self.list_files(docs_path, file_suffix=".md")
        ]

    def path(self, path: str | Path, /) -> Path:
        """Construct a path relative to the dbt project directory.

        Args:
            path: Relative path from the dbt project root.

        Returns:
            Absolute Path object.

        """
        return settings.path(path)

    def log(self, msg: str, level: Literal["INFO", "DEBUG", "WARN"] = "DEBUG") -> None:
        """Log a message at the specified level.

        Args:
            msg: Message to log.
            level: Log level (INFO, DEBUG, WARN). DEBUG messages only show
                   when debug mode is enabled in settings.

        """
        if settings.debug and level == "DEBUG":
            logger.debug(msg)
        elif level == "INFO":
            logger.info(msg)
        elif level == "WARN":
            logger.warning(msg)


utils = Utils()
