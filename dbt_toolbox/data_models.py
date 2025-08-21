"""Module collecting all data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property
from hashlib import md5
from typing import TYPE_CHECKING, Any

import yamlium
from typing_extensions import Self

from dbt_toolbox.constants import EXECUTION_TIMESTAMP
from dbt_toolbox.settings import settings

if TYPE_CHECKING:
    from pathlib import Path

    from sqlglot.expressions import Select


class DbtProfile:
    """Represents a dbt profile configuration with dynamic properties."""

    type: str

    def __init__(self, target: str | None = None) -> None:
        """Build a dynamic property factory for dbt target.

        Loads the profiles.yml file, finds the default target, and
        dynamically sets all target properties as instance attributes.
        """
        yaml = yamlium.parse(settings.dbt_profiles_yaml_path)
        # If we don't have a specified target, find default target
        if target is None:
            for k, v, _ in yaml.walk_keys():
                if k == "target":
                    target = str(v)
                    break

        if target is None:
            raise ValueError("No target found.")

        # Next up, find the values behind the target
        values = None
        for k, v, _ in yaml.walk_keys():
            if k == target:
                values = v
                break

        if values is None:
            raise ValueError(f"Target `{target}` not found.")

        # Set dynamic typing on the profile
        for key, value in values.to_dict().items():  # type: ignore
            setattr(self, key, value)
        self.name = target  # Set the target as the name


@dataclass
class CLIOptions:
    """Common options for all commands."""

    target: str | None = None


class TableType(Enum):
    """Different types of tables."""

    EXTERNAL = "external"
    CTE = "cte"
    SUBQUERY = "subquery"
    AMBIGUOUS = "ambiguous"


@dataclass
class Table:
    """A table reference dataclass."""

    name: str
    type: TableType
    available_columns: list[str] = field(default_factory=list)


@dataclass(kw_only=True)
class ColumnReference:
    """Metadata about each column."""

    name: str
    reference_type: TableType
    table: str | None = None
    resolved: bool | None = None
    context: list[str] | None = None
    id: int


@dataclass
class MacroBase:
    """A macro with name and raw code."""

    file_name: str
    name: str
    raw_code: str
    macro_path: Path
    source: str | None = None

    @property
    def is_test(self) -> bool:
        """Whether the macro is a test macro."""
        return "{% test" in self.raw_code or "{%- test" in self.raw_code

    @property
    def code_hash(self) -> str:
        """Get id as name+hash of macro."""
        return self.name + md5(self.raw_code.encode()).hexdigest()[:5]  # noqa: S324

    @property
    def code(self) -> str:
        """Get the macro code as text."""
        return self.raw_code


@dataclass
class Macro(MacroBase):
    """Macro storage class."""

    # This flag will show when the macro was last built.
    # Used to check when to invalidate cache and execute model.
    last_built: datetime = EXECUTION_TIMESTAMP


@dataclass
class DependsOn:
    """List of a model's dependencies."""

    sources: list[str] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    macros: list[str] = field(default_factory=list)


@dataclass
class ModelBase:
    """The baseline model with id."""

    name: str
    path: Path
    raw_code: str

    @property
    def code_hash(self) -> str:
        """Get a model's hash based on name and code."""
        return self.name + md5(self.raw_code.encode()).hexdigest()[:5]  # noqa: S324


@dataclass
class ColDocs:
    """Column documentation."""

    name: str
    description: str | None
    raw_description: str | None


@dataclass
class ColumnChanges:
    """Column changes detected between existing and new columns."""

    added: list[str]
    removed: list[str]
    reordered: bool


@dataclass
class DbtExecutionParams:
    """Parameters for dbt execution commands (build, run, etc.)."""

    command_name: str
    model: str | None = None
    full_refresh: bool = False
    threads: int | None = None
    vars: str | None = None
    target: str | None = None
    analyze_only: bool = False
    disable_smart: bool = False

    def to_execute_kwargs(self, command_name: str | None = None) -> dict[str, Any]:
        """Convert to kwargs for execute_dbt_with_smart_selection."""
        return {
            "command_name": command_name or self.command_name,
            "model": self.model,
            "full_refresh": self.full_refresh,
            "threads": self.threads,
            "vars": self.vars,
            "target": self.target,
            "analyze_only": self.analyze_only,
            "disable_smart": self.disable_smart,
        }


@dataclass
class Source:
    """A dbt source table."""

    name: str
    source_name: str
    description: str | None
    path: Path
    columns: list[ColDocs]

    @property
    def full_name(self) -> str:
        """Get the full source name as source_name__table_name."""
        return f"{self.source_name}__{self.name}"

    @property
    def compiled_columns(self) -> list[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]


@dataclass
class Seed:
    """A dbt seed CSV file."""

    name: str
    path: Path

    @property
    def id(self) -> str:
        """Get id as name+hash of file modification time."""
        stat = self.path.stat()
        return self.name + md5(str(stat.st_mtime).encode()).hexdigest()[:5]  # noqa: S324


@dataclass
class YamlDocs:
    """Documentation from a model yaml."""

    model_description: str | None
    path: Path
    config: dict
    columns: list[ColDocs] | None


@dataclass
class Model(ModelBase):
    """A model object."""

    rendered_code: str
    glot_code: Select
    upstream: DependsOn
    # The configuration found in jinja config block or dbt_project.yml
    config: dict = field(default_factory=dict)
    # Column references are ALL selects within the model
    # along with their origins.
    column_references: list[ColumnReference] | None = None
    optimized_glot_code: Select | None = None
    yaml_docs: YamlDocs | None = None
    _yaml_docs_index: int | None = None
    # This flag will show when the model was last built.
    # Used to check when to invalidate cache and execute model.
    last_built: datetime | None = None
    # Flag indicating whether the most recent build was successful
    # None = never attempted, True = successful, False = failed
    last_build_failed: bool | None = None
    # Will be set when we discover code hash differences
    code_changed: bool = False
    # Will be set when we discover upstream macro changes
    upstream_macros_changed: bool = False
    # Time it took to compute the model. Stored after a build.
    compute_time_seconds: float | None = None

    def set_build_successful(self, compute_time_seconds: float) -> None:
        """Flag that the model built and everything is fresh."""
        self.last_built = EXECUTION_TIMESTAMP
        self.last_build_failed = False
        self.code_changed = False
        self.upstream_macros_changed = False
        self.compute_time_seconds = compute_time_seconds

    def set_build_failed(self) -> None:
        """Flag the model as last build failed."""
        self.last_built = EXECUTION_TIMESTAMP
        self.last_build_failed = True

    def copy_attributes(self, other_model: Model) -> Self:
        """Copy attributes not yet set from other model.

        Args:
            other_model: The model to copy attributes from.

        """
        for attr, val in other_model.__dict__.items():
            if getattr(self, attr, None) is None:
                setattr(self, attr, val)
        return self

    @property
    def cache_outdated(self) -> bool:
        """Check whether the cache has timed out or not."""
        if self.last_built is None:
            return True
        return (
            self.last_built + timedelta(minutes=settings.cache_validity_minutes)
            < EXECUTION_TIMESTAMP
        )

    @property
    def is_fresh(self) -> bool:
        """Check if the cache is fresh and the model was built successfully.

        Returns:
            True if the model was built successfully and cache is recent.
            False if never built (None) or failed (False) or cache is stale.

        """
        # Check if build was successful (None = never built, False = failed)
        return not (
            self.last_build_failed is None
            or self.last_built is None
            or self.last_build_failed
            or self.code_changed
            or self.cache_outdated
            or self.upstream_macros_changed
        )

    @cached_property
    def final_columns(self) -> list[str]:
        """The final selected columns or "output" of the model.

        Not to be confused with .column_references which consists
        of all selects within the model, and their origins.
        """
        cols = (
            self.optimized_glot_code.selects
            if self.optimized_glot_code
            else self.glot_code.selects
        )
        return [col.alias_or_name for col in cols]

    @property
    def column_descriptions(self) -> list[ColDocs]:
        """Get all column descriptions."""
        if not self.yaml_docs or not self.yaml_docs.columns:
            return []
        return self.yaml_docs.columns

    @property
    def documented_columns(self) -> list[str]:
        """All documented columns."""
        return [c.name for c in self.column_descriptions]

    @property
    def columns_missing_description(self) -> list[str]:
        """Columns that are missing a description."""
        return [c for c in self.final_columns if c not in self.documented_columns]

    @property
    def superfluent_column_descriptions(self) -> list[str]:
        """Columns that are described but not in model."""
        return [c for c in self.documented_columns if c not in self.final_columns]

    @cached_property
    def load_yaml(self) -> yamlium.Mapping | None:
        """Load the full yaml containing the model."""
        if not self.yaml_docs:
            return None
        return yamlium.parse(self.yaml_docs.path)

    @property
    def load_model_yaml(self) -> tuple[int, yamlium.Mapping | None]:
        """Load the model's yaml object and return its index and content.

        Returns:
            Tuple of (index, yaml_mapping) where index is the position in the models list
            and yaml_mapping is the model's yaml configuration, or (0, None) if not found.

        """
        if not self.load_yaml:
            return 0, None
        for i, m in enumerate(self.load_yaml["models"]):
            if m["name"] == self.name:
                self._yaml_docs_index = i
                return i, m  # type: ignore
        return 0, None

    def update_model_yaml(
        self,
        yml: yamlium.Mapping,
    ) -> None:
        """Update the model's yaml configuration in the schema file.

        Args:
            yml: The new yaml configuration to write for this model.

        Raises:
            ValueError: If no yaml docs are found for the model.

        """
        if self._yaml_docs_index is None:
            self.load_model_yaml  # noqa: B018

        full_yaml = self.load_yaml
        if full_yaml is None or self.yaml_docs is None:
            raise ValueError("No yaml docs found.")
        yml_models = full_yaml["models"]  # type: ignore
        yml_models = [
            *yml_models[0 : self._yaml_docs_index],  # type: ignore
            yml,
            *yml_models[self._yaml_docs_index + 1 :],  # type: ignore
        ]

        full_yaml["models"] = yml_models
        self.yaml_docs.path.write_text(
            "\n".join([x for x in full_yaml.to_yaml().split("\n") if x]) + "\n",
        )
