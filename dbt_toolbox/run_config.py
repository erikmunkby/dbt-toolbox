"""Module for handling runtime configurations."""

import sys
from functools import cached_property

import typer

from dbt_toolbox.data_models import DbtProfile
from dbt_toolbox.settings import Setting, settings


class RunConfig:
    """Runtime configuration based on flags."""

    def __init__(self, target: str | None = None) -> None:
        """Instantiate runtime config."""
        self.target = target
        self.dbt_profile = DbtProfile(target=target)

    @cached_property
    def _sql_dialect(self) -> Setting:
        if hasattr(self.dbt_profile, "type"):
            return Setting(
                value=self.dbt_profile.type,
                source="dbt",
                location=str(settings.dbt_profiles_yaml_path),
            )
        typer.secho("dbt dialect must be set.", fg=typer.colors.RED)
        sys.exit(1)

    @cached_property
    def sql_dialect(self) -> str:
        """The sql dialect used by dbt."""
        return self._sql_dialect.value

    @cached_property
    def _dbt_target(self) -> Setting:
        if self.target:
            return Setting(
                value=self.target,
                source="dbt",
                location="--target",
            )
        return Setting(
            value=self.dbt_profile.name,
            source="dbt",
            location=str(settings.dbt_profiles_yaml_path),
        )

    def get_all_config_with_sources(self) -> dict[str, Setting]:
        """Get all runtime configs with their source information.

        Returns:
            Dictionary mapping setting names to Setting objects.

        """
        return {
            config: getattr(self, f"_{config}")
            for config in [
                "dbt_target",
                "sql_dialect",
            ]
        }
