"""Analyze settings module."""

from dbt_toolbox.run_config import RunConfig
from dbt_toolbox.settings import Setting, settings


def get_all_settings(target: str | None = None) -> dict[str, Setting]:
    """Get a list of all available settings."""
    return {
        **settings.get_all_settings_with_sources(),
        **RunConfig(target=target).get_all_config_with_sources(),
    }
