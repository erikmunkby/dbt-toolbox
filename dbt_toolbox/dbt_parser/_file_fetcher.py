"""Collection of file fetching functions."""

import re
from collections import defaultdict
from pathlib import Path

from dbt_toolbox import utils
from dbt_toolbox.constants import CUSTOM_MACROS
from dbt_toolbox.data_models import MacroBase, ModelBase
from dbt_toolbox.settings import settings


def _parse_macros_from_file(file_path: Path) -> dict[str, MacroBase]:
    """Parse individual macros from a SQL file.

    Args:
        file_path: Path to the SQL file containing macros.

    Returns:
        List of tuples containing (macro_name, macro_code) for each macro found.

    """
    if not file_path.exists():
        return {}
    content = file_path.read_text()

    # Regex to match macro definitions
    # Matches: {% macro macro_name(...) %} ... {% endmacro %}
    # Also handles {%- macro ... -%} variations
    macro_pattern = re.compile(
        r"{%\s*-?\s*macro\s+(\w+)\s*\([^)]*\)\s*-?\s*%}(.*?){%\s*-?\s*endmacro\s*-?\s*%}",
        re.DOTALL | re.IGNORECASE,
    )

    macros = {}
    for match in macro_pattern.finditer(content):
        macro_name = match.group(1)
        macro_code = match.group(0)  # Full macro including {% macro %} and {% endmacro %}
        macros[macro_name] = MacroBase(
            file_name=file_path.stem,
            name=macro_name,
            raw_code=macro_code,
            macro_path=file_path,
        )

    return macros


def _fetch_macros_from_source(folder: Path, source: str) -> list[MacroBase]:
    """Fetch all individual macros from a specific folder.

    Args:
        folder: Path to the folder containing macro files.
        source: Source identifier for the macros (e.g., 'custom', package name).

    Returns:
        List of MacroBase objects representing all individual macros found in .sql files.

    """
    utils.log(f"Loading macros from folder: {folder}")
    macros = []

    for path in utils.list_files(folder, ".sql"):
        for macro in _parse_macros_from_file(path).values():
            macro.source = source
            macros.append(macro)

    return macros


def read_macro(macro_name: str, path: Path) -> MacroBase | None:
    return _parse_macros_from_file(path).get(macro_name)


def read_macros() -> dict[str, list[MacroBase]]:
    """Get all macros of the project from custom and package sources.

    Scans both the project's macro paths and any installed dbt packages
    to collect all available macro files.

    Returns:
        Dictionary mapping source names to lists of RawMacro objects.
        Keys include 'custom' for project macros and package names for
        installed packages.

    """
    macros = defaultdict(list)

    for folder in settings.dbt_project.macro_paths:
        macros[CUSTOM_MACROS].extend(
            _fetch_macros_from_source(folder=utils.build_path(folder), source=CUSTOM_MACROS),
        )

    packages_path = utils.build_path("dbt_packages")
    if packages_path.exists():
        for folder in packages_path.iterdir():
            macros[folder.stem] = _fetch_macros_from_source(
                folder=folder / "macros",
                source=folder.stem,
            )

    return macros


def read_models() -> list[ModelBase]:
    """Fetch all dbt model files from the project.

    Scans all configured model paths in the dbt project to collect
    SQL model files and create ModelBase objects.

    Returns:
        List of ModelBase objects representing all .sql model files
        found in the project's model paths.

    """
    return [
        ModelBase(name=file_path.stem, path=file_path, raw_code=file_path.read_text())
        for path in settings.dbt_project.model_paths
        for file_path in utils.list_files(path=path, file_suffix=".sql")
    ]
