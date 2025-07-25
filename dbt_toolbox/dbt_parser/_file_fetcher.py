"""Collection of file fetching functions."""

import re
from collections import defaultdict
from pathlib import Path

from dbt_toolbox.constants import CUSTOM_MACROS
from dbt_toolbox.data_models import MacroBase, ModelBase
from dbt_toolbox.utils import utils


def _parse_macros_from_file(file_path: Path) -> list[tuple[str, str]]:
    """Parse individual macros from a SQL file.

    Args:
        file_path: Path to the SQL file containing macros.

    Returns:
        List of tuples containing (macro_name, macro_code) for each macro found.

    """
    content = file_path.read_text()

    # Regex to match macro definitions
    # Matches: {% macro macro_name(...) %} ... {% endmacro %}
    # Also handles {%- macro ... -%} variations
    macro_pattern = re.compile(
        r"{%\s*-?\s*macro\s+(\w+)\s*\([^)]*\)\s*-?\s*%}(.*?){%\s*-?\s*endmacro\s*-?\s*%}",
        re.DOTALL | re.IGNORECASE,
    )

    macros = []
    for match in macro_pattern.finditer(content):
        macro_name = match.group(1)
        macro_code = match.group(0)  # Full macro including {% macro %} and {% endmacro %}
        macros.append((macro_name, macro_code))

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
        file_macros = _parse_macros_from_file(path)
        for macro_name, macro_code in file_macros:
            macros.append(
                MacroBase(
                    file_name=path.stem,
                    name=macro_name,
                    raw_code=macro_code,
                    macro_path=path,
                    source=source,
                ),
            )

    return macros


def fetch_macros() -> dict[str, list[MacroBase]]:
    """Get all macros of the project from custom and package sources.

    Scans both the project's macro paths and any installed dbt packages
    to collect all available macro files.

    Returns:
        Dictionary mapping source names to lists of RawMacro objects.
        Keys include 'custom' for project macros and package names for
        installed packages.

    """
    macros = defaultdict(list)

    for folder in utils.dbt_project.macro_paths:
        macros[CUSTOM_MACROS].extend(
            _fetch_macros_from_source(folder=utils.path(folder), source=CUSTOM_MACROS),
        )

    packages_path = utils.path("dbt_packages")
    if packages_path.exists():
        for folder in packages_path.iterdir():
            macros[folder.stem] = _fetch_macros_from_source(
                folder=folder / "macros",
                source=folder.stem,
            )

    return macros


def fetch_models() -> list[ModelBase]:
    """Fetch all dbt model files from the project.

    Scans all configured model paths in the dbt project to collect
    SQL model files and create ModelBase objects.

    Returns:
        List of ModelBase objects representing all .sql model files
        found in the project's model paths.

    """
    return [
        ModelBase(name=file_path.stem, path=file_path, raw_code=file_path.read_text())
        for path in utils.dbt_project.model_paths
        for file_path in utils.list_files(path=path, file_suffix=".sql")
    ]
