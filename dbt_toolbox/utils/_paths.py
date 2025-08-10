from pathlib import Path

from dbt_toolbox.settings import settings


def build_path(path: str | Path, /) -> Path:
    """Construct a path relative to the dbt project directory.

    Args:
        path: Relative path from the dbt project root.

    Returns:
        Absolute Path object.

    """
    return settings.dbt_project_dir / path


def list_files(path: Path | str, file_suffix: str | list[str]) -> list[Path]:
    """Do a glob search of files using file type.

    Args:
    path: Directory path to search in.
    file_suffix: File suffix(es) to match (e.g., '.sql', ['.yml', '.yaml']).

    Returns:
    List of matching file paths.

    """
    if isinstance(path, str):
        path = build_path(path)
    if isinstance(file_suffix, str):
        file_suffix = [file_suffix]
    return [p for suffix in file_suffix for p in path.rglob(f"*{suffix}")]
