"""Builder helper functions."""

import re
from pathlib import Path

from jinja2.nodes import Call, Output
from sqlglot import parse_one
from sqlglot.optimizer import optimize

from dbt_toolbox.data_models import DependsOn, Macro, MacroBase, Model, ModelBase
from dbt_toolbox.dbt_parser._column_resolver import resolve_column_lineage
from dbt_toolbox.dbt_parser._jinja_handler import Jinja
from dbt_toolbox.utils import list_files, log


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
    log(f"Loading macros from folder: {folder}")
    macros = []

    for path in list_files(folder, ".sql"):
        for macro in _parse_macros_from_file(path).values():
            macro.source = source
            macros.append(macro)

    return macros


def _build_macro(m: MacroBase) -> Macro:
    """Build a complete Macro object from a MacroBase.

    Args:
        m: Base macro containing name, path, and raw code.

    Returns:
        Complete Macro object with execution timestamp.

    """
    return Macro(
        source=m.source,
        file_name=m.file_name,
        name=m.name,
        raw_code=m.raw_code,
        macro_path=m.macro_path,
    )


def _build_model(m: ModelBase, jinja: Jinja, sql_dialect: str) -> Model:
    """Build a complete Model object from a ModelBase.

    Parses Jinja templates to extract dependencies, renders the code,
    and creates optimized SQL representations.

    Args:
        m: Base model containing name, path, and raw code.
        jinja: A jinja environment
        sql_dialect: The sql dialect

    Returns:
        Complete Model object with dependencies and SQL parsing.

    Raises:
        NotImplementedError: If source() calls are found (not yet supported).

    """
    deps = DependsOn()
    for obj in jinja.parse(m.raw_code).body:
        if not isinstance(obj, Output):
            continue
        for node in obj.nodes:
            if isinstance(node, Call):
                node_name: str = node.node.name  # type: ignore
                # When we find {{ ref() }}
                if node_name == "ref":
                    deps.models.append(node.args[0].value)  # type: ignore
                # When we find {{ source() }}
                elif node_name == "source":
                    min_source_args = 2  # source('source_name', 'table_name')
                    if len(node.args) >= min_source_args:
                        source_name = node.args[0].value  # type: ignore
                        table_name = node.args[1].value  # type: ignore
                        deps.sources.append(f"{source_name}__{table_name}")
                # When we find any other e.g. {{ my_macro() }}
                else:
                    deps.macros.append(node_name)
    rendered_code = jinja.render(m.raw_code)
    glot_code = parse_one(rendered_code, dialect=sql_dialect)  # type: ignore
    try:
        optimized_glot_code = optimize(glot_code, dialect=sql_dialect)
    except:  # noqa: E722
        optimized_glot_code = None

    return Model(
        name=m.name,
        raw_code=m.raw_code,
        path=m.path,
        rendered_code=rendered_code,
        upstream=deps,
        glot_code=glot_code,  # type: ignore
        optimized_glot_code=optimized_glot_code,  # type: ignore
        column_references=resolve_column_lineage(glot_code),
    )
