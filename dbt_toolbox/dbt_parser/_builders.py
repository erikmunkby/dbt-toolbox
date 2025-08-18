"""Builder helper functions."""

import re
from pathlib import Path
from typing import Any

from jinja2.nodes import Call, Const, Dict, Keyword, Output
from sqlglot import parse_one
from sqlglot.optimizer import optimize

from dbt_toolbox.data_models import DependsOn, Macro, MacroBase, Model, ModelBase, YamlDocs
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


def fetch_macros_from_source(folder: Path, source: str) -> list[MacroBase]:
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


def build_macro(m: MacroBase) -> Macro:
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


def _parse_config_kwargs(obj: Any) -> Any:  # noqa: ANN401
    """Recursively parse the jinja config block."""
    if isinstance(obj, Const):
        return obj.value
    if isinstance(obj, Keyword):
        return {obj.key: _parse_config_kwargs(obj.value)}
    if isinstance(obj, list):
        return [_parse_config_kwargs(x) for x in obj]
    if isinstance(obj, Dict):
        return {pair.key.value: _parse_config_kwargs(pair.value) for pair in obj.items}  # type: ignore
    return None


def _get_project_config_for_model(model_path: Path, dbt_project_dict: dict) -> dict:
    """Extract project-level configuration for a specific model path.

    Args:
        model_path: Path to the model file relative to models directory
        dbt_project_dict: The dbt_project.yml as a dictionary

    Returns:
        Dictionary of configuration from dbt_project.yml for this model path

    """
    # Get project-level model configuration
    models_config = dbt_project_dict.get("models", {})
    if not models_config:
        return {}

    # Get project name from dbt_project.yml
    project_name = dbt_project_dict.get("name")
    if not project_name or project_name not in models_config:
        return {}

    # Build path hierarchy for configuration lookup
    # Convert model path to configuration path
    # e.g., models/marts/finance/revenue.sql -> ['marts', 'finance']
    model_parts = model_path.parent.parts
    if model_parts and model_parts[0] == "models":
        model_parts = model_parts[1:]  # Remove 'models' prefix

    # Traverse configuration hierarchy from general to specific
    current_config = models_config[project_name]
    project_config = {}

    # Apply project-level defaults first
    for key, value in current_config.items():
        if key.startswith("+"):
            config_key = key[1:]  # Remove '+' prefix
            project_config[config_key] = value

    # Apply path-specific configurations
    for part in model_parts:
        if part in current_config and isinstance(current_config[part], dict):
            current_config = current_config[part]
            # Apply configurations with '+' prefix at this level
            for key, value in current_config.items():
                if key.startswith("+"):
                    config_key = key[1:]  # Remove '+' prefix
                    project_config[config_key] = value

    return project_config


def _resolve_model_config(
    model_path: Path,
    config_kwargs: dict,
    dbt_project_dict: dict,
    yaml_docs: YamlDocs | None = None,
) -> dict:
    """Resolve model configuration using dbt's precedence hierarchy.

    Configuration precedence (highest to lowest):
    1. Jinja config block in model file: {{ config(...) }}
    2. YAML documentation config: schema.yml model config
    3. dbt_project.yml models configuration with path-based inheritance

    Args:
        model_path: Path to the model file relative to models directory
        config_kwargs: Configuration from Jinja config block in model
        dbt_project_dict: The dbt_project.yml as a dictionary
        yaml_docs: Optional YAML documentation containing model config

    Returns:
        Resolved configuration dictionary with all applicable settings

    """
    # Start with project-level configuration (lowest precedence)
    resolved_config = _get_project_config_for_model(model_path, dbt_project_dict)

    # Apply YAML docs config (middle precedence)
    if yaml_docs is not None and yaml_docs.config:
        resolved_config.update(yaml_docs.config)

    # Apply Jinja config block (highest precedence)
    resolved_config.update(config_kwargs)

    # Set default materialization if not specified
    if "materialized" not in resolved_config:
        resolved_config["materialized"] = "view"

    return resolved_config


def build_model(
    m: ModelBase,
    jinja: Jinja,
    sql_dialect: str,
    dbt_project_dict: dict,
    yaml_docs: YamlDocs | None,
) -> Model:
    """Build a complete Model object from a ModelBase.

    Parses Jinja templates to extract dependencies, renders the code,
    and creates optimized SQL representations.

    Args:
        m: Base model containing name, path, and raw code.
        jinja: A jinja environment
        sql_dialect: The sql dialect
        dbt_project_dict: The dbt project as a dictionary
        yaml_docs: Optional YAML documentation containing model config

    Returns:
        Complete Model object with dependencies and SQL parsing.

    Raises:
        NotImplementedError: If source() calls are found (not yet supported).

    """
    deps = DependsOn()
    config_kwargs = {}
    for obj in jinja.parse(m.raw_code).body:
        if not isinstance(obj, Output):
            continue
        for node in obj.nodes:
            if isinstance(node, Call):
                node_name: str = node.node.name  # type: ignore
                # When we find {{ ref() }}
                if node_name == "config":
                    for kwarg in _parse_config_kwargs(node.kwargs):
                        config_kwargs.update(kwarg)
                elif node_name == "ref":
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
        config=_resolve_model_config(
            model_path=m.path,
            config_kwargs=config_kwargs,
            dbt_project_dict=dbt_project_dict,
            yaml_docs=yaml_docs,
        ),
        upstream=deps,
        yaml_docs=yaml_docs,
        glot_code=glot_code,  # type: ignore
        optimized_glot_code=optimized_glot_code,  # type: ignore
        column_references=resolve_column_lineage(glot_code),
    )
