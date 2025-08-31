"""Module for mcp server."""

import json
from dataclasses import asdict
from typing import Literal

from fastmcp import FastMCP

from dbt_toolbox.actions.all_settings import get_all_settings
from dbt_toolbox.actions.analyze_columns_references import analyze_column_references
from dbt_toolbox.actions.build_docs import YamlBuilder
from dbt_toolbox.actions.dbt_executor import create_execution_plan
from dbt_toolbox.data_models import DbtExecutionParams
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.utils import dict_utils

mcp_server = FastMCP("dbt-toolbox")


@mcp_server.tool()
def analyze_models(target: str | None = None, model: str | None = None) -> str:
    """Analyze and validate all models in the dbt project.

    This will analyze and make sure all model references, column references and CTE references
    are valid. Use this tool frequently in order to verify that no incorrect selections are made.

    If there are models with a large amount of errors, you can ask the user if they want the model
    to be ignored. This can be configured in the pyproject.toml settings via:

    [tool.dbt_toolbox]
    models_ignore_validation = ["my_model"]

    Args:
        target: Specify dbt target environment
        model: Select models to analyze (same as dbt --select/--model)

    Example output with descriptions:
    {
        "overall_status": "ISSUES_FOUND", # One of "OK" or "ISSUES_FOUND"
        "model_results": [ # List of all dbt models with issues
            {
                "model_name": "my_model", # Name of the dbt model
                "model_path": "/some/path/models/my_model.sql", # Path to the model
                "column_issues": [{ # All referenced columns not found
                    # The model or source the column was referenced from
                    "referenced_object": "other_model",
                    "missing_columns": ["my_column"] # The column that is missing
                }],
                "non_existent_references": ["my_table"], # A table that is not found
                "cte_issues": [{ # Issues found in CTE references
                    "cte_name": "my_cte", # The CTE in question
                    "missing_columns": ["my_column"] # Any columns not found within CTE
                }]
            }
        ]
    }

    """
    dbt_parser = dbtParser(target=target)

    # Filter models if selection is provided
    if model:
        target_model_names = dbt_parser.parse_selection_query(model)
        target_models = [m for m in dbt_parser.models.values() if m.name in target_model_names]
    else:
        target_models = None

    result = analyze_column_references(dbt_parser=dbt_parser, target_models=target_models)
    return json.dumps(dict_utils.remove_empty_values(asdict(result)))


@mcp_server.tool()
def show_docs(  # noqa: PLR0911
    model_name: str,
    model_type: Literal["model", "source"] = "model",
    target: str | None = None,
) -> str:
    """Show documentation for a specific model or source.

    Args:
        model_name: Name of the model or source to show documentation for
        model_type: Type of object - either "model" or "source" (default: "model")
        target: Specify dbt target environment

    Returns:
        JSON string containing model/source documentation including:
        - Model/source description
        - Column names and descriptions
        - YAML file path where documentation is defined
        - For sources: source name and table name

    Note:
        You can use the 'dt docs' command to generate documentation for models
        that don't have existing documentation yet.

    Example usage:
        show_docs("customers", "model")          # Show model documentation
        show_docs("raw_orders", "source")        # Show source documentation

    """
    dbt_parser = dbtParser(target=target)

    if model_type == "model":
        # Check if model exists in yaml docs
        if model_name not in dbt_parser.yaml_docs:
            # Check if model exists at all
            if model_name not in dbt_parser.models:
                return json.dumps(
                    {"status": "error", "message": f"Model '{model_name}' not found in project"}
                )
            return json.dumps(
                {
                    "status": "no_documentation",
                    "model_name": model_name,
                    "model_type": model_type,
                    "message": f"Model '{model_name}' exists but has no YAML documentation",
                    "suggestion": "Use 'dt docs --model {model_name}' to generate documentation",
                }
            )

        yaml_docs = dbt_parser.yaml_docs[model_name]
        columns = []
        if yaml_docs.columns:
            columns = [
                {
                    "name": col.name,
                    "description": col.description,
                    "raw_description": col.raw_description,
                }
                for col in yaml_docs.columns
            ]

        return json.dumps(
            {
                "status": "success",
                "model_name": model_name,
                "model_type": model_type,
                "description": yaml_docs.model_description,
                "yaml_file_path": str(yaml_docs.path),
                "columns": columns,
                "config": yaml_docs.config,
            }
        )

    if model_type == "source":
        # For sources, we need to find by full name or check all sources
        matching_sources = []
        for source_key, source in dbt_parser.sources.items():
            # Check if model_name matches either the table name or the full name
            if model_name in (source.name, source.full_name):
                matching_sources.append((source_key, source))

        if not matching_sources:
            return json.dumps(
                {
                    "status": "error",
                    "message": f"Source '{model_name}' not found in project. "
                    "Available sources: {list(dbt_parser.sources.keys())}",
                }
            )

        if len(matching_sources) > 1:
            return json.dumps(
                {
                    "status": "multiple_matches",
                    "message": f"Multiple sources found matching '{model_name}': "
                    f"{[s[0] for s in matching_sources]}",
                    "matches": [s[0] for s in matching_sources],
                }
            )

        source_key, source = matching_sources[0]
        columns = [
            {
                "name": col.name,
                "description": col.description,
                "raw_description": col.raw_description,
            }
            for col in source.columns
        ]

        return json.dumps(
            {
                "status": "success",
                "model_name": model_name,
                "model_type": model_type,
                "source_name": source.source_name,
                "table_name": source.name,
                "full_name": source.full_name,
                "description": source.description,
                "yaml_file_path": str(source.path),
                "columns": columns,
            }
        )

    return json.dumps(
        {
            "status": "error",
            "message": f"Invalid model_type '{model_type}'. Must be either 'model' or 'source'",
        }
    )


@mcp_server.tool()
def list_dbt_objects(
    type: Literal["model", "source"],  # noqa: A002
    target: str | None = None,
) -> str:
    """List all available models or sources in the dbt project.

    Args:
        type: Type of objects to list - either "model" or "source"
        target: Specify dbt target environment

    Returns:
        JSON string containing list of models or sources with their metadata:

        For models:
        - model_name: Name of the model
        - sql_path: Path to the SQL file
        - yaml_path: Path to YAML documentation (if exists)

        For sources:
        - source_name: Name of the source schema
        - table_name: Name of the source table
        - full_name: Full source name (source_name__table_name)
        - yaml_path: Path to YAML file where source is defined

    Example usage:
        list("model")    # List all models
        list("source")   # List all sources

    """
    dbt_parser = dbtParser(target=target)

    if type == "model":
        models_list = [
            {
                "model_name": name,
                "sql_path": str(model.path),
                "yaml_path": model.yaml_docs.path if model.yaml_docs else None,
            }
            for name, model in dbt_parser.models.items()
        ]

        return json.dumps(
            {
                "status": "success",
                "type": "model",
                "count": len(models_list),
                "items": sorted(models_list, key=lambda x: x["model_name"]),
            }
        )
    if type == "source":
        sources_list = [
            {
                "source_name": source.source_name,
                "table_name": source.name,
                "full_name": source.full_name,
                "yaml_path": str(source.path),
            }
            for source in dbt_parser.sources.values()
        ]
        return json.dumps(
            {
                "status": "success",
                "type": "source",
                "count": len(sources_list),
                "items": sorted(sources_list, key=lambda x: x["full_name"]),
            }
        )
    return json.dumps(
        {
            "status": "error",
            "message": f"Invalid type '{type}'. Must be either 'model' or 'source'",
        }
    )


@mcp_server.tool()
def build_models(  # noqa: PLR0913
    model: str | None = None,
    full_refresh: bool = False,
    threads: int | None = None,
    vars: str | None = None,  # noqa: A002
    target: str | None = None,
    analyze_only: bool = False,
    disable_smart: bool = False,
) -> str:
    """Build dbt models with intelligent cache-based execution.

    This command provides the same functionality as 'dbt build' with smart execution
    by default - it analyzes which models need execution based on cache validity
    and dependency changes, validates lineage references, and only runs those models
    that actually need updating.

    Args:
        model: Select models to build (same as dbt --select/--model)
        full_refresh: Incremental models only: Will rebuild an incremental model
        threads: Number of threads to use
        vars: Supply variables to the project (YAML string)
        target: Specify dbt target environment
        analyze_only: Only analyze which models need execution, don't run dbt
        disable_smart: Disable the intelligent caching and force a rebuild

    Smart Execution Features:
        • Cache Analysis: Only rebuilds models with outdated cache or dependency changes
        • Lineage Validation: Validates column and model references before execution
        • Optimized Selection: Automatically filters to models that need execution

    Returns:
        JSON string with execution results and model status information.

    Examples:
        build_models()                               # Smart execution (default)
        build_models(model="customers")              # Only run customers if needed
        build_models(model="customers+", analyze_only=True)  # Show what would be executed
        build_models(model="customers", disable_smart=True)  # Force run customers
        build_models(threads=4, target="prod")       # Smart execution with options

    Instructions:
        -   When applicable try to run e.g. "+my_model+" in order to apply changes
            both up and downstream.
        -   After tool use, if status=success, highlight nbr models skipped and time saved.

    """
    # Create parameters object
    params = DbtExecutionParams(
        command_name="build",
        model=model,
        full_refresh=full_refresh,
        threads=threads,
        vars=vars,
        target=target,
        analyze_only=analyze_only,
        disable_smart=disable_smart,
    )

    try:
        # Execute using the existing CLI infrastructure
        plan = create_execution_plan(params)
        result = plan.run()
        output = {
            "status": "success" if result.return_code == 0 else "error",
            "models_executed": plan.models_to_execute,
            "models_skipped": [m.name for m in plan.models_to_skip],
            "nbr_models_skipped": len(plan.models_to_skip),
            "seconds_saved_by_skipping_models": plan.compute_time_saved_seconds,
            **dict_utils.remove_empty_values(asdict(result.parsed_logs)),
        }
        if result.return_code != 0:
            output["dbt_logs"] = result.raw_logs
        return json.dumps(output)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"status": "error", "message": f"Build failed: {e!s}"})


@mcp_server.tool()
def list_settings(target: str | None = None) -> str:
    """List all dbt-toolbox settings with their values and sources.

    Shows configuration from environment variables, TOML files, dbt profiles,
    and default values with clear indication of where each setting comes from.

    Args:
        target: Specify dbt target environment to include target-specific settings

    Returns:
        JSON string with all settings, their values, sources, and metadata.

    Example output:
    {
        "cache_path": {
            "value": ".dbt_toolbox",
            "source": "default",
            "description": "Directory for cache storage"
        },
        "debug": {
            "value": false,
            "source": "toml",
            "description": "Enable debug logging"
        }
    }

    """
    try:
        all_settings = get_all_settings(target=target)
        return json.dumps({name: setting._asdict() for name, setting in all_settings.items()})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"status": "error", "message": f"Failed to get settings: {e!s}"})


@mcp_server.tool()
def generate_docs(
    model: str,
    target: str | None = None,
    fix_inplace: bool = True,
) -> str:
    r"""Generate YAML documentation for a specific dbt model.

    This tool provides intelligent YAML documentation generation with:
    - Automatic column description inheritance from upstream models and macros
    - Detection of column changes (additions, removals, reordering)
    - Placeholder counting and validation
    - Detailed error reporting

    !IMPORTANT: Use this tool before manually writing documentation.
    It will save a lot of time and tokens.

    Args:
        model: Name of the dbt model to generate documentation for
        target: Specify dbt target environment (optional)
        fix_inplace: If True, updates the schema file directly. If False, returns YAML content

    Returns:
        JSON string with documentation generation results including:
        - Success status and any error messages
        - Column changes detected (added, removed, reordered)
        - Number of columns with placeholder descriptions
        - YAML content (only when fix_inplace=False)
        - Model metadata (name, path)

    Example outputs:

    When fix_inplace=True (file update mode):
    {
        "success": true,
        "model_name": "customers",
        "model_path": "/path/to/customers.sql",
        "changes": {
            "added": ["new_column"],
            "removed": [],
            "reordered": false
        },
        "nbr_columns_with_placeholders": 2,
        "yaml_content": null,
        "error_message": null
    }

    When fix_inplace=False (preview mode):
    {
        "success": true,
        "model_name": "customers",
        "yaml_content": "models:\\n  - name: customers\\n    columns: [...]",
        "changes": {...},
        "nbr_columns_with_placeholders": 0,
        "error_message": null
    }

    Error example:
    {
        "success": false,
        "error_message": "Permission denied when writing to schema file",
        "model_name": "customers",
        "changes": {...}
    }

    Instructions:
    - Use fix_inplace=True to actually update the schema file
    - Check the "changes" field to see what modifications were detected and report to user
    - Pay attention to "nbr_columns_with_placeholders" for documentation completeness

    """
    try:
        dbt_parser = dbtParser(target=target)
    except Exception as e:  # noqa: BLE001
        return json.dumps(
            {"status": "error", "message": f"Failed to initialize dbt parser: {e!s}"}
        )

    if model not in dbt_parser.models:
        max_models_to_show = 5
        available_models = list(dbt_parser.models.keys())[:max_models_to_show]
        models_info = f"Available models include: {', '.join(available_models)}"
        if len(dbt_parser.models) > max_models_to_show:
            models_info += f" ... and {len(dbt_parser.models) - max_models_to_show} more"

        return json.dumps(
            {"status": "error", "message": f"Model '{model}' not found. {models_info}"}
        )

    try:
        builder = YamlBuilder(model, dbt_parser)
        result = builder.build(fix_inplace=fix_inplace)

        return json.dumps(dict_utils.remove_empty_values(asdict(result)))
    except Exception as e:  # noqa: BLE001
        return json.dumps(
            {"status": "error", "message": f"Unexpected error while generating docs: {e!s}"}
        )
