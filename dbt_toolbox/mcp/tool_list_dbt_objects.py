"""MCP tool list dbt objects."""

import re
from typing import Literal

from dbt_toolbox.dbt_parser._dbt_parser import dbtParser
from dbt_toolbox.mcp._utils import mcp_json_response


def list_dbt_objects(
    pattern: str | None = None,
    type: Literal["model", "source"] | None = None,  # noqa: A002
    target: str | None = None,
) -> str:
    """List models and sources in the dbt project with flexible filtering options.

    Args:
        pattern: Optional regex pattern to match against model/source names. Takes priority
            over type.
        type: Optional type filter - either "model" or "source". Ignored if pattern is provided.
        target: Specify dbt target environment

    Returns:
        JSON string containing list of matching models and sources with their metadata:

        For models:
        - object_type: "model"
        - model_name: Name of the model
        - sql_path: Path to the SQL file
        - yaml_path: Path to YAML documentation (if exists)

        For sources:
        - object_type: "source"
        - source_name: Name of the source schema
        - table_name: Name of the source table
        - full_name: Full source name (source_name__table_name)
        - yaml_path: Path to YAML file where source is defined

    Priority:
        1. If pattern is provided: Match both models and sources against the regex
        2. If only type is provided: Return only models or sources
        3. If neither provided: Return all models and sources

    Example usage:
        list_dbt_objects()                           # All models and sources
        list_dbt_objects(type="model")               # Only models
        list_dbt_objects(pattern="^staging_.*")      # Models/sources starting with "staging_"
        list_dbt_objects(pattern="customer", type="model")  # Pattern takes priority, ignores type

    """
    # Validate regex pattern if provided
    regex_pattern = None
    if pattern is not None:
        try:
            regex_pattern = re.compile(pattern)
        except re.error as e:
            return mcp_json_response(
                {
                    "status": "error",
                    "message": f"Invalid regex pattern '{pattern}': {e}",
                }
            )

    dbt_parser = dbtParser(target=target)
    matching_objects = []

    # Pattern takes priority - search both models and sources
    if pattern is not None and regex_pattern is not None:
        # Search models
        matching_models = [
            {
                "object_type": "model",
                "model_name": name,
                "sql_path": str(model.path),
                "yaml_path": str(model.yaml_docs.path) if model.yaml_docs else None,
            }
            for name, model in dbt_parser.models.items()
            if regex_pattern.search(name)
        ]

        # Search sources
        matching_sources = [
            {
                "object_type": "source",
                "source_name": source.source_name,
                "table_name": source.name,
                "full_name": source.full_name,
                "yaml_path": str(source.path),
            }
            for source in dbt_parser.sources.values()
            if (
                regex_pattern.search(source.source_name)
                or regex_pattern.search(source.name)
                or regex_pattern.search(source.full_name)
            )
        ]

        matching_objects = matching_models + matching_sources
        filter_description = f"pattern '{pattern}'"

    # Type filter only
    elif type is not None:
        if type == "model":
            matching_objects = [
                {
                    "object_type": "model",
                    "model_name": name,
                    "sql_path": str(model.path),
                    "yaml_path": str(model.yaml_docs.path) if model.yaml_docs else None,
                }
                for name, model in dbt_parser.models.items()
            ]
        elif type == "source":
            matching_objects = [
                {
                    "object_type": "source",
                    "source_name": source.source_name,
                    "table_name": source.name,
                    "full_name": source.full_name,
                    "yaml_path": str(source.path),
                }
                for source in dbt_parser.sources.values()
            ]
        filter_description = f"type '{type}'"

    # No filter - return all objects
    else:
        # All models
        all_models = [
            {
                "object_type": "model",
                "model_name": name,
                "sql_path": str(model.path),
                "yaml_path": str(model.yaml_docs.path) if model.yaml_docs else None,
            }
            for name, model in dbt_parser.models.items()
        ]

        # All sources
        all_sources = [
            {
                "object_type": "source",
                "source_name": source.source_name,
                "table_name": source.name,
                "full_name": source.full_name,
                "yaml_path": str(source.path),
            }
            for source in dbt_parser.sources.values()
        ]

        matching_objects = all_models + all_sources
        filter_description = "all objects"

    # Sort by object type first, then by name
    matching_objects.sort(
        key=lambda x: (x["object_type"], x.get("model_name") or x.get("full_name", ""))
    )

    return mcp_json_response(
        {
            "status": "success",
            "filter": filter_description,
            "count": len(matching_objects),
            "items": matching_objects,
        }
    )
