"""MCP show docs tool."""

from typing import Literal

from dbt_toolbox.dbt_parser._dbt_parser import dbtParser
from dbt_toolbox.mcp._utils import mcp_json_response


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
                return mcp_json_response(
                    {"status": "error", "message": f"Model '{model_name}' not found in project"}
                )
            return mcp_json_response(
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

        return mcp_json_response(
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
            return mcp_json_response(
                {
                    "status": "error",
                    "message": f"Source '{model_name}' not found in project. "
                    f"Available sources: {list(dbt_parser.sources.keys())}",
                }
            )

        if len(matching_sources) > 1:
            return mcp_json_response(
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

        return mcp_json_response(
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

    return mcp_json_response(
        {
            "status": "error",
            "message": f"Invalid model_type '{model_type}'. Must be either 'model' or 'source'",
        }
    )
