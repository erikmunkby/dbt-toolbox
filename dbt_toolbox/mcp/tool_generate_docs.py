"""MCP tool generate docs."""

from dataclasses import asdict

from dbt_toolbox.actions.build_docs import YamlBuilder
from dbt_toolbox.dbt_parser._dbt_parser import dbtParser
from dbt_toolbox.mcp._utils import mcp_json_response


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

    IMPORTANT: Use this tool before writing documentation,
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
        return mcp_json_response(
            {"status": "error", "message": f"Failed to initialize dbt parser: {e!s}"}
        )

    if model not in dbt_parser.models:
        max_models_to_show = 5
        available_models = list(dbt_parser.models.keys())[:max_models_to_show]
        models_info = f"Available models include: {', '.join(available_models)}"
        if len(dbt_parser.models) > max_models_to_show:
            models_info += f" ... and {len(dbt_parser.models) - max_models_to_show} more"

        return mcp_json_response(
            {"status": "error", "message": f"Model '{model}' not found. {models_info}"}
        )

    try:
        builder = YamlBuilder(model, dbt_parser)
        result = builder.build(fix_inplace=fix_inplace)

        return mcp_json_response(asdict(result))
    except Exception as e:  # noqa: BLE001
        return mcp_json_response(
            {"status": "error", "message": f"Unexpected error while generating docs: {e!s}"}
        )
