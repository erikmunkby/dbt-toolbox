"""Module for resolving column lineage through SQL joins."""

import sqlglot
import sqlglot.expressions
from sqlglot.expressions import Expression, Select

from dbt_toolbox.constants import TABLE_REF_SEP


def resolve_column_lineage(glot_code: Select) -> dict[str, str | None]:
    """Resolve column lineage through joins, mapping columns to their source tables.

    Args:
        glot_code: SQLGlot parsed SELECT statement

    Returns:
        Dictionary mapping column names to their source table names.
        For columns from joins, returns the actual table name, not the alias.

    """
    result = {}

    if not glot_code:
        return result

    # Build alias-to-table mapping from FROM and JOIN clauses
    alias_to_table = _build_alias_mapping(glot_code)

    # Process each selected column
    for select_expr in glot_code.selects:
        if isinstance(select_expr, sqlglot.expressions.Star):
            continue

        # Extract all column references from the expression (handles nested functions)
        column_refs = _extract_all_column_references(select_expr, alias_to_table)
        result.update(column_refs)

    return result


def _build_alias_mapping(glot_code: Select) -> dict[str, str]:
    """Build mapping from table aliases to actual table names.

    Args:
        glot_code: SQLGlot parsed SELECT statement

    Returns:
        Dictionary mapping alias -> table_name

    """
    alias_to_table = {}

    # Get all table references (FROM and JOINs)
    all_tables = glot_code.find_all(sqlglot.expressions.Table)

    for table in all_tables:
        table_name = table.name
        alias = table.alias if table.alias else table_name

        # Extract meaningful table name from dbt naming convention
        # e.g., "___source___raw_data__raw_orders___" -> "raw_data__raw_orders"
        # and, "___ref___products___" -> "products"
        if table_name.startswith(TABLE_REF_SEP):
            clean_name = table_name.strip(TABLE_REF_SEP).split(TABLE_REF_SEP)[-1]
            alias_to_table[alias] = clean_name
        else:
            alias_to_table[alias] = table_name

    # Also handle subqueries - they appear as aliases without corresponding Table nodes
    # For now, we'll keep subquery aliases as-is
    subqueries = glot_code.find_all(sqlglot.expressions.Subquery)
    for subquery in subqueries:
        if subquery.alias:
            alias_to_table[subquery.alias] = subquery.alias

    return alias_to_table


def _extract_all_column_references(
    select_expr: Expression, alias_to_table: dict[str, str],
) -> dict[str, str | None]:
    """Extract all column references from a select expression and map them to source tables.

    Args:
        select_expr: SQLGlot select expression
        alias_to_table: Mapping from aliases to table names

    Returns:
        Dictionary mapping column names to their source table names

    """
    result = {}

    # Find all column references in the expression (handles nested functions)
    column_refs = list(select_expr.find_all(sqlglot.expressions.Column))

    for col_ref in column_refs:
        column_name = col_ref.name

        # Resolve the source table for this column
        if hasattr(col_ref, "table") and col_ref.table:
            table_alias = col_ref.table
            source_table = alias_to_table.get(table_alias)
        elif len(alias_to_table) == 1:
            # Single table context - assign to that table
            source_table = next(iter(alias_to_table.values()))
        else:
            # Cannot resolve ambiguous column
            source_table = None

        result[column_name] = source_table

    return result


