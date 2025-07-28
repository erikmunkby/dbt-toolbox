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

    # Collect all CTE names to identify what should be ignored
    cte_names = set()
    if hasattr(glot_code, "ctes") and glot_code.ctes:
        for cte in glot_code.ctes:
            cte_names.add(cte.alias)

    # Process all SELECT statements in the query (main + CTEs)
    all_selects = [glot_code]
    if hasattr(glot_code, "ctes") and glot_code.ctes:
        all_selects.extend([cte.this for cte in glot_code.ctes])

    for select_stmt in all_selects:
        # Build alias-to-table mapping from FROM and JOIN clauses
        alias_to_table = _build_alias_mapping(select_stmt)

        # Process each selected column
        for select_expr in select_stmt.selects:
            if isinstance(select_expr, sqlglot.expressions.Star):
                continue

            # Extract all column references from the expression (handles nested functions)
            column_refs = _extract_all_column_references(select_expr, alias_to_table, cte_names)
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

    # Get table references from FROM clause
    from_clause = glot_code.find(sqlglot.expressions.From)
    if from_clause:
        from_tables = from_clause.find_all(sqlglot.expressions.Table)
        for table in from_tables:
            table_name = table.name
            alias = table.alias if table.alias else table_name

            # Extract meaningful table name from dbt naming convention
            if table_name.startswith(TABLE_REF_SEP):
                clean_name = table_name.strip(TABLE_REF_SEP).split(TABLE_REF_SEP)[-1]
                alias_to_table[alias] = clean_name
            else:
                alias_to_table[alias] = table_name

    # Get table references from JOIN clauses
    joins = glot_code.find_all(sqlglot.expressions.Join)
    for join in joins:
        join_tables = join.find_all(sqlglot.expressions.Table)
        for table in join_tables:
            table_name = table.name
            alias = table.alias if table.alias else table_name

            # Extract meaningful table name from dbt naming convention
            if table_name.startswith(TABLE_REF_SEP):
                clean_name = table_name.strip(TABLE_REF_SEP).split(TABLE_REF_SEP)[-1]
                alias_to_table[alias] = clean_name
            else:
                alias_to_table[alias] = table_name

    # Also handle subqueries in FROM and JOINs
    from_subqueries = []
    if from_clause:
        from_subqueries.extend(from_clause.find_all(sqlglot.expressions.Subquery))
    for join in joins:
        from_subqueries.extend(join.find_all(sqlglot.expressions.Subquery))

    for subquery in from_subqueries:
        if subquery.alias:
            alias_to_table[subquery.alias] = subquery.alias

    return alias_to_table


def _extract_all_column_references(
    select_expr: Expression,
    alias_to_table: dict[str, str],
    cte_names: set[str] | None = None,
) -> dict[str, str | None]:
    """Extract all column references from a select expression and map them to source tables.

    Args:
        select_expr: SQLGlot select expression
        alias_to_table: Mapping from aliases to table names
        cte_names: Set of CTE names to ignore

    Returns:
        Dictionary mapping column names to their source table names

    """
    result = {}
    cte_names = cte_names or set()

    # Find all column references in the expression (handles nested functions)
    column_refs = list(select_expr.find_all(sqlglot.expressions.Column))

    for col_ref in column_refs:
        column_name = col_ref.name

        # Resolve the source table for this column
        if hasattr(col_ref, "table") and col_ref.table:
            table_alias = col_ref.table

            # Skip columns that reference CTEs
            if table_alias in cte_names:
                continue

            source_table = alias_to_table.get(table_alias)
        elif len(alias_to_table) == 1:
            # Single table context - check if it's a CTE
            table_alias = next(iter(alias_to_table.keys()))
            if table_alias in cte_names:
                continue
            source_table = next(iter(alias_to_table.values()))
        else:
            # Cannot resolve ambiguous column
            source_table = None

        result[column_name] = source_table

    return result
