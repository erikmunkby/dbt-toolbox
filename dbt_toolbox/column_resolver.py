"""Module for resolving column lineage through SQL joins."""

from dataclasses import dataclass
from enum import Enum

import sqlglot
import sqlglot.expressions
from sqlglot.expressions import Expression, Select

from dbt_toolbox.constants import TABLE_REF_SEP


class ReferenceType(Enum):
    """Type of table reference."""

    EXTERNAL = "external"  # Reference to external table/model
    CTE = "cte"  # Reference to CTE
    SUBQUERY = "subquery"  # Reference to subquery alias
    INTERNAL = "internal"  # Other internal reference


@dataclass
class ColumnReference:
    """Represents a column reference and its source information."""

    column_name: str
    table_reference: str | None  # The table/alias name being referenced
    reference_type: ReferenceType
    resolved: bool | None  # True if resolved, False if not, None if external (undecided)


def resolve_column_lineage(glot_code: Expression) -> list[ColumnReference]:  # noqa: PLR0912, PLR0915
    """Resolve column lineage through joins, identifying reference types and resolution status.

    Args:
        glot_code: SQLGlot parsed SELECT statement

    Returns:
        List of ColumnReference objects with detailed information about each column.
        Each ColumnReference contains:
        - column_name: The name of the column
        - table_reference: The table/alias being referenced (None if unresolved)
        - reference_type: Type of reference (EXTERNAL, CTE, SUBQUERY, INTERNAL)
        - resolved: True if resolved internally, False if not, None if external validation needed

    """
    if not glot_code or not isinstance(glot_code, sqlglot.expressions.Select):
        return []

    result = []
    seen_columns = set()  # Track column names to avoid duplicates

    # Collect all CTE names from the entire query (including subqueries)
    cte_names = _collect_all_cte_names(glot_code)

    # Build mapping from subquery/CTE aliases to their column sources for lineage tracing
    subquery_column_sources = _build_subquery_column_mapping(glot_code, cte_names)

    # Build mapping from CTE names to their available columns
    cte_available_columns = _build_cte_available_columns(glot_code)

    # Process the main SELECT statement first
    alias_to_table = _build_alias_mapping(glot_code)

    for select_expr in glot_code.selects:
        if isinstance(select_expr, sqlglot.expressions.Star):
            continue

        # Extract all column references from the expression
        column_refs = _extract_all_column_references(select_expr, alias_to_table, cte_names)

        for col_name, source_table in column_refs.items():
            if col_name in seen_columns:
                continue  # Skip duplicates
            seen_columns.add(col_name)

            # Determine reference type and resolution status
            if source_table is None:
                # Ambiguous or unresolved reference
                ref = ColumnReference(
                    column_name=col_name,
                    table_reference=None,
                    reference_type=ReferenceType.INTERNAL,
                    resolved=False,
                )
            elif source_table in cte_names:
                # Reference to CTE - check if column is available in CTE output
                cte_has_column = (
                    source_table in cte_available_columns
                    and col_name in cte_available_columns[source_table]
                )
                if cte_has_column:
                    # Column exists in CTE - trace it back to source if possible
                    has_source_mapping = (
                        source_table in subquery_column_sources
                        and col_name in subquery_column_sources[source_table]
                    )
                    if has_source_mapping:
                        ultimate_source = subquery_column_sources[source_table][col_name]
                        if ultimate_source:
                            # Successfully traced to base table
                            ref = ColumnReference(
                                column_name=col_name,
                                table_reference=ultimate_source,
                                reference_type=ReferenceType.EXTERNAL,
                                resolved=None,  # External validation needed
                            )
                        else:
                            # Could not trace (computed column in CTE)
                            ref = ColumnReference(
                                column_name=col_name,
                                table_reference=source_table,
                                reference_type=ReferenceType.CTE,
                                resolved=True,  # Resolved within CTE
                            )
                    else:
                        # CTE column exists but no tracing info
                        ref = ColumnReference(
                            column_name=col_name,
                            table_reference=source_table,
                            reference_type=ReferenceType.CTE,
                            resolved=True,  # Resolved within CTE
                        )
                else:
                    # Column does not exist in CTE output - invalid reference
                    ref = ColumnReference(
                        column_name=col_name,
                        table_reference=source_table,
                        reference_type=ReferenceType.CTE,
                        resolved=False,  # Invalid CTE reference
                    )
            elif source_table in subquery_column_sources:
                # Reference to subquery - trace to ultimate source
                ultimate_source = subquery_column_sources[source_table].get(col_name)
                if ultimate_source:
                    # Successfully traced to base table
                    ref = ColumnReference(
                        column_name=col_name,
                        table_reference=ultimate_source,
                        reference_type=ReferenceType.EXTERNAL,
                        resolved=None,  # External validation needed
                    )
                else:
                    # Could not trace (computed column)
                    ref = ColumnReference(
                        column_name=col_name,
                        table_reference=source_table,
                        reference_type=ReferenceType.SUBQUERY,
                        resolved=True,
                    )
            else:
                # Reference to external table
                ref = ColumnReference(
                    column_name=col_name,
                    table_reference=source_table,
                    reference_type=ReferenceType.EXTERNAL,
                    resolved=None,  # Cannot decide - external validation needed
                )

            result.append(ref)

    # Also process columns from within CTEs and subqueries that reference base tables
    all_selects = _collect_all_select_statements(glot_code)
    for select_stmt in all_selects:
        if select_stmt == glot_code:
            continue  # Skip main query, already processed

        alias_to_table = _build_alias_mapping(select_stmt)

        # Only process if this SELECT doesn't reference any CTEs
        has_cte_references = any(alias in cte_names for alias in alias_to_table)
        if has_cte_references:
            continue

        for select_expr in select_stmt.selects:
            if isinstance(select_expr, sqlglot.expressions.Star):
                continue

            column_refs = _extract_all_column_references(select_expr, alias_to_table, cte_names)

            for col_name, source_table in column_refs.items():
                if col_name in seen_columns:
                    continue  # Skip duplicates
                seen_columns.add(col_name)

                if source_table and source_table not in cte_names:
                    ref = ColumnReference(
                        column_name=col_name,
                        table_reference=source_table,
                        reference_type=ReferenceType.EXTERNAL,
                        resolved=None,
                    )
                    result.append(ref)

    return result


def _build_cte_available_columns(glot_code: Select) -> dict[str, set[str]]:
    """Build mapping from CTE names to their available output columns.

    Args:
        glot_code: SQLGlot parsed SELECT statement

    Returns:
        Dictionary mapping cte_name -> set of available column names

    """
    cte_columns = {}

    if hasattr(glot_code, "ctes") and glot_code.ctes:
        for cte in glot_code.ctes:
            cte_name = cte.alias
            available_columns = set()

            # Get all selected columns from the CTE
            for select_expr in cte.this.selects:
                if isinstance(select_expr, sqlglot.expressions.Star):
                    # TODO: Handle SELECT * in CTEs - would need table schema
                    continue

                column_name = select_expr.alias_or_name
                available_columns.add(column_name)

            cte_columns[cte_name] = available_columns

    return cte_columns


def _build_subquery_column_mapping(
    glot_code: Select, cte_names: set[str]
) -> dict[str, dict[str, str | None]]:
    """Build mapping from subquery/CTE aliases to their column sources.

    Args:
        glot_code: SQLGlot parsed SELECT statement
        cte_names: Set of CTE names

    Returns:
        Dictionary mapping subquery_alias -> {column_name -> source_table}

    """
    column_mapping = {}
    all_selects = _collect_all_select_statements(glot_code)

    for select_stmt in all_selects:
        if select_stmt == glot_code:
            continue  # Skip main query

        # Find the alias for this SELECT (from parent subquery or CTE)
        select_alias = None
        parent = select_stmt
        while parent.parent:
            parent = parent.parent
            if (isinstance(parent, sqlglot.expressions.Subquery) and parent.alias) or (
                isinstance(parent, sqlglot.expressions.CTE) and parent.alias
            ):
                select_alias = parent.alias
                break

        if not select_alias:
            continue

        # Build alias-to-table mapping for this SELECT
        alias_to_table = _build_alias_mapping(select_stmt)
        column_mapping[select_alias] = {}

        # Map each column in this SELECT to its source
        for select_expr in select_stmt.selects:
            if isinstance(select_expr, sqlglot.expressions.Star):
                continue

            column_name = select_expr.this.name

            # Extract column references from this expression
            col_refs = _extract_all_column_references(select_expr, alias_to_table, cte_names)

            # If this expression references exactly one column from a base table, record it
            if len(col_refs) == 1:
                ref_col, ref_table = next(iter(col_refs.items()))
                column_mapping[select_alias][column_name] = ref_table
            else:
                # Complex expression or multiple references - can't trace
                column_mapping[select_alias][column_name] = None

    return column_mapping


def _collect_all_cte_names(glot_code: Select) -> set[str]:
    """Collect all CTE names from the entire query, including subqueries.

    Args:
        glot_code: SQLGlot parsed SELECT statement

    Returns:
        Set of all CTE names found in the query

    """
    cte_names = set()

    # Collect CTEs from the main query
    if hasattr(glot_code, "ctes") and glot_code.ctes:
        for cte in glot_code.ctes:
            cte_names.add(cte.alias)

    # Recursively collect CTEs from all subqueries
    subqueries = glot_code.find_all(sqlglot.expressions.Subquery)
    for subquery in subqueries:
        if hasattr(subquery.this, "ctes") and subquery.this.ctes:
            for cte in subquery.this.ctes:
                cte_names.add(cte.alias)

    return cte_names


def _collect_all_select_statements(glot_code: Select) -> list[Select]:
    """Collect all SELECT statements from the entire query, including CTEs and subqueries.

    Args:
        glot_code: SQLGlot parsed SELECT statement

    Returns:
        List of all SELECT statements found in the query

    """
    all_selects = []

    # Process the main SELECT statement
    _add_select_and_ctes(glot_code, all_selects)

    # Process all subqueries recursively
    subqueries = glot_code.find_all(sqlglot.expressions.Subquery)
    for subquery in subqueries:
        _add_select_and_ctes(subquery.this, all_selects)

    return all_selects


def _add_select_and_ctes(select_stmt: Select, all_selects: list[Select]) -> None:
    """Add a SELECT statement and its CTEs to the list, handling CTE-containing SELECTs properly.

    Args:
        select_stmt: SELECT statement to process
        all_selects: List to add SELECT statements to

    """
    # Always process the main SELECT statement - CTE filtering will handle the rest
    all_selects.append(select_stmt)

    # If this SELECT has CTEs, also add the CTE definitions
    if hasattr(select_stmt, "ctes") and select_stmt.ctes:
        all_selects.extend([cte.this for cte in select_stmt.ctes])


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

    # Find all column references in the expression, but exclude nested subqueries
    column_refs = []

    # If this expression contains subqueries, don't extract from within them
    if select_expr.find(sqlglot.expressions.Subquery):
        # For expressions with subqueries, skip them - don't extract nested references
        column_refs = []
    else:
        # For simple expressions, get all column references
        column_refs = list(select_expr.find_all(sqlglot.expressions.Column))

    for col_ref in column_refs:
        column_name = col_ref.name

        # Resolve the source table for this column
        if hasattr(col_ref, "table") and col_ref.table:
            table_alias = col_ref.table

            source_table = alias_to_table.get(table_alias)
        elif len(alias_to_table) == 1:
            # Single table context
            table_alias = next(iter(alias_to_table.keys()))
            source_table = next(iter(alias_to_table.values()))
        else:
            # Cannot resolve ambiguous column
            source_table = None

        result[column_name] = source_table

    return result
