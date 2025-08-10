"""Recursively resolve each column and see which are valid."""

import sqlglot.expressions as expr

from dbt_toolbox.constants import TABLE_REF_SEP
from dbt_toolbox.data_models import ColumnReference, Table, TableType
from dbt_toolbox.settings import settings
from dbt_toolbox.utils._printers import cprint


def _debug_print(col: expr.Column, tables: dict[str, Table], context: list[str]) -> None:
    cprint("column", col.name, str(context), highlight_idx=1)
    for name, t in tables.items():
        p = (
            f"cte({name})" + " columns: " + ", ".join(t.available_columns)
            if t.type == TableType.CTE
            else f"tbl({name}) columns: unknown"
        )
        cprint("  " + p)


def _clean_tbl_name(name: str) -> str:
    if name.startswith(TABLE_REF_SEP):
        return name.strip(TABLE_REF_SEP).split(TABLE_REF_SEP)[-1]
    return name


def _build_col(
    col: expr.Column,
    tables: dict[str, Table],
    context: list[str],
    existing_cols: list[ColumnReference],
) -> ColumnReference | None:
    col_id = hash(str(col) + str(col.parent))
    if isinstance(col.this, expr.Star) or col_id in [c.id for c in existing_cols]:
        return None
    if settings.debug:
        _debug_print(col=col, tables=tables, context=context)
    if col.table:
        t = tables.get(col.table)
        if not t:
            return ColumnReference(
                id=col_id, name=col.name, reference_type=TableType.EXTERNAL, context=context
            )
    elif len(tables) == 1:
        t = next(iter(tables.values()))
    else:
        # No table reference found, but multiple tables available. Ambiguous column reference.
        return ColumnReference(
            id=col_id, name=col.name, reference_type=TableType.AMBIGUOUS, context=context
        )
    # Now figure out if column is resolved
    if col.name in t.available_columns:
        resolved = True
    elif "*" in t.available_columns:
        resolved = None
    else:
        resolved = False

    if t.type == TableType.EXTERNAL:
        return ColumnReference(
            id=col_id,
            name=col.name,
            reference_type=TableType.EXTERNAL,
            table=_clean_tbl_name(t.name),
            context=context,
        )
    return ColumnReference(
        id=col_id,
        name=col.name,
        reference_type=t.type if t.type else TableType.AMBIGUOUS,
        table=t.name,
        resolved=resolved,
        context=context,
    )


def _resolve_from_clause(stmnt: expr.From, ctes: dict[str, Table]) -> dict[str, Table]:
    if isinstance(stmnt.this, expr.Subquery):
        subq = stmnt.this
        subq_select = subq.this
        return {
            subq.alias_or_name: Table(
                name=subq.alias_or_name,
                type=TableType.SUBQUERY,
                available_columns=[c.alias_or_name for c in subq_select.selects]
                if isinstance(subq_select, expr.Select)
                else [],
            )
        }
    return {
        stmnt.alias_or_name: ctes[stmnt.name]
        if stmnt.name in ctes
        else Table(name=stmnt.name, type=TableType.EXTERNAL)
    }


def _recursive_resolve(
    select_stmt: expr.Select,
    context: list[str],
    tables: dict[str, Table] | None = None,
    ctes: dict[str, Table] | None = None,
) -> list[ColumnReference]:
    if tables is None:
        tables = {}
    if ctes is None:
        ctes = {}

    results: list[ColumnReference] = []
    # Find all available CTEs
    for cte in select_stmt.ctes:
        ctes[cte.alias] = Table(
            name=cte.alias,
            available_columns=[c.alias_or_name for c in cte.selects],
            type=TableType.CTE,
        )
        results.extend(
            _recursive_resolve(
                cte.this, context=[*context, cte.alias_or_name], tables=tables, ctes=ctes
            )
        )

    # Find all available tables (from + joins)
    from_clause = select_stmt.find(expr.From)
    if from_clause is not None and from_clause.parent == select_stmt:
        tables = _resolve_from_clause(from_clause, ctes=ctes)
        if isinstance(from_clause.this, expr.Subquery):
            results.extend(
                _recursive_resolve(
                    select_stmt=from_clause.this.this, context=[*context, "from_subquery"]
                )
            )

    # Find all joined tables
    for join in select_stmt.find_all(expr.Join):
        # Only take joins within current context
        if join.parent != select_stmt:
            continue
        join_table = join.find(expr.Table)
        if join_table is None:
            continue
        tables[join_table.alias_or_name] = (
            ctes[join_table.name]
            if join_table.name in ctes
            else Table(name=join_table.name, type=TableType.EXTERNAL)
        )

    subq_id = 0
    for obj in select_stmt.selects:
        col = obj.this if isinstance(obj, expr.Alias) else obj
        # Resolve any subqueries
        if isinstance(col, expr.Subquery):
            results.extend(
                _recursive_resolve(col.this, context=[*context, f"sub#{subq_id}"], tables=tables)
            )
            subq_id += 1
        # Resolve normal columns
        if isinstance(col, expr.Column):
            built_col = _build_col(col=col, tables=tables, context=context, existing_cols=results)
            if built_col:
                results.append(built_col)
        else:
            # Finally investigate all other columns, unless they have already been investigated.
            for subcol in col.find_all(expr.Column):
                built_sub_col = _build_col(
                    col=subcol, tables=tables, context=context, existing_cols=results
                )
                if built_sub_col:
                    results.append(built_sub_col)

    return results


def resolve_column_lineage(glot_code: expr.Expression) -> list[ColumnReference]:
    """Recursively resolve column references in a SQL expression.

    This function analyzes a SQL expression and returns detailed information about
    each column reference, including its source table, reference type, resolution
    status, and context within the SQL query structure.

    Args:
        glot_code:  A SQLGlot expression, typically a Select statement to analyze.
                    Other expression types will return an empty list.

    Returns:
        A list of ColumnReference objects, each containing:
        - name: The column name
        - table: The source table name (None if ambiguous)
        - reference_type: TableType enum value (EXTERNAL, CTE, SUBQUERY, or AMBIGUOUS)
        - resolved: Boolean indicating if the column exists in the source:
            - True: Column exists in the referenced table/CTE
            - False: Column does not exist in the referenced table/CTE
            - None: Cannot determine (e.g., SELECT * or external table)
        - context: List of strings showing the query context path (e.g., ["root", "my_cte"])

    """
    if not isinstance(glot_code, expr.Select):
        return []
    # Clean up table names before returning
    return _recursive_resolve(glot_code, context=["root"])
