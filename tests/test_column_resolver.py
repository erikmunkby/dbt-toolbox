"""Tests for column resolution functionality."""

import sqlglot

from dbt_toolbox.dbt_parser._column_resolver import (
    ColumnReference,
    TableType,
    resolve_column_lineage,
)


def _convert_to_legacy_dict(column_refs: list[ColumnReference]) -> dict[str, str | None]:
    """Convert new ColumnReference list to legacy dict format for existing tests."""
    result = {}
    for ref in column_refs:
        if ref.reference_type == TableType.EXTERNAL:
            result[ref.name] = ref.table
    return result


def _assert_column_references_match(
    actual_refs: list[ColumnReference], expected_refs: list[ColumnReference]
) -> None:
    """Assert that two lists of column references match, ignoring context_path."""

    # Create comparable tuples that exclude context_path
    def to_comparable(ref: ColumnReference) -> tuple:
        return (ref.name, ref.table, ref.reference_type, ref.resolved)

    actual_set = {to_comparable(ref) for ref in actual_refs}
    expected_set = {to_comparable(ref) for ref in expected_refs}

    # Check that we have the same number of references
    assert len(actual_refs) == len(expected_refs), (
        f"Different number of references: got {len(actual_refs)}, expected {len(expected_refs)}"
    )

    # Check that all expected references are present
    for expected_ref in expected_refs:
        expected_tuple = to_comparable(expected_ref)
        assert expected_tuple in actual_set, f"Missing expected reference: {expected_ref}"

    # Check that no unexpected references are present
    for actual_ref in actual_refs:
        actual_tuple = to_comparable(actual_ref)
        assert actual_tuple in expected_set, f"Unexpected reference found: {actual_ref}"


class TestColumnResolver:
    """Test column lineage resolution."""

    def test_simple_select_no_joins(self) -> None:
        """Test simple SELECT without joins."""
        sql = """
        SELECT
            customer_id,
            name,
            email
        FROM customers
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "customer_id": "customers",
            "name": "customers",
            "email": "customers",
        }
        assert result == expected

    def test_simple_join_with_aliases(self) -> None:
        """Test simple join with table aliases."""
        sql = """
        SELECT
            c.customer_id,
            c.full_name,
            o.order_id,
            o.ordered_at
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore

        expected_refs = [
            ColumnReference(
                id=1,
                name="customer_id",
                table="customers",
                reference_type=TableType.EXTERNAL,
                resolved=None,
            ),
            ColumnReference(
                id=2,
                name="full_name",
                table="customers",
                reference_type=TableType.EXTERNAL,
                resolved=None,
            ),
            ColumnReference(
                id=3,
                name="order_id",
                table="orders",
                reference_type=TableType.EXTERNAL,
                resolved=None,
            ),
            ColumnReference(
                id=4,
                name="ordered_at",
                table="orders",
                reference_type=TableType.EXTERNAL,
                resolved=None,
            ),
        ]

        # Check reference types and resolution status
        _assert_column_references_match(column_refs, expected_refs)

    def test_complex_join_with_dbt_naming(self) -> None:
        """Test complex join with dbt naming convention."""
        sql = """
        SELECT
            "___source___inventory__products___"."id" as "product_id",
            "cat"."name" as "category_name",
            "cat"."department" as "department"
        FROM
            "___source___inventory__products___"
            as "___source___inventory__products___"
        LEFT JOIN
            "___source___inventory__categories___" as "cat"
            ON "___source___inventory__products___"."category_id" = "cat"."id"
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "id": "inventory__products",
            "name": "inventory__categories",
            "department": "inventory__categories",
        }
        assert result == expected

    def test_function_expressions_with_table_references(self) -> None:
        """Test function expressions that reference columns from specific tables."""
        sql = """
        SELECT
            from_big_endian_64(
                xxhash64(
                    cast(
                        cast("___source___inventory__products___"."id" as varchar)
                        || cast("cat"."department" as varchar)
                        || cast("cat"."name" as varchar) as varbinary
                    )
                )
            ) as "product_guid",
            "cat"."name" as "category_name"
        FROM
            "___source___inventory__products___"
            as "___source___inventory__products___"
        LEFT JOIN
            "___source___inventory__categories___" as "cat"
            ON "___source___inventory__products___"."category_id" = "cat"."id"
        """

        parsed = sqlglot.parse_one(sql, dialect="athena")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "id": "inventory__products",  # First column ref found in expression
            "name": "inventory__categories",
            "department": "inventory__categories",
        }
        assert result == expected

    def test_multiple_joins(self) -> None:
        """Test query with multiple joins."""
        sql = """
        SELECT
            c.customer_id,
            o.order_id,
            p.product_name,
            s.store_name
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN products p ON o.product_id = p.product_id
        LEFT JOIN stores s ON o.store_id = s.store_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "customer_id": "customers",
            "order_id": "orders",
            "product_name": "products",
            "store_name": "stores",
        }
        assert result == expected

    def test_self_join(self) -> None:
        """Test self-join with different aliases."""
        sql = """
        SELECT
            mgr.name as manager_name,
            emp.name as employee_name
        FROM employees mgr
        LEFT JOIN employees emp ON mgr.employee_id = emp.manager_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "name": "employees",
        }
        assert result == expected

    def test_columns_without_table_prefix(self) -> None:
        """Test columns without explicit table prefix in join context."""
        sql = """
        SELECT
            customer_id,  -- Ambiguous column
            c.name,
            o.order_total
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "name": "customers",
            "order_total": "orders",
        }
        assert result == expected

    def test_empty_select(self) -> None:
        """Test empty or None SQLGlot object."""
        column_refs = resolve_column_lineage(None)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)
        assert result == {}

    def test_select_star(self) -> None:
        """Test SELECT * query."""
        sql = """
        SELECT *
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        # SELECT * creates a Star expression, not individual columns
        expected = {}  # Can't resolve SELECT *
        assert result == expected

    def test_subquery_in_from(self) -> None:
        """Test subquery in FROM clause."""
        sql = """
        SELECT
            sub.customer_id,
            sub.order_count,
            c.name
        FROM (
            SELECT customer_id, COUNT(*) as order_count
            FROM orders
            GROUP BY customer_id
        ) sub
        LEFT JOIN customers c ON sub.customer_id = c.customer_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "customer_id": "orders",  # From subquery alias
            "name": "customers",
        }
        assert result == expected

    def test_cte_columns(self) -> None:
        """Test subquery from within a CTE."""
        sql = """
        with my_cte as (
            select
                a,
                b
            from tbl
        )
        select
            c,
            d
        from my_cte
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "a": "tbl",
            "b": "tbl",
        }
        assert result == expected

    def test_subquery_cte_columns(self) -> None:
        """Test subquery from within a CTE."""
        sql = """
        select
            order,
            (
                with my_cte as (
                    select customer
                    from tbl
                )
                select renamed from my_cte
            ) as final_name,
            more_data
        from tbl
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        expected_refs = [
            ColumnReference(
                id=1,
                name="order",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root"],
            ),
            ColumnReference(
                id=2,
                name="customer",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root"],
            ),
            ColumnReference(
                id=3,
                name="more_data",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root"],
            ),
            ColumnReference(
                id=4,
                name="renamed",
                table="my_cte",
                reference_type=TableType.CTE,
                resolved=False,
                context=["root"],
            ),
        ]

        # Check reference types and resolution status using helper
        _assert_column_references_match(column_refs, expected_refs)

    def test_new_api_detailed_column_references(self) -> None:
        """Test the new detailed column reference API."""
        sql = """
        SELECT
            sub.customer_id,
            sub.order_count,
            c.name,
            (
                with my_cte as (
                    select customer
                    from tbl
                )
                select customer from my_cte
            ) as cte_customer
        FROM (
            SELECT customer_id, COUNT(*) as order_count
            FROM orders
            GROUP BY customer_id
        ) sub
        LEFT JOIN customers c ON sub.customer_id = c.customer_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        expected_refs = [
            ColumnReference(
                id=1,
                name="customer_id",
                table="sub",
                reference_type=TableType.SUBQUERY,
                resolved=True,
                context=["root", "sub#0"],
            ),
            ColumnReference(
                id=2,
                name="order_count",
                table="sub",
                reference_type=TableType.SUBQUERY,
                resolved=True,
                context=["root", "sub#0"],
            ),
            ColumnReference(
                id=3,
                name="name",
                table="customers",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root"],
            ),
            ColumnReference(
                id=4,
                name="customer",
                table="my_cte",
                reference_type=TableType.CTE,
                resolved=True,
                context=["root", "my_cte"],
            ),
            ColumnReference(
                id=5,
                name="customer",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "my_cte"],
            ),
            ColumnReference(
                id=6,
                name="customer_id",
                table="orders",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "my_cte"],
            ),
        ]

        # Check reference types and resolution status
        _assert_column_references_match(column_refs, expected_refs)

    def test_invalid_cte_reference(self) -> None:
        """Test the new detailed column reference API."""
        sql = """
        with my_cte as (
            select a, b from tbl
        )
        select c, b from my_cte
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        expected_refs = [
            ColumnReference(
                id=1,
                name="c",
                table="my_cte",
                reference_type=TableType.CTE,
                resolved=False,
                context=["root"],
            ),
            ColumnReference(
                id=2,
                name="b",
                table="my_cte",
                reference_type=TableType.CTE,
                resolved=True,
                context=["root"],
            ),
            ColumnReference(
                id=3,
                name="a",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "my_cte"],
            ),
            ColumnReference(
                id=4,
                name="b",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "my_cte"],
            ),
        ]

        # Check reference types and resolution status
        _assert_column_references_match(column_refs, expected_refs)

    def test_invalid_nested_cte_reference(self) -> None:
        """Test the new detailed column reference API."""
        sql = """
        with my_cte as (
            select a, b from tbl
        ),
        second_cte as (
            select b from my_cte
        )
        select d, b from second_cte
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        expected_refs = [
            ColumnReference(
                id=1,
                name="a",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "my_cte"],
            ),
            ColumnReference(
                id=2,
                name="b",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "my_cte"],
            ),
            ColumnReference(
                id=3,
                name="b",
                table="my_cte",
                reference_type=TableType.CTE,
                resolved=True,
                context=["root", "second_cte"],
            ),
            ColumnReference(
                id=4,
                name="b",
                table="second_cte",
                reference_type=TableType.CTE,
                resolved=True,
                context=["root"],
            ),
            ColumnReference(
                id=5,
                name="d",
                table="second_cte",
                reference_type=TableType.CTE,
                resolved=False,
                context=["root"],
            ),
        ]

        # Check reference types and resolution status
        _assert_column_references_match(column_refs, expected_refs)

    def test_mixed_star_cte(self) -> None:
        """Test the new detailed column reference API."""
        sql = """
        with my_cte as(
            select
                hey,
                *,
                yo
            from tbl
            )
        select hey, a, b from my_cte
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        expected_refs = [
            ColumnReference(
                id=1,
                name="hey",
                reference_type=TableType.EXTERNAL,
                table="tbl",
                resolved=None,
            ),
            ColumnReference(
                id=1,
                name="yo",
                reference_type=TableType.EXTERNAL,
                table="tbl",
                resolved=None,
            ),
            ColumnReference(
                id=1,
                name="hey",
                reference_type=TableType.CTE,
                table="my_cte",
                resolved=True,
            ),
            ColumnReference(
                id=1,
                name="a",
                reference_type=TableType.CTE,
                table="my_cte",
                resolved=None,
            ),
            ColumnReference(
                id=1,
                name="b",
                reference_type=TableType.CTE,
                table="my_cte",
                resolved=None,
            ),
        ]

        # Check reference types and resolution status
        _assert_column_references_match(column_refs, expected_refs)

    def test_multi_subquery_cte_same_name(self) -> None:
        """Test multiple ctes with the same name."""
        sql = """
        select
            a,
            (
                with my_cte as (
                    select
                        y
                )
                select y from my_cte
            ) as x,
            (
                with my_cte as (
                    select
                        d
                )
                select d from my_cte
            ) as g
        from tbl
        """
        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        column_refs = resolve_column_lineage(parsed)  # type: ignore
        expected_refs = [
            ColumnReference(
                id=1,
                name="a",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root"],
            ),
            ColumnReference(
                id=2,
                name="y",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "my_cte"],
            ),
            ColumnReference(
                id=3,
                name="y",
                table="my_cte",
                reference_type=TableType.CTE,
                resolved=True,
                context=["root"],
            ),
            ColumnReference(
                id=4,
                name="d",
                table="tbl",
                reference_type=TableType.EXTERNAL,
                resolved=None,
                context=["root", "sub#0", "my_cte"],
            ),
            ColumnReference(
                id=5,
                name="d",
                table="my_cte",
                reference_type=TableType.CTE,
                resolved=True,
                context=["root", "sub#0"],
            ),
        ]
        _assert_column_references_match(column_refs, expected_refs)
