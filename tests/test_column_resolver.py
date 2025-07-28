"""Tests for column resolution functionality."""

import sqlglot

from dbt_toolbox.column_resolver import resolve_column_lineage


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
        result = resolve_column_lineage(parsed)  # type: ignore

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
        result = resolve_column_lineage(parsed)  # type: ignore

        expected = {
            "customer_id": "customers",
            "full_name": "customers",
            "order_id": "orders",
            "ordered_at": "orders",
        }
        assert result == expected

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
        result = resolve_column_lineage(parsed)  # type: ignore

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
        result = resolve_column_lineage(parsed)  # type: ignore

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
        result = resolve_column_lineage(parsed)  # type: ignore

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
        result = resolve_column_lineage(parsed)  # type: ignore

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
        result = resolve_column_lineage(parsed)  # type: ignore

        expected = {
            "customer_id": None,  # Can't resolve ambiguous column
            "name": "customers",
            "order_total": "orders",
        }
        assert result == expected

    def test_empty_select(self) -> None:
        """Test empty or None SQLGlot object."""
        result = resolve_column_lineage(None)
        assert result == {}

    def test_select_star(self) -> None:
        """Test SELECT * query."""
        sql = """
        SELECT *
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        """

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        result = resolve_column_lineage(parsed)  # type: ignore

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
        result = resolve_column_lineage(parsed)  # type: ignore

        expected = {
            "customer_id": "sub",  # From subquery alias
            "order_count": "sub",
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
        result = resolve_column_lineage(parsed)  # type: ignore

        expected = {
            "a": "tbl",
            "b": "tbl",
        }
        assert result == expected
