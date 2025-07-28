"""Tests for column resolution functionality."""

import sqlglot

from dbt_toolbox.column_resolver import ColumnReference, ReferenceType, resolve_column_lineage


def _convert_to_legacy_dict(column_refs: list[ColumnReference]) -> dict[str, str | None]:
    """Convert new ColumnReference list to legacy dict format for existing tests."""
    result = {}
    for ref in column_refs:
        if ref.reference_type == ReferenceType.EXTERNAL:
            result[ref.column_name] = ref.table_reference
    return result


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
        result = _convert_to_legacy_dict(column_refs)

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
        result = _convert_to_legacy_dict(column_refs)

        expected = {
            "order": "tbl",
            "customer": "tbl",
            "more_data": "tbl",
        }
        assert result == expected

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

        # Check we have the expected columns
        column_names = [ref.column_name for ref in column_refs]
        assert "customer_id" in column_names
        assert "order_count" in column_names
        assert "name" in column_names
        assert "customer" in column_names

        # Check reference types and resolution status
        for ref in column_refs:
            if ref.column_name == "customer_id":
                assert ref.reference_type == ReferenceType.EXTERNAL
                assert ref.table_reference == "orders"
                assert ref.resolved is None  # External validation needed
            elif ref.column_name == "order_count":
                assert ref.reference_type == ReferenceType.SUBQUERY
                assert ref.table_reference == "sub"
                assert ref.resolved is True  # Resolved internally
            elif ref.column_name == "name":
                assert ref.reference_type == ReferenceType.EXTERNAL
                assert ref.table_reference == "customers"
                assert ref.resolved is None  # External validation needed
            elif ref.column_name == "customer":
                assert ref.reference_type == ReferenceType.EXTERNAL
                assert ref.table_reference == "tbl"
                assert ref.resolved is None  # External validation needed

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

        # Check we have the expected columns
        column_names = [ref.column_name for ref in column_refs]
        assert "a" in column_names
        assert "b" in column_names
        assert "c" in column_names

        # Check reference types and resolution status
        for ref in column_refs:
            if ref.column_name == "a":
                assert ref.reference_type == ReferenceType.EXTERNAL
                assert ref.table_reference == "tbl"
                assert ref.resolved is None  # External validation needed
            if ref.column_name == "b":
                assert ref.reference_type == ReferenceType.EXTERNAL
                assert ref.table_reference == "tbl"
                assert ref.resolved is None  # External validation needed
            if ref.column_name == "c":
                assert ref.reference_type == ReferenceType.CTE
                assert ref.table_reference == "my_cte"
                assert not ref.resolved
