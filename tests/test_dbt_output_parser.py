"""Tests for the dbt output parser."""

from dbt_toolbox.cli._dbt_output_parser import DbtExecutionResult, ModelResult, dbt_output_parser


class TestDbtOutputParser:
    """Test the dbt output parser functionality."""

    def test_parse_successful_models(self) -> None:
        """Test parsing successful model executions."""
        output = """
15:23:45  Running with dbt=1.5.0
15:23:45  1 of 3 OK created table model test_db.customers ................... [SELECT 123 in 0.45s]
15:23:46  2 of 3 OK created view model test_db.orders ...................... [SELECT 456 in 0.32s]
15:23:46  3 of 3 OK created incremental model test_db.payments ............. [INSERT 0 in 0.12s]
        """

        result = dbt_output_parser.parse_output(output)

        assert isinstance(result, DbtExecutionResult)
        assert len(result.successful_models) == 3
        assert "customers" in result.successful_models
        assert "orders" in result.successful_models
        assert "payments" in result.successful_models
        assert len(result.failed_models) == 0
        assert len(result.skipped_models) == 0

    def test_parse_failed_models(self) -> None:
        """Test parsing failed model executions."""
        output = """
15:23:45  Running with dbt=1.5.0
15:23:45  1 of 2 OK created table model test_db.customers ................... [SELECT 123 in 0.45s]
15:23:46  2 of 2 ERROR creating table model test_db.orders .................. [COMPILE ERROR]
        """

        result = dbt_output_parser.parse_output(output)

        assert len(result.successful_models) == 1
        assert "customers" in result.successful_models
        assert len(result.failed_models) == 1
        assert "orders" in result.failed_models
        assert len(result.skipped_models) == 0

    def test_parse_skipped_models(self) -> None:
        """Test parsing skipped model executions."""
        output = """
15:23:45  Running with dbt=1.5.0
15:23:45  1 of 3 OK created table model test_db.customers ................... [SELECT 123 in 0.45s]
15:23:46  2 of 3 SKIP relation test_db.temp_model ............................ [SKIP]
15:23:46  3 of 3 ERROR creating table model test_db.orders .................. [COMPILE ERROR]
        """

        result = dbt_output_parser.parse_output(output)

        assert len(result.successful_models) == 1
        assert "customers" in result.successful_models
        assert len(result.failed_models) == 1
        assert "orders" in result.failed_models
        assert len(result.skipped_models) == 1
        assert "temp_model" in result.skipped_models

    def test_parse_mixed_output_formats(self) -> None:
        """Test parsing different dbt output formats."""
        output = """
15:23:45  Running with dbt=1.5.0
OK created table model test_db.legacy_model .......................... [SELECT 123 in 0.45s]
15:23:45  1 of 2 OK created table model test_db.new_model .................. [SELECT 123 in 0.45s]
ERROR creating table model test_db.broken_model ...................... [COMPILE ERROR]
        """

        result = dbt_output_parser.parse_output(output)

        assert len(result.successful_models) == 2
        assert "legacy_model" in result.successful_models
        assert "new_model" in result.successful_models
        assert len(result.failed_models) == 1
        assert "broken_model" in result.failed_models

    def test_parse_empty_output(self) -> None:
        """Test parsing empty or whitespace-only output."""
        result = dbt_output_parser.parse_output("")

        assert len(result.successful_models) == 0
        assert len(result.failed_models) == 0
        assert len(result.skipped_models) == 0
        assert len(result.all_results) == 0

    def test_parse_no_models_in_output(self) -> None:
        """Test parsing output with no model execution lines."""
        output = """
15:23:45  Running with dbt=1.5.0
15:23:45  Found 5 models, 2 tests, 0 snapshots, 0 analyses, 425 macros, 0 operations, 0 seed files
15:23:45
15:23:45  Concurrency: 4 threads (target='dev')
15:23:45  Done. PASS=0 WARN=0 ERROR=0 SKIP=0 TOTAL=0
        """

        result = dbt_output_parser.parse_output(output)

        assert len(result.successful_models) == 0
        assert len(result.failed_models) == 0
        assert len(result.skipped_models) == 0
        assert len(result.all_results) == 0

    def test_model_result_structure(self) -> None:
        """Test that ModelResult is correctly structured."""
        result = ModelResult(name="test_model", status="OK", error_message=None)

        assert result.name == "test_model"
        assert result.status == "OK"
        assert result.error_message is None

    def test_parse_sql_prefix_models(self) -> None:
        """Test parsing models with 'sql' prefix in output format."""
        output = """
11:10:58  Running with dbt=1.5.0
11:10:58  1 of 3 OK created sql table model dev.customers .......... [SELECT 123 in 0.45s]
11:10:58  2 of 3 OK created sql view model dev.orders ............ [SELECT 456 in 0.32s]
11:10:58  3 of 3 ERROR creating sql view model dev.customer_orders . [ERROR in 0.02s]
        """

        result = dbt_output_parser.parse_output(output)

        assert result.successful_models == ["customers", "orders"]
        assert result.failed_models == ["customer_orders"]
        assert len(result.all_results) == 3

        # Verify specific model results
        customers_result = next(r for r in result.all_results if r.name == "customers")
        assert customers_result.status == "OK"
        assert customers_result.error_message is None

        customer_orders_result = next(r for r in result.all_results if r.name == "customer_orders")
        assert customer_orders_result.status == "ERROR"
        assert customer_orders_result.error_message is not None
