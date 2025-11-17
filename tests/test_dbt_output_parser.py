"""Tests for the dbt output parser."""
# ruff: noqa: E501

from dbt_toolbox.cli._dbt_output_parser import DbtParsedLogs, ModelResult, parse_dbt_output


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

        result = parse_dbt_output(output)

        assert isinstance(result, DbtParsedLogs)
        assert len(result.successful_models) == 3
        assert "customers" in result.successful_models
        assert "orders" in result.successful_models
        assert "payments" in result.successful_models
        assert len(result.failed_models) == 0

    def test_parse_failed_models(self) -> None:
        """Test parsing failed model executions."""
        output = """
15:23:45  Running with dbt=1.5.0
15:23:45  1 of 2 OK created table model test_db.customers ................... [SELECT 123 in 0.45s]
15:23:46  2 of 2 ERROR creating table model test_db.orders .................. [COMPILE ERROR]
        """

        result = parse_dbt_output(output)

        assert len(result.successful_models) == 1
        assert "customers" in result.successful_models
        assert len(result.failed_models) == 1
        assert "orders" in result.failed_models

    def test_parse_skipped_models(self) -> None:
        """Test parsing skipped model executions."""
        output = """
15:23:45  Running with dbt=1.5.0
15:23:45  1 of 3 OK created table model test_db.customers ................... [SELECT 123 in 0.45s]
15:23:46  2 of 3 SKIP relation test_db.temp_model ............................ [SKIP]
15:23:46  3 of 3 ERROR creating table model test_db.orders .................. [COMPILE ERROR]
        """

        result = parse_dbt_output(output)

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

        result = parse_dbt_output(output)

        assert len(result.successful_models) == 2
        assert "legacy_model" in result.successful_models
        assert "new_model" in result.successful_models
        assert len(result.failed_models) == 1
        assert "broken_model" in result.failed_models

    def test_parse_empty_output(self) -> None:
        """Test parsing empty or whitespace-only output."""
        result = parse_dbt_output("")

        assert len(result.successful_models) == 0
        assert len(result.failed_models) == 0

        assert len(result.models) == 0

    def test_parse_no_models_in_output(self) -> None:
        """Test parsing output with no model execution lines."""
        output = """
15:23:45  Running with dbt=1.5.0
15:23:45  Found 5 models, 2 tests, 0 snapshots, 0 analyses, 425 macros, 0 operations, 0 seed files
15:23:45
15:23:45  Concurrency: 4 threads (target='dev')
15:23:45  Done. PASS=0 WARN=0 ERROR=0 SKIP=0 TOTAL=0
        """

        result = parse_dbt_output(output)

        assert len(result.successful_models) == 0
        assert len(result.failed_models) == 0

        assert len(result.models) == 0

    def test_model_result_structure(self) -> None:
        """Test that ModelResult is correctly structured."""
        result = ModelResult(
            name="test_model", status="OK", execution_time_seconds=1.5, error_message=None
        )

        assert result.name == "test_model"
        assert result.status == "OK"
        assert result.execution_time_seconds == 1.5
        assert result.error_message is None

    def test_parse_sql_prefix_models(self) -> None:
        """Test parsing models with 'sql' prefix in output format."""
        output = """
11:10:58  Running with dbt=1.5.0
11:10:58  1 of 3 OK created sql table model dev.customers .......... [SELECT 123 in 0.45s]
11:10:58  2 of 3 OK created sql view model dev.orders ............ [SELECT 456 in 0.32s]
11:10:58  3 of 3 ERROR creating sql view model dev.customer_orders . [ERROR in 0.02s]
        """

        result = parse_dbt_output(output)

        assert result.successful_models == ["customers", "orders"]
        assert result.failed_models == ["customer_orders"]
        assert len(result.models) == 3

        # Verify specific model results
        customers_result = result.models["customers"]
        assert customers_result.status == "OK"
        assert customers_result.error_message is None

        customer_orders_result = result.models["customer_orders"]
        assert customer_orders_result.status == "ERROR"
        assert customer_orders_result.error_message is not None

    def test_parse_execution_times(self) -> None:
        """Test that execution times are correctly parsed."""
        output = """
11:10:58  Running with dbt=1.5.0
11:10:58  1 of 3 OK created sql table model dev.customers .......... [SELECT 123 in 0.45s]
11:10:58  2 of 3 OK created sql view model dev.orders ............ [INSERT 456 in 0.32s]
11:10:58  3 of 3 OK creating sql view model dev.customer_orders . [OK in 0.02s]
        """

        result = parse_dbt_output(output)

        assert len(result.models) == 3

        # Check execution times for successful models
        customers_result = result.models["customers"]
        assert customers_result.execution_time_seconds == 0.45

        orders_result = result.models["orders"]
        assert orders_result.execution_time_seconds == 0.32

        # Check execution time for failed model
        customer_orders_result = result.models["customer_orders"]
        assert customer_orders_result.execution_time_seconds == 0.02

    def test_parse_passing_tests(self) -> None:
        """Test parsing passing test executions."""
        output = """
21:17:54  5 of 8 START test not_null_customers_customer_id ............................... [RUN]
21:17:54  6 of 8 START test not_null_orders_order_id ..................................... [RUN]
21:17:54  5 of 8 PASS not_null_customers_customer_id ..................................... [PASS in 0.04s]
21:17:54  6 of 8 PASS not_null_orders_order_id ........................................... [PASS in 0.04s]
21:17:54  7 of 8 PASS unique_customers_customer_id ....................................... [PASS in 0.04s]
21:17:54  8 of 8 PASS unique_orders_order_id ............................................. [PASS in 0.01s]
        """

        result = parse_dbt_output(output)

        assert len(result.tests) == 4
        assert "not_null_customers_customer_id" in result.tests
        assert "not_null_orders_order_id" in result.tests
        assert "unique_customers_customer_id" in result.tests
        assert "unique_orders_order_id" in result.tests

        # Check all are PASS status
        for test_name, test_result in result.tests.items():
            assert test_result.status == "PASS", f"{test_name} should be PASS"
            assert test_result.execution_time_seconds is not None

    def test_parse_tests_with_namespace(self) -> None:
        """Test parsing tests with namespace prefix."""
        output = """
21:17:54  4 of 8 START test dbt_utils_expression_is_true_customers_1_1 ................... [RUN]
21:17:54  4 of 8 PASS dbt_utils_expression_is_true_customers_1_1 ......................... [PASS in 0.04s]
        """

        result = parse_dbt_output(output)

        assert len(result.tests) == 1
        assert "dbt_utils_expression_is_true_customers_1_1" in result.tests

        test = result.tests["dbt_utils_expression_is_true_customers_1_1"]
        assert test.status == "PASS"
        assert test.execution_time_seconds == 0.04

    def test_parse_mixed_models_and_tests(self) -> None:
        """Test parsing output with both models and tests."""
        output = """
21:17:54  1 of 8 START sql view model dev.customers ...................................... [RUN]
21:17:54  1 of 8 OK created sql view model dev.customers ................................. [OK in 0.06s]
21:17:54  3 of 8 OK created sql view model dev.orders .................................... [OK in 0.06s]
21:17:54  5 of 8 START test not_null_customers_customer_id ............................... [RUN]
21:17:54  6 of 8 START test not_null_orders_order_id ..................................... [RUN]
21:17:54  5 of 8 PASS not_null_customers_customer_id ..................................... [PASS in 0.04s]
21:17:54  6 of 8 PASS not_null_orders_order_id ........................................... [PASS in 0.04s]
        """

        result = parse_dbt_output(output)

        # Check models
        assert len(result.models) == 2
        assert "customers" in result.models
        assert "orders" in result.models

        # Check tests
        assert len(result.tests) == 2
        assert "not_null_customers_customer_id" in result.tests
        assert "not_null_orders_order_id" in result.tests

    def test_extract_model_name_from_test(self) -> None:
        """Test that model names are correctly extracted from test names."""
        from dbt_toolbox.cli._dbt_output_parser import _extract_model_name_from_test

        # Test without namespace
        assert _extract_model_name_from_test("not_null_customers_customer_id") == "customers"
        assert _extract_model_name_from_test("unique_orders_order_id") == "orders"

        # Test with namespace
        assert (
            _extract_model_name_from_test("dbt_utils_expression_is_true_customers_1_2")
            == "customers"
        )

    def test_test_result_includes_model_name(self) -> None:
        """Test that TestResult includes the model_name field."""
        output = """
21:17:54  5 of 8 PASS not_null_customers_customer_id ..................................... [PASS in 0.04s]
21:17:54  6 of 8 PASS unique_orders_order_id ............................................. [PASS in 0.04s]
21:17:54  7 of 8 FAIL 930 dbt_utils_expression_is_true_customers_1_2 ..................... [FAIL 930 in 0.05s]
        """

        result = parse_dbt_output(output)

        # Check that all tests have model_name populated
        assert result.tests["not_null_customers_customer_id"].model_name == "customers"
        assert result.tests["unique_orders_order_id"].model_name == "orders"
        assert result.tests["dbt_utils_expression_is_true_customers_1_2"].model_name == "customers"
