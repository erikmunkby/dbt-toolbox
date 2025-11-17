"""Tests for dbt test tracking in cache system."""

import pytest

from dbt_toolbox.analysees.data_models import ExecutionReason
from dbt_toolbox.analysees.models import analyze_model_statuses
from dbt_toolbox.data_models import EXECUTION_TIMESTAMP, DbtTestResult
from dbt_toolbox.dbt_parser._dbt_parser import dbtParser
from dbt_toolbox.dbt_parser._yaml_test_parser import parse_tests_from_yaml


class TestYamlTestParser:
    """Tests for YAML test parsing."""

    def test_parse_tests_from_yaml(self, dbt_project) -> None:
        """Test parsing tests from schema.yml files."""
        parser = dbtParser()
        tests_by_model = parse_tests_from_yaml(parser.model_yaml_paths)

        # orders model has 2 tests on order_id column
        assert "orders" in tests_by_model
        assert "unique_orders_order_id" in tests_by_model["orders"]
        assert "not_null_orders_order_id" in tests_by_model["orders"]
        assert len(tests_by_model["orders"]) == 2

        # customers model has 2 tests on customer_id column
        assert "customers" in tests_by_model
        assert "unique_customers_customer_id" in tests_by_model["customers"]
        assert "not_null_customers_customer_id" in tests_by_model["customers"]
        assert len(tests_by_model["customers"]) == 2


class TestModelTestTracking:
    """Tests for test tracking in Model objects."""

    def test_model_initializes_with_empty_tests(self, fresh_parser) -> None:
        """Test that models initialize with empty test list."""
        model = fresh_parser.models.get("orders")
        assert model is not None
        assert isinstance(model.tests, list)
        # Tests should be populated after integration
        # For now, this just verifies the structure exists

    def test_all_tests_pass_property_empty_tests(self, fresh_parser) -> None:
        """Test all_tests_pass returns True when no tests exist."""
        model = fresh_parser.models.get("some_other_model")
        assert model is not None
        model.tests = []
        assert model.all_tests_pass is True

    def test_all_tests_pass_property_with_passing_tests(self, fresh_parser) -> None:
        """Test all_tests_pass returns True when all tests pass."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        model.tests = [
            DbtTestResult(name="test1", status="pass", last_executed=EXECUTION_TIMESTAMP),
            DbtTestResult(name="test2", status="pass", last_executed=EXECUTION_TIMESTAMP),
        ]
        assert model.all_tests_pass is True

    def test_all_tests_pass_property_with_failed_test(self, fresh_parser) -> None:
        """Test all_tests_pass returns False when any test fails."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        model.tests = [
            DbtTestResult(name="test1", status="pass", last_executed=EXECUTION_TIMESTAMP),
            DbtTestResult(name="test2", status="fail", last_executed=EXECUTION_TIMESTAMP),
        ]
        assert model.all_tests_pass is False

    def test_all_tests_pass_property_with_never_run_test(self, fresh_parser) -> None:
        """Test all_tests_pass returns False when any test hasn't run."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        model.tests = [
            DbtTestResult(name="test1", status="pass", last_executed=EXECUTION_TIMESTAMP),
            DbtTestResult(name="test2", status="never_run", last_executed=None),
        ]
        assert model.all_tests_pass is False

    def test_set_build_successful_clears_test_results(self, fresh_parser) -> None:
        """Test that set_build_successful clears all test results."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        # Set up tests with various statuses
        model.tests = [
            DbtTestResult(name="test1", status="pass", last_executed=EXECUTION_TIMESTAMP),
            DbtTestResult(name="test2", status="fail", last_executed=EXECUTION_TIMESTAMP),
        ]

        # Rebuild the model
        model.set_build_successful(compute_time_seconds=1.5)

        # All tests should be reset to never_run
        assert len(model.tests) == 2
        assert all(test.status == "never_run" for test in model.tests)
        assert all(test.last_executed is None for test in model.tests)


class TestExecutionAnalysis:
    """Tests for execution analysis with test tracking."""

    def test_model_with_never_run_tests_needs_execution(self, fresh_parser) -> None:
        """Test that models with never-run tests are flagged for execution."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        # Simulate tests that have never been run
        model.tests = [
            DbtTestResult(name="test1", status="never_run", last_executed=None),
        ]
        model.last_built = EXECUTION_TIMESTAMP
        model.last_build_failed = False

        # Analyze should detect tests need execution
        results = analyze_model_statuses(fresh_parser, {"orders": model})
        orders_result = next((r for r in results if r.model.name == "orders"), None)

        # Verify tests need execution
        assert orders_result is not None
        assert orders_result.needs_execution is True
        assert orders_result.reason == ExecutionReason.TESTS_NEED_EXECUTION

    def test_model_with_failed_tests_needs_execution(self, fresh_parser) -> None:
        """Test that models with failed tests are flagged for execution."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        # Simulate failed tests
        model.tests = [
            DbtTestResult(name="test1", status="fail", last_executed=EXECUTION_TIMESTAMP),
        ]
        model.last_built = EXECUTION_TIMESTAMP
        model.last_build_failed = False

        # Analyze should detect tests need execution
        results = analyze_model_statuses(fresh_parser, {"orders": model})
        orders_result = next((r for r in results if r.model.name == "orders"), None)

        # Verify tests need execution
        assert orders_result is not None
        assert orders_result.needs_execution is True
        assert orders_result.reason == ExecutionReason.TESTS_NEED_EXECUTION

    def test_model_with_all_tests_passing_no_execution(self, fresh_parser) -> None:
        """Test that models with all tests passing don't need execution."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        # Simulate all tests passing
        model.tests = [
            DbtTestResult(name="test1", status="pass", last_executed=EXECUTION_TIMESTAMP),
            DbtTestResult(name="test2", status="pass", last_executed=EXECUTION_TIMESTAMP),
        ]
        model.last_built = EXECUTION_TIMESTAMP
        model.last_build_failed = False
        model.code_changed = False
        model.upstream_macros_changed = False

        results = analyze_model_statuses(fresh_parser, {"orders": model})
        orders_result = next((r for r in results if r.model.name == "orders"), None)

        # Model should be fresh and not need execution
        assert orders_result is not None
        assert orders_result.needs_execution is False


class TestCacheIntegration:
    """Tests for test tracking integration with cache system."""

    def test_adding_new_test_in_yaml_marks_never_run(self, fresh_parser) -> None:
        """Test that adding a new test in YAML marks it as never_run without invalidating model."""
        # This test validates that YAML changes create new TestResult with never_run status
        # Will be implemented once _dbt_parser.py integration is complete
        pytest.skip("Requires _dbt_parser.py integration")

    def test_removing_test_from_yaml_removes_from_cache(self, fresh_parser) -> None:
        """Test that removing a test from YAML removes it from cache."""
        # This test validates that YAML changes remove TestResult objects
        # Will be implemented once _dbt_parser.py integration is complete
        pytest.skip("Requires _dbt_parser.py integration")

    def test_model_rebuild_via_run_clears_tests(self, fresh_parser) -> None:
        """Test that 'dt run' rebuilding a model clears test results."""
        model = fresh_parser.models.get("orders")
        assert model is not None

        # Set up tests with passing status
        model.tests = [
            DbtTestResult(name="test1", status="pass", last_executed=EXECUTION_TIMESTAMP),
        ]

        # Simulate model rebuild (like 'dt run' would do)
        model.set_build_successful(compute_time_seconds=1.0)

        # Tests should be cleared
        assert model.tests[0].status == "never_run"
        assert model.tests[0].last_executed is None
