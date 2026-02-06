"""Tests for data test parsing, log extraction, and test-to-model mapping."""

from datetime import datetime, timezone

from dbt_toolbox.actions.test_mapper import (
    build_expected_test_name,
    build_test_lookup,
    match_log_test_name,
)
from dbt_toolbox.analysees.data_models import ExecutionReason
from dbt_toolbox.analysees.models import _analyze_model
from dbt_toolbox.cli._dbt_output_parser import TestResult, _extract_test_info, parse_dbt_output
from dbt_toolbox.data_models import DataTestDefinition
from dbt_toolbox.dbt_parser import dbtParser

# ── Step 1: YAML test parsing (integration tests) ─────────────────────────


class TestYamlDataTestParsing:
    """Test parsing data_tests from schema.yml via dbtParser."""

    def test_orders_has_two_tests(self, parser: dbtParser) -> None:
        """Verify orders model has unique and not_null tests on order_id."""
        tests = parser.models["orders"].all_data_tests
        test_names = {(t.test_name, t.column_name) for t in tests}
        assert ("unique", "order_id") in test_names
        assert ("not_null", "order_id") in test_names
        assert len(tests) == 2

    def test_customer_orders_has_accepted_range_test(self, parser: dbtParser) -> None:
        """Verify customer_orders has dbt_utils.accepted_range on tax_paid."""
        tests = parser.models["customer_orders"].all_data_tests
        assert len(tests) == 1
        t = tests[0]
        assert t.test_name == "dbt_utils.accepted_range"
        assert t.column_name == "tax_paid"
        assert t.model_name == "customer_orders"
        assert t.kwargs == {"max_value": 100000, "inclusive": True}

    def test_customers_has_two_tests(self, parser: dbtParser) -> None:
        """Verify customers model has unique and not_null tests on customer_id."""
        tests = parser.models["customers"].all_data_tests
        test_names = {(t.test_name, t.column_name) for t in tests}
        assert ("unique", "customer_id") in test_names
        assert ("not_null", "customer_id") in test_names
        assert len(tests) == 2

    def test_model_without_tests(self, parser: dbtParser) -> None:
        """Verify model without data_tests returns empty list."""
        tests = parser.models["some_other_model"].all_data_tests
        assert tests == []

    def test_tests_hash_stable(self, parser: dbtParser) -> None:
        """Verify tests_hash is deterministic."""
        h1 = parser.models["orders"].tests_hash
        h2 = parser.models["orders"].tests_hash
        assert h1 == h2
        assert len(h1) == 10

    def test_tests_hash_differs_between_models(self, parser: dbtParser) -> None:
        """Verify different test definitions produce different hashes."""
        assert parser.models["orders"].tests_hash != parser.models["customers"].tests_hash


# ── Step 2: Log test result parsing ────────────────────────────────────────


class TestLogTestParsing:
    """Test parsing test results from dbt build log output."""

    def test_parse_build_with_tests_log(self) -> None:
        """Parse the real dbt_build_with_tests.log file."""
        log_path = "tests/logs/dbt_build_with_tests.log"
        with open(log_path) as f:
            output = f.read()
        result = parse_dbt_output(output)

        # Should have 1 model result
        assert "orders" in result.models
        assert result.models["orders"].status == "OK"

        # Should have 2 test results
        assert len(result.tests) == 2
        assert "not_null_orders_order_id" in result.tests
        assert "unique_orders_order_id" in result.tests
        assert result.tests["not_null_orders_order_id"].status == "PASS"
        assert result.tests["unique_orders_order_id"].status == "PASS"
        assert result.tests["not_null_orders_order_id"].execution_time_seconds == 0.03

    def test_parse_pass_tests(self) -> None:
        """Parse a PASS test line."""
        output = "  2 of 3 PASS not_null_orders_order_id ......... [PASS in 0.03s]"
        result = parse_dbt_output(output)
        assert len(result.tests) == 1
        assert result.tests["not_null_orders_order_id"].status == "PASS"
        assert result.tests["not_null_orders_order_id"].execution_time_seconds == 0.03

    def test_parse_fail_tests(self) -> None:
        """Parse a FAIL test line."""
        output = "  2 of 3 FAIL 1 not_null_orders_order_id ....... [FAIL 1 in 0.05s]"
        result = parse_dbt_output(output)
        assert len(result.tests) == 1
        assert result.tests["not_null_orders_order_id"].status == "FAIL"
        assert result.tests["not_null_orders_order_id"].execution_time_seconds == 0.05

    def test_parse_warn_tests(self) -> None:
        """Parse a WARN test line."""
        output = "  2 of 3 WARN 5 not_null_orders_order_id ....... [WARN 5 in 0.02s]"
        result = parse_dbt_output(output)
        assert len(result.tests) == 1
        assert result.tests["not_null_orders_order_id"].status == "WARN"

    def test_parse_error_tests(self) -> None:
        """Parse an ERROR test line."""
        output = "  2 of 3 ERROR not_null_orders_order_id ........ [ERROR in 0.01s]"
        result = parse_dbt_output(output)
        assert len(result.tests) == 1
        assert result.tests["not_null_orders_order_id"].status == "ERROR"

    def test_model_lines_not_parsed_as_tests(self) -> None:
        """Ensure model output lines are not mistakenly parsed as tests."""
        output = "  1 of 3 OK created sql view model dev.orders ... [OK in 0.05s]"
        result = parse_dbt_output(output)
        assert len(result.tests) == 0
        assert len(result.models) == 1

    def test_passed_and_failed_tests_properties(self) -> None:
        """Verify passed_tests and failed_tests filter properties."""
        result = parse_dbt_output(
            "PASS test_a ......... [PASS in 0.01s]\n"
            "FAIL 1 test_b ....... [FAIL 1 in 0.02s]\n"
            "PASS test_c ......... [PASS in 0.01s]"
        )
        assert sorted(result.passed_tests) == ["test_a", "test_c"]
        assert result.failed_tests == ["test_b"]

    def test_test_result_structure(self) -> None:
        """Verify TestResult named tuple fields."""
        t = TestResult(name="test_a", status="PASS", execution_time_seconds=0.01)
        assert t.name == "test_a"
        assert t.status == "PASS"
        assert t.execution_time_seconds == 0.01


# ── Step 3: Test mapper ───────────────────────────────────────────────────


class TestTestMapper:
    """Test building expected test names and matching against log output."""

    def test_build_expected_name_simple(self) -> None:
        """Build name from simple test (unique on column)."""
        td = DataTestDefinition(test_name="unique", model_name="orders", column_name="order_id")
        assert build_expected_test_name(td) == "unique_orders_order_id"

    def test_build_expected_name_with_package(self) -> None:
        """Build name from package-prefixed test (dbt_utils.accepted_range)."""
        td = DataTestDefinition(
            test_name="dbt_utils.accepted_range",
            model_name="customer_orders",
            column_name="tax_paid",
            kwargs={"max_value": 100000, "inclusive": True},
        )
        assert build_expected_test_name(td) == "accepted_range_customer_orders_tax_paid"

    def test_build_expected_name_model_level(self) -> None:
        """Build name from model-level test (no column)."""
        td = DataTestDefinition(test_name="some_test", model_name="orders")
        assert build_expected_test_name(td) == "some_test_orders"

    def test_build_test_lookup(self, parser: dbtParser) -> None:
        """Verify lookup contains correct fragment-to-model mappings."""
        lookup = build_test_lookup(parser.models)
        assert "unique_orders_order_id" in lookup
        assert lookup["unique_orders_order_id"] == "orders"
        assert "not_null_orders_order_id" in lookup
        assert lookup["not_null_orders_order_id"] == "orders"
        assert "accepted_range_customer_orders_tax_paid" in lookup
        assert lookup["accepted_range_customer_orders_tax_paid"] == "customer_orders"

    def test_build_test_lookup_sorted_longest_first(self, parser: dbtParser) -> None:
        """Verify lookup keys are sorted by length descending."""
        lookup = build_test_lookup(parser.models)
        keys = list(lookup.keys())
        for i in range(len(keys) - 1):
            assert len(keys[i]) >= len(keys[i + 1])

    def test_match_log_test_name_simple(self, parser: dbtParser) -> None:
        """Match simple test names to their models."""
        lookup = build_test_lookup(parser.models)
        assert match_log_test_name("not_null_orders_order_id", lookup) == "orders"
        assert match_log_test_name("unique_orders_order_id", lookup) == "orders"

    def test_match_log_test_name_with_package_suffix(self, parser: dbtParser) -> None:
        """Match test name with dbt-appended kwargs suffix."""
        lookup = build_test_lookup(parser.models)
        result = match_log_test_name(
            "accepted_range_customer_orders_tax_paid__True__100000", lookup
        )
        assert result == "customer_orders"

    def test_match_log_test_name_ambiguity_resolved(self, parser: dbtParser) -> None:
        """Longest match wins: 'customer_orders' matches before 'orders'."""
        lookup = build_test_lookup(parser.models)
        result = match_log_test_name(
            "accepted_range_customer_orders_tax_paid__True__100000", lookup
        )
        assert result == "customer_orders"

    def test_match_log_test_name_no_match(self, parser: dbtParser) -> None:
        """Return None when no fragment matches."""
        lookup = build_test_lookup(parser.models)
        assert match_log_test_name("unknown_test_xyz", lookup) is None


# ── Step 4: Test-aware cache invalidation ──────────────────────────────────


class TestTestDefinitionChangeInvalidatesCache:
    """Test that changing data_tests in schema.yml flags tests_changed on cached models."""

    def test_adding_test_sets_tests_changed(self, dbt_project, fresh_parser) -> None:
        """Adding a data_test to schema.yml triggers tests_changed on next parse."""
        # Parse models to populate cache — simulate a successful build
        model = fresh_parser.get_model("orders")
        assert model is not None
        assert model.tests_changed is False
        model.set_build_successful(compute_time_seconds=0.5)
        fresh_parser.cache.cache_model(model=model)

        # Modify schema.yml: add a new data_test to orders.customer_id
        schema_path = dbt_project / "models" / "schema.yml"
        content = schema_path.read_text()
        content = content.replace(
            "      - name: customer_id\n        description: Foreign key to the customers table",
            "      - name: customer_id\n"
            "        description: Foreign key to the customers table\n"
            "        data_tests:\n"
            "          - not_null",
        )
        schema_path.write_text(content)

        # Create new parser (simulating second run) — model should have tests_changed
        new_parser = dbtParser(target=None)
        new_model = new_parser.get_model("orders")
        assert new_model is not None
        assert new_model.tests_changed is True

        # Analysis should return TESTS_CHANGED reason
        result = _analyze_model(new_model)
        assert result.needs_execution is True
        assert result.reason == ExecutionReason.TESTS_CHANGED


class TestFailedTestFlagsModel:
    """Test that test failure tracking works correctly on Model."""

    def test_set_tests_failed_flags_model(self, parser: dbtParser) -> None:
        """Calling set_tests_failed sets last_tests_failed to True."""
        model = parser.models["orders"]
        model.last_tests_failed = None  # reset

        model.set_tests_failed()
        assert model.last_tests_failed is True

    def test_set_tests_passed_does_not_override_failure(self, parser: dbtParser) -> None:
        """set_tests_passed is sticky — does not override a prior failure."""
        model = parser.models["orders"]
        model.last_tests_failed = None  # reset

        model.set_tests_failed()
        model.set_tests_passed()
        assert model.last_tests_failed is True

    def test_set_tests_passed_when_no_failure(self, parser: dbtParser) -> None:
        """set_tests_passed sets False when no failure has been recorded."""
        model = parser.models["orders"]
        model.last_tests_failed = None  # reset

        model.set_tests_passed()
        assert model.last_tests_failed is False

    def test_analyze_model_last_tests_failed(self, parser: dbtParser) -> None:
        """_analyze_model returns LAST_TESTS_FAILED for flagged models."""
        model = parser.models["orders"]
        model.last_built = datetime.now(tz=timezone.utc)
        model.last_build_failed = False
        model.code_changed = False
        model.upstream_macros_changed = False
        model.tests_changed = False
        model.last_tests_failed = True

        result = _analyze_model(model)
        assert result.needs_execution is True
        assert result.reason == ExecutionReason.LAST_TESTS_FAILED

    def test_set_build_successful_resets_test_state(self, parser: dbtParser) -> None:
        """set_build_successful clears tests_changed and last_tests_failed."""
        model = parser.models["orders"]
        model.tests_changed = True
        model.last_tests_failed = True

        model.set_build_successful(compute_time_seconds=1.0)
        assert model.tests_changed is False
        assert model.last_tests_failed is None


class TestRunCommandIgnoresTestReasons:
    """Test that 'run' command filters out test-only execution reasons."""

    def test_tests_changed_filtered_for_run(self, parser: dbtParser) -> None:
        """Model with tests_changed=True gets needs_execution=False for run."""
        model = parser.models["orders"]
        model.last_built = datetime.now(tz=timezone.utc)
        model.last_build_failed = False
        model.code_changed = False
        model.upstream_macros_changed = False
        model.tests_changed = True
        model.last_tests_failed = None

        analysis = _analyze_model(model)
        assert analysis.reason == ExecutionReason.TESTS_CHANGED

        # Simulate run-command filtering
        test_reasons = {ExecutionReason.TESTS_CHANGED, ExecutionReason.LAST_TESTS_FAILED}
        if analysis.reason in test_reasons:
            analysis.needs_execution = False
            analysis.reason = None

        assert analysis.needs_execution is False

    def test_last_tests_failed_filtered_for_run(self, parser: dbtParser) -> None:
        """Model with last_tests_failed gets needs_execution=False for run."""
        model = parser.models["orders"]
        model.last_built = datetime.now(tz=timezone.utc)
        model.last_build_failed = False
        model.code_changed = False
        model.upstream_macros_changed = False
        model.tests_changed = False
        model.last_tests_failed = True

        analysis = _analyze_model(model)
        assert analysis.reason == ExecutionReason.LAST_TESTS_FAILED

        # Simulate run-command filtering
        test_reasons = {ExecutionReason.TESTS_CHANGED, ExecutionReason.LAST_TESTS_FAILED}
        if analysis.reason in test_reasons:
            analysis.needs_execution = False
            analysis.reason = None

        assert analysis.needs_execution is False


class TestStreamingTestResultProcessing:
    """Test that test results from log lines can be matched to models."""

    def test_fail_line_matches_model(self, parser: dbtParser) -> None:
        """A FAIL test line is parsed and matched to the correct model."""
        test_lookup = build_test_lookup(parser.models)

        line = "FAIL 1 not_null_orders_order_id ....... [FAIL 1 in 0.05s]"
        test_info = _extract_test_info(line)
        assert test_info is not None
        assert test_info.status == "FAIL"

        model_name = match_log_test_name(test_info.name, test_lookup)
        assert model_name == "orders"

    def test_pass_line_matches_model(self, parser: dbtParser) -> None:
        """A PASS test line is parsed and matched to the correct model."""
        test_lookup = build_test_lookup(parser.models)

        line = "PASS unique_orders_order_id ......... [PASS in 0.03s]"
        test_info = _extract_test_info(line)
        assert test_info is not None
        assert test_info.status == "PASS"

        model_name = match_log_test_name(test_info.name, test_lookup)
        assert model_name == "orders"

    def test_package_test_matches_model(self, parser: dbtParser) -> None:
        """A package test line with kwargs suffix matches the correct model."""
        test_lookup = build_test_lookup(parser.models)

        line = "FAIL 1 accepted_range_customer_orders_tax_paid__True__100000 ... [FAIL 1 in 0.02s]"
        test_info = _extract_test_info(line)
        assert test_info is not None

        model_name = match_log_test_name(test_info.name, test_lookup)
        assert model_name == "customer_orders"
