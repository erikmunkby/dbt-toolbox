"""Map dbt test definitions to log output names for test-to-model matching."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_toolbox.data_models import DataTestDefinition, Model


def build_expected_test_name(test_def: DataTestDefinition) -> str:
    """Build the expected dbt test name fragment from a YAML test definition.

    Args:
        test_def: A DataTestDefinition from parsed schema.yml.

    Examples:
        unique + orders + order_id -> "unique_orders_order_id"
        dbt_utils.accepted_range + customer_orders + tax_paid
            -> "accepted_range_customer_orders_tax_paid"

    """
    # Replace package separator with underscore to match dbt log output
    # e.g. "dbt_utils.accepted_range" -> "dbt_utils_accepted_range"
    test_name = test_def.test_name.replace(".", "_")

    parts = [test_name, test_def.model_name]
    if test_def.column_name:
        parts.append(test_def.column_name)
    return "_".join(parts)


def build_test_lookup(models: dict[str, Model]) -> dict[str, str]:
    """Build a lookup from expected test name fragment to model name.

    Args:
        models: Dict of model_name -> Model for models being built.

    Returns:
        Dict mapping expected_fragment -> model_name, sorted longest-first.

    """
    lookup: dict[str, str] = {}
    for model_name, model in models.items():
        for test_def in model.all_data_tests:
            fragment = build_expected_test_name(test_def)
            lookup[fragment] = model_name

    # Sort by fragment length descending to avoid substring ambiguity
    return dict(sorted(lookup.items(), key=lambda x: len(x[0]), reverse=True))


def match_log_test_name(
    log_test_name: str,
    test_lookup: dict[str, str],
) -> str | None:
    """Match a test name from dbt logs against the pre-built lookup.

    Args:
        log_test_name: Test name as it appears in dbt log output.
        test_lookup: Pre-built lookup from build_test_lookup (longest-first).

    Returns:
        The model name if matched, None otherwise.

    """
    for fragment, model_name in test_lookup.items():
        if log_test_name.startswith(fragment):
            return model_name
    return None
