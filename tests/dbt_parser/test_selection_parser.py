"""Tests for SelectionParser class.

Sample project structure:
- customers -> customer_orders
- orders -> customer_orders
- some_other_model (isolated)
"""

import pytest

from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.dbt_parser._selection_parser import SelectionParser

# Note: Using session-scoped 'parser' fixture from conftest.py for performance
# All tests in this file are read-only, so they can share the parser instance

@pytest.fixture(scope="session")
def selection_parser(parser: dbtParser) -> SelectionParser:
    """Get a session-scoped SelectionParser instance for read-only tests."""
    return parser.selection_parser


def test_direct_selection(selection_parser: SelectionParser) -> None:
    """Test direct model selection with various separators."""
    # Single model
    assert selection_parser.parse("customers") == {"customers"}

    # Multiple models (comma and space separated)
    assert selection_parser.parse("customers,orders") == {"customers", "orders"}
    assert selection_parser.parse("customers orders") == {"customers", "orders"}

    # None/empty returns all models
    assert len(selection_parser.parse(None)) == 7
    assert len(selection_parser.parse("")) == 7


def test_upstream_selection(selection_parser: SelectionParser) -> None:
    """Test upstream selection (+model) includes dependencies."""
    # Model with upstream models
    assert selection_parser.parse("+customer_orders") == {
        "customer_orders",
        "customers",
        "orders",
    }

    # Model with source dependencies (sources are included in upstream)
    assert selection_parser.parse("+customers") == {"customers", "raw_customers"}

    # Isolated model
    assert selection_parser.parse("+some_other_model") == {"some_other_model"}


def test_downstream_selection(selection_parser: SelectionParser) -> None:
    """Test downstream selection (model+) includes dependents."""
    # Model with downstream
    assert selection_parser.parse("customers+") == {"customers", "customer_orders"}

    # Leaf model (no downstream)
    assert selection_parser.parse("customer_orders+") == {"customer_orders"}

    # Multiple selections deduplicate shared downstream
    assert selection_parser.parse("customers+ orders+") == {
        "customers",
        "orders",
        "customer_orders",
    }


def test_combined_selection(selection_parser: SelectionParser) -> None:
    """Test combined selection (+model+) includes both directions."""
    # Model with both upstream and downstream
    assert selection_parser.parse("+customers+") == {
        "customers",
        "customer_orders",
        "raw_customers",
    }

    # Leaf model with upstream
    assert selection_parser.parse("+customer_orders+") == {
        "customer_orders",
        "customers",
        "orders",
    }


def test_edge_cases(selection_parser: SelectionParser) -> None:
    """Test whitespace, duplicates, and overlapping selections."""
    # Whitespace handling
    assert selection_parser.parse("  customers  ,  orders  ") == {"customers", "orders"}

    # Duplicates are deduplicated
    assert selection_parser.parse("customers,customers") == {"customers"}

    # Overlapping selections merge correctly
    assert selection_parser.parse("customers,customers+") == {
        "customers",
        "customer_orders",
    }


def test_parse_return_models(selection_parser: SelectionParser) -> None:
    """Test parse_return_models returns Model objects."""
    # Returns Model objects
    result = selection_parser.parse_return_models("customers")
    assert len(result) == 1
    assert result["customers"].name == "customers"
    assert hasattr(result["customers"], "raw_code")

    # None returns all models
    all_models = selection_parser.parse_return_models(None)
    assert len(all_models) == 7
    assert all(hasattr(m, "raw_code") for m in all_models.values())


def test_backward_compatibility(parser: dbtParser) -> None:
    """Test SelectionParser matches old dbtParser behavior."""
    test_cases = ["customers", "+customer_orders", "customers+", None]

    for selection in test_cases:
        # parse() matches parse_selection_query()
        assert parser.parse_selection_query(selection) == parser.selection_parser.parse(selection)

        # parse_return_models() matches parse_selection_query_return_models()
        old_result = parser.parse_selection_query_return_models(selection)
        new_result = parser.selection_parser.parse_return_models(selection)
        assert set(old_result.keys()) == set(new_result.keys())
