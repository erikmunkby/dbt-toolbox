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

# Expected number of models in the sample dbt project
EXPECTED_MODEL_COUNT = 9


@pytest.fixture(scope="session")
def selection_parser(parser: dbtParser) -> SelectionParser:
    """Get a session-scoped SelectionParser instance for read-only tests."""
    return parser.selection_parser


def test_direct_selection(selection_parser: SelectionParser) -> None:
    """Test direct model selection with various separators."""
    # Single model
    result = selection_parser.parse("customers")
    assert set(result.model_names) == {"customers"}
    assert result.had_path_selection is False

    # Multiple models (comma and space separated)
    result = selection_parser.parse("customers,orders")
    assert set(result.model_names) == {"customers", "orders"}
    assert result.had_path_selection is False

    result = selection_parser.parse("customers orders")
    assert set(result.model_names) == {"customers", "orders"}
    assert result.had_path_selection is False

    # None/empty returns all models
    result = selection_parser.parse(None)
    assert len(result.model_names) == EXPECTED_MODEL_COUNT
    assert result.had_path_selection is False

    result = selection_parser.parse("")
    assert len(result.model_names) == EXPECTED_MODEL_COUNT
    assert result.had_path_selection is False


def test_upstream_selection(selection_parser: SelectionParser) -> None:
    """Test upstream selection (+model) includes dependencies."""
    # Model with upstream models
    result = selection_parser.parse("+customer_orders")
    assert set(result.model_names) == {"customer_orders", "customers", "orders"}
    assert result.had_path_selection is False

    # Model with source dependencies (sources are NOT included, only models)
    result = selection_parser.parse("+customers")
    assert set(result.model_names) == {"customers"}  # raw_customers is a source, not a model
    assert result.had_path_selection is False

    # Isolated model
    result = selection_parser.parse("+some_other_model")
    assert set(result.model_names) == {"some_other_model"}
    assert result.had_path_selection is False


def test_downstream_selection(selection_parser: SelectionParser) -> None:
    """Test downstream selection (model+) includes dependents."""
    # Model with downstream
    result = selection_parser.parse("customers+")
    assert set(result.model_names) == {"customers", "customer_orders"}
    assert result.had_path_selection is False

    # Leaf model (no downstream)
    result = selection_parser.parse("customer_orders+")
    assert set(result.model_names) == {"customer_orders"}
    assert result.had_path_selection is False

    # Multiple selections deduplicate shared downstream
    result = selection_parser.parse("customers+ orders+")
    assert set(result.model_names) == {"customers", "orders", "customer_orders"}
    assert result.had_path_selection is False


def test_combined_selection(selection_parser: SelectionParser) -> None:
    """Test combined selection (+model+) includes both directions."""
    # Model with both upstream and downstream
    result = selection_parser.parse("+customers+")
    assert set(result.model_names) == {"customers", "customer_orders"}  # raw_customers is a source
    assert result.had_path_selection is False

    # Leaf model with upstream
    result = selection_parser.parse("+customer_orders+")
    assert set(result.model_names) == {"customer_orders", "customers", "orders"}
    assert result.had_path_selection is False


def test_edge_cases(selection_parser: SelectionParser) -> None:
    """Test whitespace, duplicates, and overlapping selections."""
    # Whitespace handling
    result = selection_parser.parse("  customers  ,  orders  ")
    assert set(result.model_names) == {"customers", "orders"}
    assert result.had_path_selection is False

    # Duplicates are deduplicated
    result = selection_parser.parse("customers,customers")
    assert set(result.model_names) == {"customers"}
    assert result.had_path_selection is False

    # Overlapping selections merge correctly
    result = selection_parser.parse("customers,customers+")
    assert set(result.model_names) == {"customers", "customer_orders"}
    assert result.had_path_selection is False


def test_selection_result_properties(selection_parser: SelectionParser) -> None:
    """Test SelectionResult properties (models, models_dict)."""
    # Returns SelectionResult with proper properties
    result = selection_parser.parse("customers")
    assert len(result.model_names) == 1
    assert len(result.models) == 1
    assert result.models[0].name == "customers"
    assert hasattr(result.models[0], "raw_code")

    # models_dict property works
    assert "customers" in result.models_dict
    assert result.models_dict["customers"].name == "customers"

    # None returns all models
    result = selection_parser.parse(None)
    assert len(result.models) == EXPECTED_MODEL_COUNT
    assert all(hasattr(m, "raw_code") for m in result.models)
    assert len(result.models_dict) == EXPECTED_MODEL_COUNT


def test_backward_compatibility(parser: dbtParser) -> None:
    """Test SelectionParser matches dbtParser behavior."""
    test_cases = ["customers", "+customer_orders", "customers+", None]

    for selection in test_cases:
        # parse_selection_query() should return SelectionResult
        result = parser.parse_selection_query(selection)
        direct_result = parser.selection_parser.parse(selection)

        # Both should return the same model names and models
        assert set(result.model_names) == set(direct_result.model_names)
        assert set(result.models_dict.keys()) == set(direct_result.models_dict.keys())


def test_path_based_selection(selection_parser: SelectionParser) -> None:
    """Test path-based selection with various formats and operators."""
    # Basic path selection - different formats (prefix, implicit, partial)
    assert set(selection_parser.parse("path:models/subfolder").model_names) == {"subfolder_model"}
    assert set(selection_parser.parse("models/subfolder").model_names) == {"subfolder_model"}
    assert set(selection_parser.parse("path:subfolder").model_names) == {"subfolder_model"}
    assert set(selection_parser.parse("subfolder/").model_names) == {"subfolder_model"}

    # File selection
    assert set(selection_parser.parse("path:models/customers.sql").model_names) == {"customers"}
    assert set(selection_parser.parse("models/customers.sql").model_names) == {"customers"}

    # All should set the path selection flag
    assert selection_parser.parse("path:subfolder").had_path_selection is True
    assert selection_parser.parse("models/subfolder").had_path_selection is True


def test_path_based_selection_with_operators(selection_parser: SelectionParser) -> None:
    """Test path-based selection with upstream/downstream operators."""
    # Upstream: no upstream for subfolder_model
    result = selection_parser.parse("+models/subfolder/subfolder_model.sql")
    assert set(result.model_names) == {"subfolder_model"}
    assert result.had_path_selection is True

    # Upstream: customer_orders has upstream models
    result = selection_parser.parse("+models/customer_orders.sql")
    assert set(result.model_names) == {"customer_orders", "customers", "orders"}
    assert result.had_path_selection is True

    # Downstream: customers has downstream
    result = selection_parser.parse("models/customers.sql+")
    assert set(result.model_names) == {"customers", "customer_orders"}
    assert result.had_path_selection is True

    # Combined upstream/downstream
    result = selection_parser.parse("+models/customers.sql+")
    assert set(result.model_names) == {"customers", "customer_orders"}  # raw_customers is a source
    assert result.had_path_selection is True


def test_path_based_selection_multiple_and_mixed(selection_parser: SelectionParser) -> None:
    """Test multiple path selections and mixing path with name selections."""
    # Multiple path selections
    result = selection_parser.parse("models/customers.sql,models/orders.sql")
    assert set(result.model_names) == {"customers", "orders"}
    assert result.had_path_selection is True

    # Mix of path and name selections
    result = selection_parser.parse("models/customers.sql,some_other_model")
    assert set(result.model_names) == {"customers", "some_other_model"}
    assert result.had_path_selection is True

    # Whitespace handling
    result = selection_parser.parse("  models/customers.sql  ")
    assert set(result.model_names) == {"customers"}
    assert result.had_path_selection is True

    # Non-existent path returns empty
    result = selection_parser.parse("models/nonexistent/path")
    assert set(result.model_names) == set()
    assert result.had_path_selection is True


def test_fuzzy_matching_off(parser: dbtParser) -> None:
    """Test fuzzy matching when mode is 'off'."""
    from unittest.mock import patch

    from dbt_toolbox.settings import Settings

    # Create a settings instance with fuzzy_model_matching = "off"
    with patch.object(Settings, "fuzzy_model_matching", "off"):
        # Get a fresh selection parser with the patched settings
        selection_parser = parser.selection_parser

        # Non-existent model returns empty (no fuzzy matching)
        result = selection_parser.parse("custmers")  # Typo in "customers"
        assert set(result.model_names) == set()
        assert result.had_path_selection is False


def test_fuzzy_matching_automatic(parser: dbtParser) -> None:
    """Test fuzzy matching in automatic mode."""
    from unittest.mock import patch

    from dbt_toolbox.settings import Settings

    # Create a settings instance with fuzzy_model_matching = "automatic"
    with patch.object(Settings, "fuzzy_model_matching", "automatic"):
        # Get a fresh selection parser with the patched settings
        selection_parser = parser.selection_parser

        # Typo should automatically resolve to correct model
        result = selection_parser.parse("custmers")  # Typo in "customers"
        assert set(result.model_names) == {"customers"}
        assert result.had_path_selection is False

        # Another typo
        result = selection_parser.parse("ordes")  # Typo in "orders"
        assert set(result.model_names) == {"orders"}
        assert result.had_path_selection is False


def test_fuzzy_matching_with_operators(parser: dbtParser) -> None:
    """Test fuzzy matching works with + operators."""
    from unittest.mock import patch

    from dbt_toolbox.settings import Settings

    # Create a settings instance with fuzzy_model_matching = "automatic"
    with patch.object(Settings, "fuzzy_model_matching", "automatic"):
        # Get a fresh selection parser with the patched settings
        selection_parser = parser.selection_parser

        # Upstream selection with typo
        result = selection_parser.parse("+custmers")  # Typo in "customers"
        assert "customers" in result.model_names
        assert result.had_path_selection is False

        # Downstream selection with typo
        result = selection_parser.parse("custmers+")  # Typo in "customers"
        assert set(result.model_names) == {"customers", "customer_orders"}
        assert result.had_path_selection is False

        # Both directions with typo
        result = selection_parser.parse("+custmers+")  # Typo in "customers"
        assert "customers" in result.model_names
        assert "customer_orders" in result.model_names
        assert result.had_path_selection is False


def test_fuzzy_matching_prompt_declined(parser: dbtParser) -> None:
    """Test fuzzy matching in prompt mode when user declines."""
    from unittest.mock import patch

    from dbt_toolbox.settings import Settings

    # Create a settings instance with fuzzy_model_matching = "prompt"
    with patch.object(Settings, "fuzzy_model_matching", "prompt"):
        # Get a fresh selection parser with the patched settings
        selection_parser = parser.selection_parser

        # Mock typer.confirm to return False (user declines)
        with patch("dbt_toolbox.dbt_parser._selection_parser.typer.confirm", return_value=False):
            result = selection_parser.parse("custmers")  # Typo in "customers"
            assert set(result.model_names) == set()  # Should be empty when declined
            assert result.had_path_selection is False


def test_fuzzy_matching_prompt_accepted(parser: dbtParser) -> None:
    """Test fuzzy matching in prompt mode when user accepts."""
    from unittest.mock import patch

    from dbt_toolbox.settings import Settings

    # Create a settings instance with fuzzy_model_matching = "prompt"
    with patch.object(Settings, "fuzzy_model_matching", "prompt"):
        # Get a fresh selection parser with the patched settings
        selection_parser = parser.selection_parser

        # Mock typer.confirm to return True (user accepts)
        with patch("dbt_toolbox.dbt_parser._selection_parser.typer.confirm", return_value=True):
            result = selection_parser.parse("custmers")  # Typo in "customers"
            assert set(result.model_names) == {"customers"}  # Should use fuzzy match
            assert result.had_path_selection is False


def test_fuzzy_matching_threshold(parser: dbtParser) -> None:
    """Test that fuzzy matching respects the 60% threshold."""
    from unittest.mock import patch

    from dbt_toolbox.settings import Settings

    # Create a settings instance with fuzzy_model_matching = "automatic"
    with patch.object(Settings, "fuzzy_model_matching", "automatic"):
        # Get a fresh selection parser with the patched settings
        selection_parser = parser.selection_parser

        # Very different string should not match (below 60% threshold)
        result = selection_parser.parse("xyz")
        assert set(result.model_names) == set()
        assert result.had_path_selection is False


def test_fuzzy_matching_path_selection_unaffected(parser: dbtParser) -> None:
    """Test that fuzzy matching does not affect path-based selection."""
    from unittest.mock import patch

    from dbt_toolbox.settings import Settings

    # Create a settings instance with fuzzy_model_matching = "automatic"
    with patch.object(Settings, "fuzzy_model_matching", "automatic"):
        # Get a fresh selection parser with the patched settings
        selection_parser = parser.selection_parser

        # Path-based selection should not trigger fuzzy matching
        result = selection_parser.parse("models/custmers.sql")  # Typo in path
        assert set(result.model_names) == set()  # No fuzzy matching for paths
        assert result.had_path_selection is True
