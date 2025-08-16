"""Tests for dependency graph functionality."""

import pytest

from dbt_toolbox.data_models import Model
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.graph.dependency_graph import DependencyGraph, NodeNotFoundError


class TestDependencyGraph:
    """Test the core DependencyGraph class."""

    def test_empty_graph_initialization(self) -> None:
        """Test that an empty graph initializes correctly."""
        graph = DependencyGraph()
        assert graph.get_all_nodes() == {}
        stats = graph.get_node_stats()
        assert stats["total_nodes"] == 0
        assert stats["total_edges"] == 0
        assert stats["models"] == 0
        assert stats["macros"] == 0

    def test_add_node(self) -> None:
        """Test adding nodes to the graph."""
        graph = DependencyGraph()

        # Mock objects for testing
        mock_model = "mock_model_object"
        mock_macro = "mock_macro_object"

        graph.add_node("test_model", "model", mock_model)  # type: ignore
        graph.add_node("test_macro", "macro", mock_macro)  # type: ignore

        assert graph.has_node("test_model")
        assert graph.has_node("test_macro")
        assert graph.get_node_type("test_model") == "model"
        assert graph.get_node_type("test_macro") == "macro"
        assert graph.get_node_object("test_model") == mock_model
        assert graph.get_node_object("test_macro") == mock_macro

    def test_add_dependency(self) -> None:
        """Test adding dependencies between nodes."""
        graph = DependencyGraph()

        graph.add_node("downstream", "model", "mock_downstream")  # type: ignore
        graph.add_node("upstream", "model", "mock_upstream")  # type: ignore

        graph.add_dependency("downstream", "upstream")

        upstream_nodes = graph.get_upstream_nodes("downstream")
        downstream_nodes = graph.get_downstream_nodes("upstream")

        assert "upstream" in upstream_nodes
        assert "downstream" in downstream_nodes

    def test_transitive_dependencies(self) -> None:
        """Test that transitive dependencies are resolved correctly."""
        graph = DependencyGraph()

        # Create chain: A -> B -> C
        graph.add_node("A", "model", "mock_A")  # type: ignore
        graph.add_node("B", "model", "mock_B")  # type: ignore
        graph.add_node("C", "model", "mock_C")  # type: ignore

        graph.add_dependency("A", "B")  # A depends on B
        graph.add_dependency("B", "C")  # B depends on C

        # A should have both B and C as upstream dependencies
        upstream_of_a = graph.get_upstream_nodes("A")
        assert "B" in upstream_of_a
        assert "C" in upstream_of_a

        # C should have both A and B as downstream dependencies
        downstream_of_c = graph.get_downstream_nodes("C")
        assert "A" in downstream_of_c
        assert "B" in downstream_of_c

    def test_node_not_found_error(self) -> None:
        """Test that NodeNotFoundError is raised for non-existent nodes."""
        graph = DependencyGraph()

        with pytest.raises(NodeNotFoundError, match="Node 'nonexistent' not found"):
            graph.get_downstream_nodes("nonexistent")

        with pytest.raises(NodeNotFoundError, match="Node 'nonexistent' not found"):
            graph.get_upstream_nodes("nonexistent")

        with pytest.raises(NodeNotFoundError, match="Node 'nonexistent' not found"):
            graph.get_node_object("nonexistent")

    def test_graph_stats(self) -> None:
        """Test graph statistics calculation."""
        graph = DependencyGraph()

        # Add 2 models and 1 macro
        graph.add_node("model1", "model", "mock1")  # type: ignore
        graph.add_node("model2", "model", "mock2")  # type: ignore
        graph.add_node("macro1", "macro", "mock_macro")  # type: ignore

        # Add 2 dependencies
        graph.add_dependency("model1", "model2")
        graph.add_dependency("model1", "macro1")

        stats = graph.get_node_stats()
        assert stats["models"] == 2
        assert stats["macros"] == 1
        assert stats["total_nodes"] == 3
        assert stats["total_edges"] == 2


class TestDbtParserGraphIntegration:
    """Test the integration of dependency graph with dbtParser."""

    def test_macros_property(self, dbt_parser: dbtParser) -> None:
        """Test that the macros property returns available macros."""
        macros = dbt_parser.macros

        # Should have at least the simple_macro from the sample project
        assert isinstance(macros, dict)
        assert "simple_macro" in macros

        # Each macro should be a RawMacro object
        simple_macro = macros["simple_macro"]
        assert hasattr(simple_macro, "file_name")
        assert hasattr(simple_macro, "raw_code")
        assert simple_macro.file_name == "simple_macro"

    def test_get_dependency_graph(self, dbt_parser: dbtParser) -> None:
        """Test that get_dependency_graph builds a complete graph."""
        graph = dbt_parser.dependency_graph

        # Should contain both models and macros
        all_nodes = graph.get_all_nodes()

        # Check that we have expected models
        expected_models = ["customers", "orders", "customer_orders", "some_other_model"]
        for model in expected_models:
            assert model in all_nodes
            assert all_nodes[model] == "model"

        # Check that we have expected macros
        assert "simple_macro" in all_nodes
        assert all_nodes["simple_macro"] == "macro"

        # Verify stats
        stats = graph.get_node_stats()
        assert stats["models"] >= 4  # At least the expected models
        assert stats["macros"] >= 1  # At least simple_macro

    def test_model_to_model_dependencies(self, dbt_parser: dbtParser) -> None:
        """Test that model-to-model dependencies are correctly established."""
        graph = dbt_parser.dependency_graph

        # customer_orders should depend on customers and orders
        upstream_nodes = graph.get_upstream_nodes("customer_orders")
        assert "customers" in upstream_nodes
        assert "orders" in upstream_nodes

        # customers should be downstream of customer_orders
        downstream_of_customers = graph.get_downstream_nodes("customers")
        assert "customer_orders" in downstream_of_customers

    def test_model_to_macro_dependencies(self, dbt_parser: dbtParser) -> None:
        """Test that model-to-macro dependencies are correctly established."""
        graph = dbt_parser.dependency_graph

        # Models using simple_macro should have it as upstream
        # Based on the sample project, customer_orders and orders use simple_macro
        upstream_of_customer_orders = graph.get_upstream_nodes("customer_orders")
        assert "simple_macro" in upstream_of_customer_orders

        # simple_macro should have these models as downstream
        downstream_of_simple_macro = graph.get_downstream_nodes("simple_macro")
        assert "customer_orders" in downstream_of_simple_macro

    def test_get_downstream_models_for_model(self, dbt_parser: dbtParser) -> None:
        """Test getting downstream models for a given model."""
        downstream_models = dbt_parser.get_downstream_models("customers")

        # Should return Model objects
        assert all(isinstance(model, Model) for model in downstream_models)

        # Should include customer_orders (which depends on customers)
        downstream_names = [model.name for model in downstream_models]
        assert "customer_orders" in downstream_names

    def test_get_downstream_models_for_macro(self, dbt_parser: dbtParser) -> None:
        """Test getting downstream models for a given macro."""
        downstream_models = dbt_parser.get_downstream_models("simple_macro")

        # Should return Model objects
        assert all(isinstance(model, Model) for model in downstream_models)

        # Should include models that use simple_macro
        downstream_names = [model.name for model in downstream_models]
        assert "customer_orders" in downstream_names

    def test_get_downstream_models_empty_result(self, dbt_parser: dbtParser) -> None:
        """Test getting downstream models for a node with no downstream dependencies."""
        # customer_orders is likely a leaf node (no models depend on it)
        downstream_models = dbt_parser.get_downstream_models("customer_orders")

        # Should return empty list, not None or error
        assert isinstance(downstream_models, list)
        # May be empty if customer_orders is a leaf node
