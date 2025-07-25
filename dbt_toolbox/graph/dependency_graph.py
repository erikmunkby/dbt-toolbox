"""Lightweight dependency graph implementation for dbt models and macros."""

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from dbt_toolbox.data_models import Macro, Model

NodeObject = Union["Model", "Macro"]


class DependencyGraphError(Exception):
    """Base exception for dependency graph operations."""


class NodeNotFoundError(DependencyGraphError):
    """Raised when a requested node is not found in the graph."""


class DependencyGraph:
    """Lightweight directed acyclic graph (DAG) for tracking dbt dependencies.

    This class manages dependencies between dbt models and macros, allowing
    efficient traversal of upstream and downstream relationships.
    """

    def __init__(self) -> None:
        """Initialize an empty dependency graph."""
        # Adjacency lists for efficient traversal
        self._upstream: dict[str, set[str]] = {}  # node -> set of upstream dependencies
        self._downstream: dict[str, set[str]] = {}  # node -> set of downstream dependents
        self._node_types: dict[str, str] = {}  # node -> type ("model" or "macro")
        self._node_objects: dict[str, NodeObject] = {}  # node -> actual object (Model or Macro)

    def add_node(self, name: str, node_type: str, node_object: NodeObject) -> None:
        """Add a node to the graph.

        Args:
            name: Node identifier (model or macro name).
            node_type: Type of node ("model" or "macro").
            node_object: The actual Model or Macro object.

        """
        if name not in self._upstream:
            self._upstream[name] = set()
        if name not in self._downstream:
            self._downstream[name] = set()

        self._node_types[name] = node_type
        self._node_objects[name] = node_object

    def add_dependency(self, downstream_node: str, upstream_node: str) -> None:
        """Add a dependency relationship between two nodes.

        Args:
            downstream_node: The node that depends on upstream_node.
            upstream_node: The node that downstream_node depends on.

        """
        # Ensure both nodes exist in the graph
        if downstream_node not in self._upstream:
            self._upstream[downstream_node] = set()
            self._downstream[downstream_node] = set()
        if upstream_node not in self._upstream:
            self._upstream[upstream_node] = set()
            self._downstream[upstream_node] = set()

        # Add the dependency relationship
        self._upstream[downstream_node].add(upstream_node)
        self._downstream[upstream_node].add(downstream_node)

    def get_downstream_nodes(self, node_name: str) -> set[str]:
        """Get all downstream nodes that depend on the given node.

        Args:
            node_name: Name of the node to find downstream dependencies for.

        Returns:
            Set of node names that depend on the given node.

        Raises:
            NodeNotFoundError: If the node is not found in the graph.

        """
        if node_name not in self._downstream:
            raise NodeNotFoundError(f"Node '{node_name}' not found in dependency graph")

        visited = set()
        result = set()

        def _dfs(current_node: str) -> None:
            if current_node in visited:
                return
            visited.add(current_node)

            for dependent in self._downstream[current_node]:
                result.add(dependent)
                _dfs(dependent)

        _dfs(node_name)
        return result

    def get_upstream_nodes(self, node_name: str) -> set[str]:
        """Get all upstream nodes that the given node depends on.

        Args:
            node_name: Name of the node to find upstream dependencies for.

        Returns:
            Set of node names that the given node depends on.

        Raises:
            NodeNotFoundError: If the node is not found in the graph.

        """
        if node_name not in self._upstream:
            raise NodeNotFoundError(f"Node '{node_name}' not found in dependency graph")

        visited = set()
        result = set()

        def _dfs(current_node: str) -> None:
            if current_node in visited:
                return
            visited.add(current_node)

            for dependency in self._upstream[current_node]:
                result.add(dependency)
                _dfs(dependency)

        _dfs(node_name)
        return result

    def get_node_object(self, node_name: str) -> NodeObject:
        """Get the actual object (Model or Macro) for a node.

        Args:
            node_name: Name of the node.

        Returns:
            The Model or Macro object associated with the node.

        Raises:
            NodeNotFoundError: If the node is not found in the graph.

        """
        if node_name not in self._node_objects:
            raise NodeNotFoundError(f"Node '{node_name}' not found in dependency graph")

        return self._node_objects[node_name]

    def get_node_type(self, node_name: str) -> str:
        """Get the type of a node.

        Args:
            node_name: Name of the node.

        Returns:
            The type of the node ("model" or "macro").

        Raises:
            NodeNotFoundError: If the node is not found in the graph.

        """
        if node_name not in self._node_types:
            raise NodeNotFoundError(f"Node '{node_name}' not found in dependency graph")

        return self._node_types[node_name]

    def get_all_nodes(self) -> dict[str, str]:
        """Get all nodes in the graph with their types.

        Returns:
            Dictionary mapping node names to their types.

        """
        return self._node_types.copy()

    def has_node(self, node_name: str) -> bool:
        """Check if a node exists in the graph.

        Args:
            node_name: Name of the node to check.

        Returns:
            True if the node exists, False otherwise.

        """
        return node_name in self._node_types

    def get_node_stats(self) -> dict[str, int]:
        """Get statistics about the dependency graph.

        Returns:
            Dictionary with node counts by type and total edges.

        """
        model_count = sum(1 for node_type in self._node_types.values() if node_type == "model")
        macro_count = sum(1 for node_type in self._node_types.values() if node_type == "macro")
        edge_count = sum(len(deps) for deps in self._upstream.values())

        return {
            "models": model_count,
            "macros": macro_count,
            "total_nodes": len(self._node_types),
            "total_edges": edge_count,
        }
