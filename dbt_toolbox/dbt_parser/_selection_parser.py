"""Module for parsing dbt model selection syntax."""

import re

from dbt_toolbox.data_models import Model
from dbt_toolbox.graph.dependency_graph import DependencyGraph


class SelectionParser:
    """Parses dbt model selection syntax to determine target models."""

    def __init__(
        self,
        models: dict[str, Model],
        dependency_graph: DependencyGraph,
    ) -> None:
        """Initialize the SelectionParser.

        Args:
            models: Dictionary mapping model names to Model objects
            dependency_graph: DependencyGraph for traversing model dependencies

        """
        self._models = models
        self._graph = dependency_graph

    def parse(self, selection: str | None, /) -> set[str]:
        """Parse dbt model selection syntax to get target model names.

        Args:
            selection:  dbt selection string (e.g., "my_model+", "+my_model", "my_model")
                        If None, returns all models.

        """
        if not selection:
            # No selection means all models
            return set(self._models.keys())

        target_models = set()

        # Handle multiple selections separated by comma or space
        selections = re.split(r"[,\s]+", selection.strip())

        for sel in selections:
            if not sel:
                continue

            # Parse selection patterns
            if sel.endswith("+"):
                # downstream selection: "model+"
                model_name = sel[:-1].removeprefix("+")
                if model_name in self._models:
                    target_models.add(model_name)
                    # Add all downstream models
                    downstream_models = self._get_downstream_models(model_name)
                    target_models.update(m.name for m in downstream_models)
            if sel.startswith("+"):
                # upstream selection: "+model"
                model_name = sel[1:].removesuffix("+")
                if model_name in self._models:
                    target_models.add(model_name)
                    # Add upstream models from both dependency graph and model's upstream list
                    # to ensure we include models that failed to parse
                    model = self._models[model_name]

                    # First, add from dependency graph (successfully parsed models)
                    upstream_nodes = self._graph.get_upstream_nodes(model_name)
                    upstream_models_from_graph = [
                        node
                        for node in upstream_nodes
                        if self._graph.get_node_type(node) == "model"
                    ]
                    target_models.update(upstream_models_from_graph)

                    # Then, add from model's upstream.models list (includes unparseable models)
                    # This ensures we don't silently ignore models that failed to parse
                    target_models.update(model.upstream.models)
            # direct model selection
            if sel in self._models:
                target_models.add(sel)

        return target_models

    def parse_return_models(self, selection_query: str | None) -> dict[str, Model]:
        """Parse the model selection query and return Model objects.

        Args:
            selection_query:    dbt selection string (e.g., "my_model+", "+my_model", "my_model")
                                If None, returns all models.

        """
        if selection_query is None:
            return self._models
        return {name: self._models[name] for name in self.parse(selection_query)}

    def _get_downstream_models(self, name: str) -> list[Model]:
        """Get all downstream models that depend on the given model or macro.

        Args:
            name: Name of the model or macro to find downstream dependencies for.

        """
        # Filter to only return models (not macros) and convert to Model objects
        return [
            self._models[node_name]
            for node_name in self._graph.get_downstream_nodes(name)
            if self._graph.get_node_type(node_name) == "model"
        ]
