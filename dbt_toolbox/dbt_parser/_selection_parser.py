"""Module for parsing dbt model selection syntax."""

import re
from pathlib import Path

import typer
from rapidfuzz import fuzz, process

from dbt_toolbox.data_models import Model, SelectionResult
from dbt_toolbox.graph.dependency_graph import DependencyGraph
from dbt_toolbox.settings import Settings
from dbt_toolbox.utils import cprint


class SelectionParser:
    """Parses dbt model selection syntax to determine target models."""

    def __init__(
        self,
        models: dict[str, Model],
        dependency_graph: DependencyGraph,
        model_root_paths: list[Path],
        source_names: set[str],
        settings: Settings,
    ) -> None:
        """Initialize the SelectionParser.

        Args:
            models: Dictionary mapping model names to Model objects
            dependency_graph: DependencyGraph for traversing model dependencies
            model_root_paths: List of root paths for models (e.g., [Path('models')])
            source_names: Set of source names to exclude from selection results
            settings: Settings instance for configuration

        """
        self._models = models
        self._graph = dependency_graph
        self._model_root_paths = model_root_paths
        self._source_names = source_names
        self._settings = settings

    def parse(self, selection: str | None, /) -> SelectionResult:
        """Parse dbt model selection syntax and return SelectionResult.

        Args:
            selection:  dbt selection string (e.g., "my_model+", "+my_model", "my_model")
                        If None, returns all models.

        Returns:
            SelectionResult with model names, models, and path selection flag.

        """
        if not selection:
            # No selection means all models
            return SelectionResult(
                model_names=sorted(self._models.keys()),
                models_dict=self._models.copy(),
                had_path_selection=False,
            )

        target_model_names = set()
        had_path_selection = False

        # Handle multiple selections separated by comma or space
        selections = re.split(r"[,\s]+", selection.strip())

        for sel in selections:
            if not sel:
                continue

            # Parse this single selection
            selected_names, is_path = self._parse_single_selection(sel)
            target_model_names.update(selected_names)
            if is_path:
                had_path_selection = True

        # Build models_dict with only models that exist in self._models
        # Note: target_model_names may include unparseable models or sources
        models_dict = {
            name: self._models[name] for name in target_model_names if name in self._models
        }

        return SelectionResult(
            model_names=sorted(target_model_names),
            models_dict=models_dict,
            had_path_selection=had_path_selection,
        )

    def _parse_single_selection(self, selector: str) -> tuple[set[str], bool]:
        """Parse a single selection string and return matching models.

        Args:
            selector: Single selection string (may include +/+ operators)

        Returns:
            Tuple of (set of model names, whether this was a path selection)

        """
        # Extract operators and base selector
        base_selector, has_upstream, has_downstream = self._extract_operators(selector)

        # Determine if this is a path-based selection
        is_path_selection = self._is_path_selection(base_selector)

        if is_path_selection:
            # Path-based selection
            matched_models = self._match_models_by_path(base_selector)
            # Apply operators to all matched models
            result = self._apply_graph_operators(matched_models, has_upstream, has_downstream)
            return result, True

        # Name-based selection
        result = set()

        if base_selector in self._models:
            # Model found directly
            result.add(base_selector)
        elif self._settings.fuzzy_model_matching != "off":
            # Try fuzzy matching
            fuzzy_match = self._fuzzy_match_model(base_selector)
            if fuzzy_match:
                if self._settings.fuzzy_model_matching == "automatic":
                    # Use fuzzy match automatically and inform user
                    cprint(
                        f"Replaced '{base_selector}' with '{fuzzy_match}' "
                        "(fuzzy_model_matching=automatic)",
                        color="yellow",
                    )
                    result.add(fuzzy_match)
                elif self._settings.fuzzy_model_matching == "prompt":
                    # Prompt user for confirmation
                    try:
                        prompt_msg = (
                            f"Model '{base_selector}' not found. Did you mean '{fuzzy_match}'?"
                        )
                        if typer.confirm(prompt_msg, default=False):
                            result.add(fuzzy_match)
                    except (OSError, EOFError):
                        # Non-interactive environment (e.g., tests, CI/CD)
                        # Skip prompting and treat as declined
                        pass

        # Apply graph operators if we have a model
        if result:
            result = self._apply_graph_operators(result, has_upstream, has_downstream)

        return result, False

    def _extract_operators(self, selector: str) -> tuple[str, bool, bool]:
        """Extract operators and base selector from a selection string.

        Args:
            selector: Selection string that may have + operators

        Returns:
            Tuple of (base_selector, has_upstream, has_downstream)

        """
        has_downstream = selector.endswith("+")
        has_upstream = selector.startswith("+")

        # Remove operators to get the base selector
        base_selector = selector.removeprefix("+").removesuffix("+")

        return base_selector, has_upstream, has_downstream

    def _apply_graph_operators(
        self, model_names: set[str], has_upstream: bool, has_downstream: bool
    ) -> set[str]:
        """Apply upstream/downstream operators to a set of model names.

        Args:
            model_names: Set of base model names
            has_upstream: Whether to include upstream models
            has_downstream: Whether to include downstream models

        Returns:
            Expanded set of model names including upstream/downstream

        """
        result = set(model_names)

        for model_name in model_names:
            if has_upstream:
                # Add upstream models from both dependency graph and model's upstream list
                # to ensure we include models that failed to parse
                model = self._models[model_name]

                # First, add from dependency graph (successfully parsed models)
                upstream_nodes = self._graph.get_upstream_nodes(model_name)
                upstream_models_from_graph = [
                    node for node in upstream_nodes if self._graph.get_node_type(node) == "model"
                ]
                result.update(upstream_models_from_graph)

                # Then, add from model's upstream.models list (includes unparseable models)
                # This ensures we don't silently ignore models that failed to parse
                # Filter out known sources to avoid including them in model selection
                upstream_models_only = [
                    upstream_model
                    for upstream_model in model.upstream.models
                    if upstream_model not in self._source_names
                ]
                result.update(upstream_models_only)

            if has_downstream:
                # Add downstream models
                downstream_models = self._get_downstream_models(model_name)
                result.update(m.name for m in downstream_models)

        return result

    def _get_downstream_models(self, name: str) -> list[Model]:
        """Get all downstream models that depend on the given model or macro.

        Args:
            name: Name of the model or macro to find downstream dependencies for.

        Returns:
            List of downstream Model objects

        """
        # Filter to only return models (not macros) and convert to Model objects
        return [
            self._models[node_name]
            for node_name in self._graph.get_downstream_nodes(name)
            if self._graph.get_node_type(node_name) == "model"
        ]

    def _is_path_selection(self, selector: str) -> bool:
        """Check if a selector is a path-based selection.

        Args:
            selector: The selection string to check (without operators like +)

        Returns:
            True if path-based selection, False otherwise

        """
        # Check for path: prefix
        if selector.startswith("path:"):
            return True
        # Check if it contains path separators (both / and \)
        return "/" in selector or "\\" in selector

    def _match_models_by_path(self, path_selector: str) -> set[str]:
        """Match models by path selection.

        Supports multiple path formats:
        - path:folder, path:some/folder
        - models/some/folder, some/folder, some/
        - Any partial path relative to model root directories

        Args:
            path_selector: The path selection string (with or without "path:" prefix)

        Returns:
            Set of matching model names

        """
        # Remove "path:" prefix if present
        path_selector = path_selector.removeprefix("path:")

        # Normalize path separators and convert to Path
        normalized_selector = Path(path_selector)

        matched_models = set()
        for model_name, model in self._models.items():
            # Get relative path from model root if possible
            model_rel_path = self._get_relative_path_from_roots(Path(model.path))

            # Check if the selector matches the model path
            if self._path_matches(model_rel_path, Path(model.path), normalized_selector):
                matched_models.add(model_name)

        return matched_models

    def _get_relative_path_from_roots(self, model_path: Path) -> Path | None:
        """Get model path relative to one of the model root paths.

        Args:
            model_path: Absolute path to the model file

        Returns:
            Relative path from model root, or None if not under any root

        """
        for root in self._model_root_paths:
            if model_path.is_relative_to(root):
                return model_path.relative_to(root)
        return None

    def _path_matches(
        self, model_rel_path: Path | None, model_abs_path: Path, selector_path: Path
    ) -> bool:
        """Check if a model path matches a selector path.

        Supports flexible matching:
        - Exact file match: path:customers.sql
        - Directory match: path:subfolder
        - Partial directory match: some/folder (matches models/some/folder/...)
        - With or without models/ prefix

        Args:
            model_rel_path: Relative path from model root (or None)
            model_abs_path: Absolute path to the model
            selector_path: The selector path to match against

        Returns:
            True if the model matches the selector

        """
        selector_str = str(selector_path)

        # Try matching against relative path if available
        if model_rel_path:
            rel_path_str = str(model_rel_path)

            # Exact match (file or directory)
            if model_rel_path == selector_path or rel_path_str == selector_str:
                return True

            # Suffix match (handles models/subfolder matching subfolder/model.sql)
            if rel_path_str.endswith(selector_str):
                return True

            # Check if selector is a parent directory of the model
            if model_rel_path.is_relative_to(selector_path):
                return True

            # Substring match within path hierarchy
            # (handles "some/" matching "models/some/folder/model.sql")
            if selector_str in rel_path_str:
                return True

        # Fallback: check absolute path for substring match
        return selector_str in str(model_abs_path)

    def _fuzzy_match_model(self, query: str) -> str | None:
        """Find the best fuzzy match for a model name.

        Args:
            query: The model name query to match

        Returns:
            Best matching model name if found with score >= 50%, None otherwise

        """
        fuzzy_threshold = 50  # Minimum similarity score for fuzzy matching

        if not self._models:
            return None

        # Use rapidfuzz to find best match
        results = process.extract(
            query,
            self._models.keys(),
            scorer=fuzz.WRatio,
            limit=1,
        )

        if results and results[0][1] >= fuzzy_threshold:
            return results[0][0]  # Return the original model name

        return None
