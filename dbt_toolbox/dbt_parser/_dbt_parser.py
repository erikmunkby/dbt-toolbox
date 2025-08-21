"""Module for parsing dbt project."""

import re
from functools import cached_property
from pathlib import Path

import yamlium
from sqlglot import ParseError

from dbt_toolbox.data_models import (
    ColDocs,
    Macro,
    MacroBase,
    Model,
    ModelBase,
    Seed,
    Source,
    YamlDocs,
)
from dbt_toolbox.dbt_parser._builders import build_macro, build_model
from dbt_toolbox.dbt_parser._cache import Cache, cache
from dbt_toolbox.dbt_parser._file_fetcher import read_macros, read_models
from dbt_toolbox.dbt_parser._jinja_handler import Jinja
from dbt_toolbox.graph.dependency_graph import DependencyGraph
from dbt_toolbox.run_config import RunConfig
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import cprint, list_files

# Finds docs macros in the docs.md files. Example:
# {% docs customer %}
# Some customer description
# {% enddocs %}
_re_find_docs_macro_definitions = re.compile(
    r"{%\s*docs\s+(\w+)\s*%}\s*(.*?)\s*{%\s*enddocs\s*%}", re.DOTALL
)
# Find doc macros referenced in descriptions. Example:
# description: '{{ doc('some_doc_macro')}}'
_re_find_docs_macro_reference = r'({{\s*doc\(\s*[\'"]([^\'"]+)[\'"]\s*\)\s*}})'


class dbtParser:  # noqa: N801
    """dbt parser class."""

    def __init__(self, target: str | None = None) -> None:
        """Instantiate the dbt parser using the dbt profile."""
        self.run_config = RunConfig(target=target)
        self.jinja = Jinja(profile=self.run_config.dbt_profile)
        self.dbt_project_dict: dict = settings.dbt_project.rendered_parse(self.jinja.env).to_dict()  # type: ignore

    @cached_property
    def docs_macros_paths(self) -> list[Path]:
        """Get a list of all docs macros paths."""
        return [
            path
            for docs_path in settings.dbt_project.docs_paths
            for path in list_files(docs_path, file_suffix=".md")
        ]

    @cached_property
    def column_macro_docs(self) -> dict[str, str]:
        """Get all docs macros."""
        result = {}
        for p in self.docs_macros_paths:
            for match in _re_find_docs_macro_definitions.findall(p.read_text()):
                result[match[0]] = match[1].strip()
        return result

    def _create_column_docs(self, col_data: dict) -> ColDocs:
        """Create ColDocs with macro references replaced by their text.

        Args:
            col_data: Dictionary potentially containing "name" and "description" keys

        """
        name = col_data.get("name", "")
        raw_description: str | None = col_data.get("description")

        if not raw_description:
            return ColDocs(name=name, description=None, raw_description=raw_description)

        # Replace doc macro references with their actual text
        resolved_description = raw_description
        for match in re.finditer(_re_find_docs_macro_reference, raw_description):
            full_reference = match.group(1)  # Full {{ doc('macro_name') }}
            macro_name = match.group(2)  # Just the macro_name

            # Look up the macro text from column_macro_docs
            macro_text = self.column_macro_docs.get(macro_name, full_reference)
            resolved_description = resolved_description.replace(full_reference, macro_text)

        return ColDocs(
            name=name, description=resolved_description, raw_description=raw_description
        )

    @cached_property
    def model_paths(self) -> list[Path]:
        """Get a list of all model paths."""
        return [
            path
            for model_path in settings.dbt_project.model_paths
            for path in list_files(model_path, [".sql"])
        ]

    @cached_property
    def model_yaml_paths(self) -> list[Path]:
        """Get a list of all model yaml paths."""
        return [
            path
            for model_path in settings.dbt_project.model_paths
            for path in list_files(model_path, [".yml", ".yaml"])
        ]

    @property
    def cache(self) -> Cache:
        """Reference to the cache."""
        return cache

    @cached_property
    def yaml_docs(self) -> dict[str, YamlDocs]:
        """Get the yaml documentation for all models."""
        result = {}
        for path in self.model_yaml_paths:
            models: list[dict] = yamlium.parse(path).to_dict().get("models", [])  # type: ignore
            for m in models:
                result[m["name"]] = YamlDocs(
                    path=path,
                    model_description=m.get("description"),
                    config=m.get("config", {}),
                    columns=[self._create_column_docs(c) for c in m.get("columns", [])],
                )
        return result

    @cached_property
    def sources(self) -> dict[str, Source]:
        """Get all sources defined in the project."""
        result = {}
        for path in self.model_yaml_paths:
            sources: list[dict] = yamlium.parse(path).to_dict().get("sources", [])  # type: ignore
            for source in sources:
                source_name = source["name"]
                for table in source.get("tables", []):
                    table_name = table["name"]
                    full_name = f"{source_name}__{table_name}"
                    result[full_name] = Source(
                        name=table_name,
                        source_name=source_name,
                        description=table.get("description"),
                        path=path,
                        columns=[self._create_column_docs(c) for c in table.get("columns", [])],
                    )
        return result

    @cached_property
    def seeds(self) -> dict[str, Seed]:
        """Get all seeds (CSV files) defined in the project."""
        result = {}
        project_dir = settings.dbt_project_dir

        for seed_path in settings.dbt_project.seed_paths:
            seed_dir = project_dir / seed_path
            if seed_dir.exists():
                for csv_file in seed_dir.glob("*.csv"):
                    seed_name = csv_file.stem  # filename without .csv extension
                    result[seed_name] = Seed(
                        name=seed_name,
                        path=csv_file,
                    )
        return result

    @cached_property
    def list_raw_models(self) -> dict[str, ModelBase]:
        """List all raw models."""
        return {m.name: m for m in read_models()}

    @cached_property
    def cached_models(self) -> dict[str, Model]:
        """Get all cached models."""
        return cache.get_all_cached_models()

    def _build_model(self, raw_model: ModelBase, /) -> Model:
        return build_model(
            raw_model,
            jinja=self.jinja,
            sql_dialect=self.run_config.sql_dialect,
            dbt_project_dict=self.dbt_project_dict,
            yaml_docs=self.yaml_docs.get(raw_model.name),
        )

    def _get_model(self, model_name: str) -> Model | None:
        raw_model = self.list_raw_models.get(model_name)
        if raw_model is None:
            return None
        cached_model = self.cached_models.get(model_name)
        if not cached_model:
            try:
                return self._build_model(raw_model)
            except ParseError:
                cprint(
                    "Failed to parse model",
                    model_name,
                    highlight_idx=1,
                    color="yellow",
                )
                return None
        if cached_model.code_hash == raw_model.code_hash:
            return cached_model

        built_model = self._build_model(raw_model)
        built_model = built_model.copy_attributes(other_model=cached_model)
        built_model.code_changed = True
        return built_model

    def get_model(self, model_name: str) -> Model | None:
        """Get a model by name."""
        model = self._get_model(model_name=model_name)
        # Even if model code hasn't changed, check if upstream macros have changed
        if not model:
            return None
        for macro in model.upstream.macros:
            if self.macro_changed(macro):
                model.upstream_macros_changed = True
                break

        cache.cache_model(model=model)
        return model

    @cached_property
    def models(self) -> dict[str, Model]:
        """Fetch all available models, prioritizing cache if valid.

        This call will also update the cache.
        """
        final_models: dict[str, Model] = {}
        for name in self.list_raw_models:
            model = self.get_model(name)
            if not model:
                cprint(
                    "Model not found: " + name,
                    highlight_idx=1,
                    color="yellow",
                )
            else:
                final_models[name] = model

        return final_models

    @cached_property
    def list_raw_macros(self) -> dict[str, list[MacroBase]]:
        """List all raw macros."""
        return read_macros()

    @cached_property
    def macros(self) -> dict[str, Macro]:
        """Fetch all available macros, prioritizing cache if valid."""
        macro_cache = cache.cache_macros.read()
        cached_macros: dict[str, Macro] = macro_cache if macro_cache else {}
        final_macros: dict[str, Macro] = {}

        for macro_list in read_macros().values():
            for m in macro_list:
                if not m.is_test:  # Exclude test macros
                    cm = cached_macros.get(m.name)
                    if not cm or m.code_hash != cm.code_hash:
                        final_macros[m.name] = build_macro(m)
                    else:
                        final_macros[m.name] = cm

        cache.cache_macros.write(final_macros)
        return final_macros

    @cached_property
    def changed_macros(self) -> dict[str, bool]:
        """Get a comprehensive dict of all macros, and whether they've changed."""
        macro_cache = cache.cache_macros.read()
        cached_macros: dict[str, Macro] = macro_cache if macro_cache else {}
        results = {}
        for macro_list in self.list_raw_macros.values():
            for m in macro_list:
                if m.name not in cached_macros:
                    results[m.name] = False
                else:
                    results[m.name] = m.code_hash != cached_macros[m.name].code_hash
        return results

    def macro_changed(self, macro_name: str, /) -> bool:
        """Check whether macro code has changed.

        Args:
            macro_name: The name of the macro to check.

        """
        return self.changed_macros.get(macro_name, False)

    @cached_property
    def dependency_graph(self) -> DependencyGraph:
        """Build and return a dependency graph of all models and macros.

        Returns:
            DependencyGraph instance containing all models and macros with their dependencies.

        """
        graph = DependencyGraph()

        # Add all models as nodes
        for model_name, model in self.models.items():
            graph.add_node(model_name, "model", model)

        # Add all macros as nodes
        for macro_name, macro in self.macros.items():
            graph.add_node(macro_name, "macro", macro)

        # Add model dependencies
        for model_name, model in self.models.items():
            # Add model-to-model dependencies
            for upstream_model in model.upstream.models:
                if upstream_model in self.models:
                    graph.add_dependency(model_name, upstream_model)

            # Add model-to-macro dependencies
            for upstream_macro in model.upstream.macros:
                if upstream_macro in self.macros:
                    graph.add_dependency(model_name, upstream_macro)

        return graph

    def get_downstream_models(self, name: str) -> list[Model]:
        """Get all downstream models that depend on the given model or macro.

        Args:
            name: Name of the model or macro to find downstream dependencies for.

        Returns:
            List of Model objects that depend on the given model or macro.

        Raises:
            NodeNotFoundError: If the model or macro is not found.

        """
        # Filter to only return models (not macros) and convert to Model objects
        return [
            self.models[node_name]
            for node_name in self.dependency_graph.get_downstream_nodes(name)
            if self.dependency_graph.get_node_type(node_name) == "model"
        ]

    def parse_dbt_selection(self, selection: str | None) -> set[str]:
        """Parse dbt model selection syntax to get target models.

        Args:
            selection: dbt selection string (e.g., "my_model+", "+my_model", "my_model")

        Returns:
            Set of model names that would be executed by dbt.

        """
        if not selection:
            # No selection means all models
            return set(self.models.keys())

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
                if model_name in self.models:
                    target_models.add(model_name)
                    # Add all downstream models
                    downstream_models = self.get_downstream_models(model_name)
                    target_models.update(m.name for m in downstream_models)
            if sel.startswith("+"):
                # upstream selection: "+model"
                model_name = sel[1:].removesuffix("+")
                if model_name in self.models:
                    target_models.add(model_name)
                    # Add all upstream models
                    upstream_nodes = self.dependency_graph.get_upstream_nodes(model_name)
                    # Filter to only models (not macros)
                    upstream_models = [
                        node
                        for node in upstream_nodes
                        if self.dependency_graph.get_node_type(node) == "model"
                    ]
                    target_models.update(upstream_models)
            # direct model selection
            if sel in self.models:
                target_models.add(sel)

        return target_models
