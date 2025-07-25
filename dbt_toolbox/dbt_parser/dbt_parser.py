"""Module for parsing dbt project."""

import re
from functools import cached_property

import yamlium
from jinja2.nodes import Call, Output
from sqlglot import ParseError, parse_one
from sqlglot.optimizer import optimize

from dbt_toolbox.data_models import (
    ColDocs,
    DependsOn,
    Macro,
    MacroBase,
    Model,
    ModelBase,
    YamlDocs,
)
from dbt_toolbox.dbt_parser._cache import Cache, cache
from dbt_toolbox.dbt_parser._file_fetcher import fetch_macros, fetch_models
from dbt_toolbox.dbt_parser._jinja_handler import jinja
from dbt_toolbox.graph.dependency_graph import DependencyGraph, NodeNotFoundError
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import printer, utils


def _build_macro(m: MacroBase, /) -> Macro:
    """Build a complete Macro object from a MacroBase.

    Args:
        m: Base macro containing name, path, and raw code.

    Returns:
        Complete Macro object with execution timestamp.

    """
    return Macro(
        source=m.source,
        file_name=m.file_name,
        name=m.name,
        raw_code=m.raw_code,
        macro_path=m.macro_path,
    )


def _get_all_macro_bases() -> list[MacroBase]:
    """Get all MacroBase objects from all sources.

    Returns:
        List of all MacroBase objects from custom and package sources.

    """
    all_macros = []
    for macro_list in fetch_macros().values():
        all_macros.extend(macro_list)
    return all_macros


def _build_model(m: ModelBase, /) -> Model:
    """Build a complete Model object from a ModelBase.

    Parses Jinja templates to extract dependencies, renders the code,
    and creates optimized SQL representations.

    Args:
        m: Base model containing name, path, and raw code.

    Returns:
        Complete Model object with dependencies and SQL parsing.

    Raises:
        NotImplementedError: If source() calls are found (not yet supported).

    """
    deps = DependsOn()
    for obj in jinja.parse(m.raw_code).body:
        if not isinstance(obj, Output):
            continue
        for node in obj.nodes:
            if isinstance(node, Call):
                node_name: str = node.node.name  # type: ignore
                # When we find {{ ref() }}
                if node_name == "ref":
                    deps.models.append(node.args[0].value)  # type: ignore
                # When we find {{ source() }}
                elif node_name == "source":
                    # TODO: Implement support for sources
                    continue
                # When we find any other e.g. {{ my_macro() }}
                else:
                    deps.macros.append(node_name)
    rendered_code = jinja.render(m.raw_code)
    glot_code = parse_one(rendered_code, dialect=settings.sql_dialect)  # type: ignore

    try:
        optmized_glot_code = optimize(glot_code, dialect=settings.sql_dialect)
    except Exception:  # noqa: BLE001
        optmized_glot_code = None
    return Model(
        name=m.name,
        raw_code=m.raw_code,
        path=m.path,
        rendered_code=rendered_code,
        upstream=deps,
        glot_code=glot_code,  # type: ignore
        optimized_glot_code=optmized_glot_code,  # type: ignore
    )


class dbtParser:  # noqa: N801
    """dbt parser class."""

    @property
    def cache(self) -> Cache:
        """Reference to the cache."""
        return cache

    @cached_property
    def yaml_docs(self) -> dict[str, YamlDocs]:
        """Get the yaml documentation for all models."""
        result = {}
        for path in utils.model_yaml_paths:
            models: list[dict] = yamlium.parse(path).to_dict().get("models", [])  # type: ignore
            for m in models:
                result[m["name"]] = YamlDocs(
                    path=path,
                    model_description=m.get("description"),
                    columns=[
                        ColDocs(name=c.get("name"), description=c.get("description"))
                        for c in m.get("columns", [])
                    ],
                )
        return result

    @cached_property
    def column_macro_docs(self) -> dict[str, str]:
        """Get all docs macros."""
        pattern = re.compile(r"{%\s*docs\s+(\w+)\s*%}\s*(.*?)\s*{%\s*enddocs\s*%}", re.DOTALL)
        result = {}
        for p in utils.docs_macros_paths:
            for match in pattern.findall(p.read_text()):
                result[match[0]] = match[1].strip()
        return result

    @cached_property
    def cached_models(self) -> dict[str, Model]:
        """Get cached models."""
        if cache.cache_models.exists():
            return cache.cache_models.read()
        return {}

    @cached_property
    def list_raw_models(self) -> dict[str, ModelBase]:
        """List all raw models."""
        return {m.name: m for m in fetch_models()}

    @cached_property
    def models(self) -> dict[str, Model]:
        """Fetch all available models, prioritizing cache if valid.

        This call will also update the cache.
        """
        cached_models = self.cached_models
        final_models: dict[str, Model] = {}
        for name, raw_model in self.list_raw_models.items():
            cached_model = cached_models.get(name)
            if not cached_model or raw_model.hash != cached_model.hash:
                try:
                    final_models[raw_model.name] = _build_model(raw_model)
                except ParseError:
                    printer.cprint(
                        "Failed to parse model",
                        raw_model.name,
                        highlight_idx=1,
                        color="yellow",
                    )
            else:
                final_models[name] = cached_model

        # Enrich with yaml docs if exists
        yaml_docs = self.yaml_docs
        for name, raw_model in final_models.items():
            raw_model.yaml_docs = yaml_docs.get(name)
        cache.cache_models.write(final_models)
        return final_models

    @cached_property
    def macros(self) -> dict[str, Macro]:
        """Fetch all available macros, prioritizing cache if valid."""
        macro_cache = cache.cache_macros.read()
        cached_macros: dict[str, Macro] = macro_cache if macro_cache else {}
        final_macros: dict[str, Macro] = {}

        for m in _get_all_macro_bases():
            if not m.is_test:  # Exclude test macros
                cm = cached_macros.get(m.name)
                if not cm or m.id != cm.id:
                    final_macros[m.name] = _build_macro(m)
                else:
                    final_macros[m.name] = cm

        cache.cache_macros.write(final_macros)
        return final_macros

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
        if not self.dependency_graph.has_node(name):
            # Provide helpful error message
            available_models = list(self.models.keys())
            available_macros = list(self.macros.keys())

            if name in available_models or name in available_macros:
                # This shouldn't happen, but just in case
                raise NodeNotFoundError(f"Node '{name}' found in data but not in graph")

            # Check if it's close to any existing names (simple suggestion)
            close_matches = [
                n
                for n in available_models + available_macros
                if name.lower() in n.lower() or n.lower() in name.lower()
            ]
            error_msg = f"Model or macro '{name}' not found"
            if close_matches:
                error_msg += f". Did you mean one of: {', '.join(close_matches[:3])}?"
            raise NodeNotFoundError(error_msg)

        # Get downstream node names
        downstream_nodes = self.dependency_graph.get_downstream_nodes(name)

        # Filter to only return models (not macros) and convert to Model objects
        return [
            self.models[node_name]
            for node_name in downstream_nodes
            if self.dependency_graph.get_node_type(node_name) == "model"
        ]


dbt_parser = dbtParser()
