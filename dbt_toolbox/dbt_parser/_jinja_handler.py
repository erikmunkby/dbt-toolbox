"""Module for the jinja environment builder."""

import datetime
import pickle
import re
from collections.abc import Iterator
from typing import Any, Literal

import pytz
from jinja2 import Environment, FileSystemBytecodeCache, FileSystemLoader, Undefined
from jinja2.nodes import Template

from dbt_toolbox import utils
from dbt_toolbox.constants import CUSTOM_MACROS, TABLE_REF_SEP
from dbt_toolbox.data_models import DbtProfile
from dbt_toolbox.settings import settings
from dbt_toolbox.warnings_collector import warnings_collector

from ._cache import Cache


class DummyAdapter:
    """Used in place of the dbt adapter.x functionality."""

    def get_relation(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003, ARG002
        """Mock implementation of dbt adapter get_relation method."""
        return "__get_relation__"

    def dispatch(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003, ARG002
        """Mock implementation of dbt adapter dispatch method."""
        return lambda *args, **kwargs: "__dispatch__"  # type: ignore  # noqa

    def quote(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003, ARG002
        """Mock implementation of dbt adapter quote method."""
        return "__quote__"

    def get_columns_in_relation(self, *args, **kwargs) -> list:  # noqa: ANN002, ANN003, ARG002
        """Mock implementation of dbt adapter get_columns_in_relation method."""
        return []


class DummyRelation:
    """Used in place of the dbt {{ this }} relation object."""

    def __init__(self) -> None:
        """Initialize the dummy relation with placeholder values."""
        self.database = "__database__"
        self.schema = "__schema__"
        self.table = "__table__"

    def __str__(self) -> str:
        """Return string representation of the relation."""
        return f"{self.database}.{self.schema}.{self.table}"


class VarsFetcher:
    """Pickleable variable holder for calling objects."""

    def __init__(self, dbt_vars: dict) -> None:
        """Initialize with dbt variables dictionary.

        Args:
            dbt_vars: Dictionary of dbt project variables.

        """
        self.vars = dbt_vars

    def __call__(self, name: str) -> Any:  # noqa: ANN401
        """Get a variable value by name.

        Args:
            name: Variable name to fetch.

        Returns:
            Variable value from the dbt project.

        """
        return self.vars[name]


class WarnUndefined(Undefined):
    """Custom Jinja undefined class that warns about unknown macros instead of failing.

    Warnings are deferred until the undefined is actually used (converted to string,
    called, or has attributes accessed), not when it's first encountered.
    """

    def __init__(self, hint=None, obj=None, name=None, exc=None) -> None:  # noqa: ANN001
        super().__init__(hint=hint, obj=obj, name=name, exc=exc)
        self._warned = False

    def _warn_about_unknown_macro(self) -> None:
        """Warn about unknown macro if not already warned and not ignored."""
        if self._warned:
            return
        self._warned = True

        macro_name = self._undefined_name
        if not macro_name or macro_name.startswith("jinja_"):
            return
        if "unknown_jinja_macro" in settings.warnings_ignored:
            return

        warning_message = f"Unknown macro '{macro_name}' encountered in template. "
        warnings_collector.add_warning(category="unknown_jinja_macro", message=warning_message)

    def _replacement_macro(self, is_called: bool = False) -> str:
        if self._undefined_name and not self._undefined_name.startswith("jinja_"):
            return f"{{{{ {self._undefined_name}{'()' if is_called else ''} }}}}"
        return super().__str__()

    def __str__(self) -> str:
        self._warn_about_unknown_macro()
        return self._replacement_macro()

    def __getattr__(self, name: str) -> "WarnUndefined":
        # Avoid infinite recursion when accessing _warned
        if name == "_warned":
            raise AttributeError(name)
        if name.startswith("jinja_"):
            return WarnUndefined(name=name)

        self._warn_about_unknown_macro()
        full_name = f"{self._undefined_name}.{name}" if self._undefined_name else name
        return WarnUndefined(name=full_name)

    def __call__(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003, ARG002
        self._warn_about_unknown_macro()
        return self._replacement_macro(is_called=True)


def _ref(x) -> str:  # noqa: ANN001
    """Mock implementation of dbt ref() function."""
    return f"{TABLE_REF_SEP}ref{TABLE_REF_SEP}{x}{TABLE_REF_SEP}"


def _source(x, y) -> str:  # noqa: ANN001
    """Mock implementation of dbt source() function."""
    return f"{TABLE_REF_SEP}source{TABLE_REF_SEP}{x}__{y}{TABLE_REF_SEP}"


def _config(**kwargs) -> Literal[""]:  # noqa: ANN003, ARG001
    """Mock implementation of dbt config() function."""
    return ""


def _return(*args) -> Literal[""]:  # noqa: ANN002, ARG001
    """Mock implementation of dbt return() function."""
    return ""


class DummyQueryResult:
    columns = []
    rows = []

    def __iter__(self) -> Iterator[Any]:
        return iter([])


def _run_query(*args, **kwargs) -> DummyQueryResult:  # noqa: ANN002, ANN003, ARG001
    return DummyQueryResult()


def _is_incremental(*args, **kwargs) -> bool:  # noqa: ANN002, ANN003, ARG001
    """Mock implementaiton of is_incremental() dbt built in macro."""
    return True


def _load_sorted_macro_dict(cache: Cache) -> dict[str, str]:
    """Load and cache sorted macro dictionary.

    Loads macros from cache if valid, otherwise fetches and sorts them
    by source priority (dbt_utils first, custom macros last).

    Returns:
        Dictionary mapping source names to concatenated macro strings.

    """
    if cache.cache_jinja_env.exists() and cache.validate_jinja_environment():
        utils.log.debug("Found valid macro cache!")
        return pickle.loads(cache.cache_jinja_env.read())  # noqa: S301
    weights = {"dbt_utils": -1, CUSTOM_MACROS: 1}
    macro_dict = dict(sorted(cache.macros_dict.items(), key=lambda x: weights.get(x[0], 0)))
    result = {}
    for source, macros in macro_dict.items():
        macro_string = ""
        for macro in macros:
            if not macro.is_test:
                macro_string += macro.code
        result[source] = macro_string
    cache.cache_jinja_env.write(pickle.dumps(result))
    return result


def _get_base_env(profile: DbtProfile) -> Environment:
    """Create base Jinja environment with dbt dummy functions.

    Sets up the core Jinja environment with necessary extensions,
    dummy implementations of dbt functions, and project variables.

    Returns:
        Configured Jinja Environment with dbt compatibility.

    """
    bytecode_cache = FileSystemBytecodeCache(str(utils.build_path("jinja_env")))
    env = Environment(
        extensions=["jinja2.ext.do"],
        loader=FileSystemLoader("templates"),
        bytecode_cache=bytecode_cache,
        autoescape=False,  # noqa: S701
        undefined=WarnUndefined,
    )
    # Other dummy functions
    _dummy_functions = {
        "ref": _ref,
        "source": _source,
        "config": _config,
        "return": _return,
        "run_query": _run_query,
        "is_incremental": _is_incremental,
        "target": profile,
        "adapter": DummyAdapter(),
        "this": DummyRelation(),
        "execute": False,  # Parsing phase flag
        "flags": type("Flags", (), {"FULL_REFRESH": False})(),
    }
    env.globals.update(_dummy_functions)
    # Python modules as supported in dbt:
    # https://docs.getdbt.com/reference/dbt-jinja-functions/modules
    env.globals["modules"] = {
        "datetime": datetime,
        "pytz": pytz,
        "re": re,
    }
    dbt_vars = VarsFetcher(settings.dbt_project.rendered_parse(env).to_dict().get("vars", {}))  # type: ignore
    env.globals.update(
        {
            "var": dbt_vars,
        },
    )
    return env


def _build_jinja_env(profile: DbtProfile, cache: Cache) -> Environment:
    """Build complete Jinja environment with macros.

    Creates the full environment by loading the base setup and then
    adding all project and package macros to the global namespace.

    Returns:
        Complete Jinja Environment ready for rendering dbt models.

    """
    env = _get_base_env(profile=profile)
    for source, macro_string in _load_sorted_macro_dict(cache=cache).items():
        modules = env.from_string(macro_string).module.__dict__
        if source == CUSTOM_MACROS:  # If they are custom macros, add them to global
            env.globals.update(modules)
        else:  # Otherwise add them under the source's namespace.
            env.globals[source] = modules
    return env


class Jinja:
    """Jinja class holder."""

    def __init__(self, cache: Cache, profile: DbtProfile | None = None) -> None:
        self.env = _build_jinja_env(profile=profile if profile else DbtProfile(), cache=cache)

    def render(self, sql: str) -> str:
        """Render a model using macros."""
        return self.env.from_string(sql).render()

    def parse(self, sql: str) -> Template:
        """Parse a model into jinja tree."""
        return self.env.parse(sql)
