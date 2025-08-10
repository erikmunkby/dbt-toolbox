"""Documentation command for yaml."""

import subprocess
from typing import Annotated

import typer
import yamlium

from dbt_toolbox.cli._common_options import Target
from dbt_toolbox.data_models import ColumnChanges
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.settings import settings
from dbt_toolbox.utils import _printers

_DESC = "description"
_NAME = "name"


class YamlBuilder:
    """Builder for generating and updating dbt model YAML documentation."""

    def __init__(self, model_name: str, dbt_parser: dbtParser) -> None:
        """Initialize the YAML builder for a specific model.

        Args:
            model_name: Name of the dbt model to build docs for.
            dbt_parser: The dbt parser instance to use.

        """
        self.dbt_parser = dbt_parser
        self.model = dbt_parser.models[model_name]
        self.idx, yml = self.model.load_model_yaml  # type: ignore
        if yml is None:
            yml: yamlium.Mapping = yamlium.from_dict(
                {
                    _NAME: model_name,
                    "columns": [],
                },
            )
        if not settings.skip_placeholders and _DESC not in yml:
            yml[_DESC] = settings.placeholder_description
        self.yml = yml
        self.yaml_docs = {c[_NAME]: c for c in self.yml.get("columns", [])}

    def _get_column_description(self, col: str, /) -> dict[str, str] | None:
        """Fetch column description for an individual column.

        Using the priority:
        - existing yaml docs
        - column macro docs
        - upstream model docs
        """
        # Existing docs
        if col in self.yaml_docs:
            return self.yaml_docs[col]

        # Macro docs
        if col in self.dbt_parser.column_macro_docs:
            return {_NAME: col, _DESC: f"\"{{{{ doc('{col}') }}}}\""}
        # Upstream model docs
        for upstream_model in self.model.upstream.models:
            if upstream_model not in self.dbt_parser.models:
                # This happens when upstream model is a seed.
                # TODO: Build support for seed docs.
                continue
            for upstream_col in self.dbt_parser.models[upstream_model].column_descriptions:
                if col == upstream_col.name:
                    return {_NAME: col, _DESC: upstream_col.description}  # type: ignore

        # Upstream source docs
        for upstream_source in self.model.upstream.sources:
            if upstream_source not in self.dbt_parser.sources:
                continue
            for upstream_col in self.dbt_parser.sources[upstream_source].columns:
                if col == upstream_col.name and upstream_col.description:
                    return {_NAME: col, _DESC: upstream_col.description}
        if settings.skip_placeholders:
            return None

        return {_NAME: col, _DESC: f'"{settings.placeholder_description}"'}

    def _detect_column_changes(self, new_columns: list[dict[str, str]]) -> ColumnChanges:
        """Detect changes between existing and new columns.

        Returns:
            ColumnChanges dataclass with added, removed, and reordered information.

        """
        existing_columns = [c[_NAME] for c in self.yml.get("columns", [])]
        new_column_names = [c[_NAME] for c in new_columns]

        added = [col for col in new_column_names if col not in existing_columns]
        removed = [col for col in existing_columns if col not in new_column_names]

        # Check if order changed (only for columns that exist in both)
        common_columns = [col for col in existing_columns if col in new_column_names]
        common_new_order = [col for col in new_column_names if col in existing_columns]
        reordered = common_columns != common_new_order

        return ColumnChanges(
            added=added,
            removed=removed,
            reordered=reordered,
        )

    def _load_description(self) -> list[dict[str, str]]:
        """Load and build the complete model description with columns.

        Returns:
            List of column dictionaries with name and description.

        """
        final_columns = []
        missing_column_docs = []
        for c in self.model.final_columns:
            desc = self._get_column_description(c)
            if desc is None:
                missing_column_docs.append(c)
            else:
                final_columns.append(desc)

        return final_columns

    def build(self, print_only: bool) -> None:
        """Build the new yaml for the model.

        Args:
            print_only: If True, prints YAML to stdout and copies to clipboard.
                        If False, updates the actual schema file.

        """
        final_columns = self._load_description()
        changes = self._detect_column_changes(final_columns)

        self.yml["columns"] = final_columns

        if print_only:
            yml = yamlium.from_dict({"models": [self.yml]}).to_yaml()
            process = subprocess.Popen(args="pbcopy", stdin=subprocess.PIPE)
            process.communicate(input=yml.encode())
            _printers.cprint(yml)
            _printers.cprint("Also exists in your clipboard (cmd+V)", color="green")
        else:
            # Check if any changes were made
            has_changes = changes.added or changes.removed or changes.reordered

            if not has_changes:
                _printers.cprint(
                    "ℹ️  No column changes detected for model",  # noqa: RUF001
                    self.model.name,
                    highlight_idx=1,
                )
                return

            # Print detailed change information
            change_messages = []
            if changes.added:
                change_messages.append(f"Added columns: {', '.join(changes.added)}")
            if changes.removed:
                change_messages.append(f"Removed columns: {', '.join(changes.removed)}")
            if changes.reordered:
                change_messages.append("Column order changed")

            _printers.cprint("✅ updated model", self.model.name, highlight_idx=1)
            for msg in change_messages:
                _printers.cprint(f"   {msg}", color="cyan")

            self.model.update_model_yaml(self.yml)


def docs(
    model: Annotated[
        str,
        typer.Option(
            "--model",
            "-m",
            help="Model name to generate documentation for",
        ),
    ],
    clipboard: Annotated[
        bool,
        typer.Option(
            "--clipboard",
            "-c",
            help="Copy output to clipboard",
        ),
    ] = False,
    target: str | None = Target,
) -> None:
    """Generate documentation for a specific dbt model.

    This is a typer command configured in cli/main.py.
    """
    dbt_parser = dbtParser(target=target)
    if model not in dbt_parser.models:
        _printers.cprint("model", model, "not found", highlight_idx=1, color="red")
        raise typer.Exit(1)
    YamlBuilder(model, dbt_parser).build(print_only=clipboard)
