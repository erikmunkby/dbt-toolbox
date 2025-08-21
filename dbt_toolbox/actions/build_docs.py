"""Build dbt yaml docs."""

from dataclasses import dataclass
from pathlib import Path

import yamlium

from dbt_toolbox.data_models import ColumnChanges
from dbt_toolbox.dbt_parser import dbtParser
from dbt_toolbox.settings import settings

_DESC = "description"
_NAME = "name"


@dataclass
class DocsResult:
    """Result of building documentation for a model."""

    model_name: str
    model_path: str
    success: bool
    changes: ColumnChanges
    nbr_columns_with_placeholders: int
    yaml_content: str | None = None
    error_message: str | None = None
    yaml_path: str | None = None
    mode: str | None = None


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

        # Build the currently existing docs
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

    def _find_existing_yml_file(self) -> Path | None:
        """Find an existing .yml or .yaml file in the same directory as the model.

        Returns:
            Path to the first .yml/.yaml file found, or None if none exist.

        """
        model_dir = self.model.path.parent

        # Look for .yml files first, then .yaml files
        for pattern in ["*.yml", "*.yaml"]:
            yml_files = list(model_dir.glob(pattern))
            if yml_files:
                return yml_files[0]  # Return the first one found

        return None

    def _create_new_yml_file(self) -> Path:
        """Create a new .yml file in the same directory as the model.

        Returns:
            Path to the newly created .yml file.

        """
        model_dir = self.model.path.parent
        return model_dir / "schema.yml"

    def _append_to_existing_yml(self, yml_file: Path) -> str:
        """Append the model documentation to an existing .yml file.

        Args:
            yml_file: Path to the existing .yml file to append to.

        Returns:
            String indicating the mode of operation.

        """
        try:
            existing_yaml = yamlium.parse(yml_file)

            # Ensure models section exists
            if "models" not in existing_yaml:
                existing_yaml["models"] = []

            # Check if this model already exists in the file
            models_list = existing_yaml["models"]
            model_exists = False

            for i, model in enumerate(models_list):
                if model.get("name") == self.model.name:
                    # Replace existing model
                    models_list[i] = self.yml
                    model_exists = True
                    break

            if not model_exists:
                # Add new model
                models_list.append(self.yml)

            # Write back to file
            yml_file.write_text(
                "\n".join([x for x in existing_yaml.to_yaml().split("\n") if x]) + "\n"
            )
        except Exception as e:
            raise ValueError(f"Failed to append to existing YAML file {yml_file}: {e}") from e
        else:
            return "updated existing" if model_exists else "added to existing file"

    def _create_new_yml_with_model(self, yml_file: Path) -> str:
        """Create a new .yml file with the model documentation.

        Args:
            yml_file: Path to the new .yml file to create.

        Returns:
            String indicating the mode of operation ("created new file").

        """
        try:
            new_yaml = yamlium.from_dict({"models": [self.yml]})

            yml_file.write_text("\n".join([x for x in new_yaml.to_yaml().split("\n") if x]) + "\n")
        except Exception as e:
            raise ValueError(f"Failed to create new YAML file {yml_file}: {e}") from e
        else:
            return "created new file"

    def build(self, fix_inplace: bool) -> DocsResult:
        """Build the new yaml for the model.

        Args:
            fix_inplace: If True, updates the actual schema file.
                        If False, returns the YAML content as a string.

        Returns:
            DocsResult with metadata about the operation and optional YAML content.

        """
        try:
            final_columns = self._load_description()
        except Exception as e:  # noqa: BLE001
            return DocsResult(
                model_name=self.model.name,
                model_path=str(self.model.path),
                success=False,
                changes=ColumnChanges(added=[], removed=[], reordered=False),
                nbr_columns_with_placeholders=0,
                yaml_content=None,
                error_message=f"Failed to load column descriptions: {e}",
                yaml_path=None,
                mode=None,
            )

        try:
            changes = self._detect_column_changes(final_columns)
        except Exception as e:  # noqa: BLE001
            return DocsResult(
                model_name=self.model.name,
                model_path=str(self.model.path),
                success=False,
                changes=ColumnChanges(added=[], removed=[], reordered=False),
                nbr_columns_with_placeholders=0,
                yaml_content=None,
                error_message=f"Failed to detect column changes: {e}",
                yaml_path=None,
                mode=None,
            )

        # Count columns with placeholder descriptions
        nbr_placeholders = sum(
            1
            for col in final_columns
            if str(col.get(_DESC, "")).strip('"') == settings.placeholder_description
        )

        self.yml["columns"] = final_columns

        yaml_content = None
        success = True
        error_message = None
        yaml_path = None
        mode = None

        if fix_inplace:
            # Check if any changes were made
            has_changes = changes.added or changes.removed or changes.reordered

            if has_changes:
                try:
                    # Try to update existing YAML docs first
                    if self.model.yaml_docs is not None:
                        self.model.update_model_yaml(self.yml)
                        yaml_path = str(self.model.yaml_docs.path)
                        mode = "updated existing"
                    else:
                        # No existing YAML docs - find or create a YAML file
                        existing_yml = self._find_existing_yml_file()

                        if existing_yml:
                            # Append to existing file
                            mode = self._append_to_existing_yml(existing_yml)
                            yaml_path = str(existing_yml)
                        else:
                            # Create new file
                            new_yml = self._create_new_yml_file()
                            mode = self._create_new_yml_with_model(new_yml)
                            yaml_path = str(new_yml)

                except FileNotFoundError as e:
                    success = False
                    error_message = f"Schema file not found: {e}"
                except PermissionError as e:
                    success = False
                    error_message = f"Permission denied when writing to schema file: {e}"
                except ValueError as e:
                    success = False
                    error_message = str(e)
                except Exception as e:  # noqa: BLE001
                    success = False
                    error_message = f"Failed to update schema file: {e}"
        else:
            # Return YAML content as string
            try:
                yaml_content = yamlium.from_dict({"models": [self.yml]}).to_yaml()
            except Exception as e:  # noqa: BLE001
                success = False
                error_message = f"Failed to generate YAML content: {e}"

        return DocsResult(
            model_name=self.model.name,
            model_path=str(self.model.path),
            success=success,
            changes=changes,
            nbr_columns_with_placeholders=nbr_placeholders,
            yaml_content=yaml_content,
            error_message=error_message,
            yaml_path=yaml_path,
            mode=mode,
        )
