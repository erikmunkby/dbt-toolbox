"""Integration tests for YAML docs builder using actual models and YAML files."""

from unittest.mock import patch

import yamlium

from dbt_toolbox.actions.build_docs import DocsResult, YamlBuilder
from dbt_toolbox.data_models import ColumnChanges
from dbt_toolbox.dbt_parser import dbtParser


class TestYamlBuilderIntegration:
    """Integration tests for YamlBuilder using real dbt models and YAML files."""

    def test_build_with_fix_inplace_false_returns_yaml_content(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test that fix_inplace=False returns YAML content without modifying files."""
        builder = YamlBuilder("customers", dbt_parser)

        result = builder.build(fix_inplace=False)

        # Verify result structure
        assert isinstance(result, DocsResult)
        assert result.model_name == "customers"
        assert result.model_path == str(dbt_parser.models["customers"].path)
        assert result.success is True
        assert isinstance(result.changes, ColumnChanges)
        assert isinstance(result.nbr_columns_with_placeholders, int)
        assert result.yaml_content is not None
        assert result.error_message is None

        # Verify YAML content is valid
        yaml_data = yamlium.parse(result.yaml_content)
        assert "models" in yaml_data
        assert len(yaml_data["models"]) == 1
        assert yaml_data["models"][0]["name"] == "customers"
        assert "columns" in yaml_data["models"][0]

    def test_build_with_fix_inplace_true_updates_file(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test that fix_inplace=True updates the actual schema file."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock the update_model_yaml method to track if it was called
        with patch.object(builder.model, "update_model_yaml") as mock_update:
            # Force some changes by adding a new column to final_columns
            original_load = builder._load_description

            def mock_load_description() -> list[dict[str, str]]:
                columns = original_load()
                # Add a new column to force changes
                columns.append({"name": "test_new_column", "description": "Test description"})
                return columns

            with patch.object(builder, "_load_description", side_effect=mock_load_description):
                result = builder.build(fix_inplace=True)

        # Verify result structure
        assert isinstance(result, DocsResult)
        assert result.model_name == "customers"
        assert result.success is True
        assert result.yaml_content is None  # Should be None when fix_inplace=True

        # Verify changes were detected
        assert "test_new_column" in result.changes.added

        # Verify update_model_yaml was called
        mock_update.assert_called_once()

    def test_build_with_fix_inplace_true_no_changes(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test fix_inplace=True when no changes are needed."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock to ensure no changes are detected
        with patch.object(builder.model, "update_model_yaml") as mock_update:
            with patch.object(builder, "_detect_column_changes") as mock_detect:
                mock_detect.return_value = ColumnChanges(added=[], removed=[], reordered=False)

                result = builder.build(fix_inplace=True)

        # Verify result
        assert result.success is True
        assert result.yaml_content is None
        assert not (result.changes.added)
        assert not (result.changes.removed)
        assert not (result.changes.reordered)

        # Verify update_model_yaml was NOT called since no changes
        mock_update.assert_not_called()

    def test_placeholder_counting_accuracy(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test that placeholder counting is accurate."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock _load_description to return known placeholders
        placeholder_desc = '"TODO: PLACEHOLDER"'
        mock_columns = [
            {"name": "col1", "description": placeholder_desc},
            {"name": "col2", "description": "Real description"},
            {"name": "col3", "description": placeholder_desc},
            {"name": "col4", "description": "Another real description"},
        ]

        with patch.object(builder, "_load_description", return_value=mock_columns):
            result = builder.build(fix_inplace=False)

        # Should count 2 placeholders
        assert result.nbr_columns_with_placeholders == 2

    def test_yaml_content_structure_and_format(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test that generated YAML content has correct structure and format."""
        builder = YamlBuilder("customers", dbt_parser)

        result = builder.build(fix_inplace=False)

        # Parse the YAML content
        yaml_data = yamlium.parse(result.yaml_content)

        # Verify structure
        assert "models" in yaml_data
        assert len(yaml_data["models"]) == 1

        model_data = yaml_data["models"][0]
        assert model_data["name"] == "customers"
        assert "columns" in model_data

        # Verify columns have required fields
        for column in model_data["columns"]:
            assert "name" in column
            assert "description" in column
            # yamlium returns Scalar objects, not native strings
            assert str(column["name"])  # Just verify they can be converted to string
            assert str(column["description"])

    def test_different_models_produce_different_results(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test that different models produce appropriately different results."""
        # Test with customers model
        customers_builder = YamlBuilder("customers", dbt_parser)
        customers_result = customers_builder.build(fix_inplace=False)

        # Test with orders model
        orders_builder = YamlBuilder("orders", dbt_parser)
        orders_result = orders_builder.build(fix_inplace=False)

        # Results should be different
        assert customers_result.model_name != orders_result.model_name
        assert customers_result.model_path != orders_result.model_path
        assert customers_result.yaml_content != orders_result.yaml_content

        # Parse both YAML contents to verify they're different
        customers_yaml = yamlium.parse(customers_result.yaml_content)
        orders_yaml = yamlium.parse(orders_result.yaml_content)

        assert customers_yaml["models"][0]["name"] == "customers"
        assert orders_yaml["models"][0]["name"] == "orders"

    def test_error_handling_during_file_update(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test error handling when file update fails."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock update_model_yaml to raise an exception
        with patch.object(
            builder.model, "update_model_yaml", side_effect=Exception("File write error")
        ):
            # Force changes to trigger update attempt
            with patch.object(builder, "_detect_column_changes") as mock_detect:
                mock_detect.return_value = ColumnChanges(
                    added=["new_col"], removed=[], reordered=False
                )

                result = builder.build(fix_inplace=True)

        # Should return success=False when update fails
        assert result.success is False
        assert result.yaml_content is None
        assert result.error_message is not None
        assert "new_col" in result.changes.added

    def test_column_changes_detection_integration(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test that column changes are properly detected in integration scenario."""
        builder = YamlBuilder("customers", dbt_parser)

        # Get current state
        builder.build(fix_inplace=False)

        # Get the original columns first to avoid recursion
        original_columns = builder._load_description()

        # Create modified columns
        modified_columns = original_columns.copy()
        # Add new column
        modified_columns.append({"name": "new_integration_col", "description": "New column"})
        # Remove first column if it exists
        if modified_columns:
            modified_columns = modified_columns[1:]

        with patch.object(builder, "_load_description", return_value=modified_columns):
            changed_result = builder.build(fix_inplace=False)

        # Verify changes were detected
        assert "new_integration_col" in changed_result.changes.added
        assert len(changed_result.changes.removed) > 0

    def test_model_with_existing_documentation(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test builder with a model that already has documentation."""
        # Use 'orders' model which has extensive documentation in schema.yml
        builder = YamlBuilder("orders", dbt_parser)

        result = builder.build(fix_inplace=False)

        # Parse the result to verify existing docs are preserved
        yaml_data = yamlium.parse(result.yaml_content)
        model_data = yaml_data["models"][0]

        # Should have preserved existing description
        assert "description" in model_data
        assert len(model_data["description"]) > 0

        # Should have columns with existing descriptions
        assert "columns" in model_data
        columns = model_data["columns"]

        # Find a column that should have existing docs (like order_id)
        order_id_col = next((col for col in columns if col["name"] == "order_id"), None)
        if order_id_col:
            assert "description" in order_id_col
            assert len(order_id_col["description"]) > 0

    def test_model_without_existing_yaml(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test builder with a model that doesn't have existing YAML documentation."""
        # Use 'some_other_model' which should have minimal/no docs
        builder = YamlBuilder("some_other_model", dbt_parser)

        result = builder.build(fix_inplace=False)

        # Should successfully generate YAML even without existing docs
        assert result.success is True
        assert result.yaml_content is not None

        # Parse the generated YAML
        yaml_data = yamlium.parse(result.yaml_content)
        model_data = yaml_data["models"][0]

        assert model_data["name"] == "some_other_model"
        assert "columns" in model_data

    def test_comprehensive_workflow_simulation(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test a comprehensive workflow that simulates real usage."""
        builder = YamlBuilder("customers", dbt_parser)

        # Step 1: Get current state (like preview mode)
        preview_result = builder.build(fix_inplace=False)
        assert preview_result.yaml_content is not None

        # Step 2: Simulate making changes and applying them
        with patch.object(builder.model, "update_model_yaml") as mock_update:
            # Force some changes
            with patch.object(builder, "_detect_column_changes") as mock_detect:
                mock_detect.return_value = ColumnChanges(
                    added=["workflow_col"], removed=[], reordered=False
                )

                update_result = builder.build(fix_inplace=True)

        # Verify the workflow
        assert preview_result.model_name == update_result.model_name
        assert preview_result.model_path == update_result.model_path
        assert preview_result.yaml_content is not None
        assert update_result.yaml_content is None  # No content when updating
        assert update_result.success is True
        assert "workflow_col" in update_result.changes.added
        mock_update.assert_called_once()

    def test_detailed_error_messages_for_different_failure_types(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test that specific error types produce helpful error messages."""
        builder = YamlBuilder("customers", dbt_parser)

        # Test FileNotFoundError
        file_error = FileNotFoundError("schema.yml")
        with patch.object(builder.model, "update_model_yaml", side_effect=file_error):
            with patch.object(builder, "_detect_column_changes") as mock_detect:
                mock_detect.return_value = ColumnChanges(
                    added=["new_col"], removed=[], reordered=False
                )

                result = builder.build(fix_inplace=True)

        assert result.success is False
        assert "Schema file not found" in result.error_message
        assert "schema.yml" in result.error_message

        # Test PermissionError
        perm_error = PermissionError("Permission denied")
        with patch.object(builder.model, "update_model_yaml", side_effect=perm_error):
            with patch.object(builder, "_detect_column_changes") as mock_detect:
                mock_detect.return_value = ColumnChanges(
                    added=["new_col"], removed=[], reordered=False
                )

                result = builder.build(fix_inplace=True)

        assert result.success is False
        assert "Permission denied when writing to schema file" in result.error_message

        # Test generic Exception
        generic_error = Exception("Generic error")
        with patch.object(builder.model, "update_model_yaml", side_effect=generic_error):
            with patch.object(builder, "_detect_column_changes") as mock_detect:
                mock_detect.return_value = ColumnChanges(
                    added=["new_col"], removed=[], reordered=False
                )

                result = builder.build(fix_inplace=True)

        assert result.success is False
        assert "Failed to update schema file" in result.error_message
        assert "Generic error" in result.error_message

    def test_error_handling_in_column_loading_phase(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test error handling when column loading fails."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock _load_description to raise an exception
        load_error = Exception("Column loading failed")
        with patch.object(builder, "_load_description", side_effect=load_error):
            result = builder.build(fix_inplace=False)

        assert result.success is False
        assert result.error_message is not None
        assert "Failed to load column descriptions" in result.error_message
        assert "Column loading failed" in result.error_message
        assert result.yaml_content is None

    def test_error_handling_in_yaml_generation_phase(
        self, dbt_project_setup: None, dbt_parser: dbtParser
    ) -> None:
        """Test error handling when YAML generation fails."""
        builder = YamlBuilder("customers", dbt_parser)

        # Mock yamlium.from_dict to raise an exception during YAML generation
        yaml_error = Exception("YAML generation failed")
        patch_path = "dbt_toolbox.actions.build_docs.yamlium.from_dict"
        with patch(patch_path, side_effect=yaml_error):
            result = builder.build(fix_inplace=False)

        assert result.success is False
        assert result.error_message is not None
        assert "Failed to generate YAML content" in result.error_message
        assert "YAML generation failed" in result.error_message
        assert result.yaml_content is None
