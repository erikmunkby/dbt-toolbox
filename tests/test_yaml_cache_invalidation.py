"""Tests for YAML documentation cache invalidation."""

from dbt_toolbox.dbt_parser import dbtParser


def test_yaml_changes_detected_on_cached_model(dbt_project, fresh_parser) -> None:
    """Test that schema.yml changes are detected even for cached models.

    Verifies that column descriptions, new columns, and model descriptions
    are all refreshed from fresh YAML parse when the model code hasn't changed.
    """
    # Parse model to populate cache
    model = fresh_parser.get_model("customers")
    assert model is not None
    assert model.yaml_docs is not None

    original_col_count = len(model.yaml_docs.columns)

    # Modify schema.yml with multiple changes
    schema_path = dbt_project / "models" / "schema.yml"
    content = schema_path.read_text()

    # Change 1: Update column description
    content = content.replace(
        "description: Unique identifier for each customer",
        "description: UPDATED CUSTOMER ID",
    )
    # Change 2: Add new column
    content = content.replace(
        "- name: full_name\n        description: Customer's complete name",
        "- name: full_name\n        description: Customer's complete name\n"
        "      - name: new_col\n        description: New column",
    )
    # Change 3: Update model description
    old_model_desc = (
        "description: >\n"
        "      A model that cleans and standardizes customer data from the raw customers table.\n"
        "      Transforms basic customer information into a consistent format."
    )
    content = content.replace(old_model_desc, "description: UPDATED MODEL DESC")
    schema_path.write_text(content)

    # Create new parser (simulating second run) and get model
    new_parser = dbtParser(target=None)
    new_model = new_parser.get_model("customers")
    assert new_model is not None
    assert new_model.yaml_docs is not None

    # Verify all changes detected
    cols = new_model.yaml_docs.columns
    assert len(cols) == original_col_count + 1  # type: ignore
    assert next(c for c in cols if c.name == "customer_id").description == "UPDATED CUSTOMER ID"  # type: ignore
    assert next((c for c in cols if c.name == "new_col"), None) is not None  # type: ignore
    assert new_model.yaml_docs.model_description == "UPDATED MODEL DESC"
