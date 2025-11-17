"""Parse dbt tests from schema.yml files."""

from pathlib import Path

import yamlium


def parse_tests_from_yaml(model_yaml_paths: list[Path]) -> dict[str, list[str]]:
    """Parse data tests from schema.yml files.

    Args:
        model_yaml_paths: List of paths to schema.yml files.

    Returns:
        Dictionary mapping model names to lists of test names.
        Test names follow dbt convention: "{test_type}_{model_name}_{column_name}"

    """
    result: dict[str, list[str]] = {}

    for path in model_yaml_paths:
        yaml_content = yamlium.parse(path).to_dict()
        models: list[dict] = yaml_content.get("models", [])

        for model in models:
            model_name = model.get("name")
            if not model_name:
                continue

            test_names = []
            columns = model.get("columns", [])

            for column in columns:
                column_name = column.get("name")
                if not column_name:
                    continue

                # Parse data_tests (new dbt syntax) or tests (legacy syntax)
                tests = column.get("data_tests", []) or column.get("tests", [])

                for test in tests:
                    # Handle both simple string tests and dict tests
                    if isinstance(test, str):
                        # Simple test: "unique", "not_null"
                        test_name = f"{test}_{model_name}_{column_name}"
                    elif isinstance(test, dict):
                        # Complex test: {"accepted_values": {"values": [...]}}
                        test_type = next(iter(test.keys()))
                        test_name = f"{test_type}_{model_name}_{column_name}"
                    else:
                        continue

                    test_names.append(test_name)

            if test_names:
                result[model_name] = test_names

    return result
