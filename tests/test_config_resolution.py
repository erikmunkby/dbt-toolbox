"""Tests for model configuration resolution logic."""

from pathlib import Path

from dbt_toolbox.dbt_parser._builders import _resolve_model_config


class TestResolveModelConfig:
    """Test model configuration resolution with various scenarios."""

    def test_default_materialization_when_no_config(self) -> None:
        """Test that 'view' is the default when no config is provided."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {}
        dbt_project_dict = {}

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "view"}

    def test_jinja_config_override_default(self) -> None:
        """Test that Jinja config overrides default materialization."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {"materialized": "table"}
        dbt_project_dict = {}

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "table"}

    def test_project_config_with_plus_prefix(self) -> None:
        """Test project-level config with + prefix."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {}
        dbt_project_dict = {
            "name": "my_project",
            "models": {"my_project": {"+materialized": "table", "+tags": ["project_tag"]}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "table", "tags": ["project_tag"]}

    def test_path_specific_config_override(self) -> None:
        """Test that path-specific config overrides project defaults."""
        model_path = Path("models/marts/finance/revenue.sql")
        config_kwargs = {}
        dbt_project_dict = {
            "name": "my_project",
            "models": {
                "my_project": {
                    "+materialized": "view",
                    "marts": {
                        "+materialized": "table",
                        "finance": {"+materialized": "incremental"},
                    },
                }
            },
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "incremental"}

    def test_jinja_config_highest_precedence(self) -> None:
        """Test that Jinja config has highest precedence over all project configs."""
        model_path = Path("models/marts/finance/revenue.sql")
        config_kwargs = {"materialized": "ephemeral"}
        dbt_project_dict = {
            "name": "my_project",
            "models": {
                "my_project": {
                    "+materialized": "view",
                    "marts": {
                        "+materialized": "table",
                        "finance": {"+materialized": "incremental"},
                    },
                }
            },
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "ephemeral"}

    def test_multiple_config_properties_merge(self) -> None:
        """Test that multiple config properties merge correctly across levels."""
        model_path = Path("models/marts/finance/revenue.sql")
        config_kwargs = {"post_hook": "grant select on {{ this }} to finance"}
        dbt_project_dict = {
            "name": "my_project",
            "models": {
                "my_project": {
                    "+materialized": "view",
                    "+tags": ["project"],
                    "marts": {
                        "+materialized": "table",
                        "+pre_hook": "create schema if not exists marts",
                        "finance": {
                            "+tags": ["finance", "critical"],
                            "+materialized": "incremental",
                        },
                    },
                }
            },
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        expected = {
            "materialized": "incremental",
            "tags": ["finance", "critical"],  # finance level overrides project level
            "pre_hook": "create schema if not exists marts",
            "post_hook": "grant select on {{ this }} to finance",
        }
        assert result == expected

    def test_no_project_name_in_dbt_project(self) -> None:
        """Test fallback when project name is missing from dbt_project.yml."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {}
        dbt_project_dict = {"models": {"some_project": {"+materialized": "table"}}}

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "view"}

    def test_project_not_in_models_config(self) -> None:
        """Test fallback when project is not configured in models section."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {}
        dbt_project_dict = {
            "name": "my_project",
            "models": {"other_project": {"+materialized": "table"}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "view"}

    def test_deep_nested_path_config(self) -> None:
        """Test configuration resolution for deeply nested model paths."""
        model_path = Path("models/staging/external/api/users.sql")
        config_kwargs = {}
        dbt_project_dict = {
            "name": "my_project",
            "models": {
                "my_project": {
                    "+materialized": "view",
                    "staging": {
                        "+materialized": "view",
                        "+tags": ["staging"],
                        "external": {
                            "+pre_hook": "create schema if not exists staging_external",
                            "api": {"+materialized": "table", "+tags": ["api", "external"]},
                        },
                    },
                }
            },
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        expected = {
            "materialized": "table",
            "tags": ["api", "external"],  # Most specific level wins
            "pre_hook": "create schema if not exists staging_external",
        }
        assert result == expected

    def test_model_in_root_models_directory(self) -> None:
        """Test model directly in models directory (no subdirectories)."""
        model_path = Path("models/simple_model.sql")
        config_kwargs = {}
        dbt_project_dict = {
            "name": "my_project",
            "models": {"my_project": {"+materialized": "table", "+tags": ["root_level"]}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "table", "tags": ["root_level"]}

    def test_config_without_plus_prefix_ignored(self) -> None:
        """Test that config without + prefix is ignored in project config."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {}
        dbt_project_dict = {
            "name": "my_project",
            "models": {
                "my_project": {
                    "+materialized": "table",
                    "materialized": "incremental",  # No + prefix, should be ignored
                    "some_folder": {
                        "other_config": "value"  # No + prefix, should be ignored
                    },
                }
            },
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        assert result == {"materialized": "table"}

    def test_complex_jinja_config_values(self) -> None:
        """Test that complex Jinja config values are preserved."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {
            "materialized": "incremental",
            "unique_key": "id",
            "incremental_strategy": "merge",
            "tags": ["complex", "incremental"],
            "indexes": [{"columns": ["id"], "unique": True}],
        }
        dbt_project_dict = {
            "name": "my_project",
            "models": {"my_project": {"+materialized": "table", "+tags": ["default"]}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict)

        expected = {
            "materialized": "incremental",  # Jinja overrides project
            "unique_key": "id",
            "incremental_strategy": "merge",
            "tags": ["complex", "incremental"],  # Jinja overrides project
            "indexes": [{"columns": ["id"], "unique": True}],
        }
        assert result == expected

    def test_yaml_docs_config_precedence(self) -> None:
        """Test that YAML docs config has precedence over project config but not Jinja config."""
        from pathlib import Path

        from dbt_toolbox.data_models import YamlDocs

        model_path = Path("models/test_model.sql")
        config_kwargs = {}
        yaml_docs = YamlDocs(
            model_description="Test model",
            path=Path("models/schema.yml"),
            config={"materialized": "table", "tags": ["yaml_tag"]},
            columns=None,
        )
        dbt_project_dict = {
            "name": "my_project",
            "models": {"my_project": {"+materialized": "view", "+tags": ["project_tag"]}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict, yaml_docs)

        # YAML docs should override project config
        assert result == {"materialized": "table", "tags": ["yaml_tag"]}

    def test_jinja_config_overrides_yaml_docs(self) -> None:
        """Test that Jinja config has highest precedence over YAML docs config."""
        from pathlib import Path

        from dbt_toolbox.data_models import YamlDocs

        model_path = Path("models/test_model.sql")
        config_kwargs = {"materialized": "incremental", "unique_key": "id"}
        yaml_docs = YamlDocs(
            model_description="Test model",
            path=Path("models/schema.yml"),
            config={"materialized": "table", "tags": ["yaml_tag"]},
            columns=None,
        )
        dbt_project_dict = {
            "name": "my_project",
            "models": {"my_project": {"+materialized": "view", "+tags": ["project_tag"]}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict, yaml_docs)

        # Jinja should override YAML docs, YAML docs should override project
        assert result == {
            "materialized": "incremental",  # From Jinja (highest precedence)
            "unique_key": "id",  # From Jinja
            "tags": ["yaml_tag"],  # From YAML docs (middle precedence)
        }

    def test_full_precedence_hierarchy(self) -> None:
        """Test complete precedence hierarchy: Jinja > YAML docs > project config."""
        from pathlib import Path

        from dbt_toolbox.data_models import YamlDocs

        model_path = Path("models/marts/finance/revenue.sql")
        config_kwargs = {"materialized": "incremental"}  # Highest precedence
        yaml_docs = YamlDocs(
            model_description="Revenue model",
            path=Path("models/schema.yml"),
            config={"tags": ["finance"], "pre_hook": "truncate staging"},  # Middle precedence
            columns=None,
        )
        dbt_project_dict = {
            "name": "my_project",
            "models": {
                "my_project": {
                    "+materialized": "view",  # Lowest precedence
                    "+tags": ["default"],  # Lowest precedence
                    "+post_hook": "grant select to analyst",  # Lowest precedence
                    "marts": {
                        "finance": {
                            "+materialized": "table"  # Should be overridden by Jinja
                        }
                    },
                }
            },
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict, yaml_docs)

        expected = {
            "materialized": "incremental",  # From Jinja (overrides all)
            "tags": ["finance"],  # From YAML docs (overrides project)
            "pre_hook": "truncate staging",  # From YAML docs
            "post_hook": "grant select to analyst",  # From project (no override)
        }
        assert result == expected

    def test_yaml_docs_none_fallback(self) -> None:
        """Test that None yaml_docs doesn't break the function."""
        model_path = Path("models/test_model.sql")
        config_kwargs = {"materialized": "table"}
        dbt_project_dict = {
            "name": "my_project",
            "models": {"my_project": {"+tags": ["project_tag"]}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict, yaml_docs=None)

        assert result == {"materialized": "table", "tags": ["project_tag"]}

    def test_yaml_docs_empty_config(self) -> None:
        """Test that empty yaml_docs.config doesn't affect resolution."""
        from pathlib import Path

        from dbt_toolbox.data_models import YamlDocs

        model_path = Path("models/test_model.sql")
        config_kwargs = {"materialized": "table"}
        yaml_docs = YamlDocs(
            model_description="Test model",
            path=Path("models/schema.yml"),
            config={},  # Empty config
            columns=None,
        )
        dbt_project_dict = {
            "name": "my_project",
            "models": {"my_project": {"+tags": ["project_tag"]}},
        }

        result = _resolve_model_config(model_path, config_kwargs, dbt_project_dict, yaml_docs)

        assert result == {"materialized": "table", "tags": ["project_tag"]}
