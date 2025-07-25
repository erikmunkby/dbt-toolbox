# dbt-toolbox

A powerful CLI toolkit that supercharges your dbt development workflow with intelligent caching, dependency analysis, and enhanced documentation generation.

## 🚀 What Makes dbt-toolbox Amazing

**Smart Caching & Performance**
- Lightning-fast model parsing with intelligent cache invalidation
- Persistent Jinja environment caching for instant macro resolution
- Dependency graph caching for rapid upstream/downstream analysis

**Enhanced dbt Commands**
- `dt build` - Drop-in replacement for `dbt build` with enhanced output and performance
- `dt run` - Drop-in replacement for `dbt run` with smart execution and caching
- Target-specific options for environment control
- Intelligent pre/post processing hooks

**Intelligent Documentation**
- `dt docs` - YAML documentation generator with smart column inheritance
- Automatically inherits descriptions from upstream models and macros
- Tracks column changes (additions, removals, reordering) between runs
- One-click clipboard integration

**Dependency Intelligence**
- Lightweight DAG implementation for model and macro relationships
- Efficient upstream/downstream traversal
- Node type tracking and statistics
- Perfect for impact analysis and refactoring

**Configuration**
- Multi-source settings hierarchy (env vars > TOML > dbt profiles > defaults)
- Dynamic dbt profile and target integration
- Source tracking for all configuration values

## 🛠️ Installation

```bash
# Using uv
uv add dbt-toolbox

# Or install with pip
pip install dbt-toolbox
```

## ⚡ Quick Start

```bash
# Initialize and explore your project
dt settings                    # View all configuration
dt clean                      # Clear cache if needed

# Enhanced dbt commands with caching
dt build --model my_model     # Build with intelligent caching
dt run --model my_model       # Run with intelligent caching
dt build --target prod        # Build against production target

# Analyze cache and dependencies
dt analyze                    # Analyze all models
dt analyze --model customers+ # Analyze specific model selection

# Generate documentation YAML
dt docs --model customers     # Generate docs for specific model
dt docs -m orders --clipboard # Copy to clipboard
```

## 📋 Core Commands

| Command | Description |
|---------|-------------|
| `dt build` | Enhanced dbt build with caching and better output |
| `dt run` | Enhanced dbt run with intelligent execution and caching |
| `dt docs` | Generate YAML documentation with smart inheritance |
| `dt analyze` | Analyze cache state and model dependencies without execution |
| `dt clean` | Clear all cached data with detailed reporting |
| `dt settings` | Inspect configuration from all sources |

## 🏗️ Key Features

### Intelligent Caching System
dbt-toolbox caches parsed models, macros, and Jinja environments in `.dbt_toolbox/` directory with smart invalidation based on file changes and project configuration.

### Dependency Graph Analysis
Lightweight DAG implementation provides efficient model relationship tracking:
- Upstream/downstream dependency resolution
- Node type classification (models, macros, sources)
- Impact analysis for refactoring

### Enhanced CLI Experience
- Colored output with progress indicators
- Global options that work across all commands
- Command shadowing for seamless dbt integration
- Comprehensive error handling and reporting

### Smart Documentation Generation
The `dt docs` command intelligently inherits column descriptions from:
- Upstream model columns with matching names
- Macro parameters that reference the columns
- Existing schema.yml documentation

## 📚 Documentation

- [CLI Reference](./CLI.md) - Detailed command documentation and examples
- [Contributing Guide](./CONTRIBUTING.md) - Development setup and guidelines
- [Architecture Overview](./CLAUDE.md) - Technical implementation details

## 🧪 Testing Integration

dbt-toolbox includes a testing module for your dbt projects:

```python
from dbt_toolbox.testing import check_column_documentation

def test_model_documentation():
    """Ensure all model columns are documented."""
    result = check_column_documentation()
    if result:
        pytest.fail(result)
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](./CONTRIBUTING.md) for development setup, coding standards, and contribution guidelines.

## 📄 License

[MIT License](LICENSE) - Feel free to use this project in your own work.

## 🙏 Acknowledgments

Built with modern Python tooling:
- [Typer](https://typer.tiangolo.com/) for the CLI framework
- [SQLGlot](https://sqlglot.com/) for SQL parsing and optimization
- [Jinja2](https://jinja.palletsprojects.com/) for template processing
- [yamlium](https://github.com/erikmunkby/yamlium) for YAML manipulation and generation
- [uv](https://docs.astral.sh/uv/) for dependency management