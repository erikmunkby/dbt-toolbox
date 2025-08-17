<div id="top">

<!-- HEADER STYLE: CONSOLE -->
<div align="center">

```console
     |  |     |        |                |  |               
  _` |   _ \   _| ____| _|   _ \   _ \  |   _ \   _ \ \ \ /
\__,_| _.__/ \__|     \__| \___/ \___/ _| _.__/ \___/  _\_\

A powerful CLI toolkit to supercharge your dbt development workflow
```

</div>

<!-- BADGES -->
<img src="https://img.shields.io/github/license/erikmunkby/dbt-toolbox?style=flat-square&logo=opensourceinitiative&logoColor=white&color=8a2be2" alt="license">
<img src="https://img.shields.io/github/last-commit/erikmunkby/dbt-toolbox?style=flat-square&logo=git&logoColor=white&color=8a2be2" alt="last-commit">
<img src="https://img.shields.io/github/languages/top/erikmunkby/dbt-toolbox?style=flat-square&color=8a2be2" alt="repo-top-language">
<img src="https://img.shields.io/github/languages/count/erikmunkby/dbt-toolbox?style=flat-square&color=8a2be2" alt="repo-language-count">

<em>Built with the tools and technologies:</em>

<img src="https://img.shields.io/badge/Typer-000000.svg?style=flat-square&logo=Typer&logoColor=white" alt="Typer">
<img src="https://img.shields.io/badge/Python-3776AB.svg?style=flat-square&logo=Python&logoColor=white" alt="Python">
<img src="https://img.shields.io/badge/yamlium-FF6B6B.svg?style=flat-square&logo=YAML&logoColor=white" alt="yamlium">
<img src="https://img.shields.io/badge/SQLGlot-4169E1.svg?style=flat-square&logo=SQL&logoColor=white" alt="SQLGlot">
<img src="https://img.shields.io/badge/uv-DE5FE9.svg?style=flat-square&logo=uv&logoColor=white" alt="uv">

</div>
<br>

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

**LLM Ready via MCP server**
- Contains mcp server to make your LLM powered `dbt` development even better.
- Tools support the most common `dbt-toolbox` featuers.

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

# Install with MCP server support
uv add "dbt-toolbox[mcp]"
```

## ⚡ Quick Start

```bash
# Initialize and explore your project
dt settings                    # View all configuration

# Enhanced dbt commands with caching
dt build                      # Build with intelligent caching
dt run --model +my_model+     # Support for most dbt selection syntax
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

### MCP Server Integration

dbt-toolbox can also run as an [MCP (Model Context Protocol)](https://www.anthropic.com/news/model-context-protocol) server, enabling integration with external tools and AI assistants like Claude Code.

**Key Features:**
- Expose dbt-toolbox functionality through standardized MCP protocol
- Same intelligent caching and validation as CLI commands
- Perfect for AI-assisted dbt development workflows

**Quick Setup:**
```bash
# Install with MCP support
uv add "dbt-toolbox[mcp]"

# Available MCP tools:
# - analyze_models: Validate model references and column lineage
# - build_models: Build models with intelligent execution
```

For detailed MCP server setup and usage, see [MCP.md](https://github.com/erikmunkby/dbt-toolbox/blob/main/MCP.md).

## 🏗️ Key Features

### Intelligent Caching System
dbt-toolbox caches parsed models, macros, and Jinja environments in `.dbt_toolbox/` directory with smart invalidation based on file changes and project configuration.

### Smart Execution & Lineage Validation
- Only executes models that actually need rebuilding based on dependency analysis
- Optional lineage validation to catch broken references before execution
- Configurable model validation ignore lists for legacy models
- Cache validity controls for optimal performance

### Dependency Graph Analysis
Lightweight DAG implementation provides efficient model relationship tracking:
- Upstream/downstream dependency resolution
- Node type classification (models, macros, sources)
- Impact analysis for refactoring

### Enhanced CLI Experience
- Colored output with progress indicators
- Target-specific execution (`--target` option)
- Command shadowing for seamless dbt integration
- Comprehensive error handling and reporting

### Smart Documentation Generation
The `dt docs` command intelligently inherits column descriptions from:
- Upstream model columns with matching names
- Macro parameters that reference the columns
- Existing schema.yml documentation

## ⚙️ Configuration

dbt-toolbox supports configuration through multiple sources with the following precedence:

1. **Environment Variables** (highest priority)
2. **TOML Configuration** (`pyproject.toml`)
3. **dbt Profiles** (for SQL dialect)
4. **Auto-detection** (for project paths)
5. **Defaults** (lowest priority)

### Key Configuration Options
For an exhaustive list see [CLI.md](https://github.com/erikmunkby/dbt-toolbox/blob/main/CLI.md)

| Setting | Description | Default |
|---------|-------------|---------|
| `dbt_project_dir` | Path to dbt project | Auto-detected |
| `cache_path` | Cache directory location | `.dbt_toolbox` |
| `cache_validity_minutes` | Cache validity duration | `1440` (24 hours) |
| `enforce_lineage_validation` | Enable lineage validation | `true` |
| `models_ignore_validation` | Models to skip validation | `[]` |
| `debug` | Enable debug logging | `false` |
| `skip_placeholder` | Skip placeholder descriptions | `false` |
| `placeholder_description` | Custom placeholder text | `"TODO: PLACEHOLDER"` |

### Configuration Examples

**Environment Variables:**
```bash
export DBT_TOOLBOX_ENFORCE_LINEAGE_VALIDATION=false
export DBT_TOOLBOX_MODELS_IGNORE_VALIDATION="legacy_model,staging_temp"
export DBT_TOOLBOX_CACHE_VALIDITY_MINUTES=720 # Default=1440
```

**TOML Configuration:**
```toml
[tool.dbt_toolbox]
enforce_lineage_validation = false
models_ignore_validation = ["legacy_model", "staging_temp"]
cache_validity_minutes = 720 # Default=1440
```

## 📚 Documentation

- [CLI Reference](https://github.com/erikmunkby/dbt-toolbox/blob/main/CLI.md) - Detailed command documentation and examples
- [MCP Server Guide](https://github.com/erikmunkby/dbt-toolbox/blob/main/MCP.md) - MCP server setup and integration
- [Contributing Guide](https://github.com/erikmunkby/dbt-toolbox/blob/main/CONTRIBUTING.md) - Development setup and guidelines

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

## ⭐ Roadmap

- [X] **`dt docs`**: <strike>Automatic yaml docs generation.</strike>
- [x] **`Smart model selection`**: <strike>Smart caching and model selection for optimized executions.</strike>
- [x] **`MCP Server`**: Publish commands via MCP server.
- [ ] **`Expand testing stack`**: Build out the `dbt_toolbox.testing` stack.
- [ ] **`dt test`** Command, with test parsing and caching.

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/erikmunkby/dbt-toolbox/blob/main/CONTRIBUTING.md) for development setup, coding standards, and contribution guidelines.

## 📄 License

[MIT License](https://github.com/erikmunkby/dbt-toolbox/blob/main/LICENSE) - Feel free to use this project in your own work.

## 🙏 Acknowledgments

Built with modern Python tooling:
- [Typer](https://typer.tiangolo.com/) for the CLI framework
- [SQLGlot](https://sqlglot.com/) for SQL parsing and optimization
- [Jinja2](https://jinja.palletsprojects.com/) for template processing
- [yamlium](https://github.com/erikmunkby/yamlium) for YAML manipulation and generation
- [uv](https://docs.astral.sh/uv/) for dependency management