# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
- `pytest` - Run all tests
- `pytest tests/test_cacher.py` - Run specific test file
- `pytest -v` - Run tests with verbose output

### Code Quality  
- `ruff check` - Run linting
- `ruff format` - Format code
- `ruff check --fix` - Auto-fix linting issues

### Development Environment
- Uses `uv` for dependency management
- Python 3.10+ required
- Install dev dependencies: `uv sync --group dev`

### CLI Usage
- `dt` - Main CLI entry point (configured in pyproject.toml)
- `dt --help` - Show all available commands
- `dt --target prod` - Global option to specify dbt target

## Architecture Overview

### Core Components

**CLI System (`dbt_toolbox/cli/`)**
- `main.py` - Main CLI application with Typer, global options, and settings command
- `_dbt_executor.py` - Shared dbt execution engine for build and run commands
- `build.py` - Shadows `dbt build` with intelligent execution and enhanced output
- `run.py` - Shadows `dbt run` with smart cache-based execution
- `build_analysis.py` - Execution analysis logic for intelligent model selection
- `clean.py` - Cache management and cleanup functionality
- `docs.py` - YAML documentation generation with YamlBuilder class

**dbt Parser System (`dbt_toolbox/dbt_parser/`)**
- `dbt_parser.py` - Main parsing interface and model/macro management
- `_cache.py` - Caching implementation with pickle-based persistence
- `_file_fetcher.py` - Fetches macros and models from filesystem
- `_jinja_handler.py` - Handles Jinja environment and template rendering

**Dependency Graph (`dbt_toolbox/graph/`)**
- `dependency_graph.py` - Lightweight DAG implementation for tracking model and macro dependencies
- Supports upstream/downstream traversal, node type tracking, and statistics

**Data Models (`data_models.py`)**
- `RawMacro` - Represents raw macro files with metadata
- `Model` - Complete model with rendered code and SQL parsing
- `ModelBase` - Base model structure
- `DependsOn` - Tracks model dependencies (refs, sources, macros)
- `YamlDocs` - Documentation from schema.yml files
- `ColumnChanges` - Tracks column additions, removals, and reordering

**Configuration (`settings.py`)**
- Advanced settings management with source tracking
- Supports environment variables, TOML files, and dbt profile integration
- Settings precedence: env vars > TOML > dbt profiles > defaults
- Dynamic dbt profile loading with target configuration

**Testing Module (`dbt_toolbox/testing/`)**
This module is NOT tests for the project itself, but helper functions for users of `dbt-toolbox` to implement tests themselves.
- Provides `check_column_documentation()` function for pytest integration

**Utilities (`utils/`)**
- `printer.py` - Enhanced console output with colors and highlighting
- `utils.py` - General utility functions

### Key Design Patterns

1. **Intelligent Execution**: Smart cache-based execution that analyzes which models need rebuilding
2. **Shared Command Infrastructure**: Common dbt execution logic via `_dbt_executor.py` factory pattern
3. **Caching Strategy**: Uses pickle serialization for caching parsed models, macros, and Jinja environments
4. **Dependency Tracking**: Lightweight DAG with efficient upstream/downstream traversal
5. **SQL Processing**: Uses SQLGlot for parsing and optimizing SQL queries
6. **CLI Design**: Typer-based with global options, command shadowing, and enhanced UX
7. **Configuration Hierarchy**: Multi-source settings with precedence and source tracking

### CLI Commands

**`dt build`** - Enhanced dbt build wrapper with intelligent execution
- Shadows `dbt build` with smart cache-based execution by default
- Analyzes which models need execution based on cache validity and dependency changes
- Supports common dbt options: `--model`, `--select`, `--full-refresh`, `--threads`, `--target`, `--vars`
- Special options: `--analyze` (show analysis only), `--disable-smart` (force all models)
- Enhanced output with colored progress indicators and execution analysis

**`dt run`** - Enhanced dbt run wrapper with intelligent execution
- Shadows `dbt run` with smart cache-based execution by default
- Analyzes which models need execution based on cache validity and dependency changes
- Supports common dbt options: `--model`, `--select`, `--full-refresh`, `--threads`, `--target`, `--vars`
- Special options: `--analyze` (show analysis only), `--disable-smart` (force all models)
- Enhanced output with colored progress indicators and execution analysis

**`dt docs`** - YAML documentation generator
- `--model/-m` - Specify model name
- `--clipboard/-c` - Copy output to clipboard
- Intelligent column description inheritance from upstream models and macros
- Tracks column changes (additions, removals, reordering)

**`dt analyze`** - Cache state analysis
- Analyzes model cache state without manipulating it
- Shows outdated models, ID mismatches, and failed models that need re-execution
- Supports model selection with dbt syntax (`--model`, `--select`)
- Provides detailed cache validity status and dependency analysis

**`dt clean`** - Cache management
- Clears all cached data with detailed reporting of removed files
- Shows cache statistics and cleanup results

**`dt settings`** - Configuration inspection
- Shows all settings with their values, sources, and locations
- Color-coded by source type (env vars, TOML, dbt, defaults)

### dbt Integration

- Configured to work with sample dbt project in `tests/dbt_sample_project/`
- Supports dbt macros, models, and documentation
- Cache invalidation based on file changes (macro IDs, project config)
- Dynamic dbt profile and target integration
- Global `--target` option for environment switching

### Testing Setup

- Uses pytest with session-scoped fixtures
- Creates temporary copy of sample dbt project for testing
- Automatic cache clearing between test runs
- Environment variables: `DBT_PROJECT_DIR` and `DBT_TOOLBOX_DEBUG`
- Comprehensive test coverage for caching, parsing, CLI, and graph functionality

## Configuration

### Environment Variables
- `DBT_PROJECT_DIR` - Override dbt project directory
- `DBT_TOOLBOX_DEBUG` - Enable debug logging
- `DBT_TOOLBOX_CACHE_PATH` - Custom cache directory
- `DBT_TOOLBOX_SKIP_PLACEHOLDER` - Skip placeholder descriptions
- `DBT_TOOLBOX_PLACEHOLDER_DESCRIPTION` - Custom placeholder text

### TOML Configuration (`pyproject.toml`)
```toml
[tool.dbt_toolbox]
dbt_project_dir = "tests/dbt_sample_project"
debug = false
cache_path = ".dbt_toolbox"
skip_placeholder = false
placeholder_description = "TODO: PLACEHOLDER"
```

### Settings Precedence
1. Environment variables (highest priority)
2. TOML file configuration
3. dbt profiles.yml (for SQL dialect)
4. Default values (lowest priority)

## General instructions

- Always run `ruff check --fix` after every finished implementation.
- When running tests, use `uv run pytest -x`