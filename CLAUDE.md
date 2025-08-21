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

### MCP Server Usage
- Server can be started as MCP server for external tool integration
- Provides `analyze_models` and `build_models` tools
- Respects same configuration as CLI commands

## Architecture Overview

### Core Components

**CLI System (`dbt_toolbox/cli/`)**
- `main.py` - Main CLI application with Typer, global options, and settings command
- `_build_run_command_factory.py` - Shared command factory for build and run commands with smart execution
- `build.py` - Shadows `dbt build` with intelligent execution and enhanced output
- `run.py` - Shadows `dbt run` with smart cache-based execution
- `_common_options.py` - Shared CLI options and types (Target, etc.)
- `_dbt_output_parser.py` - Parses dbt command output for execution results
- `analyze.py` - Cache state analysis command implementation
- `clean.py` - Cache management and cleanup functionality
- `docs.py` - YAML documentation generation with YamlBuilder class

**dbt Parser System (`dbt_toolbox/dbt_parser/`)**
- `_dbt_parser.py` - Main parsing interface and model/macro management with dbtParser class
- `_cache.py` - Caching implementation with pickle-based persistence
- `_file_fetcher.py` - Fetches macros and models from filesystem
- `_jinja_handler.py` - Handles Jinja environment and template rendering
- `_builders.py` - Model and macro building logic with SQLGlot parsing
- `_column_resolver.py` - SQL column resolution and dependency analysis

**Dependency Graph (`dbt_toolbox/graph/`)**
- `dependency_graph.py` - Lightweight DAG implementation for tracking model and macro dependencies
- Supports upstream/downstream traversal, node type tracking, and statistics

**Analysis System (`dbt_toolbox/analysees/`)**
- `analyze_models.py` - Model execution analysis logic with ExecutionReason and AnalysisResult classes
- `analyze_columns_references.py` - Column lineage analysis and validation logic with ColumnIssue, CTEIssue, ModelAnalysisResult
- `dbt_executor.py` - Core dbt execution engine with ExecutionPlan, DbtCommandResult, and DbtExecutionResults

**MCP Server (`dbt_toolbox/mcp/`)**
- `mcp.py` - FastMCP server implementation with analyze_models and build_models tools for external integration

**Data Models (`data_models.py`)**
- `RawMacro` - Represents raw macro files with metadata
- `Model` - Complete model with rendered code and SQL parsing
- `ModelBase` - Base model structure
- `DependsOn` - Tracks model dependencies (refs, sources, macros)
- `YamlDocs` - Documentation from schema.yml files
- `ColumnChanges` - Tracks column additions, removals, and reordering
- `DbtExecutionParams` - Parameters for dbt execution commands (build, run, etc.)

**Configuration System**
- `settings.py` - Advanced settings management with source tracking and Setting class
- `run_config.py` - Runtime configuration handling with RunConfig class for target management
- `data_models.py` - Core data models including DbtProfile for profile management
- Supports environment variables, TOML files, and dbt profile integration
- Settings precedence: env vars > TOML > dbt profiles > defaults
- Dynamic dbt profile loading with target configuration

**Testing Module (`dbt_toolbox/testing/`)**
This module is NOT tests for the project itself, but helper functions for users of `dbt-toolbox` to implement tests themselves.
- Provides `check_column_documentation()` function for pytest integration

**Utilities (`utils/`)**
- `_printers.py` - Enhanced console output with colors and highlighting (replaces old printer.py)
- `_paths.py` - Path utility functions and build_path helper

### Key Design Patterns

1. **Intelligent Execution**: Smart cache-based execution that analyzes which models need rebuilding
2. **Shared Command Infrastructure**: Common dbt execution logic via `_build_run_command_factory.py` and `dbt_executor.py`
3. **Instance-based Parser**: dbtParser is instantiated with target parameter for better isolation
4. **Caching Strategy**: Uses pickle serialization for caching parsed models, macros, and Jinja environments
5. **Dependency Tracking**: Lightweight DAG with efficient upstream/downstream traversal
6. **SQL Processing**: Uses SQLGlot for parsing and optimizing SQL queries with advanced column resolution
7. **CLI Design**: Typer-based with global options, command shadowing, and enhanced UX
8. **Configuration Hierarchy**: Multi-source settings with precedence and source tracking
9. **Lineage Validation**: Optional column and model reference validation before execution
10. **Target Management**: Runtime configuration with support for dbt target environments
11. **MCP Integration**: FastMCP server for external tool integration with analyze and build capabilities
12. **Modular Analysis**: Separated analysis logic into dedicated modules for maintainability

### CLI Commands

**`dt build`** - Enhanced dbt build wrapper with intelligent execution
- Shadows `dbt build` with smart cache-based execution by default
- **Smart mode features**:
  - Analyzes which models need execution based on cache validity and dependency changes
  - **Lineage validation**: Validates column and model references before execution (configurable)
  - Only executes models that actually need to be rebuilt
  - Tracks execution times and models skipped for performance insights
- Supports common dbt options: `--model`, `--select`, `--full-refresh`, `--threads`, `--target`, `--vars`
- Special options: `--analyze` (show analysis only), `--disable-smart` (force all models, skip lineage validation)
- Enhanced output with colored progress indicators and execution analysis

**`dt run`** - Enhanced dbt run wrapper with intelligent execution
- Shadows `dbt run` with smart cache-based execution by default
- **Smart mode features**:
  - Analyzes which models need execution based on cache validity and dependency changes
  - **Lineage validation**: Validates column and model references before execution (configurable)
  - Only executes models that actually need to be rebuilt
  - Tracks execution times and models skipped for performance insights
- Supports common dbt options: `--model`, `--select`, `--full-refresh`, `--threads`, `--target`, `--vars`
- Special options: `--analyze` (show analysis only), `--disable-smart` (force all models, skip lineage validation)
- Enhanced output with colored progress indicators and execution analysis

**`dt docs`** - YAML documentation generator
- `--model/-m` - Specify model name
- `--clipboard/-c` - Copy output to clipboard
- `--target` - Specify dbt target environment
- Intelligent column description inheritance from upstream models and macros
- Tracks column changes (additions, removals, reordering)

**`dt analyze`** - Cache state analysis
- Analyzes model cache state without manipulating it
- Shows outdated models, ID mismatches, and failed models that need re-execution
- `--model/-m/-s/--select` - Analyze specific models (dbt selection syntax)
- `--target` - Specify dbt target environment
- Provides detailed cache validity status and dependency analysis

**`dt clean`** - Cache management
- Clears all cached data with detailed reporting of removed files
- `--models/-m` - Clean specific models from cache (comma-separated), if not provided cleans entire cache
- `--target` - Specify dbt target environment
- Shows cache statistics and cleanup results

**`dt settings`** - Configuration inspection
- Shows all settings with their values, sources, and locations
- Color-coded by source type (env vars, TOML, dbt, defaults)

### MCP Server Integration

**MCP Server (`dbt-toolbox` as MCP server)**
- Provides FastMCP server implementation for external tool integration
- **Available tools**:
  - `analyze_models()` - Analyze and validate all model references, column references, and CTE references
  - `build_models()` - Build dbt models with intelligent cache-based execution (same functionality as CLI)
- **Usage**: Can be used with MCP-compatible clients like Claude Code for external integration
- **Configuration**: Respects same settings as CLI commands (pyproject.toml, environment variables)
- **Benefits**: Enables dbt-toolbox functionality in external tools and workflows

### dbt Integration

- Configured to work with sample dbt project in `tests/dbt_sample_project/`
- Supports dbt macros, models, and documentation
- Cache invalidation based on file changes (macro IDs, project config)
- Dynamic dbt profile and target integration
- Global `--target` option for environment switching

### Architecture Overview

**dbtParser Instantiation Pattern:**
- `dbtParser(target=target)` class instantiated per command with target parameter
- Benefits: Target isolation, better testability, cleaner dependency management

**Function Signatures:**
- `create_execution_plan(params: DbtExecutionParams)` - creates ExecutionPlan with DbtExecutionParams
- `analyze_model_statuses(dbt_parser: dbtParser, dbt_selection: str | None = None)` - requires dbt_parser instance
- `analyze_column_references(dbt_parser: dbtParser)` - validates column and model references
- All CLI commands accept `target: str | None = Target` parameter

**Key Classes and Enums:**
- `ExecutionReason` enum: `UPSTREAM_MODEL_CHANGED`, `UPSTREAM_MACRO_CHANGED`, `OUTDATED_MODEL`, `LAST_EXECUTION_FAILED`, `CODE_CHANGED`
- `AnalysisResult` dataclass: Contains model analysis results with `needs_execution` property
- `DbtExecutionParams` dataclass: Parameters for dbt execution commands with `to_execute_kwargs()` method
- `ExecutionPlan` class: Manages dbt execution with smart selection and execution tracking
- `DbtCommandResult` and `DbtExecutionResults` classes: Handle execution results and logging
- `ColumnAnalysis`, `ModelAnalysisResult`, `ColumnIssue`, `CTEIssue`: Column validation data structures
- `RunConfig` class: Manages runtime configuration and dbt profile handling
- `Target` type: Common CLI option type for dbt target specification

### Testing Setup

- Uses pytest with session-scoped fixtures
- Creates temporary copy of sample dbt project for testing
- Automatic cache clearing between test runs
- Environment variables: `DBT_PROJECT_DIR` and `DBT_TOOLBOX_DEBUG`
- Comprehensive test coverage for caching, parsing, CLI, and graph functionality


A test dbt project exists within `tests/dbt_sample_project`.
This project is temporarily copied for each test by the `dbt_project_setup` fixture.
This fixture is automatically applied to all tests and do not need to be included in the tests signature.

### MCP Server Setup

**Running as MCP Server:**
- The `dbt_toolbox.mcp.mcp:app` FastMCP application can be used as an MCP server
- Provides external tool integration with dbt-toolbox functionality
- Supports the same configuration system as CLI commands (environment variables, TOML, dbt profiles)

**Available MCP Tools:**
- `analyze_models()` - Validates all model references, column references, and CTE references
- `build_models(model, full_refresh, threads, vars, target, analyze_only, disable_smart)` - Builds models with intelligent execution

**Integration Benefits:**
- Enables dbt-toolbox features in external tools and workflows
- Maintains consistency with CLI functionality
- Provides JSON-structured responses for programmatic consumption

## Configuration

### Environment Variables
**`dbt` variables**
- `DBT_PROJECT_DIR` - Override dbt project directory
- `DBT_PROFILES_DIR` - Custom dbt profiles directory

**`dbt-toolbox` variables**
- `DBT_TOOLBOX_DEBUG` - Enable debug logging
- `DBT_TOOLBOX_CACHE_PATH` - Custom cache directory
- `DBT_TOOLBOX_SKIP_PLACEHOLDER` - Skip placeholder descriptions
- `DBT_TOOLBOX_PLACEHOLDER_DESCRIPTION` - Custom placeholder text
- `DBT_TOOLBOX_CACHE_VALIDITY_MINUTES` - Cache validity in minutes (default: 1440)
- `DBT_TOOLBOX_ENFORCE_LINEAGE_VALIDATION` - Enable/disable lineage validation (default: true)
- `DBT_TOOLBOX_MODELS_IGNORE_VALIDATION` - Comma-separated list of models to ignore during validation

### TOML Configuration (`pyproject.toml`)
```toml
[tool.dbt_toolbox]
dbt_project_dir = "tests/dbt_sample_project"
debug = false
cache_path = ".dbt_toolbox"
dbt_profiles_dir = "path/to/profiles"
skip_placeholder = false
placeholder_description = "TODO: PLACEHOLDER"
cache_validity_minutes = 1440
enforce_lineage_validation = true
models_ignore_validation = ["legacy_model", "staging_temp"]
```

### Settings Precedence
1. Environment variables (highest priority)
2. TOML file configuration
3. Auto-detected values (for dbt_project_dir)
4. dbt profiles.yml (for SQL dialect)
5. Default values (lowest priority)

## General instructions

- After building any implementations, run the following command: `make fix`
- When running tests, use `uv run pytest -x`
- When building complex return statements, instead build a dataclass
- Never run code in notebooks when testing, instead either build tests or run as `python -c`
- When implementing new features always do the following:
  1. Implement a minimal test (if one not already exists)
  2. Implement minimal amount of code for the test to succeed
- Any time we do feature implementations or structural changes, remember to update:
  1. `README.md` regarding any high-level project information and getting started stuff. The `README.md` should primarily be targeted towards users of the tool
  2. `CLI.md` regarding any CLI functionality. Also targeted towards users of the tool
  3. `CONTRIBUTING.md` regarding any development changes for other contributors to know
- For any dbt log parsing purposes, you will find some example logs in tests/
- When working with MCP functionality, ensure consistency between CLI and MCP tool interfaces

## Development Patterns

**When adding new CLI commands:**
1. Add `target: str | None = Target` parameter to command function
2. Create `dbt_parser = dbtParser(target=target)` instance
3. Use `DbtExecutionParams` for standardized parameter handling
4. Use `create_execution_plan()` for dbt command execution
5. Use `from dbt_toolbox.cli._common_options import Target` for target type

**When writing tests:**
1. Mock `dbtParser` constructor calls: `@patch("module.dbtParser")`
2. For dbt execution tests: Use `DbtExecutionParams` and mock `create_execution_plan`
3. For analysis tests: Use `AnalysisResult` with proper `ExecutionReason` enum values
4. Always provide mock dbt_parser instances to functions that require them
5. Use `ColumnAnalysis` and related classes for column validation tests

**When working with utilities:**
- Use `from dbt_toolbox.utils import _printers` for colored console output
- Use `_printers.cprint()` for colored console output
- Use `from dbt_toolbox.utils._paths import build_path` for path utilities
- Use `from dbt_toolbox.utils.dict_utils import remove_empty_values` for clean data structures

**Import patterns:**
- `from dbt_toolbox.dbt_parser._dbt_parser import dbtParser`
- `from dbt_toolbox.actions.analyze_models import AnalysisResult, ExecutionReason`
- `from dbt_toolbox.actions.analyze_columns_references import analyze_column_references, ColumnAnalysis`
- `from dbt_toolbox.actions.dbt_executor import create_execution_plan, DbtExecutionParams`
- `from dbt_toolbox.run_config import RunConfig`
- `from dbt_toolbox.cli._common_options import Target`
- `from dbt_toolbox.data_models import DbtExecutionParams`

## Documentation Guidelines

Documentation should always reflect the current state of the codebase. Never use terms like "post-refactoring", "new:", "old:", or references to past states. Always describe things as they currently are.