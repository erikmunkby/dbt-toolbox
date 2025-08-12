# CLI Reference Guide

Complete documentation for all dbt-toolbox CLI commands with detailed examples and use cases.

## Table of Contents

<details>
<summary>Table of Contents</summary>

- [Global Options](#global-options)
- [Commands Overview](#commands-overview)
- [`dt build`](#dt-build) - Enhanced dbt build with intelligent caching
- [`dt run`](#dt-run) - Enhanced dbt run with intelligent caching
- [`dt docs`](#dt-docs) - YAML documentation generation
- [`dt analyze`](#dt-analyze) - Cache state analysis
- [`dt clean`](#dt-clean) - Cache management
- [`dt settings`](#dt-settings) - Configuration inspection
- [Configuration](#configuration)
- [Examples & Workflows](#examples--workflows)

</details>

## Global Options

All commands support the `--target` (`-t`) option to specify which dbt target to use:

```bash
dt build --target prod
dt docs --model customers --target staging
```

## Commands Overview

| Command | Purpose | Key Features |
|---------|---------|--------------|
| `build` | Enhanced dbt build | Smart caching, lineage validation, intelligent execution, analysis mode, optimized selection |
| `run` | Enhanced dbt run | Smart caching, lineage validation, intelligent execution, analysis mode, optimized selection |
| `docs` | YAML generation | Column inheritance from upstream models, change detection, clipboard support |
| `analyze` | Cache analysis | Non-destructive inspection, cache validity status, dependency analysis |
| `clean` | Cache management | Complete cache clearing with detailed file reporting |
| `settings` | Configuration | Multi-source inspection with precedence and location tracking |

---

## `dt build`

Enhanced dbt build command with intelligent caching that analyzes which models need execution based on cache validity, validates lineage references, and only runs models that have changed or are invalidated. Dramatically reduces build times while maintaining dependency relationships.

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--model` | `-m`, `-s` | Select specific models to build |
| `--target` | `-t` | Which target to load for the given profile |
| `--analyze` | | Show execution analysis without running |
| `--disable-smart` | | Disable intelligent execution and lineage validation |

Plus standard dbt options: `--full-refresh`, `--threads`, `--vars`, etc.

### Example

```bash
# Smart execution - only runs models that need updating
dt build --model customers+

# Show analysis without execution
dt build --analyze
```

### Output Example

```
🔨 Building models: customers+
📊 Execution Analysis:
✅ customers - Cache valid, no execution needed
🔥 customer_orders - Dependencies changed, needs execution  
🔥 customer_metrics - Downstream of changes, needs execution

🎯 Optimized selection: customer_orders customer_metrics
🚀 Executing: dbt build --select customer_orders customer_metrics
```

---

## `dt run`

Enhanced dbt run command with intelligent caching and lineage validation. Same features as `dt build` but runs models without tests. Supports all the same options and smart execution behavior.

```bash
# Smart execution - only runs models that need updating
dt run --model customers+

# Show analysis without execution
dt run --analyze
```

---

## `dt docs`

Generate YAML documentation for dbt models with column inheritance from upstream models, macro docs integration, and change detection.

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--model` | `-m` | Model name to generate docs for (required) |
| `--clipboard` | `-c` | Copy output to clipboard instead of updating file |
| `--target` | `-t` | Which target to load for the given profile |

### Example

```bash
# Generate docs for customers model (updates schema.yml)
dt docs --model customers

# Copy to clipboard
dt docs --model customers --clipboard
```

### Output Example

```
✅ updated model customers
   Added columns: email, phone_number
   Removed columns: old_field
   Column order changed
```

The docs command preserves existing documentation, inherits descriptions from upstream models, and references `{{ doc('column_name') }}` macros when available.

---

## `dt analyze`

Analyze cache state and model dependencies without executing any models. Shows which models need execution and why, with support for dbt model selection syntax.

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--model` | `-m`, `-s`, `--select` | Analyze specific models (dbt selection syntax) |
| `--target` | `-t` | Which target to load for the given profile |

### Example

```bash
# Analyze all models
dt analyze

# Analyze specific model and dependencies
dt analyze --model customers+
```

### Output Example

```
🔍 Cache Analysis Results
Total models analyzed: 15
Models needing execution: 4

❌ Failed Models (1):
┌─────────────────┬────────────────────────────────────────────┐
│ Model           │ Issue                                      │
├─────────────────┼────────────────────────────────────────────┤
│ customer_orders │ Model failed in last execution             │
└─────────────────┴────────────────────────────────────────────┘

🔄 Modified Models (2):
┌──────────────────┬─────────────────────────────────────────────┐
│ Model            │ Issue                                       │
├──────────────────┼─────────────────────────────────────────────┤
│ customers        │ Model code has been modified since last     │
│                  │ cache                                       │
└──────────────────┴─────────────────────────────────────────────┘

✅ Valid Models (11):
   • raw_customers - Last updated: 15 minutes ago
   • raw_orders - Last updated: 15 minutes ago
   ...

💡 Tip: Run 'dt build' to execute the 4 models that need updates.
```

---

## `dt clean`

Clear all cached data including models, macros, Jinja environments, and dependency graphs. Can also clean specific models from cache.

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--models` | `-m` | Specific models to clean from cache (comma-separated) |
| `--target` | `-t` | Which target to load for the given profile |

### Examples

```bash
# Clean entire cache
dt clean

# Clean specific models from cache
dt clean --models customers,orders

# Clean with specific target
dt clean --target prod
```

### Output Example

```
🧹 Cache cleaned successfully!
Removed 8 cache files:
  • models.cache
  • macros.cache  
  • jinja_env.cache
  • dependency_graph.cache
  ...
```

---

## `dt settings`

Inspect configuration from all sources (environment variables, TOML files, dbt profiles, defaults) with precedence and location tracking.

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--target` | `-t` | Which target to load for the given profile |

### Example

```bash
dt settings
dt settings --target prod
```

### Output Example

```
dbt-toolbox Settings:
==================================================

dbt_project_dir:
  value: /Users/dev/my-dbt-project
  source: environment variable
  location: DBT_PROJECT_DIR

debug:
  value: false
  source: TOML file  
  location: pyproject.toml

cache_path:
  value: .dbt_toolbox
  source: default
```

---

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DBT_PROJECT_DIR` | Override dbt project directory | `/path/to/dbt/project` |
| `DBT_PROFILES_DIR` | Custom dbt profiles directory | `/path/to/profiles` |
| `DBT_TOOLBOX_DEBUG` | Enable debug logging | `true` |
| `DBT_TOOLBOX_CACHE_PATH` | Custom cache directory | `.cache` |
| `DBT_TOOLBOX_SKIP_PLACEHOLDER` | Skip placeholder descriptions | `true` |
| `DBT_TOOLBOX_PLACEHOLDER_DESCRIPTION` | Custom placeholder text | `"TODO: Add docs"` |
| `DBT_TOOLBOX_CACHE_VALIDITY_MINUTES` | Cache validity in minutes | `720` |
| `DBT_TOOLBOX_ENFORCE_LINEAGE_VALIDATION` | Enable/disable lineage validation | `true` |
| `DBT_TOOLBOX_MODELS_IGNORE_VALIDATION` | Models to ignore during validation | `"legacy_model,temp"` |

### TOML Configuration

Add to `pyproject.toml`:

```toml
[tool.dbt_toolbox]
dbt_project_dir = "path/to/dbt/project"
dbt_profiles_dir = "path/to/profiles"
debug = false
cache_path = ".dbt_toolbox"  
skip_placeholder = false
placeholder_description = "TODO: PLACEHOLDER"
cache_validity_minutes = 1440
enforce_lineage_validation = true
models_ignore_validation = ["legacy_model", "staging_temp"]
```

### dbt Profile Integration

dbt-toolbox automatically reads your `~/.dbt/profiles.yml` for:
- SQL dialect configuration
- Target-specific settings
- Profile and target names

---

## Examples & Workflows

### Daily Development

```bash
# Check what needs execution
dt analyze

# Smart execution during development
dt build --model my_model
dt docs --model my_model --clipboard
```

### Troubleshooting

```bash
# Check configuration and clear cache
dt settings
dt clean

# Force execution bypassing cache
dt build --model problematic_model --disable-smart
```