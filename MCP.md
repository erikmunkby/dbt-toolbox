# MCP Server Integration

dbt-toolbox provides an [MCP (Model Context Protocol)](https://www.anthropic.com/news/model-context-protocol) server implementation, enabling integration with AI assistants and external tools. This allows you to leverage dbt-toolbox's intelligent caching, dependency analysis, and validation capabilities from within other applications.

## 🚀 Quick Start

### Installation

```bash
# Install dbt-toolbox with MCP support
uv add "dbt-toolbox[mcp]"

# Or with pip
pip install "dbt-toolbox[mcp]"
```

**Claude CLI**

Set it up in claude by running:
```bash
claude mcp add dbt-toolbox -- uv run dt start-mcp-server
```

**VSCode Copilot**

Set it up in Copilot by adding the following to your `.vscode/mcp.json` and click "start":
```json
{
    "servers": {
        "dbt-toolbox": {
            "type": "stdio",
            "command": "uv",
            "args": [
                "run",
                "dt",
                "start-mcp-server",
            ]
        }
    }
}
```



### Basic Usage

The MCP server exposes dbt-toolbox functionality through standardized MCP tools that can be called by MCP clients like Claude Code, Copilot, and other AI development tools.

```bash
# The server is accessible via the FastMCP app
# Location: dbt_toolbox.mcp.mcp:app
```

## 🛠️ Available Tools

### `analyze_models()`

Validates all model references, column references, and CTE references in your dbt project.

**Purpose:** Ensures data lineage integrity and catches broken references before execution.

**Returns:** JSON object with validation results

**Key Features:**
- Validates column existence across model dependencies
- Checks CTE reference integrity
- Identifies non-existent table references
- Respects validation ignore lists from configuration

### `build_models()`

Executes dbt build with intelligent cache-based execution and enhanced capabilities.

**Parameters:**
- `model` (str, optional): Select models to build (dbt selection syntax)
- `full_refresh` (bool, default=False): Drop incremental models and rebuild
- `threads` (int, optional): Number of threads to use
- `vars` (str, optional): Supply variables to the project (YAML string)
- `target` (str, optional): Specify dbt target environment
- `analyze_only` (bool, default=False): Only analyze which models need execution
- `disable_smart` (bool, default=False): Disable intelligent execution

**Returns:** JSON object with execution results and performance metrics

**Smart Execution Features:**
- **Cache Analysis:** Only rebuilds models with outdated cache or dependency changes
- **Lineage Validation:** Validates column and model references before execution
- **Performance Tracking:** Reports time saved by skipping unnecessary model executions
- **Optimized Selection:** Automatically filters to models that need execution

## 🔧 Configuration

The MCP server respects the same configuration system as the CLI commands:

### Configuration Sources (by precedence):
1. **Environment Variables** (highest priority)
2. **TOML Configuration** (`pyproject.toml`)
3. **dbt Profiles** (for SQL dialect and connection)
4. **Auto-detection** (for project paths)
5. **Defaults** (lowest priority)

### Key Settings for MCP Usage

```toml
[tool.dbt_toolbox]
# Core settings
dbt_project_dir = "path/to/your/dbt/project"
cache_path = ".dbt_toolbox"
cache_validity_minutes = 1440  # 24 hours

# Validation settings
enforce_lineage_validation = true
models_ignore_validation = ["legacy_model", "temp_model"]

# Performance settings  
debug = false
```

### Environment Variables

```bash
# Project configuration
export DBT_PROJECT_DIR="/path/to/dbt/project"
export DBT_PROFILES_DIR="/path/to/profiles"

# dbt-toolbox specific
export DBT_TOOLBOX_ENFORCE_LINEAGE_VALIDATION=true
export DBT_TOOLBOX_MODELS_IGNORE_VALIDATION="legacy_model,temp_model"
export DBT_TOOLBOX_CACHE_VALIDITY_MINUTES=1440
export DBT_TOOLBOX_DEBUG=false
```

## 🏗️ Integration Patterns

### AI Assistant Integration

Perfect for AI-powered dbt development workflows:

1. **Pre-execution Validation:** Always run `analyze_models()` before making changes
2. **Intelligent Builds:** Use `build_models()` with smart execution for faster iterations  
3. **Impact Analysis:** Use selection syntax like `+model+` to understand dependencies
4. **Performance Monitoring:** Track skipped models and time savings

### Development Tools Integration

Enable dbt-toolbox in your development environment:

1. **IDE Extensions:** MCP-compatible editors can call tools directly
2. **CLI Wrappers:** Create custom scripts that leverage MCP tools
3. **Notebooks:** Use MCP tools in Jupyter notebooks for exploratory analysis

## 🔍 Troubleshooting

### Common Issues

**Connection Errors:**
- Ensure dbt profiles are configured correctly
- Check `DBT_PROFILES_DIR` environment variable
- Verify target exists in profiles.yml

**Validation Failures:**
- Review models listed in validation results
- Add problematic models to `models_ignore_validation`
- Check for missing sources or macros

**Performance Issues:**
- Increase `cache_validity_minutes` for longer cache retention
- Use targeted model selection instead of full project builds
- Check `debug=true` to understand cache behavior

## 📊 Benefits

### Development Efficiency
- **Faster Iterations:** Smart caching eliminates unnecessary model rebuilds
- **Early Error Detection:** Validation catches issues before expensive builds
- **Performance Insights:** Clear metrics on time saved and models skipped

### Code Quality
- **Lineage Validation:** Ensures data integrity across model dependencies
- **Reference Checking:** Catches broken table and column references
- **CTE Validation:** Validates complex SQL patterns and Common Table Expressions

### Integration Flexibility
- **Standardized Protocol:** MCP enables consistent tool integration
- **AI-Powered Workflows:** Perfect for AI assistant development patterns
- **Configuration Consistency:** Same settings as CLI commands

## 🤝 Contributing

The MCP server is part of the main dbt-toolbox codebase. See the main [Contributing Guide](https://github.com/erikmunkby/dbt-toolbox/blob/main/CONTRIBUTING.md) for development setup and guidelines.

### MCP-Specific Development

When working on MCP functionality:

1. **Consistency:** Ensure MCP tools provide same functionality as CLI commands
2. **Error Handling:** Return structured JSON responses for all scenarios
3. **Documentation:** Keep tool docstrings comprehensive for AI assistants
4. **Testing:** Test both CLI and MCP interfaces for feature parity

## 📚 Additional Resources

- [Model Context Protocol Specification](https://www.anthropic.com/news/model-context-protocol)
- [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- [dbt-toolbox CLI Reference](https://github.com/erikmunkby/dbt-toolbox/blob/main/CLI.md)
- [Main Documentation](https://github.com/erikmunkby/dbt-toolbox/blob/main/README.md)