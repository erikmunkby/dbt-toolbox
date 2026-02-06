# CLAUDE.md

## What is dbt-toolbox?

An ultra-fast drop-in replacement for dbt executions with:
- **Smart caching** - Avoids expensive warehouse reruns by tracking what actually changed
- **Continuous analysis** - Validates column references, model dependencies, and CTE usage before execution
- **Documentation automation** - Generates YAML docs with column inheritance from doc macros & upstream models
- **MCP server** - Enables all functionality for LLM integrations

## Tech Stack

- Python 3.10+, uv for dependencies
- Typer (CLI), FastMCP (MCP server), SQLGlot (SQL parsing)
- CLI entry point: `dt`

## Essential Commands

```bash
# Development
make test                 # Run tests
make fix                  # Format + lint (run after changes)

# CLI testing
dt build --model <name>   # Smart build with caching
dt analyze                # Show what needs execution
dt docs -m <name>         # Generate YAML docs
dt settings               # Show configuration
```

## Project Structure

```
dbt_toolbox/
├── cli/           # CLI commands
├── dbt_parser/    # Core parsing, caching, SQLGlot
├── actions/       # Analysis & execution logic
├── mcp/           # MCP server
├── _context.py    # Lightweight runtime global context (CLI/MCP mode, current command)
└── data_models.py # Core dataclasses
```

## Development Guidelines

- Test first: Write minimal test, then minimal code to pass
- Keep it simple: Only add what's explicitly needed
- Update docs: README.md (users), CLI.md (CLI reference), CONTRIBUTING.md (contributors)

## Progressive Disclosure

Read these when relevant to your task:
- `docs/development.md` - Architecture details, key classes, import patterns
- `CLI.md` - Complete CLI reference with examples
- `CONTRIBUTING.md` - Full development setup and workflows
- `tests/dbt_sample_project/` - Sample dbt project for testing
- `tests/` - Example dbt logs for parsing work
