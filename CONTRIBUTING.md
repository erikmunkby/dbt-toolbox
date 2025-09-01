# Contributing to dbt-toolbox

Thank you for your interest in contributing to dbt-toolbox! This guide will help you get started with development setup, coding standards, and contribution workflows.

## 🚀 Quick Development Setup

### Prerequisites
- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) for dependency management
- Git

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/your-org/dbt-toolbox
cd dbt-toolbox

# Install dependencies (including dev dependencies)
uv sync --group dev

# Verify installation
dt --help

# Run tests to ensure everything works
uv run pytest
```

### Development Environment

```bash
# Install in editable mode for development
uv sync --group dev

# Run the CLI locally
dt --help

# Clear cache during development
dt clean
```


## 🧪 Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_cacher.py

# Run with verbose output
uv run pytest -v

# Run tests with coverage and stop at first failure (recommended during development)
uv run pytest -x

# Run tests with coverage
uv run pytest --cov=dbt_toolbox
```

### Test Structure

- `tests/conftest.py` - Shared fixtures and test configuration  
- `tests/dbt_sample_project/` - Sample dbt project for testing
- Session-scoped fixtures create temporary project copies
- Automatic cache clearing between test runs

### Environment Variables for Testing

```bash
export DBT_PROJECT_DIR="tests/dbt_sample_project"
export DBT_TOOLBOX_DEBUG=true
```

### Writing Tests

When adding new features, ensure you:

1. Add unit tests for core functionality
2. Add integration tests for CLI commands
3. Test caching behavior and invalidation
4. Test error handling and edge cases
5. Use existing fixtures for dbt project setup

## 🎨 Code Style and Standards

### Code Formatting

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Check code style
uv run ruff check

# Auto-fix issues
uv run ruff check --fix

# Format code
uv run ruff format

# Run all quality checks (recommended)
make fix
```

### Code Quality Requirements

- **Type Hints**: All functions must have proper type annotations
- **Docstrings**: Public functions and classes require docstrings
- **Error Handling**: Proper exception handling with meaningful messages
- **Performance**: Consider caching and performance implications
- **Testing**: New features require comprehensive tests

### Configuration

Our Ruff configuration in `pyproject.toml`:
- Line length: 99 characters
- Comprehensive rule set with specific ignores for practical development
- Separate rules for test files (more lenient)

## 📝 Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types
- `feat`: New feature
- `fix`: Bug fix  
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### Examples
```bash
feat(cli): add --clipboard option to docs command
fix(cache): resolve cache invalidation for macro changes  
docs: update README with new CLI examples
test(parser): add tests for Jinja template parsing
```

## 🔄 Pull Request Process

### Before Submitting

1. **Run the full test suite**: `uv run pytest -x`
2. **Run code quality checks**: `make fix`
3. **Update documentation** if needed (README.md, CLI.md, CONTRIBUTING.md)
4. **Add tests** for new functionality
5. **Update CHANGELOG** for user-facing changes

### Pull Request Template

When creating a PR, include:

- **Description**: What does this PR do and why?
- **Testing**: How was this tested?
- **Breaking Changes**: Any breaking changes?
- **Documentation**: Documentation updates needed?

### Review Process

- PRs require at least one approving review
- All tests must pass
- Code quality checks must pass
- Documentation must be updated for user-facing changes

## 🏷️ Release Process

### Versioning

We follow [Semantic Versioning](https://semver.org/):
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)  
- **PATCH**: Bug fixes (backward compatible)

### Release Checklist

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` with release notes
3. Create git tag: `git tag v0.1.0`
4. Push tag: `git push origin v0.1.0` 
5. Create GitHub release with changelog

## 🐛 Bug Reports and Feature Requests

### Bug Reports

When reporting bugs, include:
- dbt-toolbox version
- Python version  
- Operating system
- Minimal reproduction case
- Expected vs actual behavior
- Relevant log output

### Feature Requests

For feature requests, describe:
- Use case and motivation
- Proposed solution (if any)
- Alternatives considered
- Potential impact on existing functionality

## 📋 Development Workflow

### Typical Development Cycle

```bash
# 1. Create feature branch
git checkout -b feat/amazing-feature

# 2. Make changes and test locally
dt --help  # Test CLI changes
uv run pytest -x    # Run tests

# 3. Ensure code quality
make fix

# 4. Commit with conventional commit message
git commit -m "feat(cli): add amazing new feature"

# 5. Push and create PR
git push origin feat/amazing-feature
```

### Development Patterns

**When adding new CLI commands:**
1. Add `target: str | None = Target` parameter to command function
2. Create `dbt_parser = dbtParser(target=target)` instance
3. Pass dbt_parser instance to functions that need it
4. Use `from dbt_toolbox.cli._common_options import Target` for target type

**When writing tests:**
1. Mock `dbtParser` constructor calls: `@patch("module.dbtParser")`
2. For execute_dbt_command tests: Access `kwargs["base_command"]` instead of positional args
3. For analyze_model_statuses tests: Use `AnalysisResult` with proper `ExecutionReason` enum values
4. Always provide mock dbt_parser instances to functions that require them

**When working with utilities:**
- Use `from dbt_toolbox.utils import _printers`
- Use `_printers.cprint()` for colored console output
- Use `from dbt_toolbox.utils._paths import build_path` for path utilities

### Local Testing Tips

```bash
# Test against different dbt projects
export DBT_PROJECT_DIR="/path/to/your/dbt/project" 
dt docs --model my_model

# Test with different targets
dt build --model my_model --target prod

# Enable debug logging
export DBT_TOOLBOX_DEBUG=true
dt build --model my_model

# Test lineage validation settings
export DBT_TOOLBOX_ENFORCE_LINEAGE_VALIDATION=false
export DBT_TOOLBOX_MODELS_IGNORE_VALIDATION="legacy_model,temp"

# Clear cache during development
dt clean
```

## 🤝 Code of Conduct

### Our Standards

- **Be Respectful**: Treat everyone with respect and kindness
- **Be Collaborative**: Work together towards common goals
- **Be Inclusive**: Welcome contributions from people of all backgrounds
- **Be Constructive**: Provide helpful feedback and suggestions
- **Be Patient**: Remember that everyone has different experience levels

### Unacceptable Behavior

- Harassment, discrimination, or inappropriate comments
- Personal attacks or inflammatory language
- Spam or off-topic discussions
- Publishing private information without consent

## 📚 Additional Resources

**dbt & Data Tools:**
- [dbt Documentation](https://docs.getdbt.com/) - Core dbt concepts and usage
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices) - dbt modeling guidelines

**Python Libraries:**
- [Typer Documentation](https://typer.tiangolo.com/) - CLI framework used for commands
- [SQLGlot Documentation](https://sqlglot.com/) - SQL parsing and transformation
- [yamlium Documentation](https://github.com/erikmunkby/yamlium) - YAML manipulation library
- [Jinja2 Documentation](https://jinja.palletsprojects.com/) - Template engine for dbt

**Development Tools:**
- [uv Documentation](https://docs.astral.sh/uv/) - Python package manager
- [Pytest Documentation](https://docs.pytest.org/) - Testing framework
- [Ruff Documentation](https://docs.astral.sh/ruff/) - Linting and formatting

**Python Development:**
- [Python Type Hints](https://docs.python.org/3/library/typing.html) - Type annotation reference
- [Pydantic Documentation](https://docs.pydantic.dev/) - Data validation library patterns

## 💬 Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Code Review**: Tag maintainers for review questions

Thank you for contributing to dbt-toolbox! 🎉