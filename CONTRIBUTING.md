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
pytest
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

## 🏗️ Architecture Overview

### Core Components

The project is organized into several key modules:

```
dbt_toolbox/
├── cli/                  # CLI commands and interface
│   ├── main.py          # Main CLI app with Typer
│   ├── build.py         # Enhanced dbt build command
│   ├── docs.py          # YAML documentation generation
│   ├── clean.py         # Cache management
│   └── globals.py       # Global state management
├── dbt_parser/          # dbt project parsing and caching
│   ├── dbt_parser.py    # Main parsing interface
│   ├── _cache.py        # Caching implementation
│   ├── _file_fetcher.py # File system operations
│   └── _jinja_handler.py# Jinja environment management
├── graph/               # Dependency graph implementation
│   └── dependency_graph.py # Lightweight DAG
├── testing/             # Testing utilities for users
│   └── column_tests.py  # Documentation test helpers
├── utils/               # Shared utilities
│   ├── printer.py       # Enhanced console output
│   └── utils.py         # General utilities
├── data_models.py       # Pydantic data models
├── settings.py          # Configuration management
└── constants.py         # Project constants
```

### Key Design Patterns

1. **Caching Strategy**: Uses pickle serialization for parsed models, macros, and Jinja environments
2. **Dependency Tracking**: Lightweight DAG with efficient upstream/downstream traversal  
3. **Configuration Hierarchy**: Multi-source settings with precedence tracking
4. **CLI Design**: Typer-based with global options and command shadowing
5. **SQL Processing**: SQLGlot for parsing and optimization

### Development Principles

- **Performance First**: Intelligent caching at every layer
- **User Experience**: Enhanced output, colors, and clear error messages  
- **dbt Integration**: Seamless integration with existing dbt workflows
- **Testability**: Comprehensive test coverage with session-scoped fixtures
- **Configuration**: Flexible, hierarchical configuration system

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_cacher.py

# Run with verbose output
pytest -v

# Run tests with coverage
pytest --cov=dbt_toolbox
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
ruff check

# Auto-fix issues
ruff check --fix

# Format code
ruff format
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

1. **Run the full test suite**: `pytest`
2. **Run code quality checks**: `ruff check --fix`
3. **Update documentation** if needed
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
pytest    # Run tests

# 3. Ensure code quality
ruff check --fix
ruff format

# 4. Commit with conventional commit message
git commit -m "feat(cli): add amazing new feature"

# 5. Push and create PR
git push origin feat/amazing-feature
```

### Local Testing Tips

```bash
# Test against different dbt projects
export DBT_PROJECT_DIR="/path/to/your/dbt/project" 
dt docs --model my_model

# Enable debug logging
export DBT_TOOLBOX_DEBUG=true
dt build --model my_model

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