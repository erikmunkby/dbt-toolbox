"""Tests for the --verbose CLI flag."""

import typer.testing

from dbt_toolbox import _context
from dbt_toolbox.cli.main import app


class TestVerboseFlag:
    """Tests for the global --verbose / -v flag."""

    def setup_method(self) -> None:
        """Reset verbose state before each test."""
        _context._verbose = False

    def test_verbose_flag_sets_verbose(self) -> None:
        """Test that --verbose sets verbose mode in _context."""
        runner = typer.testing.CliRunner()
        result = runner.invoke(app, ["--verbose"])
        assert result.exit_code == 0
        assert _context.is_verbose()

    def test_short_verbose_flag_sets_verbose(self) -> None:
        """Test that -v sets verbose mode in _context."""
        runner = typer.testing.CliRunner()
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert _context.is_verbose()

    def test_no_verbose_flag_does_not_set_verbose(self) -> None:
        """Test that verbose mode is not set without the flag."""
        runner = typer.testing.CliRunner()
        runner.invoke(app)
        assert not _context.is_verbose()
