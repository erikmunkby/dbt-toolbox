"""Tests for the clean command."""

from typer.testing import CliRunner

from dbt_toolbox.cli.main import app


class TestCleanCommand:
    """Test the dt clean command."""

    def test_clean_command_exists(self) -> None:
        """Test that the clean command is registered."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "clean" in result.stdout

    def test_clean_command_help(self) -> None:
        """Test that the clean command shows help."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["clean", "--help"])

        assert result.exit_code == 0
        assert "clean" in result.stdout.lower()

    def test_clean_executes_successfully(self) -> None:
        """Test that clean command executes without error."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["clean"])

        # Should succeed
        assert result.exit_code == 0

    def test_clean_with_models_option(self) -> None:
        """Test clean command with --models option."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["clean", "--models", "customers,orders"])

        # Should execute without error
        assert result.exit_code == 0

    def test_clean_with_target(self) -> None:
        """Test clean command with --target option."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(app, ["clean", "--target", "dev"])

        # Should execute without error
        assert result.exit_code == 0
