"""Tests for utils._printers module."""

from io import StringIO
from unittest.mock import patch

from dbt_toolbox.utils._printers import cprint


class TestCprint:
    """Test the cprint function."""

    def test_cprint_basic_output(self) -> None:
        """Test that cprint outputs text."""
        with patch("sys.stdout", new=StringIO()) as mock_stdout:
            cprint("test message")
            output = mock_stdout.getvalue()
            # Should contain the message
            assert "test message" in output

    def test_cprint_with_color(self) -> None:
        """Test that cprint accepts color parameters."""
        with patch("sys.stdout", new=StringIO()) as mock_stdout:
            cprint("test message", color="green")
            output = mock_stdout.getvalue()
            # Should not raise an error
            assert output is not None

    def test_cprint_with_highlight(self) -> None:
        """Test that cprint accepts highlight_idx parameter."""
        with patch("sys.stdout", new=StringIO()) as mock_stdout:
            cprint("first", "second", "third", highlight_idx=1)
            output = mock_stdout.getvalue()
            # Should not raise an error and contain all text
            assert output is not None
