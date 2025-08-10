"""Simple color terminal printing support."""

from typing import Literal

import typer
from loguru import logger

from dbt_toolbox.settings import settings


def _red(text: str, /) -> str:
    """Print text in red color."""
    return f"\033[31m{text}\033[0m"


def _green(text: str, /) -> str:
    """Print text in green color."""
    return f"\033[32m{text}\033[0m"


def _yellow(text: str, /) -> str:
    """Print text in yellow color."""
    return f"\033[33m{text}\033[0m"


def _cyan(text: str, /) -> str:
    """Print text in cyan color."""
    return f"\033[36m{text}\033[0m"


def _bright_black(text: str, /) -> str:
    """Print text in bright black color (gray)."""
    return f"\033[90m{text}\033[0m"


def cprint(
    *texts,  # noqa: ANN002
    highlight_idx: int = -1,
    color: Literal["red", "green", "yellow", "nocolor", "cyan", "bright_black"] = "nocolor",
) -> None:
    """Print text and highlight specific sgement."""
    colored_texts = []
    for i, t in enumerate(texts):
        if i == highlight_idx:
            colored_texts.append(_cyan(t))
        else:
            colored_texts.append(
                _red(t)
                if color == "red"
                else _green(t)
                if color == "green"
                else _yellow(t)
                if color == "yellow"
                else _cyan(t)
                if color == "cyan"
                else _bright_black(t)
                if color == "bright_black"
                else t,
            )
    typer.echo(" ".join(colored_texts), color=True)


def log(msg: str, level: Literal["INFO", "DEBUG", "WARN"] = "DEBUG") -> None:
    """Log a message at the specified level.

    Args:
        msg: Message to log.
        level:  Log level (INFO, DEBUG, WARN). DEBUG messages only show
                when debug mode is enabled in settings.

    """
    if settings.debug and level == "DEBUG":
        logger.debug(msg)
    elif level == "INFO":
        logger.info(msg)
    elif level == "WARN":
        logger.warning(msg)
