"""Shared logging setup for the Kafka pipeline modules.

One-line, timestamped, color-coded by component when stderr is a tty.
Falls back to plain text when piped into a file (which is what the
dev stack script does), so the on-disk logs stay grep-friendly.
"""

from __future__ import annotations

import logging
import os
import sys

_COLOURS = {
    "DEBUG": "\033[2;37m",
    "INFO": "\033[0;36m",
    "WARNING": "\033[0;33m",
    "ERROR": "\033[0;31m",
    "CRITICAL": "\033[1;31m",
}
_RESET = "\033[0m"


class _ColourFormatter(logging.Formatter):
    def __init__(self, use_colour: bool) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)-5s %(name)s :: %(message)s",
            datefmt="%H:%M:%S",
        )
        self._use_colour = use_colour

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        if self._use_colour:
            colour = _COLOURS.get(record.levelname, "")
            return f"{colour}{line}{_RESET}"
        return line


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_ColourFormatter(use_colour=sys.stderr.isatty()))
    logger.addHandler(handler)
    logger.propagate = False
    return logger
