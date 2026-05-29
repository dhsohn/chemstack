"""Shared helpers for user-facing CLI error output.

Errors and their recovery hints are written to ``stderr`` (so they stay out of
piped ``stdout`` data and ``--json`` payloads) and the ``error:`` prefix is
tinted red when the stream is a TTY.
"""

from __future__ import annotations

import sys
from typing import Any

from chemstack import cli_style


def emit_error(message: Any, *, hint: str | None = None) -> None:
    """Print ``error: <message>`` to stderr, with an optional ``hint:`` line."""

    prefix = cli_style.paint("error:", cli_style.RED, stream=sys.stderr)
    print(f"{prefix} {message}", file=sys.stderr)
    if hint:
        print(
            cli_style.paint(f"hint: {hint}", cli_style.DIM, stream=sys.stderr),
            file=sys.stderr,
        )


__all__ = ["emit_error"]
