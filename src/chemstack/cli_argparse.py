"""Argument parser that routes usage errors through the shared CLI styling.

``argparse`` writes its own ``prog: error: ...`` line straight to ``stderr``,
bypassing :mod:`chemstack.cli_errors`. This subclass funnels those errors
through :func:`chemstack.cli_errors.emit_error` so every user-facing failure —
runtime or argument-parsing — shares one ``error:``/``hint:`` format, and adds a
"did you mean" suggestion when an unknown subcommand looks like a typo.

``add_subparsers`` defaults ``parser_class`` to ``type(self)``, so every nested
subparser inherits this behavior automatically once the top parser uses it.
"""

from __future__ import annotations

import argparse
import difflib
import re
from typing import NoReturn

from chemstack.cli_errors import emit_error

# Matches argparse's stock invalid-choice message. Older Python quotes each
# choice (``choose from 'queue', 'run-dir'``); 3.12+ drops the quotes
# (``choose from queue, run-dir``), so both forms are handled below.
_INVALID_CHOICE_RE = re.compile(
    r"invalid choice: '(?P<value>[^']*)' \(choose from (?P<choices>.+)\)"
)


def _suggestion_hint(message: str) -> str | None:
    """Return a "did you mean ...?" hint for an invalid-choice ``message``."""

    match = _INVALID_CHOICE_RE.search(message)
    if not match:
        return None
    choices = [part.strip().strip("'") for part in match.group("choices").split(",")]
    choices = [choice for choice in choices if choice]
    if not choices:
        return None
    close = difflib.get_close_matches(match.group("value"), choices, n=1, cutoff=0.5)
    if close:
        return f"did you mean `{close[0]}`?"
    return f"valid choices: {', '.join(choices)}"


class ChemStackArgumentParser(argparse.ArgumentParser):
    """``ArgumentParser`` whose errors use the shared ``error:`` styling."""

    def error(self, message: str) -> NoReturn:
        hint = _suggestion_hint(message)
        if hint is None:
            hint = f"run `{self.prog} --help` for usage."
        emit_error(message, hint=hint)
        self.exit(2)


__all__ = ["ChemStackArgumentParser"]
