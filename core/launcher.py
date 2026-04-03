"""Backward-compatible entrypoint shim.

Older generated console scripts still import ``core.launcher:main``.
Keep this thin wrapper so editable installs continue to work until their
entrypoints are regenerated.
"""

from __future__ import annotations

from .cli import main

__all__ = ["main"]
