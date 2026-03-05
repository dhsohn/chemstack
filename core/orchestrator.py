"""Backward-compatible re-export shim.

All logic has been moved to core.commands submodules.
This module re-exports public names so that existing imports
(e.g. ``from core.orchestrator import default_config_path``) keep working.

.. deprecated::
    Import directly from ``core.commands._helpers`` or the appropriate
    submodule instead.
"""
from __future__ import annotations

from .commands._helpers import (  # noqa: F401
    CONFIG_ENV_VAR,
    _emit,
    default_config_path,
)
