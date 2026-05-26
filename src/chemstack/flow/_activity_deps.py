from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ._activity_model import ActivityListRequest, ActivityRecord, ResolvedActivitySources


@dataclass(frozen=True)
class OrcaActivityDeps:
    sibling_runtime_paths: Any
    _unique_texts: Any
    _path_aliases: Any
    _timestamp_metadata: Any


@dataclass(frozen=True)
class ActivityCancelDeps:
    cancel_crest_target: Any
    cancel_xtb_target: Any
    cancel_orca_target: Any
    _discover_orca_repo_root: Any


@dataclass(frozen=True)
class ActivityListProvider:
    source: str
    collect: Callable[[ResolvedActivitySources, ActivityListRequest], list[ActivityRecord]]


__all__ = [
    "ActivityCancelDeps",
    "ActivityListProvider",
    "OrcaActivityDeps",
]
