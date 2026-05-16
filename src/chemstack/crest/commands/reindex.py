from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from chemstack.core.commands import engine_reindex as _engine_reindex

from ..config import load_config
from ..tracking import index_root_for_path, record_from_artifacts
from ..state import load_organized_ref, load_report_json, load_state

_REINDEX_DEPS_COMPAT = (
    load_config,
    load_state,
    load_report_json,
    load_organized_ref,
    index_root_for_path,
    record_from_artifacts,
)


def _scan_roots(cfg: Any, raw_root: str | None) -> list[Path]:
    return _engine_reindex.scan_roots(cfg, raw_root, engine="crest")


def _iter_candidate_dirs(root: Path) -> set[Path]:
    return _engine_reindex.iter_candidate_dirs(root)


def cmd_reindex(args: Any) -> int:
    return _engine_reindex.cmd_reindex(
        args,
        engine="crest",
        deps=sys.modules[__name__],
    )
