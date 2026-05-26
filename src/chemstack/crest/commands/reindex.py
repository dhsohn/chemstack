from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.commands import engine_reindex as _engine_reindex

from ..config import load_config
from ..state import load_organized_ref, load_report_json, load_state
from ..job_locations import index_root_for_path, record_from_artifacts

_ReindexDeps = _engine_reindex.ReindexDeps


def _scan_roots(cfg: Any, raw_root: str | None) -> list[Path]:
    return _engine_reindex.scan_roots(cfg, raw_root, engine="crest")


_iter_candidate_dirs = _engine_reindex.iter_candidate_dirs


def _reindex_deps() -> _ReindexDeps:
    return _ReindexDeps(
        load_config=load_config,
        load_state=load_state,
        load_report_json=load_report_json,
        load_organized_ref=load_organized_ref,
        index_root_for_path=index_root_for_path,
        record_from_artifacts=record_from_artifacts,
        _scan_roots=_scan_roots,
        _iter_candidate_dirs=_iter_candidate_dirs,
    )


def cmd_reindex(args: Any) -> int:
    return _engine_reindex.cmd_reindex(
        args,
        engine="crest",
        deps=_reindex_deps(),
    )
