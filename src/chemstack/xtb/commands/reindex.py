from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from chemstack.core.commands import engine_reindex as _engine_reindex

from ..config import load_config
from ..state import load_organized_ref, load_report_json, load_state
from ..tracking import index_root_for_cfg as _index_root_for_cfg, index_root_for_path, record_from_artifacts

index_root_for_cfg = _index_root_for_cfg
_REINDEX_DEPS_COMPAT = (
    load_config,
    load_state,
    load_report_json,
    load_organized_ref,
    index_root_for_path,
    record_from_artifacts,
)


def _scan_roots(cfg: Any, raw_root: str | None) -> list[Path]:
    return _engine_reindex.scan_roots(cfg, raw_root, engine="xtb")


def _iter_candidate_dirs(root: Path) -> set[Path]:
    return _engine_reindex.iter_candidate_dirs(root)


def cmd_reindex(args: Any) -> int:
    return _engine_reindex.cmd_reindex(
        args,
        engine="xtb",
        deps=sys.modules[__name__],
        default_payload_kind_name="default_job_type",
        default_payload_kind="path_search",
    )
