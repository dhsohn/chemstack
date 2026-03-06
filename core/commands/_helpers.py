from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List

from ..config import AppConfig
from ..pathing import is_subpath

logger = logging.getLogger(__name__)

RETRY_INP_RE = re.compile(r"\.retry\d+$", re.IGNORECASE)
# ORCA가 자동 생성하는 중간 inp 파일 패턴 (사용자 작성 inp가 아님)
ORCA_GENERATED_INP_RE = re.compile(
    r"\.(scfgrad|scfhess|cis|autoci|cipsi|mrci|mdci|eprnmr|loc|nbo|compound|hess)"
    r"(\.retry\d+)?$",
    re.IGNORECASE,
)
CONFIG_ENV_VAR = "ORCA_AUTO_CONFIG"
_MAX_SAMPLE_FILES = 10


def default_config_path() -> str:
    env_path = os.getenv(CONFIG_ENV_VAR, "").strip()
    if env_path:
        return env_path

    repo_default = Path(__file__).resolve().parents[2] / "config" / "orca_auto.yaml"
    if repo_default.exists():
        return str(repo_default)

    home_default = Path.home() / "orca_auto" / "config" / "orca_auto.yaml"
    if home_default.exists():
        return str(home_default)

    return str(repo_default)


def _to_resolved_local(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def _validate_reaction_dir(cfg: AppConfig, reaction_dir_raw: str) -> Path:
    reaction_dir = _to_resolved_local(reaction_dir_raw)
    if not reaction_dir.exists() or not reaction_dir.is_dir():
        raise ValueError(f"Reaction directory not found: {reaction_dir}")

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if not is_subpath(reaction_dir, allowed_root):
        raise ValueError(
            f"Reaction directory must be under allowed root: {allowed_root}. got={reaction_dir}"
        )
    return reaction_dir


def _validate_root_scan_dir(cfg: AppConfig, root_raw: str) -> Path:
    root = _to_resolved_local(root_raw)
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Root directory not found: {root}")

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if root != allowed_root:
        raise ValueError(
            f"--root must exactly match allowed_root: {allowed_root}. got={root}"
        )
    return root


def _validate_organized_root_dir(cfg: AppConfig, root_raw: str) -> Path:
    root = _to_resolved_local(root_raw)
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Root directory not found: {root}")
    organized_root = _to_resolved_local(cfg.runtime.organized_root)
    if root != organized_root:
        raise ValueError(
            f"--root must exactly match organized_root: {organized_root}. got={root}"
        )
    return root


def _validate_cleanup_reaction_dir(cfg: AppConfig, reaction_dir_raw: str) -> Path:
    reaction_dir = _to_resolved_local(reaction_dir_raw)
    if not reaction_dir.exists() or not reaction_dir.is_dir():
        raise ValueError(f"Reaction directory not found: {reaction_dir}")
    organized_root = _to_resolved_local(cfg.runtime.organized_root)
    if not is_subpath(reaction_dir, organized_root):
        raise ValueError(
            f"Reaction directory must be under organized_root: {organized_root}. got={reaction_dir}"
        )
    return reaction_dir


def _human_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(value) < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TB"


def finalize_batch_apply(
    summary: Dict[str, Any],
    emit_fn: Callable[[Dict[str, Any], bool], None],
    as_json: bool,
    failures: List[Dict[str, Any]],
) -> int:
    emit_fn(summary, as_json)
    return 1 if failures else 0


def _emit(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return
    for key in [
        "status",
        "reaction_dir",
        "selected_inp",
        "attempt_count",
        "reason",
        "run_state",
        "report_json",
        "report_md",
    ]:
        if key in payload:
            print(f"{key}: {payload[key]}")
