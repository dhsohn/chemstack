from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.utils import atomic_write_json, now_utc_iso

STATE_FILE_NAME = "job_state.json"
REPORT_JSON_FILE_NAME = "job_report.json"
REPORT_MD_FILE_NAME = "job_report.md"
ORGANIZED_REF_FILE_NAME = "organized_ref.json"


def write_state(job_dir: Path, payload: dict[str, Any]) -> Path:
    path = job_dir / STATE_FILE_NAME
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def write_report_json(job_dir: Path, payload: dict[str, Any]) -> Path:
    path = job_dir / REPORT_JSON_FILE_NAME
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def write_report_md(job_dir: Path, *, job_id: str, status: str, reason: str, selected_xyz: str) -> Path:
    path = job_dir / REPORT_MD_FILE_NAME
    lines = [
        "# crest_auto Report",
        "",
        f"- Job ID: `{job_id}`",
        f"- Status: `{status}`",
        f"- Reason: `{reason}`",
        f"- Selected XYZ: `{selected_xyz}`",
        f"- Updated At: `{now_utc_iso()}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_report_md_lines(job_dir: Path, lines: list[str]) -> Path:
    path = job_dir / REPORT_MD_FILE_NAME
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_organized_ref(job_dir: Path, payload: dict[str, Any]) -> Path:
    path = job_dir / ORGANIZED_REF_FILE_NAME
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def load_state(job_dir: Path) -> dict[str, Any] | None:
    path = job_dir / STATE_FILE_NAME
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def load_report_json(job_dir: Path) -> dict[str, Any] | None:
    path = job_dir / REPORT_JSON_FILE_NAME
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def load_organized_ref(job_dir: Path) -> dict[str, Any] | None:
    path = job_dir / ORGANIZED_REF_FILE_NAME
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw
