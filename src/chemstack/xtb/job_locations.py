from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, get_job_location, resolve_job_location, upsert_job_location

from .config import AppConfig
from .state import load_organized_ref, load_report_json, load_state

_KEY_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


def _normalize_text(value: Any) -> str:
    return str(value).strip()


def index_root_for_cfg(cfg: AppConfig) -> Path:
    return Path(cfg.runtime.allowed_root).expanduser().resolve()


def job_type_identifier(job_type: str) -> str:
    normalized = _normalize_text(job_type).lower() or "unknown"
    return f"xtb_{normalized}"


def normalize_key(value: str) -> str:
    collapsed = _KEY_RE.sub("_", _normalize_text(value)).strip("._-")
    return collapsed.lower() or "unknown_key"


def reaction_key_from_job_dir(job_dir: Path) -> str:
    return normalize_key(job_dir.name)


def reaction_key_from_selected_xyz(selected_input_xyz: str, job_dir: Path) -> str:
    raw = _normalize_text(selected_input_xyz)
    if raw:
        stem = Path(raw).stem.strip()
        if stem and stem.lower() not in {"r1", "reactant", "input"}:
            return normalize_key(stem)
    return reaction_key_from_job_dir(job_dir)


def resource_dict(max_cores: int, max_memory_gb: int) -> dict[str, int]:
    return {
        "max_cores": max(1, int(max_cores)),
        "max_memory_gb": max(1, int(max_memory_gb)),
    }


def build_job_location_record(
    *,
    existing: JobLocationRecord | None = None,
    job_id: str,
    status: str,
    job_dir: Path,
    job_type: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    reaction_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    resolved_job_dir = job_dir.expanduser().resolve()
    existing_original = Path(existing.original_run_dir).expanduser().resolve() if existing and existing.original_run_dir else None
    original_run_dir = existing_original or resolved_job_dir

    existing_selected = _normalize_text(existing.selected_input_xyz) if existing is not None else ""
    selected_input_xyz_text = _normalize_text(selected_input_xyz) or existing_selected

    existing_key = _normalize_text(existing.molecule_key) if existing is not None else ""
    reaction_key_text = _normalize_text(reaction_key) or existing_key
    if not reaction_key_text:
        reaction_key_text = reaction_key_from_job_dir(original_run_dir)

    existing_resource_request = dict(existing.resource_request) if existing is not None else {}
    existing_resource_actual = dict(existing.resource_actual) if existing is not None else {}
    resource_request_text = dict(resource_request or existing_resource_request)
    resource_actual_text = dict(resource_actual or existing_resource_actual or resource_request_text)

    organized_dir = organized_output_dir
    if organized_dir is None and existing is not None and existing.organized_output_dir:
        organized_dir = Path(existing.organized_output_dir).expanduser().resolve()

    latest_known_path = organized_dir or resolved_job_dir
    return JobLocationRecord(
        job_id=_normalize_text(job_id),
        app_name="xtb_auto",
        job_type=job_type_identifier(job_type),
        status=_normalize_text(status),
        original_run_dir=str(original_run_dir),
        molecule_key=reaction_key_text,
        selected_input_xyz=selected_input_xyz_text,
        organized_output_dir=str(organized_dir.resolve()) if organized_dir is not None else "",
        latest_known_path=str(latest_known_path.resolve()),
        resource_request=resource_request_text,
        resource_actual=resource_actual_text,
    )


def upsert_job_record(
    cfg: AppConfig,
    *,
    job_id: str,
    status: str,
    job_dir: Path,
    job_type: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    reaction_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    root = index_root_for_cfg(cfg)
    existing = get_job_location(root, job_id)
    record = build_job_location_record(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        reaction_key=reaction_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
    return upsert_job_location(root, record)


def resolve_latest_job_dir(index_root: str | Path, target: str) -> Path | None:
    record = resolve_job_location(index_root, target)
    if record is None:
        candidate = Path(_normalize_text(target)).expanduser()
        try:
            resolved = candidate.resolve()
        except OSError:
            return None
        return resolved if resolved.exists() and resolved.is_dir() else None

    candidates = [record.latest_known_path, record.organized_output_dir, record.original_run_dir]
    for latest in candidates:
        if not latest:
            continue
        path = Path(latest).expanduser()
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if resolved.exists() and resolved.is_dir():
            return resolved
    return None


def load_job_artifacts(
    index_root: str | Path,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    job_dir = resolve_latest_job_dir(index_root, target)
    if job_dir is None:
        return None, None, None
    return job_dir, load_state(job_dir), load_report_json(job_dir)


def is_terminal_status(status: str) -> bool:
    return _normalize_text(status).lower() in _TERMINAL_STATUSES


def record_from_artifacts(
    *,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
    existing: JobLocationRecord | None = None,
    default_job_type: str = "path_search",
) -> JobLocationRecord | None:
    state = state or {}
    report = report or {}
    organized_ref = organized_ref or {}

    job_id = _normalize_text(
        report.get("job_id")
        or state.get("job_id")
        or organized_ref.get("job_id")
        or (existing.job_id if existing else "")
    )
    if not job_id:
        return None

    status = _normalize_text(report.get("status") or state.get("status") or organized_ref.get("status") or "unknown") or "unknown"
    job_type = _normalize_text(
        report.get("job_type")
        or state.get("job_type")
        or organized_ref.get("job_type")
        or default_job_type
    ) or default_job_type
    selected_input_xyz = _normalize_text(
        report.get("selected_input_xyz")
        or state.get("selected_input_xyz")
        or organized_ref.get("selected_input_xyz")
        or (existing.selected_input_xyz if existing else "")
    )
    reaction_key = _normalize_text(
        report.get("reaction_key")
        or state.get("reaction_key")
        or organized_ref.get("reaction_key")
        or (existing.molecule_key if existing else "")
    )

    original_run_dir = _normalize_text(
        report.get("original_run_dir")
        or state.get("original_run_dir")
        or organized_ref.get("original_run_dir")
        or (existing.original_run_dir if existing else "")
        or str(job_dir)
    )
    if not reaction_key:
        reaction_key = reaction_key_from_job_dir(Path(original_run_dir))

    resource_request = report.get("resource_request") or state.get("resource_request") or organized_ref.get("resource_request")
    if not isinstance(resource_request, dict):
        resource_request = dict(existing.resource_request) if existing is not None else {}
    resource_actual = report.get("resource_actual") or state.get("resource_actual") or organized_ref.get("resource_actual")
    if not isinstance(resource_actual, dict):
        resource_actual = dict(existing.resource_actual) if existing is not None else {}

    organized_output_dir = _normalize_text(
        report.get("organized_output_dir")
        or state.get("organized_output_dir")
        or organized_ref.get("organized_output_dir")
        or (existing.organized_output_dir if existing else "")
    )

    return build_job_location_record(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=Path(original_run_dir),
        job_type=job_type,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=Path(organized_output_dir).expanduser().resolve() if organized_output_dir else None,
        reaction_key=reaction_key,
        resource_request={str(key): int(value) for key, value in resource_request.items()},
        resource_actual={str(key): int(value) for key, value in resource_actual.items()},
    )


def collect_reindex_payload(job_dir: Path) -> dict[str, Any] | None:
    resolved_job_dir = job_dir.expanduser().resolve()
    state = load_state(resolved_job_dir) or {}
    report = load_report_json(resolved_job_dir) or {}
    organized_ref = load_organized_ref(resolved_job_dir) or {}

    job_id = _normalize_text(report.get("job_id") or state.get("job_id") or organized_ref.get("job_id"))
    if not job_id:
        return None

    status = _normalize_text(report.get("status") or state.get("status") or organized_ref.get("status")) or "unknown"
    job_type = _normalize_text(report.get("job_type") or state.get("job_type") or "path_search") or "path_search"
    selected_input_xyz = _normalize_text(report.get("selected_input_xyz") or state.get("selected_input_xyz"))
    original_run_dir = _normalize_text(report.get("original_run_dir") or state.get("original_run_dir") or resolved_job_dir)
    reaction_key = _normalize_text(report.get("reaction_key") or state.get("reaction_key"))
    if not reaction_key:
        reaction_key = reaction_key_from_job_dir(Path(original_run_dir))
    resource_request = report.get("resource_request") or state.get("resource_request") or organized_ref.get("resource_request") or {}
    resource_actual = report.get("resource_actual") or state.get("resource_actual") or organized_ref.get("resource_actual") or {}
    organized_output_dir = _normalize_text(
        organized_ref.get("organized_output_dir")
        or report.get("organized_output_dir")
        or state.get("organized_output_dir")
    )

    return {
        "job_id": job_id,
        "status": status,
        "job_type": job_type,
        "job_dir": original_run_dir,
        "selected_input_xyz": selected_input_xyz,
        "reaction_key": reaction_key,
        "organized_output_dir": organized_output_dir,
        "resource_request": {str(key): int(value) for key, value in resource_request.items()} if isinstance(resource_request, dict) else {},
        "resource_actual": {str(key): int(value) for key, value in resource_actual.items()} if isinstance(resource_actual, dict) else {},
    }
