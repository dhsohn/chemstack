from __future__ import annotations

import re
import sys
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any

from core.config import AppConfig
from core.molecule_key import resolve_molecule_key
from core.result_organizer import detect_job_type
from . import _job_location_index_fallback
from .state import load_organized_ref, load_report_json, load_state

_MOLECULE_KEY_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
JobLocationRecord = Any


@lru_cache(maxsize=1)
def _chem_core_indexing_module() -> Any:
    try:
        return import_module("chem_core.indexing")
    except ModuleNotFoundError as exc:
        if exc.name not in {"chem_core", "chem_core.indexing"}:
            raise
        repo_root = Path(__file__).resolve().parents[2] / "chem_core"
        if repo_root.is_dir():
            repo_root_text = str(repo_root)
            if repo_root_text not in sys.path:
                sys.path.insert(0, repo_root_text)
            try:
                return import_module("chem_core.indexing")
            except ModuleNotFoundError as retry_exc:
                if retry_exc.name not in {"chem_core", "chem_core.indexing"}:
                    raise
        return _job_location_index_fallback


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_path_text(value: Any) -> str:
    raw = _normalize_text(value)
    if not raw:
        return ""
    try:
        candidate = Path(raw).expanduser()
    except OSError:
        return raw
    try:
        return str(candidate.resolve())
    except OSError:
        return str(candidate)


def _resource_dict_from_any(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, raw in value.items():
        key_text = _normalize_text(key)
        if not key_text:
            continue
        try:
            result[key_text] = int(raw)
        except (TypeError, ValueError):
            continue
    return result


def index_root_for_cfg(cfg: AppConfig) -> Path:
    return Path(cfg.runtime.allowed_root).expanduser().resolve()


def job_type_identifier(job_type: str) -> str:
    normalized = _normalize_text(job_type).lower()
    if normalized.startswith("orca_"):
        return normalized
    return f"orca_{normalized or 'other'}"


def normalize_molecule_key(value: str) -> str:
    collapsed = _MOLECULE_KEY_RE.sub("_", _normalize_text(value)).strip("._-")
    return collapsed or "unknown"


def molecule_key_from_selected_inp(selected_inp: str, job_dir: Path) -> str:
    raw = _normalize_text(selected_inp)
    if raw:
        try:
            candidate = Path(raw).expanduser()
            resolved = candidate.resolve()
        except OSError:
            resolved = None
        if resolved is not None and resolved.exists():
            return resolve_molecule_key(resolved).key
        stem = Path(raw).stem.strip()
        if stem:
            return normalize_molecule_key(stem)
    return normalize_molecule_key(job_dir.name)


def resolve_job_metadata(selected_inp: str, job_dir: Path) -> tuple[str, str]:
    job_type = "other"
    raw = _normalize_text(selected_inp)
    if raw:
        try:
            candidate = Path(raw).expanduser()
            resolved = candidate.resolve()
        except OSError:
            resolved = None
        if resolved is not None and resolved.exists():
            job_type = detect_job_type(resolved)
    molecule_key = molecule_key_from_selected_inp(raw, job_dir)
    return job_type, molecule_key


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
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    resolved_job_dir = job_dir.expanduser().resolve()
    existing_original = Path(existing.original_run_dir).expanduser().resolve() if existing and existing.original_run_dir else None
    original_run_dir = existing_original or resolved_job_dir

    existing_selected = _normalize_path_text(existing.selected_input_xyz) if existing is not None else ""
    selected_input_text = _normalize_path_text(selected_input_xyz) or existing_selected

    existing_molecule_key = _normalize_text(existing.molecule_key) if existing is not None else ""
    molecule_key_text = _normalize_text(molecule_key) or existing_molecule_key
    if not molecule_key_text:
        molecule_key_text = molecule_key_from_selected_inp(selected_input_text, original_run_dir)

    existing_resource_request = dict(existing.resource_request) if existing is not None else {}
    existing_resource_actual = dict(existing.resource_actual) if existing is not None else {}
    resource_request_text = dict(resource_request or existing_resource_request)
    resource_actual_text = dict(resource_actual or existing_resource_actual or resource_request_text)

    organized_dir = organized_output_dir
    if organized_dir is None and existing is not None and existing.organized_output_dir:
        organized_dir = Path(existing.organized_output_dir).expanduser().resolve()

    latest_known_path = organized_dir or resolved_job_dir
    return _chem_core_indexing_module().JobLocationRecord(
        job_id=_normalize_text(job_id),
        app_name="orca_auto",
        job_type=job_type_identifier(job_type),
        status=_normalize_text(status) or "unknown",
        original_run_dir=str(original_run_dir),
        molecule_key=molecule_key_text,
        selected_input_xyz=selected_input_text,
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
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    root = index_root_for_cfg(cfg)
    existing = _chem_core_indexing_module().get_job_location(root, job_id)
    record = build_job_location_record(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
    return _chem_core_indexing_module().upsert_job_location(root, record)


def list_job_location_records(index_root: str | Path) -> list[JobLocationRecord]:
    return list(_chem_core_indexing_module().list_job_locations(index_root))


def resolve_record_job_dir(record: JobLocationRecord) -> Path | None:
    for value in (record.latest_known_path, record.organized_output_dir, record.original_run_dir):
        raw = _normalize_text(value)
        if not raw:
            continue
        try:
            resolved = Path(raw).expanduser().resolve()
        except OSError:
            continue
        if resolved.exists() and resolved.is_dir():
            return resolved
    return None


def _resolve_existing_job_dir(value: Any) -> Path | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        resolved = Path(raw).expanduser().resolve()
    except OSError:
        return None
    if not resolved.exists():
        return None
    return resolved.parent if resolved.is_file() else resolved


def _organized_job_dir(job_dir: Path) -> Path | None:
    organized_ref = load_organized_ref(job_dir)
    if not organized_ref:
        return None
    organized_dir = _resolve_existing_job_dir(organized_ref.get("organized_output_dir"))
    if organized_dir is None or not organized_dir.is_dir():
        return None
    return organized_dir


def _matching_tracked_job_dirs(index_root: str | Path, target: str) -> list[Path]:
    target_text = _normalize_text(target)
    if not target_text:
        return []

    candidates: list[Path] = []
    seen: set[Path] = set()
    for record in list_job_location_records(index_root):
        job_dir = resolve_record_job_dir(record)
        if job_dir is None or job_dir in seen:
            continue

        state = load_state(job_dir)
        report = load_report_json(job_dir) or {}
        organized_ref = load_organized_ref(job_dir) or {}

        if not organized_ref:
            original_dir = _resolve_existing_job_dir(record.original_run_dir)
            if original_dir is not None and original_dir != job_dir:
                organized_ref = load_organized_ref(original_dir) or {}

        lookup_values = (
            record.job_id,
            report.get("job_id"),
            (state or {}).get("job_id"),
            organized_ref.get("job_id"),
            report.get("run_id"),
            (state or {}).get("run_id"),
            organized_ref.get("run_id"),
        )
        if any(_normalize_text(value) == target_text for value in lookup_values):
            seen.add(job_dir)
            candidates.append(job_dir)

    return candidates


def _job_dir_candidates(index_root: str | Path, target: str) -> list[Path]:
    record = _chem_core_indexing_module().resolve_job_location(index_root, target)
    raw_candidates: list[Any] = []
    if record is not None:
        raw_candidates.extend(
            [record.latest_known_path, record.organized_output_dir, record.original_run_dir]
        )
    raw_candidates.append(target)

    candidates: list[Path] = []
    seen: set[Path] = set()
    for value in raw_candidates:
        candidate = _resolve_existing_job_dir(value)
        if candidate is None or not candidate.is_dir():
            continue
        for resolved in (_organized_job_dir(candidate), candidate):
            if resolved is None or resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(resolved)

    for candidate in _matching_tracked_job_dirs(index_root, target):
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)
    return candidates


def resolve_latest_job_dir(index_root: str | Path, target: str) -> Path | None:
    candidates = _job_dir_candidates(index_root, target)
    return candidates[0] if candidates else None


def load_job_artifacts(
    index_root: str | Path,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    candidates = _job_dir_candidates(index_root, target)
    if not candidates:
        return None, None, None

    state_payload: dict[str, Any] | None = None
    report_payload: dict[str, Any] | None = None
    for job_dir in candidates:
        if state_payload is None:
            state = load_state(job_dir)
            state_payload = dict(state) if state is not None else None
        if report_payload is None:
            report_payload = load_report_json(job_dir)
        if state_payload is not None and report_payload is not None:
            break

    return candidates[0], state_payload, report_payload


def is_terminal_status(status: str) -> bool:
    return _normalize_text(status).lower() in _TERMINAL_STATUSES


def record_from_artifacts(
    *,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
    existing: JobLocationRecord | None = None,
    fallback_job_id: str = "",
    default_job_type: str = "other",
) -> JobLocationRecord | None:
    state = state or {}
    report = report or {}
    organized_ref = organized_ref or {}

    job_id = _normalize_text(
        report.get("job_id")
        or state.get("job_id")
        or organized_ref.get("job_id")
        or fallback_job_id
        or (existing.job_id if existing else "")
        or report.get("run_id")
        or state.get("run_id")
        or organized_ref.get("run_id")
    )
    if not job_id:
        return None

    status = _normalize_text(report.get("status") or state.get("status") or organized_ref.get("status") or "unknown") or "unknown"
    selected_input_xyz = _normalize_path_text(
        report.get("selected_inp")
        or state.get("selected_inp")
        or organized_ref.get("selected_input_xyz")
        or organized_ref.get("selected_inp")
        or (existing.selected_input_xyz if existing else "")
    )

    derived_job_type, derived_molecule_key = resolve_job_metadata(selected_input_xyz, job_dir)
    job_type = _normalize_text(
        report.get("job_type")
        or state.get("job_type")
        or organized_ref.get("job_type")
        or derived_job_type
        or default_job_type
    ) or default_job_type
    molecule_key = _normalize_text(
        report.get("molecule_key")
        or state.get("molecule_key")
        or organized_ref.get("molecule_key")
        or (existing.molecule_key if existing else "")
        or derived_molecule_key
    )

    resource_request = (
        _resource_dict_from_any(report.get("resource_request"))
        or _resource_dict_from_any(state.get("resource_request"))
        or _resource_dict_from_any(organized_ref.get("resource_request"))
        or (dict(existing.resource_request) if existing is not None else {})
    )
    resource_actual = (
        _resource_dict_from_any(report.get("resource_actual"))
        or _resource_dict_from_any(state.get("resource_actual"))
        or _resource_dict_from_any(organized_ref.get("resource_actual"))
        or (dict(existing.resource_actual) if existing is not None else {})
    )

    original_run_dir = _normalize_text(
        report.get("original_run_dir")
        or state.get("original_run_dir")
        or organized_ref.get("original_run_dir")
        or (existing.original_run_dir if existing else "")
        or str(job_dir)
    )
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
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def collect_reindex_payload(job_dir: Path) -> dict[str, Any] | None:
    resolved_job_dir = job_dir.expanduser().resolve()
    state_data = load_state(resolved_job_dir)
    state = dict(state_data) if state_data is not None else {}
    report = load_report_json(resolved_job_dir) or {}
    organized_ref = load_organized_ref(resolved_job_dir) or {}

    record = record_from_artifacts(
        job_dir=resolved_job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
    )
    if record is None:
        return None

    return {
        "job_id": record.job_id,
        "status": record.status,
        "job_type": record.job_type,
        "job_dir": record.original_run_dir,
        "selected_input_xyz": record.selected_input_xyz,
        "molecule_key": record.molecule_key,
        "organized_output_dir": record.organized_output_dir,
        "resource_request": dict(record.resource_request),
        "resource_actual": dict(record.resource_actual),
    }


def reindex_job_locations(cfg: AppConfig) -> int:
    root = index_root_for_cfg(cfg)
    if not root.exists():
        return 0

    candidate_dirs: set[Path] = set()
    for pattern in ("run_state.json", "run_report.json", "organized_ref.json"):
        for artifact in root.rglob(pattern):
            candidate_dirs.add(artifact.parent)

    updated = 0
    for job_dir in sorted(candidate_dirs):
        state_data = load_state(job_dir)
        state = dict(state_data) if state_data is not None else None
        report = load_report_json(job_dir)
        organized_ref = load_organized_ref(job_dir)
        record = record_from_artifacts(
            job_dir=job_dir,
            state=state,
            report=report,
            organized_ref=organized_ref,
        )
        if record is None:
            continue
        _chem_core_indexing_module().upsert_job_location(root, record)
        updated += 1
    return updated
