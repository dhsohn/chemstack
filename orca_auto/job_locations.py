from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any

from core.config import AppConfig
from core.molecule_key import resolve_molecule_key
from core.result_organizer import detect_job_type
from . import _job_location_index_fallback
from .state import (
    REPORT_JSON_NAME,
    REPORT_MD_NAME,
    STATE_FILE_NAME,
    load_organized_ref,
    load_report_json,
    load_state,
)

_MOLECULE_KEY_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
QUEUE_FILE_NAME = "queue.json"
INDEX_DIR_NAME = "index"
LEGACY_RECORDS_FILE_NAME = "records.jsonl"
JobLocationRecord = Any


@dataclass(frozen=True)
class JobArtifactContext:
    record: JobLocationRecord | None = None
    job_dir: Path | None = None
    state: dict[str, Any] | None = None
    report: dict[str, Any] | None = None
    organized_ref: dict[str, Any] | None = None


@dataclass(frozen=True)
class JobRuntimeContext:
    artifact: JobArtifactContext = field(default_factory=JobArtifactContext)
    queue_entry: dict[str, Any] | None = None
    organized_dir: Path | None = None


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


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize_text(value).lower() in {"1", "true", "yes", "y", "on"}


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _resolve_artifact_path(path_value: Any, base_dir: Path | None) -> str:
    raw = _normalize_text(path_value)
    if not raw:
        return ""
    try:
        candidate = Path(raw).expanduser()
    except OSError:
        return raw
    if candidate.is_absolute():
        try:
            return str(candidate.resolve())
        except OSError:
            return str(candidate)
    if base_dir is None:
        return raw
    try:
        return str((base_dir / candidate).resolve())
    except OSError:
        return str(base_dir / candidate)


def _resolve_existing_path(value: Any) -> Path | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        resolved = Path(raw).expanduser().resolve()
    except OSError:
        return None
    return resolved if resolved.exists() else None


def _derive_selected_input_xyz(selected_inp: str) -> str:
    inp_path = _resolve_existing_path(selected_inp)
    if inp_path is None or inp_path.is_dir():
        return ""
    try:
        text = inp_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or not stripped.startswith("*"):
            continue
        if "xyzfile" not in stripped.lower():
            continue
        parts = stripped.split()
        if len(parts) >= 5:
            return _resolve_artifact_path(parts[-1], inp_path.parent)
    return ""


def _iter_existing_dirs(*candidates: Path | None) -> list[Path]:
    rows: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            continue
        if not resolved.exists() or not resolved.is_dir() or resolved in seen:
            continue
        seen.add(resolved)
        rows.append(resolved)
    return rows


def _is_subpath(candidate: Path, root: Path | None) -> bool:
    if root is None:
        return False
    try:
        candidate.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def _prefer_orca_optimized_xyz(
    *,
    selected_inp: str,
    selected_input_xyz: str,
    current_dir: Path | None,
    organized_dir: Path | None,
    latest_known_path: str,
    last_out_path: str,
) -> str:
    selected_inp_path = _resolve_existing_path(selected_inp)
    selected_input_xyz_path = _resolve_existing_path(selected_input_xyz)
    last_out = _resolve_existing_path(last_out_path)
    latest_known_dir = _resolve_existing_path(latest_known_path)
    if latest_known_dir is not None and not latest_known_dir.is_dir():
        latest_known_dir = latest_known_dir.parent

    search_dirs = _iter_existing_dirs(
        selected_inp_path.parent if selected_inp_path is not None and not selected_inp_path.is_dir() else None,
        current_dir,
        organized_dir,
        latest_known_dir,
        last_out.parent if last_out is not None and not last_out.is_dir() else None,
    )
    preferred_names: list[str] = []
    if selected_inp_path is not None and not selected_inp_path.is_dir():
        preferred_names.append(f"{selected_inp_path.stem}.xyz")
    if last_out is not None and not last_out.is_dir():
        preferred_names.append(f"{last_out.stem}.xyz")

    for search_dir in search_dirs:
        for filename in preferred_names:
            candidate = search_dir / filename
            if candidate.exists():
                try:
                    return str(candidate.resolve())
                except OSError:
                    return str(candidate)

    source_input = None
    if selected_input_xyz_path is not None and not selected_input_xyz_path.is_dir():
        try:
            source_input = selected_input_xyz_path.resolve()
        except OSError:
            source_input = selected_input_xyz_path

    xyz_candidates: list[Path] = []
    seen_files: set[Path] = set()
    for search_dir in search_dirs:
        try:
            files = sorted(
                (item for item in search_dir.glob("*.xyz") if item.is_file()),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            continue
        for item in files:
            try:
                resolved = item.resolve()
            except OSError:
                resolved = item
            if source_input is not None and resolved == source_input:
                continue
            if resolved in seen_files:
                continue
            seen_files.add(resolved)
            xyz_candidates.append(item)

    if not xyz_candidates:
        return ""
    try:
        return str(xyz_candidates[0].resolve())
    except OSError:
        return str(xyz_candidates[0])


def _attempt_count(state: dict[str, Any], report: dict[str, Any]) -> int:
    report_count = _safe_int(report.get("attempt_count"), default=-1)
    if report_count >= 0:
        return report_count
    attempts = state.get("attempts")
    if isinstance(attempts, list):
        return len(attempts)
    return 0


def _max_retries(state: dict[str, Any], report: dict[str, Any]) -> int:
    report_value = _safe_int(report.get("max_retries"), default=-1)
    if report_value >= 0:
        return report_value
    return _safe_int(state.get("max_retries"), default=0)


def _coerce_attempts(state: dict[str, Any], report: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_attempts = report.get("attempts")
    if not isinstance(raw_attempts, list):
        raw_attempts = state.get("attempts")
    if not isinstance(raw_attempts, list):
        return ()

    attempts: list[dict[str, Any]] = []
    for raw in raw_attempts:
        if not isinstance(raw, dict):
            continue
        index = _safe_int(raw.get("index"), default=0)
        attempt_number = max(0, index - 1) if index > 0 else 0
        attempts.append(
            {
                "index": index,
                "attempt_number": attempt_number,
                "inp_path": _normalize_text(raw.get("inp_path")),
                "out_path": _normalize_text(raw.get("out_path")),
                "return_code": _safe_int(raw.get("return_code"), default=0),
                "analyzer_status": _normalize_text(raw.get("analyzer_status")),
                "analyzer_reason": _normalize_text(raw.get("analyzer_reason")),
                "markers": list(raw["markers"]) if isinstance(raw.get("markers"), list) else [],
                "patch_actions": list(raw["patch_actions"]) if isinstance(raw.get("patch_actions"), list) else [],
                "started_at": _normalize_text(raw.get("started_at")),
                "ended_at": _normalize_text(raw.get("ended_at")),
            }
        )
    return tuple(attempts)


def _final_result_payload(state: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    payload = report.get("final_result")
    if not isinstance(payload, dict):
        payload = state.get("final_result")
    if not isinstance(payload, dict):
        return {}
    return {str(key): value for key, value in payload.items()}


def _status_from_payloads(
    *,
    queue_entry: dict[str, Any] | None,
    state: dict[str, Any],
    report: dict[str, Any],
) -> tuple[str, str, str, str]:
    queue_status = _normalize_text((queue_entry or {}).get("status")).lower()
    cancel_requested = _normalize_bool((queue_entry or {}).get("cancel_requested"))

    state_status = _normalize_text(state.get("status")).lower()
    report_status = _normalize_text(report.get("status")).lower()
    final_result = report.get("final_result") if isinstance(report.get("final_result"), dict) else state.get("final_result")
    final = final_result if isinstance(final_result, dict) else {}
    final_status = _normalize_text(final.get("status")).lower()
    analyzer_status = _normalize_text(final.get("analyzer_status"))
    reason = _normalize_text(final.get("reason"))
    completed_at = _normalize_text(final.get("completed_at"))

    if final_status in {"completed", "failed"}:
        return final_status, analyzer_status, reason, completed_at
    if queue_status == "cancelled":
        return "cancelled", analyzer_status, reason or "cancelled", completed_at
    if queue_status == "running" and cancel_requested:
        return "cancel_requested", analyzer_status, reason, completed_at
    if queue_status == "pending":
        return "queued", analyzer_status, reason, completed_at
    if queue_status == "running":
        return "running", analyzer_status, reason, completed_at
    if state_status in {"completed", "failed"}:
        return state_status, analyzer_status, reason, completed_at
    if state_status in {"created", "running", "retrying"}:
        return "running", analyzer_status, reason, completed_at
    if report_status in {"completed", "failed"}:
        return report_status, analyzer_status, reason, completed_at
    if queue_status:
        return queue_status, analyzer_status, reason, completed_at
    if state_status:
        return state_status, analyzer_status, reason, completed_at
    return "unknown", analyzer_status, reason, completed_at


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            raw = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(raw, dict):
            rows.append(raw)
    return rows


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


def _record_matches_job_dir(record: JobLocationRecord, job_dir: Path) -> bool:
    resolved_job_dir = job_dir.expanduser().resolve()
    for value in (record.latest_known_path, record.organized_output_dir, record.original_run_dir):
        raw = _normalize_text(value)
        if not raw:
            continue
        try:
            resolved = Path(raw).expanduser().resolve()
        except OSError:
            continue
        if resolved == resolved_job_dir:
            return True
    return False


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


def _first_artifact_context(index_root: str | Path, targets: tuple[str, ...]) -> JobArtifactContext:
    for raw_target in targets:
        target = _normalize_text(raw_target)
        if not target:
            continue
        context = load_job_artifact_context(index_root, target)
        if context.job_dir is not None:
            return context
    return JobArtifactContext()


def _hydrated_organized_ref(context: JobArtifactContext) -> dict[str, Any] | None:
    payload = dict(context.organized_ref) if isinstance(context.organized_ref, dict) else None
    if payload:
        return payload
    if context.record is None:
        return payload
    original_dir = _resolve_existing_job_dir(context.record.original_run_dir)
    if original_dir is None or original_dir == context.job_dir:
        return payload
    return load_organized_ref(original_dir)


def _job_artifact_context(
    *,
    record: JobLocationRecord | None,
    job_dir: Path | None,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
) -> JobArtifactContext:
    return JobArtifactContext(
        record=record,
        job_dir=job_dir,
        state=dict(state) if isinstance(state, dict) else None,
        report=dict(report) if isinstance(report, dict) else None,
        organized_ref=dict(organized_ref) if isinstance(organized_ref, dict) else None,
    )


def _find_queue_entry(
    *,
    index_root: Path,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    entries = _load_json_list(index_root / QUEUE_FILE_NAME)
    if not entries:
        return None

    direct_target = _resolve_existing_job_dir(target)
    resolved_reaction_dir = _resolve_existing_job_dir(reaction_dir)

    def _entry_matches(entry: dict[str, Any]) -> bool:
        entry_queue_id = _normalize_text(entry.get("queue_id"))
        entry_task_id = _normalize_text(entry.get("task_id"))
        entry_run_id = _normalize_text(entry.get("run_id"))
        entry_reaction_dir = _resolve_existing_job_dir(entry.get("reaction_dir"))

        if queue_id and entry_queue_id == queue_id:
            return True
        if target and entry_queue_id == target:
            return True
        if target and entry_task_id == target:
            return True
        if run_id and entry_run_id == run_id:
            return True
        if target and entry_run_id == target:
            return True
        if resolved_reaction_dir is not None and entry_reaction_dir == resolved_reaction_dir:
            return True
        if direct_target is not None and entry_reaction_dir == direct_target:
            return True
        return False

    for entry in reversed(entries):
        if _entry_matches(entry):
            return dict(entry)
    return None


def _record_organized_dir(record: JobLocationRecord | None) -> Path | None:
    if record is None:
        return None
    organized_candidate = _resolve_existing_job_dir(record.organized_output_dir)
    if organized_candidate is not None and organized_candidate.is_dir():
        return organized_candidate
    latest_known_candidate = _resolve_existing_job_dir(record.latest_known_path)
    original_candidate = _resolve_existing_job_dir(record.original_run_dir)
    if (
        latest_known_candidate is not None
        and latest_known_candidate.is_dir()
        and latest_known_candidate != original_candidate
    ):
        return latest_known_candidate
    return None


def _find_organized_dir_from_legacy_records(
    *,
    organized_root: Path | None,
    target: str,
    run_id: str,
    reaction_dir: str,
) -> Path | None:
    if organized_root is None:
        return None
    records = _load_jsonl_records(organized_root / INDEX_DIR_NAME / LEGACY_RECORDS_FILE_NAME)
    if not records:
        return None

    direct_target = _resolve_existing_job_dir(target)
    resolved_reaction_dir = _resolve_existing_job_dir(reaction_dir)

    def _record_dir(record: dict[str, Any]) -> Path | None:
        reaction_dir_value = _resolve_existing_job_dir(record.get("reaction_dir"))
        if reaction_dir_value is not None:
            return reaction_dir_value
        organized_path = _normalize_text(record.get("organized_path"))
        if not organized_path:
            return None
        return _resolve_existing_job_dir(organized_root / organized_path)

    for record in reversed(records):
        record_run_id = _normalize_text(record.get("run_id"))
        record_dir = _record_dir(record)
        if run_id and record_run_id == run_id:
            return record_dir
        if target and record_run_id == target:
            return record_dir
        if direct_target is not None and record_dir == direct_target:
            return record_dir
        if resolved_reaction_dir is not None and record_dir == resolved_reaction_dir:
            return record_dir
    return None


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


def load_job_artifact_context(
    index_root: str | Path,
    target: str,
) -> JobArtifactContext:
    candidates = _job_dir_candidates(index_root, target)
    if not candidates:
        return JobArtifactContext()

    record = _chem_core_indexing_module().resolve_job_location(index_root, target)
    primary_dir = candidates[0]
    if record is None:
        for candidate_record in list_job_location_records(index_root):
            if _record_matches_job_dir(candidate_record, primary_dir):
                record = candidate_record
                break

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

    organized_ref_payload = load_organized_ref(primary_dir)
    if not organized_ref_payload and record is not None:
        original_dir = _resolve_existing_job_dir(record.original_run_dir)
        if original_dir is not None and original_dir != primary_dir:
            organized_ref_payload = load_organized_ref(original_dir)

    return JobArtifactContext(
        record=record,
        job_dir=primary_dir,
        state=state_payload,
        report=report_payload,
        organized_ref=organized_ref_payload,
    )


def load_job_runtime_context(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> JobRuntimeContext:
    resolved_index_root = Path(index_root).expanduser().resolve()
    resolved_organized_root = Path(organized_root).expanduser().resolve() if organized_root else None

    artifact = _first_artifact_context(resolved_index_root, (target, run_id, reaction_dir))
    queue_entry = _find_queue_entry(
        index_root=resolved_index_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )

    queue_reaction_dir = _resolve_existing_job_dir((queue_entry or {}).get("reaction_dir"))
    if artifact.job_dir is None and queue_reaction_dir is not None:
        artifact = _first_artifact_context(
            resolved_index_root,
            (str(queue_reaction_dir), target, run_id, reaction_dir),
        )

    artifact = _job_artifact_context(
        record=artifact.record,
        job_dir=artifact.job_dir,
        state=artifact.state,
        report=artifact.report,
        organized_ref=_hydrated_organized_ref(artifact),
    )

    state_payload = dict(artifact.state) if isinstance(artifact.state, dict) else {}
    report_payload = dict(artifact.report) if isinstance(artifact.report, dict) else {}
    organized_ref_payload = dict(artifact.organized_ref) if isinstance(artifact.organized_ref, dict) else {}
    current_dir = artifact.job_dir or _resolve_existing_job_dir(reaction_dir) or queue_reaction_dir

    resolved_run_id = (
        _normalize_text(run_id)
        or _normalize_text(state_payload.get("run_id"))
        or _normalize_text(report_payload.get("run_id"))
        or _normalize_text(organized_ref_payload.get("run_id"))
        or _normalize_text((queue_entry or {}).get("run_id"))
    )
    organized_dir = _record_organized_dir(artifact.record) or _find_organized_dir_from_legacy_records(
        organized_root=resolved_organized_root,
        target=target,
        run_id=resolved_run_id,
        reaction_dir=str(current_dir) if current_dir is not None else reaction_dir,
    )

    if organized_dir is not None and (
        current_dir is None
        or not current_dir.exists()
        or (not state_payload and not report_payload)
    ):
        refreshed = _first_artifact_context(
            resolved_index_root,
            (str(organized_dir), target, resolved_run_id, reaction_dir),
        )
        refreshed_dir = refreshed.job_dir or organized_dir
        artifact = _job_artifact_context(
            record=refreshed.record or artifact.record,
            job_dir=refreshed_dir,
            state=refreshed.state or dict(load_state(refreshed_dir) or {}),
            report=refreshed.report or load_report_json(refreshed_dir),
            organized_ref=_hydrated_organized_ref(refreshed) or load_organized_ref(refreshed_dir),
        )

    return JobRuntimeContext(
        artifact=artifact,
        queue_entry=queue_entry,
        organized_dir=organized_dir,
    )


def load_orca_contract_payload(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> dict[str, Any]:
    runtime = load_job_runtime_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )
    artifact = runtime.artifact
    record = artifact.record
    queue_entry = dict(runtime.queue_entry) if isinstance(runtime.queue_entry, dict) else {}
    state = dict(artifact.state) if isinstance(artifact.state, dict) else {}
    report = dict(artifact.report) if isinstance(artifact.report, dict) else {}
    organized_ref = dict(artifact.organized_ref) if isinstance(artifact.organized_ref, dict) else {}
    current_dir = artifact.job_dir or _resolve_existing_job_dir(reaction_dir) or _resolve_existing_job_dir(queue_entry.get("reaction_dir"))
    resolved_organized_root = Path(organized_root).expanduser().resolve() if organized_root else None

    if record is None and current_dir is None and not queue_entry:
        return {}

    resolved_run_id = (
        _normalize_text(run_id)
        or _normalize_text(state.get("run_id"))
        or _normalize_text(report.get("run_id"))
        or _normalize_text(organized_ref.get("run_id"))
        or _normalize_text(queue_entry.get("run_id"))
    )

    latest_known_path = ""
    if record is not None and _normalize_text(record.latest_known_path):
        latest_known_path = _normalize_text(record.latest_known_path)
    elif runtime.organized_dir is not None:
        latest_known_path = str(runtime.organized_dir)
    elif current_dir is not None:
        latest_known_path = str(current_dir)
    else:
        latest_known_path = _normalize_text(target)

    state_status = _normalize_text(state.get("status")).lower()
    status, analyzer_status, reason, completed_at = _status_from_payloads(
        queue_entry=queue_entry,
        state=state,
        report=report,
    )
    tracked_status = _normalize_text(record.status if record is not None else "").lower()
    if status == "unknown" and tracked_status:
        status = tracked_status

    base_dir = current_dir
    selected_inp = _resolve_artifact_path(
        state.get("selected_inp")
        or report.get("selected_inp")
        or organized_ref.get("selected_inp")
        or organized_ref.get("selected_input_xyz")
        or (record.selected_input_xyz if record is not None else ""),
        base_dir,
    )
    state_final_result = state.get("final_result")
    state_final = state_final_result if isinstance(state_final_result, dict) else {}
    report_final_result = report.get("final_result")
    report_final = report_final_result if isinstance(report_final_result, dict) else {}
    last_out_path = _resolve_artifact_path(
        state_final.get("last_out_path") or report_final.get("last_out_path"),
        base_dir,
    )
    selected_input_xyz = _resolve_artifact_path(
        organized_ref.get("selected_input_xyz") or (record.selected_input_xyz if record is not None else ""),
        base_dir,
    )
    if not selected_input_xyz.lower().endswith(".xyz"):
        selected_input_xyz = ""
    selected_input_xyz = selected_input_xyz or _derive_selected_input_xyz(selected_inp)
    optimized_xyz_path = _prefer_orca_optimized_xyz(
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        current_dir=current_dir,
        organized_dir=runtime.organized_dir,
        latest_known_path=latest_known_path,
        last_out_path=last_out_path,
    )

    resource_request = _resource_dict_from_any(queue_entry.get("resource_request")) or _resource_dict_from_any(
        record.resource_request if record is not None else {}
    )
    resource_actual = _resource_dict_from_any(queue_entry.get("resource_actual")) or _resource_dict_from_any(
        record.resource_actual if record is not None else {}
    ) or dict(resource_request)

    organized_output_dir = _normalize_text(
        (record.organized_output_dir if record is not None else "")
        or organized_ref.get("organized_output_dir")
        or (str(runtime.organized_dir) if runtime.organized_dir is not None else "")
        or (str(current_dir) if current_dir is not None and _is_subpath(current_dir, resolved_organized_root) else "")
    )

    run_state_path = (
        str((current_dir / STATE_FILE_NAME).resolve())
        if current_dir is not None and (current_dir / STATE_FILE_NAME).exists()
        else ""
    )
    report_json_path = (
        str((current_dir / REPORT_JSON_NAME).resolve())
        if current_dir is not None and (current_dir / REPORT_JSON_NAME).exists()
        else ""
    )
    report_md_path = (
        str((current_dir / REPORT_MD_NAME).resolve())
        if current_dir is not None and (current_dir / REPORT_MD_NAME).exists()
        else ""
    )

    return {
        "run_id": resolved_run_id,
        "status": status,
        "reason": reason,
        "state_status": state_status,
        "reaction_dir": str(current_dir) if current_dir is not None else _normalize_text(reaction_dir),
        "latest_known_path": latest_known_path,
        "organized_output_dir": organized_output_dir,
        "optimized_xyz_path": optimized_xyz_path,
        "queue_id": _normalize_text(queue_entry.get("queue_id") or queue_id),
        "queue_status": _normalize_text(queue_entry.get("status")).lower(),
        "cancel_requested": _normalize_bool(queue_entry.get("cancel_requested")),
        "selected_inp": selected_inp,
        "selected_input_xyz": selected_input_xyz,
        "analyzer_status": analyzer_status,
        "completed_at": completed_at,
        "last_out_path": last_out_path,
        "run_state_path": run_state_path,
        "report_json_path": report_json_path,
        "report_md_path": report_md_path,
        "attempt_count": _attempt_count(state, report),
        "max_retries": _max_retries(state, report),
        "attempts": _coerce_attempts(state, report),
        "final_result": _final_result_payload(state, report),
        "resource_request": resource_request,
        "resource_actual": resource_actual,
    }


def load_job_artifacts(
    index_root: str | Path,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    context = load_job_artifact_context(index_root, target)
    return context.job_dir, context.state, context.report


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
