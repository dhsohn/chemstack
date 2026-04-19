from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator

from core import lock_utils
from core.persistence_utils import atomic_write_json, now_utc_iso

JOB_LOCATION_INDEX_FILE_NAME = "job_locations.json"
JOB_LOCATION_INDEX_LOCK_NAME = "job_locations.lock"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobLocationRecord:
    job_id: str
    app_name: str
    job_type: str
    status: str
    original_run_dir: str
    molecule_key: str = ""
    selected_input_xyz: str = ""
    organized_output_dir: str = ""
    latest_known_path: str = ""
    resource_request: dict[str, int] = field(default_factory=dict)
    resource_actual: dict[str, int] = field(default_factory=dict)


class JobLocationIndexError(RuntimeError):
    """Raised when the job location index cannot satisfy a lookup."""


def _index_path(root: Path) -> Path:
    return root / JOB_LOCATION_INDEX_FILE_NAME


def _lock_path(root: Path) -> Path:
    return root / JOB_LOCATION_INDEX_LOCK_NAME


def _normalize_text(value: Any) -> str:
    return str(value).strip()


def _normalize_resources(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, int] = {}
    for key, value in raw.items():
        key_text = str(key).strip()
        if not key_text:
            continue
        try:
            result[key_text] = int(value)
        except (TypeError, ValueError):
            continue
    return result


def _record_from_dict(raw: dict[str, Any]) -> JobLocationRecord:
    return JobLocationRecord(
        job_id=_normalize_text(raw.get("job_id", "")),
        app_name=_normalize_text(raw.get("app_name", "")),
        job_type=_normalize_text(raw.get("job_type", "")),
        status=_normalize_text(raw.get("status", "")),
        original_run_dir=_normalize_text(raw.get("original_run_dir", "")),
        molecule_key=_normalize_text(raw.get("molecule_key", "")),
        selected_input_xyz=_normalize_text(raw.get("selected_input_xyz", "")),
        organized_output_dir=_normalize_text(raw.get("organized_output_dir", "")),
        latest_known_path=_normalize_text(raw.get("latest_known_path", "")),
        resource_request=_normalize_resources(raw.get("resource_request")),
        resource_actual=_normalize_resources(raw.get("resource_actual")),
    )


def _record_to_dict(record: JobLocationRecord) -> dict[str, Any]:
    return asdict(record)


def _load_records(root: Path) -> list[JobLocationRecord]:
    path = _index_path(root)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [_record_from_dict(item) for item in raw if isinstance(item, dict)]


def _save_records(root: Path, records: list[JobLocationRecord]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        _index_path(root),
        [_record_to_dict(record) for record in records],
        ensure_ascii=True,
        indent=2,
    )


def _resolve_candidate_path(path_text: str) -> Path | None:
    raw = _normalize_text(path_text)
    if not raw:
        return None
    try:
        return Path(raw).expanduser().resolve()
    except OSError:
        return None


def _record_paths(record: JobLocationRecord) -> list[Path]:
    paths: list[Path] = []
    for value in (
        record.original_run_dir,
        record.selected_input_xyz,
        record.organized_output_dir,
        record.latest_known_path,
    ):
        candidate = _resolve_candidate_path(value)
        if candidate is not None and candidate not in paths:
            paths.append(candidate)
    return paths


def _current_process_lock_payload() -> dict[str, Any]:
    payload: dict[str, Any] = {"pid": os.getpid(), "started_at": now_utc_iso()}
    start_ticks = lock_utils.current_process_start_ticks()
    if start_ticks is not None:
        payload["process_start_ticks"] = start_ticks
    return payload


def _active_lock_error(lock_pid: int, _lock_info: dict[str, Any], lock_path: Path) -> RuntimeError:
    return RuntimeError(
        f"Job location index lock is held by active process (pid={lock_pid}). Lock: {lock_path}"
    )


def _unreadable_lock_error(lock_path: Path) -> RuntimeError:
    return RuntimeError(f"Job location index lock file unreadable. Remove manually: {lock_path}")


def _stale_lock_remove_error(lock_pid: int, lock_path: Path, exc: OSError) -> RuntimeError:
    return RuntimeError(
        f"Failed to remove stale job location index lock (pid={lock_pid}): {lock_path}. error={exc}"
    )


@contextmanager
def _acquire_index_lock(root: Path, *, timeout_seconds: int = 10) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with lock_utils.acquire_file_lock(
        lock_path=_lock_path(root),
        lock_payload_obj=_current_process_lock_payload(),
        parse_lock_info_fn=lock_utils.parse_lock_info,
        is_process_alive_fn=lock_utils.is_process_alive,
        process_start_ticks_fn=lock_utils.process_start_ticks,
        logger=logger,
        acquired_log_template="Job location index lock acquired: %s",
        released_log_template="Job location index lock released: %s",
        stale_pid_reuse_log_template=(
            "Stale job location index lock (PID reuse, pid=%d, expected=%d, observed=%d): %s"
        ),
        stale_lock_log_template="Stale job location index lock (pid=%d), removing: %s",
        timeout_seconds=timeout_seconds,
        active_lock_error_builder=_active_lock_error,
        unreadable_lock_error_builder=_unreadable_lock_error,
        stale_remove_error_builder=_stale_lock_remove_error,
    ):
        yield


def list_job_locations(root: str | Path) -> list[JobLocationRecord]:
    resolved_root = Path(root).expanduser().resolve()
    with _acquire_index_lock(resolved_root):
        return _load_records(resolved_root)


def get_job_location(root: str | Path, job_id: str) -> JobLocationRecord | None:
    target = _normalize_text(job_id)
    if not target:
        return None
    resolved_root = Path(root).expanduser().resolve()
    with _acquire_index_lock(resolved_root):
        for record in _load_records(resolved_root):
            if record.job_id == target:
                return record
    return None


def upsert_job_location(root: str | Path, record: JobLocationRecord) -> JobLocationRecord:
    resolved_root = Path(root).expanduser().resolve()
    replacement = JobLocationRecord(
        job_id=_normalize_text(record.job_id),
        app_name=_normalize_text(record.app_name),
        job_type=_normalize_text(record.job_type),
        status=_normalize_text(record.status),
        original_run_dir=_normalize_text(record.original_run_dir),
        molecule_key=_normalize_text(record.molecule_key),
        selected_input_xyz=_normalize_text(record.selected_input_xyz),
        organized_output_dir=_normalize_text(record.organized_output_dir),
        latest_known_path=_normalize_text(record.latest_known_path),
        resource_request=_normalize_resources(record.resource_request),
        resource_actual=_normalize_resources(record.resource_actual),
    )

    with _acquire_index_lock(resolved_root):
        records = _load_records(resolved_root)
        updated = False
        for index, existing in enumerate(records):
            if existing.job_id != replacement.job_id:
                continue
            records[index] = replacement
            updated = True
            break
        if not updated:
            records.append(replacement)
        _save_records(resolved_root, records)
    return replacement


def resolve_job_location(root: str | Path, lookup_target: str) -> JobLocationRecord | None:
    target = _normalize_text(lookup_target)
    if not target:
        return None

    resolved_root = Path(root).expanduser().resolve()
    candidate_path = _resolve_candidate_path(target)

    with _acquire_index_lock(resolved_root):
        records = _load_records(resolved_root)

    for record in records:
        if record.job_id == target:
            return record

    if candidate_path is None:
        return None

    for record in records:
        if candidate_path in _record_paths(record):
            return record
    return None
