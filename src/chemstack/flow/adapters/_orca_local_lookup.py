from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord


def _orca_module():
    from . import orca as o

    return o


def load_json_dict_impl(path: Path) -> dict[str, Any]:
    o = _orca_module()
    if not path.exists():
        return {}
    try:
        raw = o.json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def load_json_list_impl(path: Path) -> list[dict[str, Any]]:
    o = _orca_module()
    if not path.exists():
        return []
    try:
        raw = o.json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def load_jsonl_records_impl(path: Path) -> list[dict[str, Any]]:
    o = _orca_module()
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return records
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            raw = o.json.loads(stripped)
        except o.json.JSONDecodeError:
            continue
        if isinstance(raw, dict):
            records.append(raw)
    return records


def resolve_job_dir_impl(index_root: Path | None, target: str) -> tuple[Path | None, JobLocationRecord | None]:
    o = _orca_module()
    record: JobLocationRecord | None = None
    candidates: list[Path] = []

    def _record_from_raw(raw: dict[str, Any]) -> JobLocationRecord:
        return JobLocationRecord(
            job_id=o._normalize_text(raw.get("job_id")),
            app_name=o._normalize_text(raw.get("app_name")),
            job_type=o._normalize_text(raw.get("job_type")),
            status=o._normalize_text(raw.get("status")),
            original_run_dir=o._normalize_text(raw.get("original_run_dir")),
            molecule_key=o._normalize_text(raw.get("molecule_key")),
            selected_input_xyz=o._normalize_text(raw.get("selected_input_xyz")),
            organized_output_dir=o._normalize_text(raw.get("organized_output_dir")),
            latest_known_path=o._normalize_text(raw.get("latest_known_path")),
            resource_request={
                str(key): int(value)
                for key, value in raw.get("resource_request", {}).items()
                if str(key).strip()
            }
            if isinstance(raw.get("resource_request"), dict)
            else {},
            resource_actual={
                str(key): int(value)
                for key, value in raw.get("resource_actual", {}).items()
                if str(key).strip()
            }
            if isinstance(raw.get("resource_actual"), dict)
            else {},
        )

    def _candidate_dirs(record: JobLocationRecord) -> list[Path]:
        rows: list[Path] = []
        for value in (record.latest_known_path, record.organized_output_dir, record.original_run_dir):
            raw = o._normalize_text(value)
            if not raw:
                continue
            try:
                candidate = o.Path(raw).expanduser().resolve()
            except OSError:
                continue
            rows.append(candidate)
        return rows

    def _run_id_matches(candidate: Path, target_text: str) -> bool:
        if candidate.name == target_text:
            return True
        for file_name in (o.STATE_FILE_NAME, o.REPORT_JSON_FILE_NAME):
            payload = o._load_json_dict(candidate / file_name)
            if o._normalize_text(payload.get("run_id")) == target_text:
                return True
        return False

    if index_root is not None:
        try:
            record = o.resolve_job_location(index_root, target)
        except Exception:
            record = None
    if record is not None:
        candidates.extend(_candidate_dirs(record))
    elif index_root is not None:
        target_text = o._normalize_text(target)
        for raw_record in reversed(o._load_json_list(index_root / "job_locations.json")):
            fallback_record = _record_from_raw(raw_record)
            fallback_candidates = _candidate_dirs(fallback_record)
            matched = next(
                (
                    candidate
                    for candidate in fallback_candidates
                    if candidate.is_dir() and _run_id_matches(candidate, target_text)
                ),
                None,
            )
            if matched is None and fallback_record.original_run_dir:
                try:
                    stub_dir = o.Path(fallback_record.original_run_dir).expanduser().resolve()
                except OSError:
                    stub_dir = None
                if stub_dir is not None:
                    organized_ref = o._load_json_dict(stub_dir / o.ORGANIZED_REF_FILE_NAME)
                    if o._normalize_text(organized_ref.get("run_id")) == target_text:
                        matched = next((candidate for candidate in fallback_candidates if candidate.is_dir()), stub_dir)
            if matched is None:
                continue
            record = fallback_record
            candidates.extend([matched, *fallback_candidates])
            break

    direct_target = o._direct_dir_target(target)
    if direct_target is not None:
        candidates.append(direct_target)

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate, record
    return direct_target, record


def find_queue_entry_impl(
    *,
    allowed_root: Path | None,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    o = _orca_module()
    if allowed_root is None:
        return None
    entries = o._load_json_list(allowed_root / o.QUEUE_FILE_NAME)
    if not entries:
        return None

    direct_target = o._direct_dir_target(target)
    resolved_reaction_dir = o._resolve_candidate_path(reaction_dir)

    def _entry_matches(entry: dict[str, Any]) -> bool:
        entry_queue_id = o._normalize_text(entry.get("queue_id"))
        entry_task_id = o._normalize_text(entry.get("task_id"))
        entry_run_id = o._normalize_text(entry.get("run_id"))
        entry_reaction_dir_text = o._normalize_text(entry.get("reaction_dir"))
        entry_reaction_dir = o._resolve_candidate_path(entry_reaction_dir_text)

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
            return entry
    return None


def find_organized_record_impl(
    *,
    organized_root: Path | None,
    target: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    o = _orca_module()
    if organized_root is None:
        return None
    records = o._load_jsonl_records(organized_root / o.INDEX_DIR_NAME / o.RECORDS_FILE_NAME)
    if not records:
        return None

    direct_target = o._direct_dir_target(target)
    resolved_reaction_dir = o._resolve_candidate_path(reaction_dir)

    def _record_dir(record: dict[str, Any]) -> Path | None:
        reaction_dir_text = o._normalize_text(record.get("reaction_dir"))
        if reaction_dir_text:
            try:
                return o.Path(reaction_dir_text).expanduser().resolve()
            except OSError:
                pass
        organized_path = o._normalize_text(record.get("organized_path"))
        if organized_path:
            try:
                return (organized_root / organized_path).expanduser().resolve()
            except OSError:
                return None
        return None

    for record in reversed(records):
        record_run_id = o._normalize_text(record.get("run_id"))
        record_dir = _record_dir(record)
        if run_id and record_run_id == run_id:
            return record
        if target and record_run_id == target:
            return record
        if direct_target is not None and record_dir == direct_target:
            return record
        if resolved_reaction_dir is not None and record_dir == resolved_reaction_dir:
            return record
    return None


def organized_dir_from_record_impl(organized_root: Path | None, record: dict[str, Any] | None) -> Path | None:
    o = _orca_module()
    if record is None:
        return None
    reaction_dir_text = o._normalize_text(record.get("reaction_dir"))
    if reaction_dir_text:
        try:
            candidate = o.Path(reaction_dir_text).expanduser().resolve()
        except OSError:
            candidate = None
        if candidate is not None:
            return candidate
    organized_path = o._normalize_text(record.get("organized_path"))
    if organized_root is None or not organized_path:
        return None
    try:
        return (organized_root / organized_path).expanduser().resolve()
    except OSError:
        return None


def record_organized_dir_impl(record: JobLocationRecord | None) -> Path | None:
    o = _orca_module()
    if record is None:
        return None
    for value in (record.latest_known_path, record.organized_output_dir):
        raw = o._normalize_text(value)
        if not raw:
            continue
        try:
            candidate = Path(raw).expanduser().resolve()
        except OSError:
            continue
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def load_tracked_organized_ref_impl(record: JobLocationRecord | None, current_dir: Path | None) -> dict[str, Any]:
    o = _orca_module()
    if record is None:
        return {}
    original_run_dir = o._normalize_text(record.original_run_dir)
    if not original_run_dir:
        return {}
    try:
        stub_dir = o.Path(original_run_dir).expanduser().resolve()
    except OSError:
        return {}
    if current_dir is not None and stub_dir == current_dir.resolve():
        return {}
    return o._load_json_dict(stub_dir / o.ORGANIZED_REF_FILE_NAME)


__all__ = [
    "find_organized_record_impl",
    "find_queue_entry_impl",
    "load_json_dict_impl",
    "load_json_list_impl",
    "load_jsonl_records_impl",
    "load_tracked_organized_ref_impl",
    "organized_dir_from_record_impl",
    "record_organized_dir_impl",
    "resolve_job_dir_impl",
]
