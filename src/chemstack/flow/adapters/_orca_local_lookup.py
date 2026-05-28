from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location
from chemstack.core.queue.metadata import (
    mapping_metadata as queue_entry_metadata_impl,
    mapping_metadata_value as queue_entry_metadata_value_impl,
)
from chemstack.core.utils.coercion import normalize_text

from ._orca_path_helpers import direct_dir_target_impl, resolve_candidate_path_impl

QUEUE_FILE_NAME = "queue.json"
ORGANIZED_REF_FILE_NAME = "organized_ref.json"
INDEX_DIR_NAME = "index"
RECORDS_FILE_NAME = "records.jsonl"

JsonPayload = dict[str, Any]
JsonPayloadList = list[JsonPayload]


def load_json_dict_impl(path: Path) -> JsonPayload:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def load_json_list_impl(path: Path) -> JsonPayloadList:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def load_jsonl_records_impl(path: Path) -> JsonPayloadList:
    if not path.exists():
        return []
    records: JsonPayloadList = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return records
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            raw = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(raw, dict):
            records.append(raw)
    return records


def _record_candidate_dirs(record: JobLocationRecord) -> list[Path]:
    rows: list[Path] = []
    for value in (record.latest_known_path, record.organized_output_dir, record.original_run_dir):
        raw = normalize_text(value)
        if not raw:
            continue
        try:
            rows.append(Path(raw).expanduser().resolve())
        except OSError:
            continue
    return rows


def _resolve_record_for_target(index_root: Path | None, target: str) -> JobLocationRecord | None:
    if index_root is None:
        return None
    try:
        return resolve_job_location(index_root, target)
    except Exception:
        return None


def resolve_job_dir_impl(
    index_root: Path | None, target: str
) -> tuple[Path | None, JobLocationRecord | None]:
    candidates: list[Path] = []
    record = _resolve_record_for_target(index_root, target)
    if record is not None:
        candidates.extend(_record_candidate_dirs(record))

    direct_target = direct_dir_target_impl(target)
    if direct_target is not None:
        candidates.append(direct_target)

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate, record
    return direct_target, record


def _queue_entry_matches(
    entry: JsonPayload,
    *,
    target: str,
    queue_id: str,
    run_id: str,
    direct_target: Path | None,
    resolved_reaction_dir: Path | None,
) -> bool:
    entry_queue_id = normalize_text(entry.get("queue_id"))
    entry_task_id = normalize_text(entry.get("task_id"))
    entry_run_id = normalize_text(queue_entry_metadata_value_impl(entry, "run_id"))
    entry_reaction_dir = resolve_candidate_path_impl(
        normalize_text(queue_entry_metadata_value_impl(entry, "reaction_dir"))
    )

    return (
        (bool(queue_id) and entry_queue_id == queue_id)
        or (bool(target) and entry_queue_id == target)
        or (bool(target) and entry_task_id == target)
        or (bool(run_id) and entry_run_id == run_id)
        or (bool(target) and entry_run_id == target)
        or (resolved_reaction_dir is not None and entry_reaction_dir == resolved_reaction_dir)
        or (direct_target is not None and entry_reaction_dir == direct_target)
    )


def find_queue_entry_impl(
    *,
    allowed_root: Path | None,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> JsonPayload | None:
    if allowed_root is None:
        return None
    entries = load_json_list_impl(allowed_root / QUEUE_FILE_NAME)
    if not entries:
        return None

    direct_target = direct_dir_target_impl(target)
    resolved_reaction_dir = resolve_candidate_path_impl(reaction_dir)

    for entry in reversed(entries):
        if _queue_entry_matches(
            entry,
            target=target,
            queue_id=queue_id,
            run_id=run_id,
            direct_target=direct_target,
            resolved_reaction_dir=resolved_reaction_dir,
        ):
            return entry
    return None


def _organized_record_dir(organized_root: Path, record: JsonPayload) -> Path | None:
    reaction_dir_text = normalize_text(record.get("reaction_dir"))
    if reaction_dir_text:
        try:
            return Path(reaction_dir_text).expanduser().resolve()
        except OSError:
            pass
    organized_path = normalize_text(record.get("organized_path"))
    if organized_path:
        try:
            return (organized_root / organized_path).expanduser().resolve()
        except OSError:
            return None
    return None


def _organized_record_matches(
    record: JsonPayload,
    *,
    target: str,
    run_id: str,
    direct_target: Path | None,
    resolved_reaction_dir: Path | None,
    record_dir: Path | None,
) -> bool:
    record_run_id = normalize_text(record.get("run_id"))
    return (
        (bool(run_id) and record_run_id == run_id)
        or (bool(target) and record_run_id == target)
        or (direct_target is not None and record_dir == direct_target)
        or (resolved_reaction_dir is not None and record_dir == resolved_reaction_dir)
    )


def find_organized_record_impl(
    *,
    organized_root: Path | None,
    target: str,
    run_id: str,
    reaction_dir: str,
) -> JsonPayload | None:
    if organized_root is None:
        return None
    records = load_jsonl_records_impl(organized_root / INDEX_DIR_NAME / RECORDS_FILE_NAME)
    if not records:
        return None

    direct_target = direct_dir_target_impl(target)
    resolved_reaction_dir = resolve_candidate_path_impl(reaction_dir)

    for record in reversed(records):
        record_dir = _organized_record_dir(organized_root, record)
        if _organized_record_matches(
            record,
            target=target,
            run_id=run_id,
            direct_target=direct_target,
            resolved_reaction_dir=resolved_reaction_dir,
            record_dir=record_dir,
        ):
            return record
    return None


def organized_dir_from_record_impl(
    organized_root: Path | None, record: JsonPayload | None
) -> Path | None:
    if record is None:
        return None
    reaction_dir_text = normalize_text(record.get("reaction_dir"))
    if reaction_dir_text:
        try:
            candidate = Path(reaction_dir_text).expanduser().resolve()
        except OSError:
            candidate = None
        if candidate is not None:
            return candidate
    organized_path = normalize_text(record.get("organized_path"))
    if organized_root is None or not organized_path:
        return None
    try:
        return (organized_root / organized_path).expanduser().resolve()
    except OSError:
        return None


def record_organized_dir_impl(record: JobLocationRecord | None) -> Path | None:
    if record is None:
        return None
    for value in (record.latest_known_path, record.organized_output_dir):
        raw = normalize_text(value)
        if not raw:
            continue
        try:
            candidate = Path(raw).expanduser().resolve()
        except OSError:
            continue
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def load_tracked_organized_ref_impl(
    record: JobLocationRecord | None, current_dir: Path | None
) -> JsonPayload:
    if record is None:
        return {}
    original_run_dir = normalize_text(record.original_run_dir)
    if not original_run_dir:
        return {}
    try:
        stub_dir = Path(original_run_dir).expanduser().resolve()
    except OSError:
        return {}
    if current_dir is not None and stub_dir == current_dir.resolve():
        return {}
    return load_json_dict_impl(stub_dir / ORGANIZED_REF_FILE_NAME)


__all__ = [
    "find_organized_record_impl",
    "find_queue_entry_impl",
    "load_json_dict_impl",
    "load_json_list_impl",
    "load_jsonl_records_impl",
    "load_tracked_organized_ref_impl",
    "organized_dir_from_record_impl",
    "queue_entry_metadata_impl",
    "queue_entry_metadata_value_impl",
    "record_organized_dir_impl",
    "resolve_job_dir_impl",
]
