from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord


@dataclass(frozen=True)
class LoadedArtifactFiles:
    job_dir: Path
    record: JobLocationRecord | None
    report: dict[str, Any]
    state: dict[str, Any]
    organized_ref: dict[str, Any]
    payload: dict[str, Any]


@dataclass(frozen=True)
class ContractArtifactBundle:
    job_dir: Path
    record: JobLocationRecord | None
    organized_ref: dict[str, Any]
    payload: dict[str, Any]
    latest_known_path: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]


def normalize_text(value: Any) -> str:
    return str(value).strip()


def load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def direct_dir_target(
    target: str,
    *,
    path_factory: Callable[[str], Any] = Path,
) -> Path | None:
    raw = normalize_text(target)
    if not raw:
        return None
    try:
        candidate = path_factory(raw).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() and candidate.is_dir() else None


def resolved_dir_candidates(
    values: Iterable[Any],
    *,
    path_factory: Callable[[str], Any] = Path,
) -> list[Path]:
    candidates: list[Path] = []
    for value in values:
        raw = normalize_text(value)
        if not raw:
            continue
        try:
            candidates.append(path_factory(raw).expanduser().resolve())
        except OSError:
            continue
    return candidates


def resolve_indexed_job_dir(
    index_root: Path,
    target: str,
    *,
    resolve_job_location_fn: Callable[[Path, str], JobLocationRecord | None],
    direct_path_target_fn: Callable[[str], Path | None],
    missing_label: str,
    path_factory: Callable[[str], Any] = Path,
) -> tuple[Path, JobLocationRecord | None]:
    record = resolve_job_location_fn(index_root, target)
    candidates: list[Path] = []
    if record is not None:
        candidates.extend(
            resolved_dir_candidates(
                (
                    record.latest_known_path,
                    record.organized_output_dir,
                    record.original_run_dir,
                ),
                path_factory=path_factory,
            )
        )
    direct = direct_path_target_fn(target)
    if direct is not None:
        candidates.append(direct)

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate, record
    raise FileNotFoundError(f"{missing_label} job directory not found for target: {target}")


def load_artifact_files(
    *,
    job_dir: Path,
    record: JobLocationRecord | None,
    load_json_dict_fn: Callable[[Path], dict[str, Any]],
    report_filename: str,
    state_filename: str,
    organized_ref_filename: str,
    missing_label: str,
    select_payload_fn: Callable[
        [dict[str, Any], dict[str, Any], dict[str, Any]], dict[str, Any]
    ]
    | None = None,
) -> LoadedArtifactFiles:
    report = load_json_dict_fn(job_dir / report_filename)
    state = load_json_dict_fn(job_dir / state_filename)
    organized_ref = load_json_dict_fn(job_dir / organized_ref_filename)
    payload = (
        select_payload_fn(report, state, organized_ref)
        if select_payload_fn is not None
        else report or state or organized_ref
    )
    if not payload:
        raise FileNotFoundError(f"{missing_label} artifact files not found in job directory: {job_dir}")
    return LoadedArtifactFiles(
        job_dir=job_dir,
        record=record,
        report=report,
        state=state,
        organized_ref=organized_ref,
        payload=payload,
    )


def validate_record_app(
    record: JobLocationRecord | None,
    expected_app_name: str,
    *,
    label: str,
) -> None:
    if record is not None and record.app_name and record.app_name != expected_app_name:
        raise ValueError(f"Expected {expected_app_name} index record, got: {record.app_name}")


def latest_known_path(record: JobLocationRecord | None, job_dir: Path) -> str:
    return normalize_text((record.latest_known_path if record is not None else "") or str(job_dir))


def load_contract_artifact_bundle(
    *,
    index_root: str | Path,
    target: str,
    resolve_job_dir_fn: Callable[[Path, str], tuple[Path, JobLocationRecord | None]],
    load_json_dict_fn: Callable[[Path], dict[str, Any]],
    report_filename: str,
    state_filename: str,
    organized_ref_filename: str,
    missing_label: str,
    expected_app_name: str,
    coerce_resource_dict_fn: Callable[[Any], dict[str, int]],
    select_payload_fn: Callable[
        [dict[str, Any], dict[str, Any], dict[str, Any]], dict[str, Any]
    ]
    | None = None,
) -> ContractArtifactBundle:
    resolved_index_root = Path(index_root).expanduser().resolve()
    job_dir, record = resolve_job_dir_fn(resolved_index_root, target)
    loaded = load_artifact_files(
        job_dir=job_dir,
        record=record,
        load_json_dict_fn=load_json_dict_fn,
        report_filename=report_filename,
        state_filename=state_filename,
        organized_ref_filename=organized_ref_filename,
        missing_label=missing_label,
        select_payload_fn=select_payload_fn,
    )
    validate_record_app(record, expected_app_name, label=missing_label)
    resource_request = coerce_resource_dict_fn(
        loaded.payload.get("resource_request")
    ) or coerce_resource_dict_fn(record.resource_request if record is not None else {})
    resource_actual = (
        coerce_resource_dict_fn(loaded.payload.get("resource_actual"))
        or coerce_resource_dict_fn(record.resource_actual if record is not None else {})
        or dict(resource_request)
    )
    return ContractArtifactBundle(
        job_dir=job_dir,
        record=record,
        organized_ref=loaded.organized_ref,
        payload=loaded.payload,
        latest_known_path=latest_known_path(record, job_dir),
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
