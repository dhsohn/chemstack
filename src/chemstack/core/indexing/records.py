from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import engine_artifacts as _engine_artifacts
from .location import JobLocationRecord
from .roots import normalize_text


@dataclass(frozen=True)
class EngineLocationSpec:
    app_name: str
    job_type_from_payload: Callable[[str], str]
    default_molecule_key: Callable[[Path, str], str]
    payload_kind_key: str
    payload_kind_default: str
    molecule_key_name: str


def resource_dict(max_cores: int, max_memory_gb: int) -> dict[str, int]:
    return {
        "max_cores": max(1, int(max_cores)),
        "max_memory_gb": max(1, int(max_memory_gb)),
    }


def resource_mapping(raw: object, *, fallback: dict[str, int] | None = None) -> dict[str, int]:
    return _engine_artifacts.resource_mapping(raw, fallback=fallback)


def build_job_location_record(
    *,
    existing: JobLocationRecord | None = None,
    job_id: str,
    app_name: str,
    job_type: str,
    status: str,
    job_dir: Path,
    selected_input_xyz: str,
    molecule_key: str = "",
    organized_output_dir: Path | None = None,
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
    default_molecule_key_fn: Callable[[Path, str], str] | None = None,
) -> JobLocationRecord:
    resolved_job_dir = job_dir.expanduser().resolve()
    existing_original = (
        Path(existing.original_run_dir).expanduser().resolve()
        if existing and existing.original_run_dir
        else None
    )
    original_run_dir = existing_original or resolved_job_dir

    existing_selected = normalize_text(existing.selected_input_xyz) if existing is not None else ""
    selected_input_xyz_text = normalize_text(selected_input_xyz) or existing_selected

    existing_key = normalize_text(existing.molecule_key) if existing is not None else ""
    molecule_key_text = normalize_text(molecule_key) or existing_key
    if not molecule_key_text and default_molecule_key_fn is not None:
        molecule_key_text = default_molecule_key_fn(original_run_dir, selected_input_xyz_text)

    existing_resource_request = dict(existing.resource_request) if existing is not None else {}
    existing_resource_actual = dict(existing.resource_actual) if existing is not None else {}
    resource_request_text = dict(resource_request or existing_resource_request)
    resource_actual_text = dict(resource_actual or existing_resource_actual or resource_request_text)

    organized_dir = organized_output_dir
    if organized_dir is None and existing is not None and existing.organized_output_dir:
        organized_dir = Path(existing.organized_output_dir).expanduser().resolve()

    latest_known_path = organized_dir or resolved_job_dir
    return JobLocationRecord(
        job_id=normalize_text(job_id),
        app_name=app_name,
        job_type=job_type,
        status=normalize_text(status),
        original_run_dir=str(original_run_dir),
        molecule_key=molecule_key_text,
        selected_input_xyz=selected_input_xyz_text,
        organized_output_dir=str(organized_dir.resolve()) if organized_dir is not None else "",
        latest_known_path=str(latest_known_path.resolve()),
        resource_request=resource_request_text,
        resource_actual=resource_actual_text,
    )


def build_engine_job_location_record(
    *,
    spec: EngineLocationSpec,
    existing: JobLocationRecord | None = None,
    job_id: str,
    status: str,
    job_dir: Path,
    payload_kind: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    return build_job_location_record(
        existing=existing,
        job_id=job_id,
        app_name=spec.app_name,
        job_type=spec.job_type_from_payload(payload_kind),
        status=status,
        job_dir=job_dir,
        selected_input_xyz=selected_input_xyz,
        molecule_key=molecule_key,
        organized_output_dir=organized_output_dir,
        resource_request=resource_request,
        resource_actual=resource_actual,
        default_molecule_key_fn=spec.default_molecule_key,
    )


def make_engine_record_builder(spec: EngineLocationSpec) -> Callable[..., JobLocationRecord]:
    def build_record_from_payload_kind(
        *,
        existing: JobLocationRecord | None = None,
        job_id: str,
        status: str,
        job_dir: Path,
        payload_kind: str,
        selected_input_xyz: str,
        organized_output_dir: Path | None = None,
        molecule_key: str = "",
        resource_request: dict[str, int] | None = None,
        resource_actual: dict[str, int] | None = None,
    ) -> JobLocationRecord:
        return build_engine_job_location_record(
            spec=spec,
            existing=existing,
            job_id=job_id,
            status=status,
            job_dir=job_dir,
            payload_kind=payload_kind,
            selected_input_xyz=selected_input_xyz,
            organized_output_dir=organized_output_dir,
            molecule_key=molecule_key,
            resource_request=resource_request,
            resource_actual=resource_actual,
        )

    return build_record_from_payload_kind


def engine_record_from_artifacts(
    *,
    spec: EngineLocationSpec,
    build_record_fn: Callable[..., JobLocationRecord],
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
    existing: JobLocationRecord | None = None,
    default_payload_kind: str | None = None,
) -> JobLocationRecord | None:
    return _engine_artifacts.engine_record_from_artifacts(
        spec=spec,
        build_record_fn=build_record_fn,
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        default_payload_kind=default_payload_kind,
    )


def collect_engine_reindex_payload(
    *,
    spec: EngineLocationSpec,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
) -> dict[str, Any] | None:
    return _engine_artifacts.collect_engine_reindex_payload(
        spec=spec,
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
    )


def collect_engine_reindex_payload_for_dir(
    *,
    spec: EngineLocationSpec,
    job_dir: Path,
    load_state_fn: Callable[[Path], dict[str, Any] | None],
    load_report_json_fn: Callable[[Path], dict[str, Any] | None],
    load_organized_ref_fn: Callable[[Path], dict[str, Any] | None],
) -> dict[str, Any] | None:
    resolved_job_dir = job_dir.expanduser().resolve()
    return collect_engine_reindex_payload(
        spec=spec,
        job_dir=resolved_job_dir,
        state=load_state_fn(resolved_job_dir),
        report=load_report_json_fn(resolved_job_dir),
        organized_ref=load_organized_ref_fn(resolved_job_dir),
    )


__all__ = [
    "EngineLocationSpec",
    "build_engine_job_location_record",
    "build_job_location_record",
    "collect_engine_reindex_payload",
    "collect_engine_reindex_payload_for_dir",
    "engine_record_from_artifacts",
    "make_engine_record_builder",
    "resource_dict",
    "resource_mapping",
]
