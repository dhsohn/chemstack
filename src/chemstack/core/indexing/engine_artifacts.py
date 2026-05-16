from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from chemstack.core.utils.coercion import normalize_text as _shared_normalize_text

from .location import JobLocationRecord


def normalize_text(value: Any) -> str:
    return _shared_normalize_text(value, none="None")


def resource_mapping(raw: object, *, fallback: dict[str, int] | None = None) -> dict[str, int]:
    if not isinstance(raw, dict):
        return dict(fallback or {})
    return {str(key): int(value) for key, value in raw.items()}


def artifact_identity(
    *,
    spec: Any,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    existing: JobLocationRecord | None,
    fallback_payload_kind: str,
) -> tuple[str, str, str, str, str]:
    job_id = normalize_text(
        report.get("job_id")
        or state.get("job_id")
        or organized_ref.get("job_id")
        or (existing.job_id if existing else "")
    )
    status = (
        normalize_text(
            report.get("status") or state.get("status") or organized_ref.get("status") or "unknown"
        )
        or "unknown"
    )
    payload_kind = (
        normalize_text(
            report.get(spec.payload_kind_key)
            or state.get(spec.payload_kind_key)
            or organized_ref.get(spec.payload_kind_key)
            or fallback_payload_kind
        )
        or fallback_payload_kind
    )
    selected_input_xyz = normalize_text(
        report.get("selected_input_xyz")
        or state.get("selected_input_xyz")
        or organized_ref.get("selected_input_xyz")
        or (existing.selected_input_xyz if existing else "")
    )
    molecule_key = normalize_text(
        report.get(spec.molecule_key_name)
        or state.get(spec.molecule_key_name)
        or organized_ref.get(spec.molecule_key_name)
        or (existing.molecule_key if existing else "")
    )
    return job_id, status, payload_kind, selected_input_xyz, molecule_key


def artifact_original_run_dir(
    *,
    job_dir: Path,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    existing: JobLocationRecord | None,
) -> str:
    return normalize_text(
        report.get("original_run_dir")
        or state.get("original_run_dir")
        or organized_ref.get("original_run_dir")
        or (existing.original_run_dir if existing else "")
        or str(job_dir)
    )


def artifact_resources(
    *,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    existing: JobLocationRecord | None,
) -> tuple[dict[str, int], dict[str, int]]:
    resource_request = resource_mapping(
        report.get("resource_request")
        or state.get("resource_request")
        or organized_ref.get("resource_request"),
        fallback=dict(existing.resource_request) if existing is not None else {},
    )
    resource_actual = resource_mapping(
        report.get("resource_actual")
        or state.get("resource_actual")
        or organized_ref.get("resource_actual"),
        fallback=dict(existing.resource_actual) if existing is not None else {},
    )
    return resource_request, resource_actual


def engine_record_from_artifacts(
    *,
    spec: Any,
    build_record_fn: Callable[..., JobLocationRecord],
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
    existing: JobLocationRecord | None = None,
    default_payload_kind: str | None = None,
) -> JobLocationRecord | None:
    state = state or {}
    report = report or {}
    organized_ref = organized_ref or {}
    fallback_payload_kind = default_payload_kind or spec.payload_kind_default

    job_id, status, payload_kind, selected_input_xyz, molecule_key = artifact_identity(
        spec=spec,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        fallback_payload_kind=fallback_payload_kind,
    )
    if not job_id:
        return None

    original_run_dir = artifact_original_run_dir(
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
    )
    if not molecule_key:
        molecule_key = spec.default_molecule_key(Path(original_run_dir), selected_input_xyz)

    resource_request, resource_actual = artifact_resources(
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
    )

    organized_output_dir = normalize_text(
        report.get("organized_output_dir")
        or state.get("organized_output_dir")
        or organized_ref.get("organized_output_dir")
        or (existing.organized_output_dir if existing else "")
    )

    return build_record_fn(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=Path(original_run_dir),
        payload_kind=payload_kind,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=Path(organized_output_dir).expanduser().resolve()
        if organized_output_dir
        else None,
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def collect_engine_reindex_payload(
    *,
    spec: Any,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
) -> dict[str, Any] | None:
    state = state or {}
    report = report or {}
    organized_ref = organized_ref or {}

    job_id = normalize_text(
        report.get("job_id") or state.get("job_id") or organized_ref.get("job_id")
    )
    if not job_id:
        return None

    status = (
        normalize_text(report.get("status") or state.get("status") or organized_ref.get("status"))
        or "unknown"
    )
    payload_kind = (
        normalize_text(
            report.get(spec.payload_kind_key)
            or state.get(spec.payload_kind_key)
            or spec.payload_kind_default
        )
        or spec.payload_kind_default
    )
    selected_input_xyz = normalize_text(
        report.get("selected_input_xyz") or state.get("selected_input_xyz")
    )
    original_run_dir = normalize_text(
        report.get("original_run_dir") or state.get("original_run_dir") or job_dir
    )
    molecule_key = normalize_text(
        report.get(spec.molecule_key_name) or state.get(spec.molecule_key_name)
    )
    if not molecule_key:
        molecule_key = spec.default_molecule_key(Path(original_run_dir), selected_input_xyz)

    organized_output_dir = normalize_text(
        organized_ref.get("organized_output_dir")
        or report.get("organized_output_dir")
        or state.get("organized_output_dir")
    )
    return {
        "job_id": job_id,
        "status": status,
        spec.payload_kind_key: payload_kind,
        "job_dir": original_run_dir,
        "selected_input_xyz": selected_input_xyz,
        spec.molecule_key_name: molecule_key,
        "organized_output_dir": organized_output_dir,
        "resource_request": resource_mapping(
            report.get("resource_request")
            or state.get("resource_request")
            or organized_ref.get("resource_request"),
        ),
        "resource_actual": resource_mapping(
            report.get("resource_actual")
            or state.get("resource_actual")
            or organized_ref.get("resource_actual"),
        ),
    }
