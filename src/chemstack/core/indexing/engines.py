from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.flow.state import (
    iter_workflow_runtime_workspaces,
    workflow_workspace_internal_engine_paths,
    workflow_workspace_internal_engine_paths_from_path,
)

from .location import JobLocationRecord
from .store import list_job_locations, resolve_job_location

_KEY_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


@dataclass(frozen=True)
class EngineLocationSpec:
    app_name: str
    job_type_from_payload: Callable[[str], str]
    default_molecule_key: Callable[[Path, str], str]
    payload_kind_key: str
    payload_kind_default: str
    molecule_key_name: str


def normalize_text(value: Any) -> str:
    return str(value).strip()


def normalize_identifier(value: str, *, default: str) -> str:
    collapsed = _KEY_RE.sub("_", normalize_text(value)).strip("._-")
    return collapsed.lower() or default


def index_root_for_cfg(cfg: Any) -> Path:
    return Path(cfg.runtime.allowed_root).expanduser().resolve()


def append_unique_root(roots: list[Path], candidate: Path) -> None:
    resolved = candidate.expanduser().resolve()
    if resolved not in roots:
        roots.append(resolved)


def runtime_roots_for_cfg(cfg: Any, *, engine: str) -> tuple[Path, ...]:
    workflow_root = normalize_text(getattr(cfg, "workflow_root", ""))
    if not workflow_root:
        return (index_root_for_cfg(cfg),)

    roots: list[Path] = []
    for workspace_dir in iter_workflow_runtime_workspaces(workflow_root, engine=engine):
        runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine=engine)
        append_unique_root(roots, runtime_paths["allowed_root"])
    return tuple(roots)


def index_root_for_path(
    cfg: Any,
    *paths: str | Path | None,
    engine: str,
) -> Path:
    workflow_root = normalize_text(getattr(cfg, "workflow_root", ""))
    if workflow_root:
        for raw_path in paths:
            text = normalize_text(raw_path)
            if not text:
                continue
            runtime_paths = workflow_workspace_internal_engine_paths_from_path(
                text,
                workflow_root=workflow_root,
                engine=engine,
            )
            if runtime_paths is None:
                continue
            return runtime_paths["allowed_root"].expanduser().resolve()
    return index_root_for_cfg(cfg)


def lookup_roots_for_target(cfg: Any, target: str, *, engine: str) -> tuple[Path, ...]:
    roots = list(runtime_roots_for_cfg(cfg, engine=engine))
    specific_root = index_root_for_path(cfg, target, engine=engine)
    if specific_root in roots:
        roots.remove(specific_root)
        roots.insert(0, specific_root)
    return tuple(roots)


def list_job_records_for_cfg(
    cfg: Any,
    *,
    engine: str,
    list_job_locations_fn: Callable[[str | Path], list[JobLocationRecord]] = list_job_locations,
) -> list[tuple[Path, JobLocationRecord]]:
    rows: list[tuple[Path, JobLocationRecord]] = []
    for root in runtime_roots_for_cfg(cfg, engine=engine):
        for record in list_job_locations_fn(root):
            rows.append((root, record))
    return rows


def resolve_job_location_for_cfg(
    cfg: Any,
    target: str,
    *,
    engine: str,
    resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None] = resolve_job_location,
) -> tuple[Path | None, JobLocationRecord | None]:
    for root in lookup_roots_for_target(cfg, target, engine=engine):
        record = resolve_job_location_fn(root, target)
        if record is not None:
            return root, record
    return None, None


def resource_dict(max_cores: int, max_memory_gb: int) -> dict[str, int]:
    return {
        "max_cores": max(1, int(max_cores)),
        "max_memory_gb": max(1, int(max_memory_gb)),
    }


def resource_mapping(raw: object, *, fallback: dict[str, int] | None = None) -> dict[str, int]:
    if not isinstance(raw, dict):
        return dict(fallback or {})
    return {str(key): int(value) for key, value in raw.items()}


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
    existing_original = Path(existing.original_run_dir).expanduser().resolve() if existing and existing.original_run_dir else None
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
    state = state or {}
    report = report or {}
    organized_ref = organized_ref or {}
    fallback_payload_kind = default_payload_kind or spec.payload_kind_default

    job_id = normalize_text(
        report.get("job_id")
        or state.get("job_id")
        or organized_ref.get("job_id")
        or (existing.job_id if existing else "")
    )
    if not job_id:
        return None

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

    original_run_dir = normalize_text(
        report.get("original_run_dir")
        or state.get("original_run_dir")
        or organized_ref.get("original_run_dir")
        or (existing.original_run_dir if existing else "")
        or str(job_dir)
    )
    if not molecule_key:
        molecule_key = spec.default_molecule_key(Path(original_run_dir), selected_input_xyz)

    resource_request = resource_mapping(
        report.get("resource_request") or state.get("resource_request") or organized_ref.get("resource_request"),
        fallback=dict(existing.resource_request) if existing is not None else {},
    )
    resource_actual = resource_mapping(
        report.get("resource_actual") or state.get("resource_actual") or organized_ref.get("resource_actual"),
        fallback=dict(existing.resource_actual) if existing is not None else {},
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
        organized_output_dir=Path(organized_output_dir).expanduser().resolve() if organized_output_dir else None,
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def collect_engine_reindex_payload(
    *,
    spec: EngineLocationSpec,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
) -> dict[str, Any] | None:
    state = state or {}
    report = report or {}
    organized_ref = organized_ref or {}

    job_id = normalize_text(report.get("job_id") or state.get("job_id") or organized_ref.get("job_id"))
    if not job_id:
        return None

    status = normalize_text(report.get("status") or state.get("status") or organized_ref.get("status")) or "unknown"
    payload_kind = (
        normalize_text(report.get(spec.payload_kind_key) or state.get(spec.payload_kind_key) or spec.payload_kind_default)
        or spec.payload_kind_default
    )
    selected_input_xyz = normalize_text(report.get("selected_input_xyz") or state.get("selected_input_xyz"))
    original_run_dir = normalize_text(report.get("original_run_dir") or state.get("original_run_dir") or job_dir)
    molecule_key = normalize_text(report.get(spec.molecule_key_name) or state.get(spec.molecule_key_name))
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
            report.get("resource_request") or state.get("resource_request") or organized_ref.get("resource_request"),
        ),
        "resource_actual": resource_mapping(
            report.get("resource_actual") or state.get("resource_actual") or organized_ref.get("resource_actual"),
        ),
    }


def resolve_latest_job_dir(
    index_root: str | Path,
    target: str,
    *,
    resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None] = resolve_job_location,
) -> Path | None:
    record = resolve_job_location_fn(index_root, target)
    if record is None:
        candidate = Path(normalize_text(target)).expanduser()
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
    *,
    load_state_fn: Callable[[Path], dict[str, Any] | None],
    load_report_json_fn: Callable[[Path], dict[str, Any] | None],
    resolve_latest_job_dir_fn: Callable[[str | Path, str], Path | None],
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    job_dir = resolve_latest_job_dir_fn(index_root, target)
    if job_dir is None:
        return None, None, None
    return job_dir, load_state_fn(job_dir), load_report_json_fn(job_dir)


def load_job_artifacts_for_cfg(
    cfg: Any,
    target: str,
    *,
    engine: str,
    load_state_fn: Callable[[Path], dict[str, Any] | None],
    load_report_json_fn: Callable[[Path], dict[str, Any] | None],
    resolve_latest_job_dir_fn: Callable[[str | Path, str], Path | None],
    resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None] = resolve_job_location,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None, JobLocationRecord | None]:
    resolved_record: JobLocationRecord | None = None
    for root in lookup_roots_for_target(cfg, target, engine=engine):
        record = resolve_job_location_fn(root, target)
        job_dir = resolve_latest_job_dir_fn(root, target)
        if job_dir is None:
            continue
        resolved_record = record
        return job_dir, load_state_fn(job_dir), load_report_json_fn(job_dir), resolved_record
    return None, None, None, resolved_record


def is_terminal_status(status: str) -> bool:
    return normalize_text(status).lower() in _TERMINAL_STATUSES
