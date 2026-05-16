from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.statuses import TERMINAL_STATUSES
from chemstack.core.utils.coercion import normalize_text as _shared_normalize_text
from chemstack.flow.state import (
    iter_workflow_runtime_workspaces,
    workflow_workspace_internal_engine_paths,
    workflow_workspace_internal_engine_paths_from_path,
)

from .location import JobLocationRecord
from .store import get_job_location, list_job_locations, resolve_job_location, upsert_job_location
from . import engine_artifacts as _engine_artifacts

_KEY_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TERMINAL_STATUSES = TERMINAL_STATUSES


@dataclass(frozen=True)
class EngineLocationSpec:
    app_name: str
    job_type_from_payload: Callable[[str], str]
    default_molecule_key: Callable[[Path, str], str]
    payload_kind_key: str
    payload_kind_default: str
    molecule_key_name: str


@dataclass(frozen=True)
class EngineLocationFacade:
    engine: str
    spec: EngineLocationSpec
    load_state_fn: Callable[[Path], dict[str, Any] | None]
    load_report_json_fn: Callable[[Path], dict[str, Any] | None]
    load_organized_ref_fn: Callable[[Path], dict[str, Any] | None]
    get_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None] = (
        get_job_location
    )
    list_job_locations_fn: Callable[[str | Path], list[JobLocationRecord]] = (
        list_job_locations
    )
    resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None] = (
        resolve_job_location
    )
    upsert_job_location_fn: Callable[[str | Path, JobLocationRecord], JobLocationRecord] = (
        upsert_job_location
    )

    def index_root_for_cfg(self, cfg: Any) -> Path:
        return index_root_for_cfg(cfg)

    def runtime_roots_for_cfg(self, cfg: Any) -> tuple[Path, ...]:
        return runtime_roots_for_cfg(cfg, engine=self.engine)

    def index_root_for_path(self, cfg: Any, *paths: str | Path | None) -> Path:
        return index_root_for_path(cfg, *paths, engine=self.engine)

    def lookup_roots_for_target(self, cfg: Any, target: str) -> tuple[Path, ...]:
        return lookup_roots_for_target(cfg, target, engine=self.engine)

    def list_job_records_for_cfg(self, cfg: Any) -> list[tuple[Path, JobLocationRecord]]:
        return list_job_records_for_cfg(
            cfg,
            engine=self.engine,
            list_job_locations_fn=self.list_job_locations_fn,
        )

    def resolve_job_location_for_cfg(
        self,
        cfg: Any,
        target: str,
    ) -> tuple[Path | None, JobLocationRecord | None]:
        return resolve_job_location_for_cfg(
            cfg,
            target,
            engine=self.engine,
            resolve_job_location_fn=self.resolve_job_location_fn,
        )

    def build_job_location_record(
        self,
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
            spec=self.spec,
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

    def upsert_job_record(
        self,
        cfg: Any,
        *,
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
        root = self.index_root_for_path(cfg, job_dir, organized_output_dir)
        existing = self.get_job_location_fn(root, job_id)
        record = self.build_job_location_record(
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
        return self.upsert_job_location_fn(root, record)

    def resolve_latest_job_dir(self, index_root: str | Path, target: str) -> Path | None:
        return resolve_latest_job_dir(
            index_root,
            target,
            resolve_job_location_fn=self.resolve_job_location_fn,
        )

    def load_job_artifacts(
        self,
        index_root: str | Path,
        target: str,
    ) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
        return load_job_artifacts(
            index_root,
            target,
            load_state_fn=self.load_state_fn,
            load_report_json_fn=self.load_report_json_fn,
            resolve_latest_job_dir_fn=self.resolve_latest_job_dir,
        )

    def load_job_artifacts_for_cfg(
        self,
        cfg: Any,
        target: str,
    ) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None, JobLocationRecord | None]:
        return load_job_artifacts_for_cfg(
            cfg,
            target,
            engine=self.engine,
            load_state_fn=self.load_state_fn,
            load_report_json_fn=self.load_report_json_fn,
            resolve_latest_job_dir_fn=self.resolve_latest_job_dir,
            resolve_job_location_fn=self.resolve_job_location_fn,
        )

    def record_from_artifacts(
        self,
        *,
        job_dir: Path,
        state: dict[str, Any] | None,
        report: dict[str, Any] | None,
        organized_ref: dict[str, Any] | None,
        existing: JobLocationRecord | None = None,
        default_payload_kind: str | None = None,
    ) -> JobLocationRecord | None:
        return engine_record_from_artifacts(
            spec=self.spec,
            build_record_fn=self.build_job_location_record,
            job_dir=job_dir,
            state=state,
            report=report,
            organized_ref=organized_ref,
            existing=existing,
            default_payload_kind=default_payload_kind,
        )

    def collect_reindex_payload(self, job_dir: Path) -> dict[str, Any] | None:
        return collect_engine_reindex_payload_for_dir(
            spec=self.spec,
            job_dir=job_dir,
            load_state_fn=self.load_state_fn,
            load_report_json_fn=self.load_report_json_fn,
            load_organized_ref_fn=self.load_organized_ref_fn,
        )


def normalize_text(value: Any) -> str:
    return _shared_normalize_text(value, none="None")


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
    resolve_job_location_fn: Callable[
        [str | Path, str], JobLocationRecord | None
    ] = resolve_job_location,
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
    resource_actual_text = dict(
        resource_actual or existing_resource_actual or resource_request_text
    )

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


def resolve_latest_job_dir(
    index_root: str | Path,
    target: str,
    *,
    resolve_job_location_fn: Callable[
        [str | Path, str], JobLocationRecord | None
    ] = resolve_job_location,
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
    resolve_job_location_fn: Callable[
        [str | Path, str], JobLocationRecord | None
    ] = resolve_job_location,
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
