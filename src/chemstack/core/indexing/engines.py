from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import engine_artifacts as _engine_artifacts
from .location import JobLocationRecord
from .roots import (
    append_unique_root,
    index_root_for_cfg,
    index_root_for_path,
    is_terminal_status,
    list_job_records_for_cfg,
    load_job_artifacts,
    load_job_artifacts_for_cfg,
    lookup_roots_for_target,
    normalize_identifier,
    normalize_text,
    resolve_job_location_for_cfg,
    resolve_latest_job_dir,
    runtime_roots_for_cfg,
)
from .store import get_job_location, list_job_locations, resolve_job_location, upsert_job_location


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


@dataclass(frozen=True)
class EngineLocationService:
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
        return _engine_artifacts.engine_record_from_artifacts(
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
        resolved_job_dir = job_dir.expanduser().resolve()
        return _engine_artifacts.collect_engine_reindex_payload(
            spec=self.spec,
            job_dir=resolved_job_dir,
            state=self.load_state_fn(resolved_job_dir),
            report=self.load_report_json_fn(resolved_job_dir),
            organized_ref=self.load_organized_ref_fn(resolved_job_dir),
        )


@dataclass(frozen=True)
class EngineLocationModule:
    """Small adapter for engine modules that expose job-location helpers.

    xTB and CREST expose module-level functions for their engine-specific
    commands. This object centralizes the repeated delegation while each
    module remains free to pass monkeypatchable store
    functions such as ``resolve_job_location`` at call time.
    """

    service: EngineLocationService
    payload_kind_kwarg: str
    molecule_key_kwarg: str
    default_payload_kind_kwarg: str

    def build_job_location_record(self, **kwargs: Any) -> JobLocationRecord:
        return self.service.build_job_location_record(
            existing=kwargs.get("existing"),
            job_id=kwargs["job_id"],
            status=kwargs["status"],
            job_dir=kwargs["job_dir"],
            payload_kind=kwargs[self.payload_kind_kwarg],
            selected_input_xyz=kwargs["selected_input_xyz"],
            organized_output_dir=kwargs.get("organized_output_dir"),
            molecule_key=kwargs.get(self.molecule_key_kwarg, ""),
            resource_request=kwargs.get("resource_request"),
            resource_actual=kwargs.get("resource_actual"),
        )

    def upsert_job_record(
        self,
        cfg: Any,
        *,
        get_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None],
        upsert_job_location_fn: Callable[[str | Path, JobLocationRecord], JobLocationRecord],
        **kwargs: Any,
    ) -> JobLocationRecord:
        root = self.service.index_root_for_path(
            cfg,
            kwargs["job_dir"],
            kwargs.get("organized_output_dir"),
        )
        existing = get_job_location_fn(root, kwargs["job_id"])
        record = self.build_job_location_record(existing=existing, **kwargs)
        return upsert_job_location_fn(root, record)

    def list_job_records_for_cfg(
        self,
        cfg: Any,
        *,
        list_job_locations_fn: Callable[[str | Path], list[JobLocationRecord]],
    ) -> list[tuple[Path, JobLocationRecord]]:
        return list_job_records_for_cfg(
            cfg,
            engine=self.service.engine,
            list_job_locations_fn=list_job_locations_fn,
        )

    def resolve_job_location_for_cfg(
        self,
        cfg: Any,
        target: str,
        *,
        resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None],
    ) -> tuple[Path | None, JobLocationRecord | None]:
        return resolve_job_location_for_cfg(
            cfg,
            target,
            engine=self.service.engine,
            resolve_job_location_fn=resolve_job_location_fn,
        )

    def resolve_latest_job_dir(
        self,
        index_root: str | Path,
        target: str,
        *,
        resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None],
    ) -> Path | None:
        return resolve_latest_job_dir(
            index_root,
            target,
            resolve_job_location_fn=resolve_job_location_fn,
        )

    def load_job_artifacts(
        self,
        index_root: str | Path,
        target: str,
        *,
        load_state_fn: Callable[[Path], dict[str, Any] | None],
        load_report_json_fn: Callable[[Path], dict[str, Any] | None],
        resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None],
    ) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
        return load_job_artifacts(
            index_root,
            target,
            load_state_fn=load_state_fn,
            load_report_json_fn=load_report_json_fn,
            resolve_latest_job_dir_fn=lambda root, lookup_target: self.resolve_latest_job_dir(
                root,
                lookup_target,
                resolve_job_location_fn=resolve_job_location_fn,
            ),
        )

    def load_job_artifacts_for_cfg(
        self,
        cfg: Any,
        target: str,
        *,
        load_state_fn: Callable[[Path], dict[str, Any] | None],
        load_report_json_fn: Callable[[Path], dict[str, Any] | None],
        resolve_job_location_fn: Callable[[str | Path, str], JobLocationRecord | None],
    ) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None, JobLocationRecord | None]:
        return load_job_artifacts_for_cfg(
            cfg,
            target,
            engine=self.service.engine,
            load_state_fn=load_state_fn,
            load_report_json_fn=load_report_json_fn,
            resolve_latest_job_dir_fn=lambda root, lookup_target: self.resolve_latest_job_dir(
                root,
                lookup_target,
                resolve_job_location_fn=resolve_job_location_fn,
            ),
            resolve_job_location_fn=resolve_job_location_fn,
        )

    def record_from_artifacts(
        self,
        *,
        job_dir: Path,
        state: dict[str, Any] | None,
        report: dict[str, Any] | None,
        organized_ref: dict[str, Any] | None,
        existing: JobLocationRecord | None = None,
        **kwargs: Any,
    ) -> JobLocationRecord | None:
        return self.service.record_from_artifacts(
            job_dir=job_dir,
            state=state,
            report=report,
            organized_ref=organized_ref,
            existing=existing,
            default_payload_kind=kwargs.get(self.default_payload_kind_kwarg),
        )


__all__ = [
    "EngineLocationService",
    "EngineLocationModule",
    "EngineLocationSpec",
    "append_unique_root",
    "build_engine_job_location_record",
    "build_job_location_record",
    "index_root_for_cfg",
    "index_root_for_path",
    "is_terminal_status",
    "list_job_records_for_cfg",
    "load_job_artifacts",
    "load_job_artifacts_for_cfg",
    "lookup_roots_for_target",
    "normalize_identifier",
    "normalize_text",
    "resolve_job_location_for_cfg",
    "resolve_latest_job_dir",
    "resource_dict",
    "runtime_roots_for_cfg",
]
