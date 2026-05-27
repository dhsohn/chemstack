from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.utils import (
    atomic_write_json,
    coerce_list as _shared_coerce_list,
    mapping_or_empty,
    now_utc_iso,
    normalize_text as _shared_normalize_text,
)

RECOVERY_PENDING_REASONS = frozenset({"worker_shutdown", "crashed_recovery"})


def normalize_text(value: Any) -> str:
    return _shared_normalize_text(value)


def coerce_dict(value: Any) -> dict[str, Any]:
    return dict(mapping_or_empty(value))


def coerce_list(value: Any) -> list[Any]:
    return _shared_coerce_list(value)


def write_json_artifact(job_dir: Path, filename: str, payload: dict[str, Any]) -> Path:
    path = job_dir / filename
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def write_text_artifact(job_dir: Path, filename: str, lines: list[str]) -> Path:
    path = job_dir / filename
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def load_json_mapping_artifact(job_dir: Path, filename: str) -> dict[str, Any] | None:
    path = job_dir / filename
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


@dataclass(frozen=True)
class EngineStateFiles:
    state_file_name: str
    report_json_file_name: str
    report_md_file_name: str
    organized_ref_file_name: str

    def write_state(self, job_dir: Path, payload: dict[str, Any]) -> Path:
        return write_json_artifact(job_dir, self.state_file_name, payload)

    def write_report_json(self, job_dir: Path, payload: dict[str, Any]) -> Path:
        return write_json_artifact(job_dir, self.report_json_file_name, payload)

    def write_report_md_lines(self, job_dir: Path, lines: list[str]) -> Path:
        return write_text_artifact(job_dir, self.report_md_file_name, lines)

    def write_organized_ref(self, job_dir: Path, payload: dict[str, Any]) -> Path:
        return write_json_artifact(job_dir, self.organized_ref_file_name, payload)

    def load_state(self, job_dir: Path) -> dict[str, Any] | None:
        return load_json_mapping_artifact(job_dir, self.state_file_name)

    def load_report_json(self, job_dir: Path) -> dict[str, Any] | None:
        return load_json_mapping_artifact(job_dir, self.report_json_file_name)

    def load_organized_ref(self, job_dir: Path) -> dict[str, Any] | None:
        return load_json_mapping_artifact(job_dir, self.organized_ref_file_name)


@dataclass(frozen=True)
class EngineStateAccess:
    files: EngineStateFiles
    report_title: str
    selected_input_label: str
    now_fn: Callable[[], str] = now_utc_iso

    def write_state(self, job_dir: Path, payload: dict[str, Any]) -> Path:
        return self.files.write_state(job_dir, payload)

    def write_report_json(self, job_dir: Path, payload: dict[str, Any]) -> Path:
        return self.files.write_report_json(job_dir, payload)

    def write_report_md(
        self,
        job_dir: Path,
        *,
        job_id: str,
        status: str,
        reason: str,
        selected_input: str,
    ) -> Path:
        lines = [
            f"# {self.report_title}",
            "",
            f"- Job ID: `{job_id}`",
            f"- Status: `{status}`",
            f"- Reason: `{reason}`",
            f"- {self.selected_input_label}: `{selected_input}`",
            f"- Updated At: `{self.now_fn()}`",
        ]
        return self.files.write_report_md_lines(job_dir, lines)

    def write_report_md_lines(self, job_dir: Path, lines: list[str]) -> Path:
        return self.files.write_report_md_lines(job_dir, lines)

    def write_organized_ref(self, job_dir: Path, payload: dict[str, Any]) -> Path:
        return self.files.write_organized_ref(job_dir, payload)

    def load_state(self, job_dir: Path) -> dict[str, Any] | None:
        return self.files.load_state(job_dir)

    def load_report_json(self, job_dir: Path) -> dict[str, Any] | None:
        return self.files.load_report_json(job_dir)

    def load_organized_ref(self, job_dir: Path) -> dict[str, Any] | None:
        return self.files.load_organized_ref(job_dir)


def create_engine_state_access(
    *,
    state_file_name: str,
    report_json_file_name: str,
    report_md_file_name: str,
    organized_ref_file_name: str,
    report_title: str,
    selected_input_label: str,
    now_fn: Callable[[], str] = now_utc_iso,
) -> EngineStateAccess:
    return EngineStateAccess(
        files=EngineStateFiles(
            state_file_name=state_file_name,
            report_json_file_name=report_json_file_name,
            report_md_file_name=report_md_file_name,
            organized_ref_file_name=organized_ref_file_name,
        ),
        report_title=report_title,
        selected_input_label=selected_input_label,
        now_fn=now_fn,
    )


def state_matches_fields(state: dict[str, Any] | None, fields: dict[str, Any]) -> bool:
    if not isinstance(state, dict):
        return False
    for key, value in fields.items():
        if normalize_text(state.get(key)) != normalize_text(value):
            return False
    return True


def is_recovery_pending_state(state: dict[str, Any] | None) -> bool:
    if not isinstance(state, dict):
        return False
    if bool(state.get("recovery_pending")):
        return True
    status = normalize_text(state.get("status")).lower()
    reason = normalize_text(state.get("reason"))
    return status == "queued" and reason in RECOVERY_PENDING_REASONS


def manifest_path_from_existing(
    job_dir: Path,
    existing: dict[str, Any],
    *,
    manifest_filename: str,
) -> str:
    manifest_path = normalize_text(existing.get("manifest_path"))
    if manifest_path:
        return manifest_path
    manifest = (job_dir / manifest_filename).resolve()
    return str(manifest) if manifest.exists() else ""


RecoveryFieldMap = Mapping[str, Any] | Callable[[dict[str, Any]], Mapping[str, Any]]


def _resolve_recovery_fields(
    fields: RecoveryFieldMap | None,
    existing: dict[str, Any],
) -> dict[str, Any]:
    if fields is None:
        return {}
    resolved = fields(existing) if callable(fields) else fields
    return {str(key): value for key, value in resolved.items()}


def recovery_pending_payload(
    job_dir: Path,
    *,
    existing: dict[str, Any],
    job_id: str,
    selected_input_xyz: str | Path,
    reason: str,
    now: str,
    manifest_filename: str,
    identity_fields: dict[str, Any],
    retained_fields: dict[str, Any],
    resource_request: dict[str, Any] | None,
    resource_actual: dict[str, Any] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job_id": normalize_text(existing.get("job_id")) or normalize_text(job_id),
        "job_dir": str(job_dir.resolve()),
        "selected_input_xyz": normalize_text(selected_input_xyz),
        **{str(key): value for key, value in identity_fields.items()},
        "status": "queued",
        "reason": normalize_text(reason),
        "created_at": normalize_text(existing.get("created_at")) or now,
        "started_at": normalize_text(existing.get("started_at")),
        "updated_at": now,
        **{str(key): value for key, value in retained_fields.items()},
        "manifest_path": manifest_path_from_existing(job_dir, existing, manifest_filename=manifest_filename),
        "resource_request": coerce_dict(resource_request) or coerce_dict(existing.get("resource_request")),
        "resource_actual": coerce_dict(resource_actual) or coerce_dict(existing.get("resource_actual")),
        "recovery_pending": True,
        "recovery_reason": normalize_text(reason),
        "recovery_count": int(existing.get("recovery_count", 0) or 0) + 1,
    }
    return payload


@dataclass(frozen=True)
class EngineRecoveryPendingWriter:
    access: EngineStateAccess
    manifest_filename: str
    now_fn: Callable[[], str] = now_utc_iso

    def write(
        self,
        job_dir: Path,
        *,
        job_id: str,
        selected_input_xyz: str | Path,
        reason: str,
        identity_fields: RecoveryFieldMap,
        resource_request: dict[str, Any] | None,
        resource_actual: dict[str, Any] | None,
        retained_fields: RecoveryFieldMap | None = None,
    ) -> dict[str, Any]:
        existing = self.access.load_state(job_dir) or {}
        payload = recovery_pending_payload(
            job_dir,
            existing=existing,
            job_id=job_id,
            selected_input_xyz=selected_input_xyz,
            reason=reason,
            now=self.now_fn(),
            manifest_filename=self.manifest_filename,
            identity_fields=_resolve_recovery_fields(identity_fields, existing),
            retained_fields=_resolve_recovery_fields(retained_fields, existing),
            resource_request=resource_request,
            resource_actual=resource_actual,
        )
        self.access.write_state(job_dir, payload)
        return payload
