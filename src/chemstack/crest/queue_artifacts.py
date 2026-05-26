from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution

from .runner import CrestRunResult
from .state import (
    is_recovery_pending,
    load_state,
    state_matches_job,
    write_report_json,
    write_report_md_lines,
    write_state,
)


def coerce_mapping(value: Any) -> dict[str, Any]:
    return _queue_execution.coerce_mapping(value)


def matching_result_state(
    entry: Any,
    result: CrestRunResult,
    job_dir: Path,
    *,
    load_state_fn: Any = load_state,
    state_matches_job_fn: Any = state_matches_job,
) -> dict[str, Any]:
    return _queue_execution.load_matching_state(
        job_dir,
        load_state_fn=load_state_fn,
        state_matches_job_fn=state_matches_job_fn,
        match_kwargs={
            "selected_input_xyz": result.selected_input_xyz,
            "mode": result.mode,
            "molecule_key": _engine_execution.entry_metadata_text(entry, "molecule_key"),
        },
    )


def build_state_payload(
    entry: Any,
    result: CrestRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_state = coerce_mapping(previous_state)
    return _engine_execution.build_terminal_state_payload(
        entry,
        result,
        job_dir_text=_engine_execution.entry_metadata_text(entry, "job_dir"),
        selected_input_xyz=result.selected_input_xyz,
        previous_state=base_state,
        resumed=bool(base_state.get("resumed", False)),
        engine_fields={
            "molecule_key": _engine_execution.entry_metadata_text(entry, "molecule_key"),
            "mode": result.mode,
        },
        detail_fields={
            "retained_conformer_count": result.retained_conformer_count,
            "retained_conformer_paths": list(result.retained_conformer_paths),
        },
    )


def build_report_payload(
    entry: Any,
    result: CrestRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_state = coerce_mapping(previous_state)
    return _engine_execution.build_terminal_report_payload(
        entry,
        result,
        selected_input_xyz=result.selected_input_xyz,
        previous_state=base_state,
        resumed=bool(base_state.get("resumed", False)),
        engine_fields={
            "mode": result.mode,
            "molecule_key": _engine_execution.entry_metadata_text(entry, "molecule_key"),
        },
        detail_fields={
            "retained_conformer_count": result.retained_conformer_count,
            "retained_conformer_paths": list(result.retained_conformer_paths),
        },
    )


def report_lines(entry: Any, result: CrestRunResult) -> list[str]:
    lines = [
        "# ChemStack CREST Report",
        "",
        f"- Job ID: `{entry.task_id}`",
        f"- Queue ID: `{entry.queue_id}`",
        f"- Status: `{result.status}`",
        f"- Reason: `{result.reason}`",
        f"- Mode: `{result.mode}`",
        f"- Selected XYZ: `{Path(result.selected_input_xyz).name}`",
        f"- Molecule Key: `{_engine_execution.entry_metadata_text(entry, 'molecule_key') or '-'}`",
        f"- Exit Code: `{result.exit_code}`",
        f"- Retained Conformers: `{result.retained_conformer_count}`",
        f"- Resource Request: `{result.resource_request}`",
        f"- Resource Actual: `{result.resource_actual}`",
        f"- Stdout Log: `{result.stdout_log}`",
        f"- Stderr Log: `{result.stderr_log}`",
    ]
    if result.retained_conformer_paths:
        lines.append("- Retained Files:")
        for path in result.retained_conformer_paths:
            lines.append(f"  - `{path}`")
    return lines


def write_execution_artifacts(
    entry: Any,
    result: CrestRunResult,
    *,
    load_state_fn: Any = load_state,
    state_matches_job_fn: Any = state_matches_job,
    write_state_fn: Any = write_state,
    write_report_json_fn: Any = write_report_json,
    write_report_md_lines_fn: Any = write_report_md_lines,
) -> None:
    job_dir_text = _engine_execution.entry_metadata_text(entry, "job_dir")
    if not job_dir_text:
        return

    job_dir = Path(job_dir_text).expanduser().resolve()
    previous_state = matching_result_state(
        entry,
        result,
        job_dir,
        load_state_fn=load_state_fn,
        state_matches_job_fn=state_matches_job_fn,
    )
    _queue_execution.write_result_artifacts(
        job_dir_text,
        state_payload=build_state_payload(entry, result, previous_state=previous_state),
        report_payload=build_report_payload(entry, result, previous_state=previous_state),
        report_lines=report_lines(entry, result),
        write_state_fn=write_state_fn,
        write_report_json_fn=write_report_json_fn,
        write_report_md_lines_fn=write_report_md_lines_fn,
    )


def depsafe_now_utc_iso() -> str:
    from chemstack.core.utils import now_utc_iso as dynamic_now_utc_iso

    return dynamic_now_utc_iso()


def resource_caps(cfg: Any) -> dict[str, int]:
    from .job_locations import resource_dict

    return _engine_execution.engine_resource_caps(cfg, resource_dict_fn=resource_dict)


def coerce_resource_dict(value: Any) -> dict[str, int]:
    return _engine_execution.coerce_resource_request(value)


def entry_resource_request(cfg: Any, entry: Any) -> dict[str, int]:
    return _engine_execution.entry_resource_request(
        cfg,
        entry,
        resource_caps_fn=resource_caps,
        coerce_resource_request_fn=coerce_resource_dict,
    )


def write_running_state(
    cfg: Any,
    entry: Any,
    *,
    load_state_fn: Any = load_state,
    state_matches_job_fn: Any = state_matches_job,
    is_recovery_pending_fn: Any = is_recovery_pending,
    write_state_fn: Any = write_state,
    now_utc_iso_fn: Any = depsafe_now_utc_iso,
) -> None:
    job_dir_text = _engine_execution.entry_metadata_text(entry, "job_dir")
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    resource_request = entry_resource_request(cfg, entry)
    previous_state = _queue_execution.load_matching_state(
        job_dir,
        load_state_fn=load_state_fn,
        state_matches_job_fn=state_matches_job_fn,
        match_kwargs={
            "selected_input_xyz": _engine_execution.entry_metadata_text(
                entry,
                "selected_input_xyz",
            ),
            "mode": _engine_execution.entry_metadata_text(entry, "mode", "standard"),
            "molecule_key": _engine_execution.entry_metadata_text(entry, "molecule_key"),
        },
    )
    resumed = bool(previous_state) and _engine_execution.is_resumed_state(
        previous_state,
        is_recovery_pending_fn=is_recovery_pending_fn,
    )
    started_at = entry.started_at or now_utc_iso_fn()
    updated_at = now_utc_iso_fn()
    write_state_fn(
        job_dir,
        _engine_execution.build_running_state_payload(
            entry,
            job_dir=job_dir,
            selected_input_xyz=_engine_execution.entry_metadata_text(
                entry,
                "selected_input_xyz",
            ),
            started_at=started_at,
            updated_at=updated_at,
            previous_state=previous_state,
            resumed=resumed,
            resource_request=resource_request,
            engine_fields={
                "molecule_key": _engine_execution.entry_metadata_text(entry, "molecule_key"),
                "mode": _engine_execution.entry_metadata_text(entry, "mode", "standard"),
            },
        ),
    )


__all__ = [
    "build_report_payload",
    "build_state_payload",
    "entry_resource_request",
    "matching_result_state",
    "resource_caps",
    "write_execution_artifacts",
    "write_running_state",
]
