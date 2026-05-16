from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class EngineSummarySpec:
    key_label: str
    record_key_labels: tuple[str, ...]
    kind_label: str
    count_label: str
    optional_artifact_fields: tuple[str, ...] = ()


def _record_key(record: Any) -> str:
    return record.molecule_key if record is not None else ""


def summary_payload(
    *,
    target: str,
    job_dir: Any,
    state: Any,
    report: Any,
    record: Any,
    spec: EngineSummarySpec,
) -> dict[str, Any]:
    record_key = _record_key(record)
    index_record = None
    if record is not None:
        index_record = {
            "job_id": record.job_id,
            "status": record.status,
            "job_type": record.job_type,
            "original_run_dir": record.original_run_dir,
            "selected_input_xyz": record.selected_input_xyz,
            "organized_output_dir": record.organized_output_dir,
            "latest_known_path": record.latest_known_path,
            "resource_request": record.resource_request,
            "resource_actual": record.resource_actual,
        }
        for label in spec.record_key_labels:
            index_record[label] = record_key
    return {
        "target": target,
        "job_dir": str(job_dir),
        "index_record": index_record,
        "state": state,
        "report": report,
    }


def print_index_record_summary(record: Any, *, spec: EngineSummarySpec) -> None:
    if record is None:
        return
    print(f"job_id: {record.job_id}")
    print(f"latest_known_path: {record.latest_known_path}")
    print(f"{spec.key_label}: {_record_key(record) or '-'}")
    print(f"selected_input_xyz: {record.selected_input_xyz or '-'}")
    if record.organized_output_dir:
        print(f"organized_output_dir: {record.organized_output_dir}")
    if record.resource_request:
        print(f"resource_request: {record.resource_request}")
    if record.resource_actual:
        print(f"resource_actual: {record.resource_actual}")


def print_optional_artifact_field(
    label: str, state: dict[str, Any], report: dict[str, Any]
) -> None:
    value = report.get(label) or state.get(label)
    if value:
        print(f"{label}: {value}")


def print_text_summary(
    *,
    job_dir: Any,
    state: Any,
    report: Any,
    record: Any,
    spec: EngineSummarySpec,
) -> None:
    state = state or {}
    report = report or {}
    record_key = _record_key(record)
    print(f"job_dir: {job_dir}")
    print_index_record_summary(record, spec=spec)
    print(f"status: {report.get('status') or state.get('status') or '-'}")
    print(f"reason: {report.get('reason') or state.get('reason') or '-'}")
    print(f"{spec.kind_label}: {report.get(spec.kind_label) or state.get(spec.kind_label) or '-'}")
    print(
        f"{spec.key_label}: "
        f"{report.get(spec.key_label) or state.get(spec.key_label) or record_key or '-'}"
    )
    print(
        f"selected_input_xyz: "
        f"{report.get('selected_input_xyz') or state.get('selected_input_xyz') or '-'}"
    )
    print(f"{spec.count_label}: {report.get(spec.count_label) or state.get(spec.count_label) or 0}")
    for label in spec.optional_artifact_fields:
        print_optional_artifact_field(label, state, report)
    print(f"stdout_log: {report.get('stdout_log') or '-'}")
    print(f"stderr_log: {report.get('stderr_log') or '-'}")


def cmd_summary(
    args: Any,
    *,
    load_config_fn: Callable[[Any], Any],
    resolve_job_location_for_cfg_fn: Callable[[Any, str], tuple[Any, Any]],
    load_job_artifacts_for_cfg_fn: Callable[[Any, str], tuple[Any, Any, Any, Any]],
    spec: EngineSummarySpec,
) -> int:
    cfg = load_config_fn(getattr(args, "config", None))
    target = str(getattr(args, "target", "")).strip()
    if not target:
        print("error: summary requires a job_id or job directory")
        return 1

    _root, record = resolve_job_location_for_cfg_fn(cfg, target)
    job_dir, state, report, record = load_job_artifacts_for_cfg_fn(cfg, target)
    if job_dir is None:
        print(f"error: job not found: {target}")
        return 1

    if bool(getattr(args, "json", False)):
        payload = summary_payload(
            target=target,
            job_dir=job_dir,
            state=state,
            report=report,
            record=record,
            spec=spec,
        )
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print_text_summary(
        job_dir=job_dir,
        state=state,
        report=report,
        record=record,
        spec=spec,
    )
    return 0
