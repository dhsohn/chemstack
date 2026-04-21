from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from chemstack.core.app_ids import ORCA_APP_NAMES


def attempt_count_impl(
    state: dict[str, Any],
    report: dict[str, Any],
    *,
    safe_int_fn: Callable[[Any, int], int],
) -> int:
    report_count = safe_int_fn(report.get("attempt_count"), -1)
    if report_count >= 0:
        return report_count
    attempts = state.get("attempts")
    if isinstance(attempts, list):
        return len(attempts)
    return 0


def max_retries_impl(
    state: dict[str, Any],
    report: dict[str, Any],
    *,
    safe_int_fn: Callable[[Any, int], int],
) -> int:
    report_value = safe_int_fn(report.get("max_retries"), -1)
    if report_value >= 0:
        return report_value
    return safe_int_fn(state.get("max_retries"), 0)


def coerce_attempts_impl(
    state: dict[str, Any],
    report: dict[str, Any],
    *,
    normalize_text_fn: Callable[[Any], str],
    safe_int_fn: Callable[[Any, int], int],
) -> tuple[dict[str, Any], ...]:
    raw_attempts = report.get("attempts")
    if not isinstance(raw_attempts, list):
        raw_attempts = state.get("attempts")
    if not isinstance(raw_attempts, list):
        return ()

    attempts: list[dict[str, Any]] = []
    for raw in raw_attempts:
        if not isinstance(raw, dict):
            continue
        index = safe_int_fn(raw.get("index"), 0)
        attempt_number = max(0, index - 1) if index > 0 else 0
        attempts.append(
            {
                "index": index,
                "attempt_number": attempt_number,
                "inp_path": normalize_text_fn(raw.get("inp_path")),
                "out_path": normalize_text_fn(raw.get("out_path")),
                "return_code": safe_int_fn(raw.get("return_code"), 0),
                "analyzer_status": normalize_text_fn(raw.get("analyzer_status")),
                "analyzer_reason": normalize_text_fn(raw.get("analyzer_reason")),
                "markers": list(raw["markers"]) if isinstance(raw.get("markers"), list) else [],
                "patch_actions": list(raw["patch_actions"]) if isinstance(raw.get("patch_actions"), list) else [],
                "started_at": normalize_text_fn(raw.get("started_at")),
                "ended_at": normalize_text_fn(raw.get("ended_at")),
            }
        )
    return tuple(attempts)


def final_result_payload_impl(state: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    payload = report.get("final_result")
    if not isinstance(payload, dict):
        payload = state.get("final_result")
    if not isinstance(payload, dict):
        return {}
    return {str(key): value for key, value in payload.items()}


def status_from_payloads_impl(
    *,
    queue_entry: dict[str, Any] | None,
    state: dict[str, Any],
    report: dict[str, Any],
    normalize_text_fn: Callable[[Any], str],
    normalize_bool_fn: Callable[[Any], bool],
) -> tuple[str, str, str, str]:
    queue_status = normalize_text_fn((queue_entry or {}).get("status")).lower()
    cancel_requested = normalize_bool_fn((queue_entry or {}).get("cancel_requested"))

    state_status = normalize_text_fn(state.get("status")).lower()
    report_status = normalize_text_fn(report.get("status")).lower()
    final_result = report.get("final_result") if isinstance(report.get("final_result"), dict) else state.get("final_result")
    final = final_result if isinstance(final_result, dict) else {}
    final_status = normalize_text_fn(final.get("status")).lower()
    analyzer_status = normalize_text_fn(final.get("analyzer_status"))
    reason = normalize_text_fn(final.get("reason"))
    completed_at = normalize_text_fn(final.get("completed_at"))

    if final_status in {"completed", "failed"}:
        return final_status, analyzer_status, reason, completed_at
    if queue_status == "cancelled":
        return "cancelled", analyzer_status, reason or "cancelled", completed_at
    if queue_status == "running" and cancel_requested:
        return "cancel_requested", analyzer_status, reason, completed_at
    if queue_status == "pending":
        return "queued", analyzer_status, reason, completed_at
    if queue_status == "running":
        return "running", analyzer_status, reason, completed_at
    if state_status in {"completed", "failed"}:
        return state_status, analyzer_status, reason, completed_at
    if state_status in {"created", "running", "retrying"}:
        return "running", analyzer_status, reason, completed_at
    if report_status in {"completed", "failed"}:
        return report_status, analyzer_status, reason, completed_at
    if queue_status:
        return queue_status, analyzer_status, reason, completed_at
    if state_status:
        return state_status, analyzer_status, reason, completed_at
    return "unknown", analyzer_status, reason, completed_at


def load_orca_artifact_contract_impl(
    *,
    target: str,
    orca_allowed_root: str | Path | None,
    orca_organized_root: str | Path | None,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
    path_type: type[Path],
    normalize_text_fn: Callable[[Any], str],
    normalize_bool_fn: Callable[[Any], bool],
    safe_int_fn: Callable[[Any, int], int],
    tracked_contract_payload_fn: Callable[..., dict[str, Any] | None],
    tracked_runtime_context_fn: Callable[..., tuple[Path | None, Any, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any] | None, Path | None] | None],
    tracked_artifact_context_fn: Callable[..., tuple[Path | None, Any, dict[str, Any], dict[str, Any], dict[str, Any]]],
    resolve_job_dir_fn: Callable[[Path | None, str], tuple[Path | None, Any]],
    find_queue_entry_fn: Callable[..., dict[str, Any] | None],
    resolve_candidate_path_fn: Callable[[Any], Path | None],
    direct_dir_target_fn: Callable[[str], Path | None],
    record_organized_dir_fn: Callable[[Any], Path | None],
    find_organized_record_fn: Callable[..., dict[str, Any] | None],
    organized_dir_from_record_fn: Callable[[Path | None, dict[str, Any] | None], Path | None],
    load_json_dict_fn: Callable[[Path], dict[str, Any]],
    load_tracked_organized_ref_fn: Callable[[Any, Path | None], dict[str, Any]],
    status_from_payloads_fn: Callable[..., tuple[str, str, str, str]],
    resolve_artifact_path_fn: Callable[[Any, Path | None], str],
    derive_selected_input_xyz_fn: Callable[[str], str],
    prefer_orca_optimized_xyz_fn: Callable[..., str],
    is_subpath_fn: Callable[[Path, Path | None], bool],
    coerce_resource_dict_fn: Callable[[Any], dict[str, int]],
    attempt_count_fn: Callable[[dict[str, Any], dict[str, Any]], int],
    max_retries_fn: Callable[[dict[str, Any], dict[str, Any]], int],
    coerce_attempts_fn: Callable[[dict[str, Any], dict[str, Any]], tuple[dict[str, Any], ...]],
    final_result_payload_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    contract_cls: type,
) -> Any:
    allowed_root = path_type(orca_allowed_root).expanduser().resolve() if orca_allowed_root else None
    organized_root = path_type(orca_organized_root).expanduser().resolve() if orca_organized_root else None

    tracked_payload = tracked_contract_payload_fn(
        index_root=allowed_root,
        organized_root=organized_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )
    if tracked_payload is not None:
        attempts_payload = tracked_payload.get("attempts")
        attempts = tuple(
            dict(item) for item in attempts_payload if isinstance(item, dict)
        ) if isinstance(attempts_payload, list) else ()
        final_result = tracked_payload.get("final_result")
        return contract_cls(
            run_id=normalize_text_fn(tracked_payload.get("run_id")),
            status=normalize_text_fn(tracked_payload.get("status")) or "unknown",
            reason=normalize_text_fn(tracked_payload.get("reason")),
            state_status=normalize_text_fn(tracked_payload.get("state_status")),
            reaction_dir=normalize_text_fn(tracked_payload.get("reaction_dir") or reaction_dir),
            latest_known_path=normalize_text_fn(tracked_payload.get("latest_known_path") or target),
            organized_output_dir=normalize_text_fn(tracked_payload.get("organized_output_dir")),
            optimized_xyz_path=normalize_text_fn(tracked_payload.get("optimized_xyz_path")),
            queue_id=normalize_text_fn(tracked_payload.get("queue_id") or queue_id),
            queue_status=normalize_text_fn(tracked_payload.get("queue_status")).lower(),
            cancel_requested=normalize_bool_fn(tracked_payload.get("cancel_requested")),
            selected_inp=normalize_text_fn(tracked_payload.get("selected_inp")),
            selected_input_xyz=normalize_text_fn(tracked_payload.get("selected_input_xyz")),
            analyzer_status=normalize_text_fn(tracked_payload.get("analyzer_status")),
            completed_at=normalize_text_fn(tracked_payload.get("completed_at")),
            last_out_path=normalize_text_fn(tracked_payload.get("last_out_path")),
            run_state_path=normalize_text_fn(tracked_payload.get("run_state_path")),
            report_json_path=normalize_text_fn(tracked_payload.get("report_json_path")),
            report_md_path=normalize_text_fn(tracked_payload.get("report_md_path")),
            attempt_count=safe_int_fn(tracked_payload.get("attempt_count"), 0),
            max_retries=safe_int_fn(tracked_payload.get("max_retries"), 0),
            attempts=attempts,
            final_result=dict(final_result) if isinstance(final_result, dict) else {},
            resource_request=coerce_resource_dict_fn(tracked_payload.get("resource_request")),
            resource_actual=coerce_resource_dict_fn(tracked_payload.get("resource_actual")),
        )

    runtime_context = tracked_runtime_context_fn(
        index_root=allowed_root,
        organized_root=organized_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )
    precomputed_organized_dir: Path | None = None
    if runtime_context is not None:
        (
            tracked_artifact_dir,
            tracked_context_record,
            tracked_state,
            tracked_report,
            tracked_organized_ref,
            queue_entry,
            precomputed_organized_dir,
        ) = runtime_context
    else:
        tracked_artifact_dir, tracked_context_record, tracked_state, tracked_report, tracked_organized_ref = tracked_artifact_context_fn(
            index_root=allowed_root,
            targets=(
                target,
                run_id,
                reaction_dir,
            ),
        )
        queue_entry = None
    direct_dir = direct_dir_target_fn(target)
    tracked_dir: Path | None = tracked_artifact_dir
    tracked_record = tracked_context_record
    if tracked_dir is None or tracked_record is None:
        fallback_dir, fallback_record = resolve_job_dir_fn(allowed_root, target)
        tracked_dir = tracked_dir or fallback_dir
        tracked_record = tracked_record or fallback_record
    if queue_entry is None:
        queue_entry = find_queue_entry_fn(
            allowed_root=allowed_root,
            target=target,
            queue_id=queue_id,
            run_id=run_id,
            reaction_dir=reaction_dir,
        )

    reaction_dir_hint = resolve_candidate_path_fn(reaction_dir)
    queue_reaction_dir = resolve_candidate_path_fn((queue_entry or {}).get("reaction_dir"))
    if not tracked_artifact_dir and queue_reaction_dir is not None:
        refreshed_dir, refreshed_record, refreshed_state, refreshed_report, refreshed_organized_ref = tracked_artifact_context_fn(
            index_root=allowed_root,
            targets=(str(queue_reaction_dir),),
        )
        tracked_artifact_dir = tracked_artifact_dir or refreshed_dir
        tracked_record = tracked_record or refreshed_record
        if not tracked_state:
            tracked_state = refreshed_state
        if not tracked_report:
            tracked_report = refreshed_report
        if not tracked_organized_ref:
            tracked_organized_ref = refreshed_organized_ref
    current_dir = tracked_artifact_dir or tracked_dir or direct_dir or reaction_dir_hint or queue_reaction_dir

    state = dict(tracked_state)
    if not state and current_dir is not None:
        state = load_json_dict_fn(current_dir / "run_state.json")
    report = dict(tracked_report)
    if not report and current_dir is not None:
        report = load_json_dict_fn(current_dir / "run_report.json")
    organized_ref = dict(tracked_organized_ref)
    if not organized_ref and current_dir is not None:
        organized_ref = load_json_dict_fn(current_dir / "organized_ref.json")
    if not organized_ref:
        organized_ref = load_tracked_organized_ref_fn(tracked_record, current_dir)

    resolved_run_id = (
        normalize_text_fn(run_id)
        or normalize_text_fn(state.get("run_id"))
        or normalize_text_fn(report.get("run_id"))
        or normalize_text_fn(organized_ref.get("run_id"))
        or normalize_text_fn((queue_entry or {}).get("run_id"))
    )
    if runtime_context is not None:
        organized_dir = precomputed_organized_dir
    else:
        organized_record = None
        tracked_organized_dir = record_organized_dir_fn(tracked_record)
        if tracked_organized_dir is None:
            organized_record = find_organized_record_fn(
                organized_root=organized_root,
                target=target,
                run_id=resolved_run_id,
                reaction_dir=str(current_dir) if current_dir is not None else reaction_dir,
            )
        organized_dir = tracked_organized_dir or organized_dir_from_record_fn(organized_root, organized_record)

        if organized_dir is not None and (current_dir is None or not current_dir.exists() or (not state and not report)):
            current_dir = organized_dir
            refreshed_dir, refreshed_record, refreshed_state, refreshed_report, refreshed_organized_ref = tracked_artifact_context_fn(
                index_root=allowed_root,
                targets=(str(current_dir), target, resolved_run_id, reaction_dir),
            )
            if refreshed_dir is not None:
                current_dir = refreshed_dir
            if tracked_record is None and refreshed_record is not None:
                tracked_record = refreshed_record
            state = dict(refreshed_state) or load_json_dict_fn(current_dir / "run_state.json")
            report = dict(refreshed_report) or load_json_dict_fn(current_dir / "run_report.json")
            organized_ref = dict(refreshed_organized_ref) or load_json_dict_fn(current_dir / "organized_ref.json")
            if not organized_ref:
                organized_ref = load_tracked_organized_ref_fn(tracked_record, current_dir)
            resolved_run_id = (
                resolved_run_id
                or normalize_text_fn(state.get("run_id"))
                or normalize_text_fn(report.get("run_id"))
                or normalize_text_fn(organized_ref.get("run_id"))
            )

    if tracked_record is not None and normalize_text_fn(tracked_record.latest_known_path):
        latest_known_path = normalize_text_fn(tracked_record.latest_known_path)
    elif organized_dir is not None:
        latest_known_path = str(organized_dir)
    elif current_dir is not None or queue_reaction_dir is not None:
        latest_known_path = str(current_dir or queue_reaction_dir)
    else:
        latest_known_path = normalize_text_fn(target)

    state_status = normalize_text_fn(state.get("status")).lower()
    status, analyzer_status, reason, completed_at = status_from_payloads_fn(
        queue_entry=queue_entry,
        state=state,
        report=report,
    )
    tracked_status = normalize_text_fn(tracked_record.status if tracked_record is not None else "").lower()
    if status == "unknown" and tracked_status:
        status = tracked_status
    base_dir = current_dir
    selected_inp = resolve_artifact_path_fn(
        state.get("selected_inp")
        or report.get("selected_inp")
        or organized_ref.get("selected_inp")
        or organized_ref.get("selected_input_xyz")
        or (tracked_record.selected_input_xyz if tracked_record is not None else ""),
        base_dir,
    )
    state_final_result = state.get("final_result")
    state_final = state_final_result if isinstance(state_final_result, dict) else {}
    report_final_result = report.get("final_result")
    report_final = report_final_result if isinstance(report_final_result, dict) else {}
    last_out_path = resolve_artifact_path_fn(
        state_final.get("last_out_path") or report_final.get("last_out_path"),
        base_dir,
    )
    selected_input_xyz = resolve_artifact_path_fn(
        organized_ref.get("selected_input_xyz") or (tracked_record.selected_input_xyz if tracked_record is not None else ""),
        base_dir,
    )
    if not selected_input_xyz.lower().endswith(".xyz"):
        selected_input_xyz = ""
    selected_input_xyz = selected_input_xyz or derive_selected_input_xyz_fn(selected_inp)
    optimized_xyz_path = prefer_orca_optimized_xyz_fn(
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        current_dir=current_dir,
        organized_dir=organized_dir,
        latest_known_path=latest_known_path,
        last_out_path=last_out_path,
    )

    resource_request = coerce_resource_dict_fn(
        ((queue_entry or {}).get("resource_request") if isinstance((queue_entry or {}).get("resource_request"), dict) else {})
    ) or coerce_resource_dict_fn(tracked_record.resource_request if tracked_record is not None else {})
    resource_actual = coerce_resource_dict_fn(
        ((queue_entry or {}).get("resource_actual") if isinstance((queue_entry or {}).get("resource_actual"), dict) else {})
    ) or coerce_resource_dict_fn(tracked_record.resource_actual if tracked_record is not None else {}) or dict(resource_request)

    if tracked_record is not None and tracked_record.app_name and tracked_record.app_name not in ORCA_APP_NAMES:
        raise ValueError(f"Expected chemstack_orca index record, got: {tracked_record.app_name}")

    organized_output_dir = normalize_text_fn(
        (tracked_record.organized_output_dir if tracked_record is not None else "")
        or organized_ref.get("organized_output_dir")
        or (str(organized_dir) if organized_dir is not None else "")
        or (str(current_dir) if current_dir is not None and is_subpath_fn(current_dir, organized_root) else "")
    )

    run_state_path = str((current_dir / "run_state.json").resolve()) if current_dir is not None and (current_dir / "run_state.json").exists() else ""
    report_json_path = str((current_dir / "run_report.json").resolve()) if current_dir is not None and (current_dir / "run_report.json").exists() else ""
    report_md_path = str((current_dir / "run_report.md").resolve()) if current_dir is not None and (current_dir / "run_report.md").exists() else ""

    queue_status = normalize_text_fn((queue_entry or {}).get("status")).lower()
    return contract_cls(
        run_id=resolved_run_id,
        status=status,
        reason=reason,
        state_status=state_status,
        reaction_dir=str(current_dir) if current_dir is not None else normalize_text_fn(reaction_dir),
        latest_known_path=latest_known_path,
        organized_output_dir=organized_output_dir,
        optimized_xyz_path=optimized_xyz_path,
        queue_id=normalize_text_fn((queue_entry or {}).get("queue_id") or queue_id),
        queue_status=queue_status,
        cancel_requested=normalize_bool_fn((queue_entry or {}).get("cancel_requested")),
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        analyzer_status=analyzer_status,
        completed_at=completed_at,
        last_out_path=last_out_path,
        run_state_path=run_state_path,
        report_json_path=report_json_path,
        report_md_path=report_md_path,
        attempt_count=attempt_count_fn(state, report),
        max_retries=max_retries_fn(state, report),
        attempts=coerce_attempts_fn(state, report),
        final_result=final_result_payload_fn(state, report),
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


__all__ = [
    "attempt_count_impl",
    "coerce_attempts_impl",
    "final_result_payload_impl",
    "load_orca_artifact_contract_impl",
    "max_retries_impl",
    "status_from_payloads_impl",
]
