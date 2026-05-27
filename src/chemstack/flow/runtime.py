from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import yaml
from chemstack.core.admission import active_slot_count
from chemstack.core.config.files import (
    default_shared_admission_root,
    engine_config_mapping,
    workflow_root_from_mapping,
)
from chemstack.core.utils import now_utc_iso, timestamped_token

from . import _runtime_common
from ._workflow_phases import phase_transition_event_payloads
from .engine_options import WorkflowEngineOptions
from .orchestration import advance_workflow
from .registry import (
    append_workflow_journal_event,
    list_workflow_registry,
    reindex_workflow_registry,
    write_workflow_worker_state,
)
from .state import load_workflow_payload, workflow_has_active_downstream, workflow_summary

WORKFLOW_WORKER_LOCK_NAME = "workflow_worker.lock"
TERMINAL_WORKFLOW_STATUSES = frozenset(
    {
        "completed",
        "failed",
        "cancelled",
        "cancel_failed",
    }
)
ACTIVE_TERMINAL_SYNC_STATUSES = frozenset(
    {"queued", "running", "submitted", "cancel_requested"}
)


@dataclass(frozen=True)
class WorkflowRuntimeContext:
    root: Path
    options: WorkflowEngineOptions
    submit_ready: bool = True
    refresh_registry: bool = False
    worker_session_id: str = ""
    interval_seconds: float | None = None
    lease_seconds: float = 60.0


@dataclass(frozen=True)
class WorkflowRegistryAdvanceRequest:
    workflow_root: str | Path
    options: WorkflowEngineOptions
    submit_ready: bool = True
    refresh_registry: bool = False
    worker_session_id: str = ""
    interval_seconds: float | None = None
    lease_seconds: float = 60.0

    @classmethod
    def from_values(
        cls,
        *,
        workflow_root: str | Path,
        crest_config: str | None = None,
        xtb_config: str | None = None,
        orca_config: str | None = None,
        orca_repo_root: str | None = None,
        submit_ready: bool = True,
        refresh_registry: bool = False,
        worker_session_id: str = "",
        interval_seconds: float | None = None,
        lease_seconds: float = 60.0,
    ) -> WorkflowRegistryAdvanceRequest:
        return cls(
            workflow_root=workflow_root,
            options=WorkflowEngineOptions.from_values(
                crest_config=crest_config,
                xtb_config=xtb_config,
                orca_config=orca_config,
                orca_repo_root=orca_repo_root,
            ),
            submit_ready=submit_ready,
            refresh_registry=refresh_registry,
            worker_session_id=worker_session_id,
            interval_seconds=interval_seconds,
            lease_seconds=lease_seconds,
        )

    def runtime_context(self) -> WorkflowRuntimeContext:
        return WorkflowRuntimeContext(
            root=Path(self.workflow_root).expanduser().resolve(),
            options=self.options,
            worker_session_id=self.worker_session_id,
            submit_ready=self.submit_ready,
            refresh_registry=self.refresh_registry,
            interval_seconds=self.interval_seconds,
            lease_seconds=self.lease_seconds,
        )


@dataclass(frozen=True)
class _WorkflowCycle:
    root: Path
    cycle_started_at: str
    session_id: str
    requested_submit_ready: bool
    cycle_submit_ready: bool
    admission_blocked: bool
    lease_expires_at: str


@dataclass(frozen=True)
class _WorkflowCycleProgress:
    workflow_results: list[dict[str, Any]]
    advanced_count: int
    skipped_count: int
    failed_count: int


def submission_admission_limit_from_config(
    config_path: str | Path,
    *,
    positive_int_fn: Callable[[Any], int | None] = _runtime_common.positive_int,
) -> int | None:
    try:
        path = Path(config_path).expanduser().resolve()
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None

    scheduler = raw.get("scheduler")
    if not isinstance(scheduler, dict):
        scheduler = {}
    return positive_int_fn(scheduler.get("max_active_simulations"))


def _mapping_section(raw: dict[str, Any], key: str) -> dict[str, Any]:
    section = raw.get(key)
    return section if isinstance(section, dict) else {}


def _resolve_configured_path(value: Any) -> Path | None:
    text = _runtime_common.normalize_text(value)
    return Path(text).expanduser().resolve() if text else None


def _submission_admission_root_from_config(
    config_path: str | Path,
    *,
    engine: str | None = None,
) -> Path | None:
    path = Path(config_path).expanduser().resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        return None

    if engine in {"xtb", "crest"}:
        workflow_root = workflow_root_from_mapping(raw)
        if not workflow_root:
            return None
        admission_root = _resolve_configured_path(
            _mapping_section(raw, "scheduler").get("admission_root")
        )
        if admission_root is None:
            admission_root = _resolve_configured_path(default_shared_admission_root(path))
        return admission_root

    if engine:
        raw = engine_config_mapping(raw, engine, inherit_keys=("scheduler", "workflow"))
    runtime = raw.get("runtime")
    scheduler = _mapping_section(raw, "scheduler")
    if isinstance(runtime, dict):
        admission_root = _resolve_configured_path(runtime.get("admission_root"))
        if admission_root is not None:
            return admission_root
    admission_root = _resolve_configured_path(scheduler.get("admission_root"))
    if admission_root is None and scheduler:
        admission_root = _resolve_configured_path(default_shared_admission_root(path))
    return admission_root


def submission_admission_has_capacity(
    config_path: str | Path,
    *,
    submission_admission_limit_from_config_fn: Callable[[str | Path], int | None]
    = submission_admission_limit_from_config,
    active_slot_count_fn: Callable[[Path], int] = active_slot_count,
    sibling_runtime_paths_fn: Callable[..., dict[str, Any]] | None = None,
) -> bool | None:
    limit = submission_admission_limit_from_config_fn(config_path)
    if limit is None:
        return None
    admission_root: Path | None = None
    for engine in (None, "xtb", "crest", "orca"):
        try:
            if sibling_runtime_paths_fn is None:
                candidate = _submission_admission_root_from_config(config_path, engine=engine)
            else:
                runtime_paths = sibling_runtime_paths_fn(str(config_path), engine=engine)
                candidate = runtime_paths.get("admission_root")
        except Exception:
            continue
        if isinstance(candidate, Path):
            admission_root = candidate
            break
    if not isinstance(admission_root, Path):
        return None
    try:
        return active_slot_count_fn(admission_root) < limit
    except Exception:
        return None


def workflow_submission_has_capacity(
    *config_paths: str | Path | None,
    submission_admission_has_capacity_fn: Callable[[str | Path], bool | None]
    | None = None,
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
) -> bool:
    has_capacity_fn = submission_admission_has_capacity_fn or submission_admission_has_capacity
    for config_path in config_paths:
        config_text = normalize_text_fn(config_path)
        if not config_text:
            continue
        has_capacity = has_capacity_fn(config_text)
        if has_capacity is not None:
            return has_capacity
    return True


def workflow_lease_expires_at(lease_seconds: float) -> str:
    if lease_seconds <= 0:
        return ""
    try:
        from datetime import datetime, timedelta, timezone

        return (datetime.now(timezone.utc) + timedelta(seconds=float(lease_seconds))).isoformat()
    except Exception:
        return ""


def start_workflow_cycle(
    *,
    context: WorkflowRuntimeContext,
    now_utc_iso_fn: Callable[[], str],
    timestamped_token_fn: Callable[[str], str],
    workflow_submission_has_capacity_fn: Callable[..., bool],
    write_workflow_worker_state_fn: Callable[..., Any],
    append_workflow_journal_event_fn: Callable[..., Any],
    workflow_lease_expires_at_fn: Callable[[float], str] = workflow_lease_expires_at,
) -> _WorkflowCycle:
    cycle_started_at = now_utc_iso_fn()
    session_id = _runtime_common.normalize_text(context.worker_session_id) or timestamped_token_fn(
        "wf_worker"
    )
    requested_submit_ready = bool(context.submit_ready)
    cycle_submit_ready = requested_submit_ready and workflow_submission_has_capacity_fn(
        context.options.crest_config,
        context.options.xtb_config,
        context.options.orca_config,
    )
    admission_blocked = requested_submit_ready and not cycle_submit_ready
    lease_expires_at = workflow_lease_expires_at_fn(context.lease_seconds)

    write_workflow_worker_state_fn(
        context.root,
        worker_session_id=session_id,
        status="running",
        workflow_root_path=context.root,
        last_cycle_started_at=cycle_started_at,
        last_heartbeat_at=cycle_started_at,
        lease_expires_at=lease_expires_at,
        interval_seconds=context.interval_seconds,
        submit_ready=cycle_submit_ready,
        metadata={"admission_blocked": True} if admission_blocked else None,
    )
    append_workflow_journal_event_fn(
        context.root,
        event_type="worker_cycle_started",
        worker_session_id=session_id,
        metadata={
            "cycle_started_at": cycle_started_at,
            "refresh_registry": bool(context.refresh_registry),
            "submit_ready": cycle_submit_ready,
            "requested_submit_ready": requested_submit_ready,
            "admission_blocked": admission_blocked,
        },
    )
    return _WorkflowCycle(
        root=context.root,
        cycle_started_at=cycle_started_at,
        session_id=session_id,
        requested_submit_ready=requested_submit_ready,
        cycle_submit_ready=cycle_submit_ready,
        admission_blocked=admission_blocked,
        lease_expires_at=lease_expires_at,
    )


def finish_workflow_cycle(
    *,
    cycle: _WorkflowCycle,
    discovered_count: int,
    progress: _WorkflowCycleProgress,
    interval_seconds: float | None,
    now_utc_iso_fn: Callable[[], str],
    write_workflow_worker_state_fn: Callable[..., Any],
    append_workflow_journal_event_fn: Callable[..., Any],
) -> str:
    cycle_finished_at = now_utc_iso_fn()
    finished_metadata = {
        "discovered_count": discovered_count,
        "advanced_count": progress.advanced_count,
        "skipped_count": progress.skipped_count,
        "failed_count": progress.failed_count,
    }
    if cycle.admission_blocked:
        finished_metadata["admission_blocked"] = True
    write_workflow_worker_state_fn(
        cycle.root,
        worker_session_id=cycle.session_id,
        status="idle",
        workflow_root_path=cycle.root,
        last_cycle_started_at=cycle.cycle_started_at,
        last_cycle_finished_at=cycle_finished_at,
        last_heartbeat_at=cycle_finished_at,
        lease_expires_at=cycle.lease_expires_at,
        interval_seconds=interval_seconds,
        submit_ready=cycle.cycle_submit_ready,
        metadata=finished_metadata,
    )
    append_workflow_journal_event_fn(
        cycle.root,
        event_type="worker_cycle_finished",
        worker_session_id=cycle.session_id,
        metadata={
            "cycle_started_at": cycle.cycle_started_at,
            "cycle_finished_at": cycle_finished_at,
            "discovered_count": discovered_count,
            "advanced_count": progress.advanced_count,
            "skipped_count": progress.skipped_count,
            "failed_count": progress.failed_count,
            "admission_blocked": cycle.admission_blocked,
        },
    )
    return cycle_finished_at


def workflow_advance_failed_result(
    record: Any, *, previous_status: str, reason: str
) -> dict[str, Any]:
    return {
        "workflow_id": record.workflow_id,
        "template_name": record.template_name,
        "previous_status": previous_status,
        "status": "advance_failed",
        "advanced": False,
        "reason": reason,
        "stage_count": record.stage_count,
    }


def workflow_skipped_terminal_result(record: Any, *, previous_status: str) -> dict[str, Any]:
    return {
        "workflow_id": record.workflow_id,
        "template_name": record.template_name,
        "previous_status": previous_status,
        "status": previous_status,
        "advanced": False,
        "reason": "terminal_status",
        "stage_count": record.stage_count,
    }


def workflow_advanced_result(
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    status: str,
    reason: str = "",
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
) -> dict[str, Any]:
    result = {
        "workflow_id": normalize_text_fn(payload.get("workflow_id")) or record.workflow_id,
        "template_name": normalize_text_fn(payload.get("template_name")) or record.template_name,
        "previous_status": previous_status,
        "status": status,
        "advanced": True,
        "changed": status != previous_status,
        "stage_count": len(payload.get("stages", []))
        if isinstance(payload.get("stages"), list)
        else record.stage_count,
    }
    if reason:
        result["reason"] = reason
    return result


def workflow_needs_terminal_sync(
    workspace_dir: str | Path,
    *,
    load_workflow_payload_fn: Callable[[str | Path], dict[str, Any]],
    workflow_has_active_downstream_fn: Callable[[dict[str, Any]], bool],
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
) -> bool:
    try:
        payload = load_workflow_payload_fn(workspace_dir)
    except (FileNotFoundError, ValueError):
        return False
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and bool(metadata.get("final_child_sync_pending")):
        return True
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        if normalize_text_fn(raw_stage.get("status")).lower() in ACTIVE_TERMINAL_SYNC_STATUSES:
            return True
        task = raw_stage.get("task")
        if (
            isinstance(task, dict)
            and normalize_text_fn(task.get("status")).lower() in ACTIVE_TERMINAL_SYNC_STATUSES
        ):
            return True
    return workflow_has_active_downstream_fn(payload)


def stage_key(stage: dict[str, Any], index: int) -> str:
    stage_id = _runtime_common.normalize_text(stage.get("stage_id"))
    if stage_id:
        return stage_id
    return f"index:{index}"


def stage_event_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    text_fields = (
        "stage_id",
        "stage_kind",
        "engine",
        "task_kind",
        "task_status",
        "queue_id",
        "reaction_dir",
        "selected_input_xyz",
        "selected_inp",
        "submission_status",
        "run_id",
        "latest_known_path",
        "organized_output_dir",
        "optimized_xyz_path",
        "analyzer_status",
        "reason",
        "reaction_handoff_status",
        "reaction_handoff_reason",
        "completed_at",
        "last_out_path",
    )
    int_fields = (
        "xtb_handoff_retries_used",
        "xtb_handoff_retry_limit",
        "orca_attempt_count",
        "orca_max_retries",
        "output_artifact_count",
    )
    for field in text_fields:
        text = _runtime_common.normalize_text(stage.get(field))
        if text:
            metadata[field] = text
    for field in int_fields:
        value = _runtime_common.safe_int(stage.get(field))
        if value is not None:
            metadata[field] = value
    return metadata


def stage_status_event_type(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
    *,
    suppress_terminal_event: bool,
) -> str:
    previous_status = _runtime_common.normalize_text(previous_stage.get("status")).lower()
    current_status = _runtime_common.normalize_text(current_stage.get("status")).lower()
    if not current_status or current_status == previous_status:
        return ""
    if current_status == "queued":
        return "workflow_stage_submitted"
    if current_status in {"submitted", "running"}:
        return "workflow_stage_status_changed"
    if suppress_terminal_event:
        return ""
    if current_status == "completed":
        return "workflow_stage_completed"
    if current_status in {"failed", "submission_failed", "cancel_failed"}:
        return "workflow_stage_failed"
    if current_status == "cancelled":
        return "workflow_stage_cancelled"
    return ""


def stage_handoff_event_type(previous_stage: dict[str, Any], current_stage: dict[str, Any]) -> str:
    engine = _runtime_common.normalize_text(
        current_stage.get("engine") or previous_stage.get("engine")
    ).lower()
    task_kind = _runtime_common.normalize_text(
        current_stage.get("task_kind") or previous_stage.get("task_kind")
    ).lower()
    if engine != "xtb" or task_kind != "path_search":
        return ""
    previous_handoff = _runtime_common.normalize_text(
        previous_stage.get("reaction_handoff_status")
    ).lower()
    current_handoff = _runtime_common.normalize_text(
        current_stage.get("reaction_handoff_status")
    ).lower()
    if not current_handoff or current_handoff == previous_handoff:
        return ""
    if current_handoff == "ready":
        return "workflow_stage_handoff_ready"
    if current_handoff == "retrying":
        return "workflow_stage_handoff_retrying"
    if current_handoff == "failed":
        return "workflow_stage_handoff_failed"
    return ""


def stage_transition_context(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
) -> dict[str, str]:
    return {
        "previous_stage_status": _runtime_common.normalize_text(
            previous_stage.get("status")
        ).lower(),
        "current_stage_status": _runtime_common.normalize_text(
            current_stage.get("status")
        ).lower(),
        "previous_handoff_status": _runtime_common.normalize_text(
            previous_stage.get("reaction_handoff_status")
        ).lower(),
        "current_handoff_status": _runtime_common.normalize_text(
            current_stage.get("reaction_handoff_status")
        ).lower(),
        "stage_id": _runtime_common.normalize_text(
            current_stage.get("stage_id") or previous_stage.get("stage_id")
        ),
        "engine": _runtime_common.normalize_text(
            current_stage.get("engine") or previous_stage.get("engine")
        ),
        "task_kind": _runtime_common.normalize_text(
            current_stage.get("task_kind") or previous_stage.get("task_kind")
        ),
    }


def stage_transition_metadata(
    metadata: dict[str, Any],
    context: dict[str, str],
    *,
    include_handoff: bool,
) -> dict[str, Any]:
    event_metadata = dict(metadata)
    if context["previous_stage_status"]:
        event_metadata["previous_stage_status"] = context["previous_stage_status"]
    if context["current_stage_status"]:
        event_metadata["stage_status"] = context["current_stage_status"]
    if include_handoff and context["previous_handoff_status"]:
        event_metadata["previous_reaction_handoff_status"] = context["previous_handoff_status"]
    if include_handoff and context["current_handoff_status"]:
        event_metadata["reaction_handoff_status"] = context["current_handoff_status"]
    return event_metadata


def status_transition_event_payload(
    *,
    event_type: str,
    current_stage: dict[str, Any],
    context: dict[str, str],
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> dict[str, Any]:
    reason = ""
    if event_type in {"workflow_stage_failed", "workflow_stage_cancelled"}:
        reason = _runtime_common.normalize_text(current_stage.get("reason"))
    return {
        "event_type": event_type,
        "workflow_id": workflow_id,
        "template_name": template_name,
        "status": context["current_stage_status"],
        "previous_status": context["previous_stage_status"],
        "reason": reason,
        "worker_session_id": worker_session_id,
        "stage_id": context["stage_id"],
        "engine": context["engine"],
        "task_kind": context["task_kind"],
        "stage_status": context["current_stage_status"],
        "previous_stage_status": context["previous_stage_status"],
        "metadata": stage_transition_metadata(metadata, context, include_handoff=False),
    }


def handoff_transition_event_payload(
    *,
    event_type: str,
    current_stage: dict[str, Any],
    context: dict[str, str],
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "workflow_id": workflow_id,
        "template_name": template_name,
        "status": context["current_handoff_status"],
        "previous_status": context["previous_handoff_status"],
        "reason": _runtime_common.normalize_text(
            current_stage.get("reaction_handoff_reason") or current_stage.get("reason")
        ),
        "worker_session_id": worker_session_id,
        "stage_id": context["stage_id"],
        "engine": context["engine"],
        "task_kind": context["task_kind"],
        "stage_status": context["current_stage_status"],
        "previous_stage_status": context["previous_stage_status"],
        "reaction_handoff_status": context["current_handoff_status"],
        "previous_reaction_handoff_status": context["previous_handoff_status"],
        "metadata": stage_transition_metadata(metadata, context, include_handoff=True),
    }


def stage_transition_event_payloads(
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> list[dict[str, Any]]:
    previous_stages = list(previous_summary.get("stage_summaries", []))
    current_stages = list(current_summary.get("stage_summaries", []))
    previous_by_key = {
        stage_key(stage, index): dict(stage) for index, stage in enumerate(previous_stages)
    }
    event_payloads: list[dict[str, Any]] = []

    for index, raw_stage in enumerate(current_stages):
        current_stage = dict(raw_stage)
        previous_stage = previous_by_key.get(stage_key(current_stage, index), {})
        handoff_event_type = stage_handoff_event_type(previous_stage, current_stage)
        status_event_type = stage_status_event_type(
            previous_stage,
            current_stage,
            suppress_terminal_event=handoff_event_type
            in {"workflow_stage_handoff_ready", "workflow_stage_handoff_failed"},
        )
        metadata = stage_event_metadata(current_stage)
        context = stage_transition_context(previous_stage, current_stage)

        if status_event_type:
            event_payloads.append(
                status_transition_event_payload(
                    event_type=status_event_type,
                    current_stage=current_stage,
                    context=context,
                    metadata=metadata,
                    workflow_id=workflow_id,
                    template_name=template_name,
                    worker_session_id=worker_session_id,
                )
            )

        if handoff_event_type:
            event_payloads.append(
                handoff_transition_event_payload(
                    event_type=handoff_event_type,
                    current_stage=current_stage,
                    context=context,
                    metadata=metadata,
                    workflow_id=workflow_id,
                    template_name=template_name,
                    worker_session_id=worker_session_id,
                )
            )
    return event_payloads


def append_stage_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
    stage_transition_event_payloads_fn: Callable[..., list[dict[str, Any]]],
    append_workflow_journal_event_fn: Callable[..., Any],
) -> None:
    for payload in stage_transition_event_payloads_fn(
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    ):
        append_workflow_journal_event_fn(workflow_root, **payload)


def append_phase_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
    phase_transition_event_payloads_fn: Callable[..., list[dict[str, Any]]],
    append_workflow_journal_event_fn: Callable[..., Any],
) -> None:
    for payload in phase_transition_event_payloads_fn(
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    ):
        append_workflow_journal_event_fn(workflow_root, **payload)


def append_workflow_advance_failed_event(
    workflow_root: str | Path,
    record: Any,
    *,
    previous_status: str,
    reason: str,
    worker_session_id: str,
    append_workflow_journal_event_fn: Callable[..., Any],
) -> None:
    append_workflow_journal_event_fn(
        workflow_root,
        event_type="workflow_advance_failed",
        workflow_id=record.workflow_id,
        template_name=record.template_name,
        previous_status=previous_status,
        status="advance_failed",
        reason=reason,
        worker_session_id=worker_session_id,
    )


def append_workflow_advanced_events(
    workflow_root: str | Path,
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    current_summary: dict[str, Any],
    previous_summary: dict[str, Any],
    worker_session_id: str,
    reason: str = "",
    append_workflow_journal_event_fn: Callable[..., Any],
    append_phase_transition_events_fn: Callable[..., None],
    append_stage_transition_events_fn: Callable[..., None],
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
) -> None:
    status = normalize_text_fn(payload.get("status")).lower()
    workflow_id = normalize_text_fn(payload.get("workflow_id")) or record.workflow_id
    template_name = normalize_text_fn(payload.get("template_name")) or record.template_name
    if status != previous_status:
        event_kwargs: dict[str, Any] = {
            "event_type": "workflow_status_changed",
            "workflow_id": workflow_id,
            "template_name": template_name,
            "previous_status": previous_status,
            "status": status,
            "worker_session_id": worker_session_id,
        }
        if reason:
            event_kwargs["reason"] = reason
        append_workflow_journal_event_fn(workflow_root, **event_kwargs)
    append_phase_transition_events_fn(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )
    append_stage_transition_events_fn(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )


def _safe_workflow_summary(
    workspace_dir: str | Path,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return workflow_summary(workspace_dir, payload=payload)
    except (FileNotFoundError, ValueError, TypeError):
        return {}


def _append_stage_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> None:
    _append_summary_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        append_fn=append_stage_transition_events,
        payloads_kwarg="stage_transition_event_payloads_fn",
        payloads_fn=stage_transition_event_payloads,
    )


def _append_summary_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
    append_fn: Any,
    payloads_kwarg: str,
    payloads_fn: Any,
) -> None:
    append_fn(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        **{
            payloads_kwarg: payloads_fn,
            "append_workflow_journal_event_fn": append_workflow_journal_event,
        },
    )


def _append_phase_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> None:
    _append_summary_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        append_fn=append_phase_transition_events,
        payloads_kwarg="phase_transition_event_payloads_fn",
        payloads_fn=phase_transition_event_payloads,
    )


def workflow_worker_lock_path(workflow_root: str | Path) -> Path:
    return Path(workflow_root).expanduser().resolve() / WORKFLOW_WORKER_LOCK_NAME


def _workflow_is_terminal_status(status: Any) -> bool:
    return _runtime_common.normalize_text(status).lower() in TERMINAL_WORKFLOW_STATUSES


def _workflow_needs_terminal_sync(workspace_dir: str | Path) -> bool:
    return workflow_needs_terminal_sync(
        workspace_dir,
        load_workflow_payload_fn=load_workflow_payload,
        workflow_has_active_downstream_fn=workflow_has_active_downstream,
        normalize_text_fn=_runtime_common.normalize_text,
    )


def _workflow_needs_terminal_child_sync(record: Any, *, previous_status: str) -> bool:
    return _workflow_is_terminal_status(previous_status) and _workflow_needs_terminal_sync(
        record.workspace_dir
    )


def _start_workflow_cycle(
    *,
    context: WorkflowRuntimeContext,
) -> _WorkflowCycle:
    def cycle_submission_has_capacity(*config_paths: str | Path | None) -> bool:
        def submission_limit(config_path: str | Path) -> int | None:
            return submission_admission_limit_from_config(
                config_path,
                positive_int_fn=_runtime_common.positive_int,
            )

        def submission_has_capacity(config_path: str | Path) -> bool | None:
            return submission_admission_has_capacity(
                config_path,
                submission_admission_limit_from_config_fn=submission_limit,
                active_slot_count_fn=active_slot_count,
            )

        return workflow_submission_has_capacity(
            *config_paths,
            submission_admission_has_capacity_fn=submission_has_capacity,
            normalize_text_fn=_runtime_common.normalize_text,
        )

    return start_workflow_cycle(
        context=context,
        now_utc_iso_fn=now_utc_iso,
        timestamped_token_fn=timestamped_token,
        workflow_submission_has_capacity_fn=cycle_submission_has_capacity,
        write_workflow_worker_state_fn=write_workflow_worker_state,
        append_workflow_journal_event_fn=append_workflow_journal_event,
        workflow_lease_expires_at_fn=workflow_lease_expires_at,
    )


def _advance_workflow_record(
    *,
    cycle: _WorkflowCycle,
    record: Any,
    options: WorkflowEngineOptions,
) -> tuple[str, dict[str, Any]]:
    previous_status = _runtime_common.normalize_text(record.status).lower()
    terminal_sync = _workflow_needs_terminal_child_sync(
        record,
        previous_status=previous_status,
    )
    if _workflow_is_terminal_status(previous_status) and not terminal_sync:
        return "skipped", workflow_skipped_terminal_result(
            record,
            previous_status=previous_status,
        )

    previous_summary = _safe_workflow_summary(record.workspace_dir)
    try:
        payload = advance_workflow(
            target=record.workflow_id,
            workflow_root=cycle.root,
            engine_options=options,
            submit_ready=False if terminal_sync else cycle.cycle_submit_ready,
        )
    except Exception as exc:
        reason = f"terminal_child_sync_failed: {exc}" if terminal_sync else str(exc)
        append_workflow_advance_failed_event(
            cycle.root,
            previous_status=previous_status,
            reason=reason,
            worker_session_id=cycle.session_id,
            record=record,
            append_workflow_journal_event_fn=append_workflow_journal_event,
        )
        return "failed", workflow_advance_failed_result(
            record,
            previous_status=previous_status,
            reason=reason,
        )

    status = _runtime_common.normalize_text(payload.get("status")).lower()
    current_summary = _safe_workflow_summary(record.workspace_dir, payload=payload)
    reason = "terminal_child_sync" if terminal_sync else ""
    append_workflow_advanced_events(
        cycle.root,
        record,
        payload,
        previous_status=previous_status,
        previous_summary=previous_summary,
        current_summary=current_summary,
        worker_session_id=cycle.session_id,
        reason=reason,
        append_workflow_journal_event_fn=append_workflow_journal_event,
        append_phase_transition_events_fn=_append_phase_transition_events,
        append_stage_transition_events_fn=_append_stage_transition_events,
        normalize_text_fn=_runtime_common.normalize_text,
    )
    return "advanced", workflow_advanced_result(
        record,
        payload,
        previous_status=previous_status,
        status=status,
        reason=reason,
        normalize_text_fn=_runtime_common.normalize_text,
    )


def _advance_workflow_records(
    *,
    cycle: _WorkflowCycle,
    records: list[Any],
    options: WorkflowEngineOptions,
) -> _WorkflowCycleProgress:
    workflow_results: list[dict[str, Any]] = []
    advanced_count = 0
    skipped_count = 0
    failed_count = 0
    for record in records:
        outcome, result = _advance_workflow_record(cycle=cycle, record=record, options=options)
        workflow_results.append(result)
        if outcome == "advanced":
            advanced_count += 1
        elif outcome == "skipped":
            skipped_count += 1
        elif outcome == "failed":
            failed_count += 1
    return _WorkflowCycleProgress(
        workflow_results=workflow_results,
        advanced_count=advanced_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
    )


def _finish_workflow_cycle(
    *,
    cycle: _WorkflowCycle,
    discovered_count: int,
    progress: _WorkflowCycleProgress,
    interval_seconds: float | None,
) -> str:
    return finish_workflow_cycle(
        cycle=cycle,
        discovered_count=discovered_count,
        progress=progress,
        interval_seconds=interval_seconds,
        now_utc_iso_fn=now_utc_iso,
        write_workflow_worker_state_fn=write_workflow_worker_state,
        append_workflow_journal_event_fn=append_workflow_journal_event,
    )


def _workflow_registry_records(context: WorkflowRuntimeContext) -> list[Any]:
    if context.refresh_registry:
        return reindex_workflow_registry(context.root)
    return list_workflow_registry(context.root)


def _workflow_registry_cycle_payload(
    *,
    context: WorkflowRuntimeContext,
    request: WorkflowRegistryAdvanceRequest,
    cycle: _WorkflowCycle,
    records: list[Any],
    progress: _WorkflowCycleProgress,
    cycle_finished_at: str,
) -> dict[str, Any]:
    return {
        "workflow_root": str(context.root),
        "worker_session_id": cycle.session_id,
        "cycle_started_at": cycle.cycle_started_at,
        "cycle_finished_at": cycle_finished_at,
        "refresh_registry": bool(request.refresh_registry),
        "submit_ready": cycle.cycle_submit_ready,
        "requested_submit_ready": cycle.requested_submit_ready,
        "admission_blocked": cycle.admission_blocked,
        "discovered_count": len(records),
        "advanced_count": progress.advanced_count,
        "skipped_count": progress.skipped_count,
        "failed_count": progress.failed_count,
        "workflow_results": progress.workflow_results,
    }


def advance_workflow_registry_once(
    *,
    workflow_root: str | Path,
    crest_config: str | None = None,
    xtb_config: str | None = None,
    orca_config: str | None = None,
    orca_repo_root: str | None = None,
    submit_ready: bool = True,
    refresh_registry: bool = False,
    worker_session_id: str = "",
    interval_seconds: float | None = None,
    lease_seconds: float = 60.0,
) -> dict[str, Any]:
    request = WorkflowRegistryAdvanceRequest.from_values(
        workflow_root=workflow_root,
        crest_config=crest_config,
        xtb_config=xtb_config,
        orca_config=orca_config,
        orca_repo_root=orca_repo_root,
        submit_ready=submit_ready,
        refresh_registry=refresh_registry,
        worker_session_id=worker_session_id,
        interval_seconds=interval_seconds,
        lease_seconds=lease_seconds,
    )
    return _advance_workflow_registry_request(request)


def _advance_workflow_registry_request(request: WorkflowRegistryAdvanceRequest) -> dict[str, Any]:
    runtime_context = request.runtime_context()
    cycle = _start_workflow_cycle(context=runtime_context)
    records = _workflow_registry_records(runtime_context)
    progress = _advance_workflow_records(
        cycle=cycle,
        records=records,
        options=request.options,
    )
    cycle_finished_at = _finish_workflow_cycle(
        cycle=cycle,
        discovered_count=len(records),
        progress=progress,
        interval_seconds=request.interval_seconds,
    )
    return _workflow_registry_cycle_payload(
        context=runtime_context,
        request=request,
        cycle=cycle,
        records=records,
        progress=progress,
        cycle_finished_at=cycle_finished_at,
    )


__all__ = [
    "TERMINAL_WORKFLOW_STATUSES",
    "WorkflowRegistryAdvanceRequest",
    "WORKFLOW_WORKER_LOCK_NAME",
    "WorkflowRuntimeContext",
    "advance_workflow_registry_once",
    "workflow_worker_lock_path",
]
