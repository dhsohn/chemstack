from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE
from chemstack.core.config.files import default_config_path_from_repo_root
from chemstack.core.utils import file_lock, now_utc_iso, timestamped_token
from chemstack.cli_common import (
    _dependency,
    _normalize_text,
    _project_root,
    _shared_chemstack_config,
    _workflow_root_from_args,
)
from . import cli_workflow_output as _workflow_output
from .engine_options import WorkflowEngineOptions
from .orchestration import (
    advance_workflow,
    create_conformer_screening_workflow,
    create_reaction_ts_search_workflow,
)
from .operations import (
    cancel_workflow,
    get_workflow,
    get_workflow_artifacts,
    get_workflow_journal,
    get_workflow_runtime_status,
    get_workflow_telemetry,
    list_workflows,
)
from .registry import (
    append_workflow_journal_event,
    reindex_workflow_registry,
    write_workflow_worker_state,
)
from .runtime import advance_workflow_registry_once, workflow_worker_lock_path
from .submitters import submit_reaction_ts_search_workflow


def cmd_workflow_create_reaction_ts_search(args: Any, *, deps: Any | None = None) -> int:
    create_workflow = _dependency(
        deps, "create_reaction_ts_search_workflow", create_reaction_ts_search_workflow
    )
    print_created_workflow = _dependency(
        deps, "_print_created_workflow", _workflow_output.emit_created_workflow
    )
    payload = create_workflow(
        reactant_xyz=getattr(args, "reactant_xyz"),
        product_xyz=getattr(args, "product_xyz"),
        workflow_root=getattr(args, "workflow_root"),
        crest_mode=str(getattr(args, "crest_mode", "standard") or "standard"),
        priority=int(getattr(args, "priority", 10) or 10),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        max_crest_candidates=int(getattr(args, "max_crest_candidates", 3) or 3),
        max_xtb_stages=int(getattr(args, "max_xtb_stages", 3) or 3),
        max_orca_stages=int(getattr(args, "max_orca_stages", 3) or 3),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
    )
    return print_created_workflow(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_create_conformer_screening(args: Any, *, deps: Any | None = None) -> int:
    create_workflow = _dependency(
        deps, "create_conformer_screening_workflow", create_conformer_screening_workflow
    )
    print_created_workflow = _dependency(
        deps, "_print_created_workflow", _workflow_output.emit_created_workflow
    )
    payload = create_workflow(
        input_xyz=getattr(args, "input_xyz"),
        workflow_root=getattr(args, "workflow_root"),
        crest_mode=str(getattr(args, "crest_mode", "standard") or "standard"),
        priority=int(getattr(args, "priority", 10) or 10),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        max_orca_stages=int(getattr(args, "max_orca_stages", 20) or 20),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
    )
    return print_created_workflow(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_advance(args: Any, *, deps: Any | None = None) -> int:
    shared_chemstack_config = _dependency(
        deps, "_shared_chemstack_config", _shared_chemstack_config
    )
    advance_workflow_fn = _dependency(deps, "advance_workflow", advance_workflow)
    emit_advance = _dependency(
        deps,
        "emit_workflow_advance",
        _workflow_output.emit_workflow_advance,
    )
    executable = _dependency(deps, "CHEMSTACK_EXECUTABLE", CHEMSTACK_EXECUTABLE)

    shared_config = shared_chemstack_config(args)
    payload = advance_workflow_fn(
        target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root"),
        crest_config=shared_config,
        crest_executable=getattr(args, "crest_executable", "chemstack_crest"),
        crest_repo_root=getattr(args, "crest_repo_root", None),
        xtb_config=shared_config,
        xtb_executable=getattr(args, "xtb_executable", "chemstack_xtb"),
        xtb_repo_root=getattr(args, "xtb_repo_root", None),
        orca_config=shared_config,
        orca_executable=getattr(args, "orca_executable", executable),
        orca_repo_root=getattr(args, "orca_repo_root", None),
        submit_ready=not bool(getattr(args, "no_submit", False)),
    )
    return emit_advance(payload, json_mode=bool(getattr(args, "json", False)))


def _emit_worker_payload(
    payload: dict[str, Any], *, json_mode: bool, single_cycle: bool, deps: Any | None = None
) -> None:
    emit_worker_payload = _dependency(
        deps,
        "emit_worker_payload",
        _workflow_output.emit_worker_payload,
    )
    emit_worker_payload(payload, json_mode=json_mode, single_cycle=single_cycle)


@dataclass(frozen=True)
class _WorkflowWorkerRuntime:
    normalize_text: Any
    shared_chemstack_config: Any
    workflow_root_from_args: Any
    token_factory: Any
    lock_factory: Any
    lock_path: Any
    now: Any
    write_state: Any
    append_event: Any
    advance_registry_once: Any
    emit_error: Any
    emit_worker_lock_error: Any
    emit_worker_payload: Any
    executable: str
    time_module: Any


@dataclass(frozen=True)
class _WorkflowWorkerOptions:
    max_cycles: int
    interval_seconds: float
    lock_timeout_seconds: float
    refresh_registry: bool
    refresh_each_cycle: bool
    service_mode: bool
    json_mode: bool
    workflow_root: Any
    workflow_root_text: str
    worker_session_id: str
    lease_seconds: float
    submit_ready: bool
    engines: WorkflowEngineOptions


def _workflow_worker_runtime(deps: Any | None) -> _WorkflowWorkerRuntime:
    return _WorkflowWorkerRuntime(
        normalize_text=_dependency(deps, "_normalize_text", _normalize_text),
        shared_chemstack_config=_dependency(
            deps,
            "_shared_chemstack_config",
            _shared_chemstack_config,
        ),
        workflow_root_from_args=_dependency(
            deps,
            "_workflow_root_from_args",
            _workflow_root_from_args,
        ),
        token_factory=_dependency(deps, "timestamped_token", timestamped_token),
        lock_factory=_dependency(deps, "file_lock", file_lock),
        lock_path=_dependency(deps, "workflow_worker_lock_path", workflow_worker_lock_path),
        now=_dependency(deps, "now_utc_iso", now_utc_iso),
        write_state=_dependency(
            deps,
            "write_workflow_worker_state",
            write_workflow_worker_state,
        ),
        append_event=_dependency(
            deps,
            "append_workflow_journal_event",
            append_workflow_journal_event,
        ),
        advance_registry_once=_dependency(
            deps,
            "advance_workflow_registry_once",
            advance_workflow_registry_once,
        ),
        emit_error=_dependency(deps, "emit_error", _workflow_output.emit_error),
        emit_worker_lock_error=_dependency(
            deps,
            "emit_worker_lock_error",
            _workflow_output.emit_worker_lock_error,
        ),
        emit_worker_payload=_dependency(deps, "_emit_worker_payload", _emit_worker_payload),
        executable=_dependency(deps, "CHEMSTACK_EXECUTABLE", CHEMSTACK_EXECUTABLE),
        time_module=_dependency(deps, "time", time),
    )


def _workflow_worker_options(args: Any, *, runtime: _WorkflowWorkerRuntime) -> _WorkflowWorkerOptions:
    once = bool(getattr(args, "once", False))
    max_cycles = int(getattr(args, "max_cycles", 0) or 0)
    if once:
        max_cycles = 1
    if max_cycles < 0:
        raise ValueError("--max-cycles must be >= 0")

    interval_seconds = float(getattr(args, "interval_seconds", 30.0) or 30.0)
    shared_config = runtime.shared_chemstack_config(args)
    workflow_root = runtime.workflow_root_from_args(args, config_path=shared_config)
    if not workflow_root:
        raise ValueError(
            "workflow_root is not configured. Pass --workflow-root or set workflow.root in "
            "chemstack.yaml."
        )

    worker_session_id = runtime.normalize_text(
        getattr(args, "worker_session_id", "")
    ) or runtime.token_factory("wf_worker")
    return _WorkflowWorkerOptions(
        max_cycles=max_cycles,
        interval_seconds=interval_seconds,
        lock_timeout_seconds=float(getattr(args, "lock_timeout_seconds", 5.0) or 5.0),
        refresh_registry=bool(getattr(args, "refresh_registry", False)),
        refresh_each_cycle=bool(getattr(args, "refresh_each_cycle", False)),
        service_mode=bool(getattr(args, "service_mode", False)),
        json_mode=bool(getattr(args, "json", False)),
        workflow_root=workflow_root,
        workflow_root_text=str(workflow_root),
        worker_session_id=worker_session_id,
        lease_seconds=max(
            float(getattr(args, "lease_seconds", 60.0) or 60.0),
            interval_seconds * 2.5,
        ),
        submit_ready=not bool(getattr(args, "no_submit", False)),
        engines=WorkflowEngineOptions.from_values(
            crest_config=shared_config,
            crest_executable=getattr(args, "crest_executable", "chemstack_crest"),
            crest_repo_root=getattr(args, "crest_repo_root", None),
            xtb_config=shared_config,
            xtb_executable=getattr(args, "xtb_executable", "chemstack_xtb"),
            xtb_repo_root=getattr(args, "xtb_repo_root", None),
            orca_config=shared_config,
            orca_executable=getattr(args, "orca_executable", runtime.executable),
            orca_repo_root=getattr(args, "orca_repo_root", None),
        ),
    )


def _write_workflow_worker_status(
    runtime: _WorkflowWorkerRuntime,
    options: _WorkflowWorkerOptions,
    *,
    status: str,
    **kwargs: Any,
) -> None:
    runtime.write_state(
        options.workflow_root_text,
        worker_session_id=options.worker_session_id,
        status=status,
        workflow_root_path=options.workflow_root_text,
        interval_seconds=options.interval_seconds,
        submit_ready=options.submit_ready,
        **kwargs,
    )


def _append_workflow_worker_event(
    runtime: _WorkflowWorkerRuntime,
    options: _WorkflowWorkerOptions,
    *,
    event_type: str,
    **kwargs: Any,
) -> None:
    runtime.append_event(
        options.workflow_root_text,
        event_type=event_type,
        worker_session_id=options.worker_session_id,
        **kwargs,
    )


def _advance_workflow_worker_cycle(
    runtime: _WorkflowWorkerRuntime,
    options: _WorkflowWorkerOptions,
    *,
    cycle_count: int,
) -> dict[str, Any]:
    engines = options.engines
    return runtime.advance_registry_once(
        workflow_root=options.workflow_root_text,
        crest_config=engines.crest.config,
        crest_executable=engines.crest.executable,
        crest_repo_root=engines.crest.repo_root,
        xtb_config=engines.xtb.config,
        xtb_executable=engines.xtb.executable,
        xtb_repo_root=engines.xtb.repo_root,
        orca_config=engines.orca.config,
        orca_executable=engines.orca.executable,
        orca_repo_root=engines.orca.repo_root,
        submit_ready=options.submit_ready,
        refresh_registry=options.refresh_each_cycle
        or (options.refresh_registry and cycle_count == 1),
        worker_session_id=options.worker_session_id,
        interval_seconds=options.interval_seconds,
        lease_seconds=options.lease_seconds,
    )


def _record_workflow_worker_started(
    runtime: _WorkflowWorkerRuntime, options: _WorkflowWorkerOptions
) -> None:
    started_at = runtime.now()
    _write_workflow_worker_status(
        runtime,
        options,
        status="starting",
        last_heartbeat_at=started_at,
    )
    _append_workflow_worker_event(
        runtime,
        options,
        event_type="worker_started",
        metadata={"started_at": started_at, "service_mode": options.service_mode},
    )


def _record_workflow_worker_stopped(
    runtime: _WorkflowWorkerRuntime,
    options: _WorkflowWorkerOptions,
    *,
    cycle_count: int,
) -> None:
    stopped_at = runtime.now()
    _write_workflow_worker_status(
        runtime,
        options,
        status="stopped",
        last_cycle_finished_at=stopped_at,
        last_heartbeat_at=stopped_at,
        metadata={
            "stop_reason": "max_cycles_reached",
            "cycle_count": cycle_count,
            "service_mode": options.service_mode,
        },
    )
    _append_workflow_worker_event(
        runtime,
        options,
        event_type="worker_stopped",
        metadata={
            "stopped_at": stopped_at,
            "reason": "max_cycles_reached",
            "cycle_count": cycle_count,
        },
    )


def _record_workflow_worker_interrupted(
    runtime: _WorkflowWorkerRuntime,
    options: _WorkflowWorkerOptions,
    *,
    cycle_count: int,
) -> None:
    stopped_at = runtime.now()
    _write_workflow_worker_status(
        runtime,
        options,
        status="interrupted",
        last_heartbeat_at=stopped_at,
        metadata={
            "stop_reason": "keyboard_interrupt",
            "cycle_count": cycle_count,
            "service_mode": options.service_mode,
        },
    )
    _append_workflow_worker_event(
        runtime,
        options,
        event_type="worker_interrupted",
        metadata={"stopped_at": stopped_at, "cycle_count": cycle_count},
    )


def _record_workflow_worker_lock_error(
    runtime: _WorkflowWorkerRuntime,
    options: _WorkflowWorkerOptions,
    *,
    error: TimeoutError,
) -> None:
    stopped_at = runtime.now()
    _write_workflow_worker_status(
        runtime,
        options,
        status="lock_error",
        last_heartbeat_at=stopped_at,
        metadata={
            "stop_reason": "worker_lock_error",
            "error": str(error),
            "service_mode": options.service_mode,
        },
    )
    _append_workflow_worker_event(
        runtime,
        options,
        event_type="worker_lock_error",
        reason=str(error),
        metadata={"stopped_at": stopped_at},
    )


def _run_workflow_worker_loop(
    runtime: _WorkflowWorkerRuntime, options: _WorkflowWorkerOptions
) -> int:
    cycle_count = 0
    try:
        with runtime.lock_factory(
            runtime.lock_path(options.workflow_root),
            timeout_seconds=options.lock_timeout_seconds,
        ):
            _record_workflow_worker_started(runtime, options)
            while True:
                cycle_count += 1
                payload = _advance_workflow_worker_cycle(
                    runtime,
                    options,
                    cycle_count=cycle_count,
                )
                runtime.emit_worker_payload(
                    payload,
                    json_mode=options.json_mode,
                    single_cycle=options.max_cycles == 1,
                )
                if options.max_cycles > 0 and cycle_count >= options.max_cycles:
                    _record_workflow_worker_stopped(
                        runtime,
                        options,
                        cycle_count=cycle_count,
                    )
                    return 0
                runtime.time_module.sleep(max(0.0, options.interval_seconds))
    except KeyboardInterrupt:
        _record_workflow_worker_interrupted(runtime, options, cycle_count=cycle_count)
        return 130
    except TimeoutError as exc:
        _record_workflow_worker_lock_error(runtime, options, error=exc)
        runtime.emit_worker_lock_error(exc)
        return 1


def cmd_workflow_worker(args: Any, *, deps: Any | None = None) -> int:
    runtime = _workflow_worker_runtime(deps)
    try:
        options = _workflow_worker_options(args, runtime=runtime)
    except ValueError as exc:
        runtime.emit_error(exc)
        return 1
    return _run_workflow_worker_loop(runtime, options)


def cmd_workflow_runtime_status(args: Any, *, deps: Any | None = None) -> int:
    get_status = _dependency(deps, "get_workflow_runtime_status", get_workflow_runtime_status)
    emit_status = _dependency(
        deps,
        "emit_workflow_runtime_status",
        _workflow_output.emit_workflow_runtime_status,
    )
    payload = get_status(workflow_root=getattr(args, "workflow_root"))
    return emit_status(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_journal(args: Any, *, deps: Any | None = None) -> int:
    get_journal = _dependency(deps, "get_workflow_journal", get_workflow_journal)
    emit_journal = _dependency(
        deps,
        "emit_workflow_journal",
        _workflow_output.emit_workflow_journal,
    )
    payload = get_journal(
        workflow_root=getattr(args, "workflow_root"),
        limit=int(getattr(args, "limit", 50) or 0),
    )
    return emit_journal(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_telemetry(args: Any, *, deps: Any | None = None) -> int:
    get_telemetry = _dependency(deps, "get_workflow_telemetry", get_workflow_telemetry)
    emit_telemetry = _dependency(
        deps,
        "emit_workflow_telemetry",
        _workflow_output.emit_workflow_telemetry,
    )
    payload = get_telemetry(
        workflow_root=getattr(args, "workflow_root"),
        limit=int(getattr(args, "limit", 200) or 0),
    )
    return emit_telemetry(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_submit_reaction_ts_search(args: Any, *, deps: Any | None = None) -> int:
    shared_chemstack_config = _dependency(
        deps, "_shared_chemstack_config", _shared_chemstack_config
    )
    default_config_path = _dependency(
        deps, "default_config_path_from_repo_root", default_config_path_from_repo_root
    )
    project_root = _dependency(deps, "_project_root", _project_root)
    submit_workflow = _dependency(
        deps, "submit_reaction_ts_search_workflow", submit_reaction_ts_search_workflow
    )
    emit_submission = _dependency(
        deps,
        "emit_workflow_submit_reaction_ts_search",
        _workflow_output.emit_workflow_submit_reaction_ts_search,
    )
    executable = _dependency(deps, "CHEMSTACK_EXECUTABLE", CHEMSTACK_EXECUTABLE)

    shared_config = shared_chemstack_config(args) or default_config_path(project_root())
    payload = submit_workflow(
        workflow_target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root", None),
        orca_config=shared_config,
        orca_executable=getattr(args, "orca_executable", executable),
        orca_repo_root=getattr(args, "orca_repo_root", None),
        skip_submitted=not bool(getattr(args, "resubmit", False)),
    )
    return emit_submission(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_bot(args: Any, *, deps: Any | None = None) -> int:
    from .telegram_bot import run_bot

    return int(run_bot())


def cmd_workflow_list(args: Any, *, deps: Any | None = None) -> int:
    list_workflows_fn = _dependency(deps, "list_workflows", list_workflows)
    emit_list = _dependency(
        deps,
        "emit_workflow_list",
        _workflow_output.emit_workflow_list,
    )
    payload = list_workflows_fn(
        workflow_root=getattr(args, "workflow_root"),
        limit=int(getattr(args, "limit", 0) or 0),
        refresh=bool(getattr(args, "refresh", False)),
    )
    return emit_list(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_get(args: Any, *, deps: Any | None = None) -> int:
    get_workflow_fn = _dependency(deps, "get_workflow", get_workflow)
    emit_get = _dependency(
        deps,
        "emit_workflow_get",
        _workflow_output.emit_workflow_get,
    )
    response = get_workflow_fn(
        target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root", None),
        sync_registry=True,
    )
    return emit_get(response, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_artifacts(args: Any, *, deps: Any | None = None) -> int:
    get_artifacts = _dependency(deps, "get_workflow_artifacts", get_workflow_artifacts)
    emit_artifacts = _dependency(
        deps,
        "emit_workflow_artifacts",
        _workflow_output.emit_workflow_artifacts,
    )
    response = get_artifacts(
        target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root", None),
        sync_registry=True,
    )
    return emit_artifacts(response, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_cancel(args: Any, *, deps: Any | None = None) -> int:
    cancel_workflow_fn = _dependency(deps, "cancel_workflow", cancel_workflow)
    emit_cancel = _dependency(
        deps,
        "emit_workflow_cancel",
        _workflow_output.emit_workflow_cancel,
    )
    emit_error = _dependency(deps, "emit_error", _workflow_output.emit_error)
    executable = _dependency(deps, "CHEMSTACK_EXECUTABLE", CHEMSTACK_EXECUTABLE)
    try:
        payload = cancel_workflow_fn(
            target=getattr(args, "target"),
            workflow_root=getattr(args, "workflow_root", None),
            crest_config=getattr(args, "crest_config", None),
            crest_executable=getattr(args, "crest_executable", "chemstack_crest"),
            crest_repo_root=getattr(args, "crest_repo_root", None),
            xtb_config=getattr(args, "xtb_config", None),
            xtb_executable=getattr(args, "xtb_executable", "chemstack_xtb"),
            xtb_repo_root=getattr(args, "xtb_repo_root", None),
            orca_config=getattr(args, "orca_config", None),
            orca_executable=getattr(args, "orca_executable", executable),
            orca_repo_root=getattr(args, "orca_repo_root", None),
        )
    except (ValueError, TimeoutError) as exc:
        emit_error(exc)
        return 1
    return emit_cancel(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_reindex(args: Any, *, deps: Any | None = None) -> int:
    reindex = _dependency(deps, "reindex_workflow_registry", reindex_workflow_registry)
    emit_reindex = _dependency(
        deps,
        "emit_workflow_reindex",
        _workflow_output.emit_workflow_reindex,
    )
    records = reindex(getattr(args, "workflow_root"))
    payload = {
        "workflow_root": str(getattr(args, "workflow_root")),
        "count": len(records),
        "workflow_ids": [record.workflow_id for record in records],
    }
    return emit_reindex(
        payload,
        records=records,
        json_mode=bool(getattr(args, "json", False)),
    )
