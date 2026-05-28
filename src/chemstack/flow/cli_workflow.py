from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR
from chemstack.core.utils import file_lock, now_utc_iso, timestamped_token
from chemstack.core.utils.coercion import normalize_text
from chemstack.cli_common import (
    _dependency,
    _shared_chemstack_config,
    _workflow_root_for_args,
)
from . import cli_workflow_output as _workflow_output
from .engine_options import WorkflowEngineOptions
from .registry import (
    append_workflow_journal_event,
    write_workflow_worker_state,
)
from .runtime import advance_workflow_registry_once, workflow_worker_lock_path


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
        normalize_text=_dependency(deps, "_normalize_text", normalize_text),
        shared_chemstack_config=_dependency(
            deps,
            "_shared_chemstack_config",
            _shared_chemstack_config,
        ),
        workflow_root_from_args=_dependency(
            deps,
            "_workflow_root_for_args",
            _workflow_root_for_args,
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
        time_module=_dependency(deps, "time", time),
    )


def _workflow_worker_options(
    args: Any, *, runtime: _WorkflowWorkerRuntime
) -> _WorkflowWorkerOptions:
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
            shared_config=shared_config,
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
        shared_config=engines.shared_config,
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.flow.cli_workflow")
    parser.add_argument(
        "--workflow-root",
        required=True,
        help="Root that directly contains workflow workspaces.",
    )
    parser.add_argument(
        "--chemstack-config",
        default=str(os.getenv(CHEMSTACK_CONFIG_ENV_VAR, "")).strip() or None,
        help="Path to shared chemstack.yaml.",
    )
    parser.add_argument(
        "--no-submit",
        action="store_true",
        help="Only sync/append stages; do not submit newly actionable stages.",
    )
    parser.add_argument("--once", action="store_true", help="Run exactly one orchestration cycle.")
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Optional cycle limit; 0 means run forever.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=30.0,
        help="Sleep interval between orchestration cycles.",
    )
    parser.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=5.0,
        help="How long to wait for the worker lock.",
    )
    parser.add_argument(
        "--refresh-registry",
        action="store_true",
        help="Reindex the workflow registry before the first cycle.",
    )
    parser.add_argument(
        "--refresh-each-cycle",
        action="store_true",
        help="Reindex the workflow registry before every cycle.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    return int(cmd_workflow_worker(build_parser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
