from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

AnyCallable = Callable[..., Any]


@dataclass(frozen=True)
class CommonCliDeps:
    _discover_shared_config_path: AnyCallable
    _discover_workflow_root: AnyCallable
    _effective_shared_config_text: AnyCallable
    shared_workflow_root_from_config: AnyCallable


@dataclass(frozen=True)
class QueueCliDeps:
    _effective_shared_config_text: AnyCallable
    _workflow_root_for_args: AnyCallable
    _queue_table_now: AnyCallable
    _queue_table_lines: AnyCallable
    _cmd_queue_list_clear: AnyCallable
    _queue_list_payload: AnyCallable
    _filtered_queue_payload: AnyCallable
    _print_queue_list_text: AnyCallable
    clear_activities: AnyCallable
    list_activities: AnyCallable
    count_global_active_simulations: AnyCallable
    cancel_activity: AnyCallable


@dataclass(frozen=True)
class WorkerCliDeps:
    _discover_shared_config_path: AnyCallable
    _effective_shared_config_text: AnyCallable
    _workflow_root_for_args: AnyCallable
    _repo_root_for_subprocess: AnyCallable
    sibling_app_command: AnyCallable
    subprocess: Any
    signal: Any
    time: Any
    _build_worker_specs: AnyCallable
    _emit_supervisor_specs_json: AnyCallable
    _detect_existing_orca_worker_conflict: AnyCallable
    _emit_existing_orca_worker_conflict: AnyCallable
    _run_worker_supervisor: AnyCallable
    _spawn_supervised_worker: AnyCallable
    _terminate_process: AnyCallable


@dataclass(frozen=True)
class RunDirCliDeps:
    _configure_orca_logging: AnyCallable
    _engine_config_for_command: AnyCallable
    _detect_run_dir_app: AnyCallable
    cmd_orca_run_dir: AnyCallable
    cmd_workflow_run_dir: AnyCallable


@dataclass(frozen=True)
class SummaryCliDeps:
    _configure_orca_logging: AnyCallable
    _engine_config_for_command: AnyCallable
    cmd_orca_summary: AnyCallable


__all__ = [
    "CommonCliDeps",
    "QueueCliDeps",
    "RunDirCliDeps",
    "SummaryCliDeps",
    "WorkerCliDeps",
]
