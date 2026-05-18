from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CommonCliDeps:
    _discover_shared_config_path: Any
    _discover_workflow_root: Any
    _effective_shared_config_text: Any
    shared_workflow_root_from_config: Any


@dataclass(frozen=True)
class QueueCliDeps:
    _effective_shared_config_text: Any
    _workflow_root_for_args: Any
    _queue_table_now: Any
    _queue_table_lines: Any
    _cmd_queue_list_clear: Any
    _queue_list_payload: Any
    _filtered_queue_payload: Any
    _print_queue_list_text: Any
    clear_activities: Any
    list_activities: Any
    count_global_active_simulations: Any
    cancel_activity: Any


@dataclass(frozen=True)
class WorkerCliDeps:
    _discover_shared_config_path: Any
    _effective_shared_config_text: Any
    _workflow_root_for_args: Any
    _repo_root_for_subprocess: Any
    sibling_app_command: Any
    subprocess: Any
    signal: Any
    time: Any
    _build_worker_specs: Any
    _emit_supervisor_specs_json: Any
    _detect_existing_orca_worker_conflict: Any
    _emit_existing_orca_worker_conflict: Any
    _run_worker_supervisor: Any
    _spawn_supervised_worker: Any
    _terminate_process: Any


@dataclass(frozen=True)
class RunDirCliDeps:
    _configure_orca_logging: Any
    _engine_config_for_command: Any
    _detect_run_dir_app: Any
    cmd_orca_run_dir: Any
    cmd_workflow_run_dir: Any


@dataclass(frozen=True)
class SummaryCliDeps:
    _configure_orca_logging: Any
    _engine_config_for_command: Any
    cmd_orca_summary: Any


__all__ = [
    "CommonCliDeps",
    "QueueCliDeps",
    "RunDirCliDeps",
    "SummaryCliDeps",
    "WorkerCliDeps",
]
