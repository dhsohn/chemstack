from __future__ import annotations

import argparse
import signal as signal
import subprocess
import time as time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from chemstack import cli_common as _cli_common
from chemstack import cli_queue as _cli_queue
from chemstack import cli_run_dir as _cli_run_dir
from chemstack import cli_summary as _cli_summary
from chemstack import cli_workers as _cli_workers
from chemstack.cli_deps import (
    CommonCliDeps as _CommonCliDeps,
    QueueCliDeps as _QueueCliDeps,
    RunDirCliDeps as _RunDirCliDeps,
    SummaryCliDeps as _SummaryCliDeps,
    WorkerCliDeps as _WorkerCliDeps,
)
from chemstack.activity_view import (
    activity_with_parent_hint as activity_with_parent_hint,
    count_global_active_simulations as count_global_active_simulations,
    queue_list_default_visible_items as queue_list_default_visible_items,
    queue_list_display_rows as queue_list_display_rows,
)
from chemstack.core.app_ids import (
    CHEMSTACK_CONFIG_ENV_VAR as CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_CREST_MODULE as CHEMSTACK_CREST_MODULE,
    CHEMSTACK_FLOW_MODULE as CHEMSTACK_FLOW_MODULE,
    CHEMSTACK_ORCA_INTERNAL_MODULE as CHEMSTACK_ORCA_INTERNAL_MODULE,
    CHEMSTACK_XTB_MODULE as CHEMSTACK_XTB_MODULE,
)
from chemstack.core.config.files import (
    shared_workflow_root_from_config as shared_workflow_root_from_config,
)
from chemstack.flow.operations import (
    cancel_activity as cancel_activity,
    clear_activities as clear_activities,
    list_activities as list_activities,
)
from chemstack.flow.run_dir_layout import inspect_workflow_run_dir as inspect_workflow_run_dir
from chemstack.flow.submitters.common import (
    normalize_text as normalize_text,
    sibling_app_command as sibling_app_command,
)

_WORKFLOW_SCAFFOLD_SHORTCUTS = (
    ("ts_search", "reaction_ts_search", "Create a reaction TS-search scaffold."),
    ("conformer_search", "conformer_screening", "Create a conformer-screening scaffold."),
)

WorkerSpec = _cli_workers.WorkerSpec
_SupervisedWorker = _cli_workers._SupervisedWorker
_ExistingWorkerConflict = _cli_workers._ExistingWorkerConflict
_SupervisorShutdown = _cli_workers._SupervisorShutdown
_QueueListRequest = _cli_queue._QueueListRequest

_WORKFLOW_ENGINE_APPS = _cli_workers._WORKFLOW_ENGINE_APPS
_ENGINE_APPS = _cli_workers._ENGINE_APPS
_KNOWN_WORKER_APPS = _cli_workers._KNOWN_WORKER_APPS
_DEFAULT_WORKER_APPS = _cli_workers._DEFAULT_WORKER_APPS
_WORKER_POLL_INTERVAL_SECONDS = _cli_workers._WORKER_POLL_INTERVAL_SECONDS
_WORKER_STARTUP_FAILURE_WINDOW_SECONDS = _cli_workers._WORKER_STARTUP_FAILURE_WINDOW_SECONDS
_WORKER_MAX_STARTUP_FAILURES = _cli_workers._WORKER_MAX_STARTUP_FAILURES
_DIRECT_ENGINE_WORKER_ENV_VAR = _cli_workers._DIRECT_ENGINE_WORKER_ENV_VAR
_DEFAULT_QUEUE_TABLE_NOW = _cli_queue._DEFAULT_QUEUE_TABLE_NOW

if TYPE_CHECKING:
    _activity_counter_config_path: Callable[..., Any]
    _add_workflow_worker_spec: Callable[..., Any]
    _build_worker_specs: Callable[..., Any]
    _classify_existing_orca_worker: Callable[..., Any]
    _cmd_queue_list_clear: Callable[..., Any]
    _command_invokes_module: Callable[..., Any]
    _command_program_name: Callable[..., Any]
    _configure_orca_logging: Callable[..., Any]
    _detect_existing_orca_worker_conflict: Callable[..., Any]
    _detect_run_dir_app: Callable[..., Any]
    _discover_shared_config_path: Callable[..., Any]
    _discover_workflow_root: Callable[..., Any]
    _effective_shared_config_text: Callable[..., Any]
    _emit_existing_orca_worker_conflict: Callable[..., Any]
    _emit_supervisor_specs_json: Callable[..., Any]
    _engine_config_for_command: Callable[..., Any]
    _engine_worker_spec: Callable[..., Any]
    _engine_worker_tail_argv: Callable[..., Any]
    _filter_activity_items: Callable[..., Any]
    _filtered_queue_payload: Callable[..., Any]
    _format_command_argv: Callable[..., Any]
    _install_supervisor_signal_handlers: Callable[..., Any]
    _normalize_filter_values: Callable[..., Any]
    _poll_supervised_workers: Callable[..., Any]
    _print_queue_list_text: Callable[..., Any]
    _queue_clear_lines: Callable[..., Any]
    _queue_display_width: Callable[..., Any]
    _queue_elapsed_text: Callable[..., Any]
    _queue_list_display_rows: Callable[..., Any]
    _queue_list_payload: Callable[..., Any]
    _queue_list_request: Callable[..., Any]
    _queue_table_lines: Callable[..., Any]
    _queue_table_now: Callable[..., Any]
    _read_process_command: Callable[..., Any]
    _repo_root: Callable[..., Any]
    _repo_root_for_subprocess: Callable[..., Any]
    _reset_stable_startup_failure_count: Callable[..., Any]
    _restore_signal_handlers: Callable[..., Any]
    _restart_or_stop_worker: Callable[..., Any]
    _run_worker_supervisor: Callable[..., Any]
    _selected_worker_apps: Callable[..., Any]
    _spawn_supervised_worker: Callable[..., Any]
    _supervise_worker_processes: Callable[..., Any]
    _terminate_process: Callable[..., Any]
    _terminate_supervised_workers: Callable[..., Any]
    _validate_engine_worker_config: Callable[..., Any]
    _worker_engine_apps: Callable[..., Any]
    _workflow_only_worker_flag_error: Callable[..., Any]
    _workflow_root_for_args: Callable[..., Any]
    _workflow_worker_spec: Callable[..., Any]
    cmd_init: Callable[..., Any]
    cmd_orca_organize: Callable[..., Any]
    cmd_orca_run_dir: Callable[..., Any]
    cmd_orca_summary: Callable[..., Any]
    cmd_queue_cancel: Callable[..., Any]
    cmd_queue_list: Callable[..., Any]
    cmd_queue_worker: Callable[..., Any]
    cmd_run_dir: Callable[..., Any]
    cmd_summary: Callable[..., Any]
    cmd_workflow_run_dir: Callable[..., Any]
    cmd_workflow_scaffold: Callable[..., Any]


def _current(name: str) -> Any:
    return globals()[name]


def _common_deps() -> _CommonCliDeps:
    return _CommonCliDeps(
        _discover_shared_config_path=_current("_discover_shared_config_path"),
        _discover_workflow_root=_current("_discover_workflow_root"),
        _effective_shared_config_text=_current("_effective_shared_config_text"),
        shared_workflow_root_from_config=shared_workflow_root_from_config,
    )


def _queue_deps() -> _QueueCliDeps:
    return _QueueCliDeps(
        _effective_shared_config_text=_current("_effective_shared_config_text"),
        _workflow_root_for_args=_current("_workflow_root_for_args"),
        _queue_table_now=_current("_queue_table_now"),
        _queue_table_lines=_current("_queue_table_lines"),
        _cmd_queue_list_clear=_current("_cmd_queue_list_clear"),
        _queue_list_payload=_current("_queue_list_payload"),
        _filtered_queue_payload=_current("_filtered_queue_payload"),
        _print_queue_list_text=_current("_print_queue_list_text"),
        clear_activities=clear_activities,
        list_activities=list_activities,
        count_global_active_simulations=count_global_active_simulations,
        cancel_activity=cancel_activity,
    )


def _worker_deps() -> _WorkerCliDeps:
    return _WorkerCliDeps(
        _discover_shared_config_path=_current("_discover_shared_config_path"),
        _effective_shared_config_text=_current("_effective_shared_config_text"),
        _workflow_root_for_args=_current("_workflow_root_for_args"),
        _repo_root_for_subprocess=_current("_repo_root_for_subprocess"),
        sibling_app_command=sibling_app_command,
        subprocess=subprocess,
        signal=signal,
        time=time,
        _build_worker_specs=_current("_build_worker_specs"),
        _emit_supervisor_specs_json=_current("_emit_supervisor_specs_json"),
        _detect_existing_orca_worker_conflict=_current("_detect_existing_orca_worker_conflict"),
        _emit_existing_orca_worker_conflict=_current("_emit_existing_orca_worker_conflict"),
        _run_worker_supervisor=_current("_run_worker_supervisor"),
        _spawn_supervised_worker=_current("_spawn_supervised_worker"),
        _terminate_process=_current("_terminate_process"),
    )


def _run_dir_deps() -> _RunDirCliDeps:
    return _RunDirCliDeps(
        _configure_orca_logging=_current("_configure_orca_logging"),
        _engine_config_for_command=_current("_engine_config_for_command"),
        _detect_run_dir_app=_current("_detect_run_dir_app"),
        cmd_orca_run_dir=_current("cmd_orca_run_dir"),
        cmd_workflow_run_dir=_current("cmd_workflow_run_dir"),
    )


def _summary_deps() -> _SummaryCliDeps:
    return _SummaryCliDeps(
        _configure_orca_logging=_current("_configure_orca_logging"),
        _engine_config_for_command=_current("_engine_config_for_command"),
        cmd_orca_summary=_current("cmd_orca_summary"),
    )


def _delegate(module: Any, name: str) -> Callable[..., Any]:
    target = getattr(module, name)

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        return target(*args, **kwargs)

    _wrapped.__name__ = name
    _wrapped.__doc__ = getattr(target, "__doc__", None)
    return _wrapped


def _delegate_with_deps(
    module: Any,
    name: str,
    deps_factory: Callable[[], Any],
) -> Callable[..., Any]:
    target = getattr(module, name)

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        return target(*args, **kwargs, deps=deps_factory())

    _wrapped.__name__ = name
    _wrapped.__doc__ = getattr(target, "__doc__", None)
    return _wrapped


_COMMON_DELEGATES = {
    "_repo_root": "_repo_root",
    "_repo_root_for_subprocess": "_repo_root_for_subprocess",
    "_discover_shared_config_path": "_discover_shared_config_path",
    "_discover_workflow_root": "_discover_workflow_root",
    "_effective_shared_config_text": "_effective_shared_config_text",
    "_configure_orca_logging": "_configure_orca_logging",
}

_COMMON_DEPS_DELEGATES = {
    "_workflow_root_for_args": "_workflow_root_for_args",
    "_engine_config_for_command": "_engine_config_for_command",
}

_QUEUE_DELEGATES = {
    "_normalize_filter_values": "_normalize_filter_values",
    "_filter_activity_items": "_filter_activity_items",
    "_activity_counter_config_path": "_activity_counter_config_path",
    "_queue_table_now": "_queue_table_now",
    "_queue_elapsed_text": "_queue_elapsed_text",
    "_queue_display_width": "_queue_display_width",
    "_queue_clear_lines": "_queue_clear_lines",
    "_queue_list_display_rows": "_queue_list_display_rows",
}

_QUEUE_DEPS_DELEGATES = {
    "_queue_table_lines": "_queue_table_lines",
    "_queue_list_request": "_queue_list_request",
    "_cmd_queue_list_clear": "_cmd_queue_list_clear",
    "_queue_list_payload": "_queue_list_payload",
    "_filtered_queue_payload": "_filtered_queue_payload",
    "_print_queue_list_text": "_print_queue_list_text",
    "cmd_queue_list": "cmd_queue_list",
    "cmd_queue_cancel": "cmd_queue_cancel",
}

_WORKER_DELEGATES = {
    "_read_process_command": "_read_process_command",
    "_command_invokes_module": "_command_invokes_module",
    "_command_program_name": "_command_program_name",
    "_classify_existing_orca_worker": "_classify_existing_orca_worker",
    "_format_command_argv": "_format_command_argv",
    "_selected_worker_apps": "_selected_worker_apps",
    "_engine_worker_tail_argv": "_engine_worker_tail_argv",
    "_workflow_worker_spec": "_workflow_worker_spec",
    "_worker_engine_apps": "_worker_engine_apps",
    "_validate_engine_worker_config": "_validate_engine_worker_config",
    "_workflow_only_worker_flag_error": "_workflow_only_worker_flag_error",
    "_add_workflow_worker_spec": "_add_workflow_worker_spec",
    "_reset_stable_startup_failure_count": "_reset_stable_startup_failure_count",
    "_emit_existing_orca_worker_conflict": "_emit_existing_orca_worker_conflict",
    "_emit_supervisor_specs_json": "_emit_supervisor_specs_json",
}

_WORKER_DEPS_DELEGATES = {
    "_detect_existing_orca_worker_conflict": "_detect_existing_orca_worker_conflict",
    "_engine_worker_spec": "_engine_worker_spec",
    "_build_worker_specs": "_build_worker_specs",
    "_terminate_process": "_terminate_process",
    "_spawn_supervised_worker": "_spawn_supervised_worker",
    "_install_supervisor_signal_handlers": "_install_supervisor_signal_handlers",
    "_restore_signal_handlers": "_restore_signal_handlers",
    "_restart_or_stop_worker": "_restart_or_stop_worker",
    "_poll_supervised_workers": "_poll_supervised_workers",
    "_supervise_worker_processes": "_supervise_worker_processes",
    "_terminate_supervised_workers": "_terminate_supervised_workers",
    "_run_worker_supervisor": "_run_worker_supervisor",
    "cmd_queue_worker": "cmd_queue_worker",
}

_RUN_DIR_DEPS_DELEGATES = {
    "cmd_init": "cmd_init",
    "cmd_orca_run_dir": "cmd_orca_run_dir",
    "cmd_orca_organize": "cmd_orca_organize",
    "cmd_workflow_scaffold": "cmd_workflow_scaffold",
    "cmd_run_dir": "cmd_run_dir",
    "cmd_workflow_run_dir": "cmd_workflow_run_dir",
}

_RUN_DIR_DELEGATES = {
    "_detect_run_dir_app": "_detect_run_dir_app",
}

_SUMMARY_DEPS_DELEGATES = {
    "cmd_orca_summary": "cmd_orca_summary",
    "cmd_summary": "cmd_summary",
}

for public_name, target_name in _COMMON_DELEGATES.items():
    globals()[public_name] = _delegate(_cli_common, target_name)

for public_name, target_name in _COMMON_DEPS_DELEGATES.items():
    globals()[public_name] = _delegate_with_deps(_cli_common, target_name, _common_deps)

for public_name, target_name in _QUEUE_DELEGATES.items():
    globals()[public_name] = _delegate(_cli_queue, target_name)

for public_name, target_name in _QUEUE_DEPS_DELEGATES.items():
    globals()[public_name] = _delegate_with_deps(_cli_queue, target_name, _queue_deps)

for public_name, target_name in _WORKER_DELEGATES.items():
    globals()[public_name] = _delegate(_cli_workers, target_name)

for public_name, target_name in _WORKER_DEPS_DELEGATES.items():
    globals()[public_name] = _delegate_with_deps(_cli_workers, target_name, _worker_deps)

for public_name, target_name in _RUN_DIR_DELEGATES.items():
    globals()[public_name] = _delegate(_cli_run_dir, target_name)

for public_name, target_name in _RUN_DIR_DEPS_DELEGATES.items():
    globals()[public_name] = _delegate_with_deps(_cli_run_dir, target_name, _run_dir_deps)

for public_name, target_name in _SUMMARY_DEPS_DELEGATES.items():
    globals()[public_name] = _delegate_with_deps(_cli_summary, target_name, _summary_deps)


def build_parser() -> argparse.ArgumentParser:
    from chemstack.cli_parsers import build_parser as _build_parser

    return _build_parser()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
