from __future__ import annotations

import argparse
import signal as signal
import subprocess
import sys
import time as time
from typing import Any, Sequence

from chemstack import cli_common as _cli_common
from chemstack import cli_queue as _cli_queue
from chemstack import cli_run_dir as _cli_run_dir
from chemstack import cli_summary as _cli_summary
from chemstack import cli_workers as _cli_workers
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


def _this_module() -> Any:
    return sys.modules[__name__]


def _repo_root() -> Any:
    return _cli_common._repo_root()


def _repo_root_for_subprocess() -> str | None:
    return _cli_common._repo_root_for_subprocess()


def _discover_shared_config_path(explicit: str | None) -> str | None:
    return _cli_common._discover_shared_config_path(explicit)


def _discover_workflow_root(explicit: str | None) -> str | None:
    return _cli_common._discover_workflow_root(explicit)


def _workflow_root_for_args(args: Any) -> str | None:
    return _cli_common._workflow_root_for_args(args, deps=_this_module())


def _effective_shared_config_text(args: argparse.Namespace) -> str:
    return _cli_common._effective_shared_config_text(args)


def _engine_config_for_command(args: argparse.Namespace) -> str | None:
    return _cli_common._engine_config_for_command(args, deps=_this_module())


def _configure_orca_logging(args: argparse.Namespace) -> None:
    return _cli_common._configure_orca_logging(args)


def _normalize_filter_values(values: Sequence[str] | None) -> tuple[str, ...]:
    return _cli_queue._normalize_filter_values(values)


def _filter_activity_items(
    items: Sequence[dict[str, Any]],
    *,
    engines: Sequence[str] | None = None,
    statuses: Sequence[str] | None = None,
    kinds: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    return _cli_queue._filter_activity_items(items, engines=engines, statuses=statuses, kinds=kinds)


def _activity_counter_config_path(
    *,
    payload: dict[str, Any],
    config_hint: str | None,
) -> str | None:
    return _cli_queue._activity_counter_config_path(payload=payload, config_hint=config_hint)


def _queue_table_now() -> Any:
    return _cli_queue._queue_table_now()


def _queue_elapsed_text(item: dict[str, Any], *, now: Any | None = None) -> str:
    return _cli_queue._queue_elapsed_text(item, now=now)


def _queue_display_width(value: str) -> int:
    return _cli_queue._queue_display_width(value)


def _queue_table_lines(rows: Sequence[tuple[int, dict[str, Any]]]) -> list[str]:
    return _cli_queue._queue_table_lines(rows, deps=_this_module())


def _queue_clear_lines(payload: dict[str, Any]) -> list[str]:
    return _cli_queue._queue_clear_lines(payload)


def _queue_list_request(args: Any) -> _QueueListRequest:
    return _cli_queue._queue_list_request(args, deps=_this_module())


def _cmd_queue_list_clear(args: Any, request: _QueueListRequest) -> int:
    return _cli_queue._cmd_queue_list_clear(args, request, deps=_this_module())


def _queue_list_payload(args: Any, request: _QueueListRequest) -> dict[str, Any]:
    return _cli_queue._queue_list_payload(args, request, deps=_this_module())


def _filtered_queue_payload(
    payload: dict[str, Any],
    request: _QueueListRequest,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    return _cli_queue._filtered_queue_payload(payload, request, deps=_this_module())


def _queue_list_display_rows(
    *,
    payload: dict[str, Any],
    filtered_activities: Sequence[dict[str, Any]],
    request: _QueueListRequest,
) -> list[tuple[int, dict[str, Any]]]:
    return _cli_queue._queue_list_display_rows(
        payload=payload,
        filtered_activities=filtered_activities,
        request=request,
    )


def _print_queue_list_text(
    *,
    payload: dict[str, Any],
    filtered_payload: dict[str, Any],
    filtered_activities: Sequence[dict[str, Any]],
    request: _QueueListRequest,
) -> int:
    return _cli_queue._print_queue_list_text(
        payload=payload,
        filtered_payload=filtered_payload,
        filtered_activities=filtered_activities,
        request=request,
        deps=_this_module(),
    )


def cmd_queue_list(args: Any) -> int:
    return _cli_queue.cmd_queue_list(args, deps=_this_module())


def cmd_queue_cancel(args: Any) -> int:
    return _cli_queue.cmd_queue_cancel(args, deps=_this_module())


def _read_process_command(pid: int) -> tuple[str, ...]:
    return _cli_workers._read_process_command(pid)


def _command_invokes_module(command_argv: Sequence[str], module_name: str) -> bool:
    return _cli_workers._command_invokes_module(command_argv, module_name)


def _command_program_name(command_argv: Sequence[str]) -> str:
    return _cli_workers._command_program_name(command_argv)


def _classify_existing_orca_worker(command_argv: Sequence[str]) -> str:
    return _cli_workers._classify_existing_orca_worker(command_argv)


def _format_command_argv(command_argv: Sequence[str]) -> str:
    return _cli_workers._format_command_argv(command_argv)


def _detect_existing_orca_worker_conflict(
    specs: Sequence[WorkerSpec],
    *,
    args: argparse.Namespace,
) -> _ExistingWorkerConflict | None:
    return _cli_workers._detect_existing_orca_worker_conflict(specs, args=args, deps=_this_module())


def _emit_existing_orca_worker_conflict(
    conflict: _ExistingWorkerConflict,
    *,
    command_name: str,
) -> int:
    return _cli_workers._emit_existing_orca_worker_conflict(conflict, command_name=command_name)


def _selected_worker_apps(values: Sequence[str] | None) -> list[str]:
    return _cli_workers._selected_worker_apps(values)


def _engine_worker_tail_argv(*, app: str, args: argparse.Namespace) -> list[str]:
    return _cli_workers._engine_worker_tail_argv(app=app, args=args)


def _engine_worker_spec(*, app: str, config_path: str, args: argparse.Namespace) -> WorkerSpec:
    return _cli_workers._engine_worker_spec(
        app=app, config_path=config_path, args=args, deps=_this_module()
    )


def _workflow_worker_spec(
    *,
    workflow_root: str,
    config_path: str | None,
    args: argparse.Namespace,
) -> WorkerSpec:
    return _cli_workers._workflow_worker_spec(
        workflow_root=workflow_root,
        config_path=config_path,
        args=args,
    )


def _worker_engine_apps(apps: Sequence[str], *, workflow_enabled: bool) -> list[str]:
    return _cli_workers._worker_engine_apps(apps, workflow_enabled=workflow_enabled)


def _validate_engine_worker_config(engine_apps: Sequence[str], config_path: str | None) -> None:
    return _cli_workers._validate_engine_worker_config(engine_apps, config_path)


def _workflow_only_worker_flag_error(args: Any) -> str | None:
    return _cli_workers._workflow_only_worker_flag_error(args)


def _add_workflow_worker_spec(
    specs: list[WorkerSpec],
    *,
    apps: Sequence[str],
    explicit_app_selection: bool,
    workflow_root: str | None,
    config_path: str | None,
    args: argparse.Namespace,
) -> None:
    return _cli_workers._add_workflow_worker_spec(
        specs,
        apps=apps,
        explicit_app_selection=explicit_app_selection,
        workflow_root=workflow_root,
        config_path=config_path,
        args=args,
    )


def _build_worker_specs(args: Any) -> list[WorkerSpec]:
    return _cli_workers._build_worker_specs(args, deps=_this_module())


def _terminate_process(proc: subprocess.Popen[Any]) -> None:
    return _cli_workers._terminate_process(proc, deps=_this_module())


def _spawn_supervised_worker(spec: WorkerSpec, *, restart: bool = False) -> _SupervisedWorker:
    return _cli_workers._spawn_supervised_worker(spec, restart=restart, deps=_this_module())


def _install_supervisor_signal_handlers(shutdown: _SupervisorShutdown) -> dict[Any, Any]:
    return _cli_workers._install_supervisor_signal_handlers(shutdown, deps=_this_module())


def _restore_signal_handlers(previous_handlers: dict[Any, Any]) -> None:
    return _cli_workers._restore_signal_handlers(previous_handlers, deps=_this_module())


def _reset_stable_startup_failure_count(
    managed: _SupervisedWorker,
    current_time: float,
) -> None:
    return _cli_workers._reset_stable_startup_failure_count(managed, current_time)


def _restart_or_stop_worker(
    processes: list[_SupervisedWorker],
    *,
    index: int,
    managed: _SupervisedWorker,
    returncode: int,
    current_time: float,
) -> int | None:
    return _cli_workers._restart_or_stop_worker(
        processes,
        index=index,
        managed=managed,
        returncode=returncode,
        current_time=current_time,
        deps=_this_module(),
    )


def _poll_supervised_workers(
    processes: list[_SupervisedWorker],
    shutdown: _SupervisorShutdown,
) -> int | None:
    return _cli_workers._poll_supervised_workers(processes, shutdown, deps=_this_module())


def _supervise_worker_processes(
    processes: list[_SupervisedWorker],
    shutdown: _SupervisorShutdown,
) -> int:
    return _cli_workers._supervise_worker_processes(processes, shutdown, deps=_this_module())


def _terminate_supervised_workers(processes: Sequence[_SupervisedWorker]) -> None:
    return _cli_workers._terminate_supervised_workers(processes, deps=_this_module())


def _run_worker_supervisor(specs: Sequence[WorkerSpec]) -> int:
    return _cli_workers._run_worker_supervisor(specs, deps=_this_module())


def _emit_supervisor_specs_json(*, key: str, specs: Sequence[WorkerSpec]) -> int:
    return _cli_workers._emit_supervisor_specs_json(key=key, specs=specs)


def cmd_queue_worker(args: Any) -> int:
    return _cli_workers.cmd_queue_worker(args, deps=_this_module())


def cmd_init(args: argparse.Namespace) -> int:
    return _cli_run_dir.cmd_init(args, deps=_this_module())


def cmd_orca_run_dir(args: argparse.Namespace) -> int:
    return _cli_run_dir.cmd_orca_run_dir(args, deps=_this_module())


def cmd_orca_organize(args: argparse.Namespace) -> int:
    return _cli_run_dir.cmd_orca_organize(args, deps=_this_module())


def cmd_orca_summary(args: argparse.Namespace) -> int:
    return _cli_summary.cmd_orca_summary(args, deps=_this_module())


def cmd_summary(args: argparse.Namespace) -> int:
    return _cli_summary.cmd_summary(args, deps=_this_module())


def cmd_workflow_scaffold(args: argparse.Namespace) -> int:
    return _cli_run_dir.cmd_workflow_scaffold(args, deps=_this_module())


def _detect_run_dir_app(args: argparse.Namespace) -> str:
    return _cli_run_dir._detect_run_dir_app(args)


def cmd_run_dir(args: Any) -> int:
    return _cli_run_dir.cmd_run_dir(args, deps=_this_module())


def cmd_workflow_run_dir(args: argparse.Namespace) -> int:
    return _cli_run_dir.cmd_workflow_run_dir(args, deps=_this_module())


def build_parser() -> argparse.ArgumentParser:
    from chemstack.cli_parsers import build_parser as _build_parser

    return _build_parser()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
