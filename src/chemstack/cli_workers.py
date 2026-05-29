from __future__ import annotations

import json
import logging
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Sequence

from chemstack.cli_common import _dependency
from chemstack.cli_errors import emit_error
from chemstack.cli_worker_conflicts import (
    _detect_existing_orca_worker_conflict,
    _emit_existing_orca_worker_conflict,
    _quoted_command,
)
from chemstack.cli_worker_specs import (
    WorkerSpec,
    _build_worker_specs,
)

_WORKER_POLL_INTERVAL_SECONDS = 1.0
_WORKER_STARTUP_FAILURE_WINDOW_SECONDS = 5.0
_WORKER_MAX_STARTUP_FAILURES = 2
LOGGER = logging.getLogger(__name__)


@dataclass
class _SupervisedWorker:
    spec: WorkerSpec
    process: subprocess.Popen[Any]
    started_at_monotonic: float
    startup_failure_count: int = 0


@dataclass
class _SupervisorShutdown:
    requested: bool = False


def _terminate_process(proc: subprocess.Popen[Any], *, deps: Any | None = None) -> None:
    timer = _dependency(deps, "time", time)
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        LOGGER.debug("failed to terminate supervised worker process", exc_info=True)
        return

    deadline = timer.monotonic() + 10.0
    while proc.poll() is None and timer.monotonic() < deadline:
        timer.sleep(0.1)
    if proc.poll() is not None:
        return

    try:
        proc.kill()
    except Exception:
        LOGGER.debug("failed to kill supervised worker process", exc_info=True)
        return

    deadline = timer.monotonic() + 5.0
    while proc.poll() is None and timer.monotonic() < deadline:
        timer.sleep(0.1)


def _spawn_supervised_worker(
    spec: WorkerSpec,
    *,
    restart: bool = False,
    deps: Any | None = None,
) -> _SupervisedWorker:
    process_module = _dependency(deps, "subprocess", subprocess)
    timer = _dependency(deps, "time", time)
    command_text = _quoted_command(spec.argv)
    action = "restarting" if restart else "starting"
    print(f"{action} worker[{spec.app}]: {command_text}")
    return _SupervisedWorker(
        spec=spec,
        process=process_module.Popen(spec.argv, cwd=spec.cwd, env=spec.env),
        started_at_monotonic=timer.monotonic(),
    )


def _install_supervisor_signal_handlers(
    shutdown: _SupervisorShutdown,
    *,
    deps: Any | None = None,
) -> dict[Any, Any]:
    signal_module = _dependency(deps, "signal", signal)

    def _request_shutdown(signum: int, frame: Any) -> None:
        del signum, frame
        shutdown.requested = True

    previous_handlers: dict[Any, Any] = {}
    for sig in (signal_module.SIGINT, signal_module.SIGTERM):
        try:
            previous_handlers[sig] = signal_module.getsignal(sig)
            signal_module.signal(sig, _request_shutdown)
        except Exception:
            LOGGER.debug("failed to install worker supervisor signal handler", exc_info=True)
            continue
    return previous_handlers


def _restore_signal_handlers(
    previous_handlers: dict[Any, Any],
    *,
    deps: Any | None = None,
) -> None:
    signal_module = _dependency(deps, "signal", signal)
    for sig, handler in previous_handlers.items():
        try:
            signal_module.signal(sig, handler)
        except Exception:
            LOGGER.debug("failed to restore worker supervisor signal handler", exc_info=True)
            continue


def _reset_stable_startup_failure_count(
    managed: _SupervisedWorker,
    current_time: float,
) -> None:
    if (
        managed.startup_failure_count > 0
        and current_time - managed.started_at_monotonic >= _WORKER_STARTUP_FAILURE_WINDOW_SECONDS
    ):
        managed.startup_failure_count = 0


def _restart_or_stop_worker(
    processes: list[_SupervisedWorker],
    *,
    index: int,
    managed: _SupervisedWorker,
    returncode: int,
    current_time: float,
    deps: Any | None = None,
) -> int | None:
    spec = managed.spec
    quick_startup_failure = (
        returncode != 0
        and current_time - managed.started_at_monotonic < _WORKER_STARTUP_FAILURE_WINDOW_SECONDS
    )
    if quick_startup_failure:
        managed.startup_failure_count += 1
        if managed.startup_failure_count >= _WORKER_MAX_STARTUP_FAILURES:
            print(
                f"worker[{spec.app}] failed repeatedly during startup; "
                "stopping supervisor to avoid a restart loop."
            )
            return returncode if returncode > 0 else 1
    else:
        managed.startup_failure_count = 0

    restarted = _spawn_supervised_worker(spec, restart=True, deps=deps)
    restarted.startup_failure_count = managed.startup_failure_count
    processes[index] = restarted
    return None


def _poll_supervised_workers(
    processes: list[_SupervisedWorker],
    shutdown: _SupervisorShutdown,
    *,
    deps: Any | None = None,
) -> int | None:
    timer = _dependency(deps, "time", time)
    current_time = timer.monotonic()
    for index, managed in enumerate(processes):
        returncode = managed.process.poll()
        if returncode is None:
            _reset_stable_startup_failure_count(managed, current_time)
            continue

        print(f"worker[{managed.spec.app}] exited with code {returncode}")
        if shutdown.requested:
            continue

        exit_code = _restart_or_stop_worker(
            processes,
            index=index,
            managed=managed,
            returncode=returncode,
            current_time=current_time,
            deps=deps,
        )
        if exit_code is not None:
            shutdown.requested = True
            return exit_code
    return None


def _supervise_worker_processes(
    processes: list[_SupervisedWorker],
    shutdown: _SupervisorShutdown,
    *,
    deps: Any | None = None,
) -> int:
    timer = _dependency(deps, "time", time)
    exit_code = 0
    while True:
        failure_exit_code = _poll_supervised_workers(processes, shutdown, deps=deps)
        if failure_exit_code is not None:
            exit_code = failure_exit_code
        if shutdown.requested:
            return exit_code
        timer.sleep(_WORKER_POLL_INTERVAL_SECONDS)


def _terminate_supervised_workers(
    processes: Sequence[_SupervisedWorker],
    *,
    deps: Any | None = None,
) -> None:
    for managed in processes:
        _terminate_process(managed.process, deps=deps)


def _run_worker_supervisor(
    specs: Sequence[WorkerSpec],
    *,
    deps: Any | None = None,
) -> int:
    if not specs:
        emit_error("no workers selected")
        return 1

    processes: list[_SupervisedWorker] = []
    shutdown = _SupervisorShutdown()
    previous_handlers = _install_supervisor_signal_handlers(shutdown, deps=deps)
    try:
        for spec in specs:
            processes.append(_spawn_supervised_worker(spec, deps=deps))
        return _supervise_worker_processes(processes, shutdown, deps=deps)
    finally:
        _terminate_supervised_workers(processes, deps=deps)
        _restore_signal_handlers(previous_handlers, deps=deps)


def _emit_supervisor_specs_json(*, key: str, specs: Sequence[WorkerSpec]) -> int:
    print(json.dumps({key: [spec.to_dict() for spec in specs]}, ensure_ascii=True, indent=2))
    return 0


def cmd_queue_worker(args: Any, *, deps: Any | None = None) -> int:
    build_worker_specs = _dependency(deps, "_build_worker_specs", _build_worker_specs)
    try:
        specs = build_worker_specs(args)
    except ValueError as exc:
        emit_error(exc)
        return 1

    if bool(getattr(args, "json", False)):
        emit_specs_json = _dependency(
            deps, "_emit_supervisor_specs_json", _emit_supervisor_specs_json
        )
        return emit_specs_json(key="workers", specs=specs)

    detect_conflict = _dependency(
        deps, "_detect_existing_orca_worker_conflict", _detect_existing_orca_worker_conflict
    )
    conflict = detect_conflict(specs, args=args)
    if conflict is not None:
        emit_conflict = _dependency(
            deps, "_emit_existing_orca_worker_conflict", _emit_existing_orca_worker_conflict
        )
        return emit_conflict(conflict, command_name="queue worker")

    run_supervisor = _dependency(deps, "_run_worker_supervisor", _run_worker_supervisor)
    return run_supervisor(specs)
