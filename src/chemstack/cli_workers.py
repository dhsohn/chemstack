from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from chemstack.cli_common import (
    _dependency,
    _discover_shared_config_path,
    _effective_shared_config_text,
    _repo_root_for_subprocess,
    _workflow_root_for_args,
)
from chemstack.core.app_ids import (
    CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_CREST_MODULE,
    CHEMSTACK_FLOW_MODULE,
    CHEMSTACK_ORCA_INTERNAL_MODULE,
    CHEMSTACK_XTB_MODULE,
)
from chemstack.flow.submitters.common import normalize_text, sibling_app_command

_WORKFLOW_ENGINE_APPS = ("crest", "xtb")
_ENGINE_APPS = ("orca",)
_KNOWN_WORKER_APPS = (*_ENGINE_APPS, "workflow")
_DEFAULT_WORKER_APPS = _ENGINE_APPS
_WORKER_POLL_INTERVAL_SECONDS = 1.0
_WORKER_STARTUP_FAILURE_WINDOW_SECONDS = 5.0
_WORKER_MAX_STARTUP_FAILURES = 2
_DIRECT_ENGINE_WORKER_ENV_VAR = "CHEMSTACK_QUEUE_WORKER_DIRECT"


@dataclass(frozen=True)
class WorkerSpec:
    app: str
    argv: tuple[str, ...]
    cwd: str | None = None
    env: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        env_payload: dict[str, str] | None = None
        if isinstance(self.env, dict):
            allowed_env_keys = (CHEMSTACK_CONFIG_ENV_VAR, "PYTHONPATH")
            env_payload = {}
            for key in allowed_env_keys:
                value = normalize_text(self.env.get(key))
                if value:
                    env_payload[key] = value
            if not env_payload:
                env_payload = None
        return {
            "app": self.app,
            "argv": list(self.argv),
            "cwd": self.cwd or "",
            "env": env_payload,
        }


@dataclass
class _SupervisedWorker:
    spec: WorkerSpec
    process: subprocess.Popen[Any]
    started_at_monotonic: float
    startup_failure_count: int = 0


@dataclass(frozen=True)
class _ExistingWorkerConflict:
    app: str
    pid: int
    allowed_root: str
    source: str
    command: str


@dataclass
class _SupervisorShutdown:
    requested: bool = False


def _read_process_command(pid: int) -> tuple[str, ...]:
    cmdline_path = Path("/proc") / str(pid) / "cmdline"
    try:
        raw = cmdline_path.read_bytes()
    except OSError:
        return ()
    parts = [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]
    return tuple(parts)


def _command_invokes_module(command_argv: Sequence[str], module_name: str) -> bool:
    target = normalize_text(module_name).lower()
    if not target:
        return False

    normalized = [normalize_text(part).lower() for part in command_argv]
    for index, part in enumerate(normalized[:-1]):
        if part == "-m" and normalized[index + 1] == target:
            return True
    return False


def _command_program_name(command_argv: Sequence[str]) -> str:
    if not command_argv:
        return ""
    raw = normalize_text(command_argv[0])
    if not raw:
        return ""
    return Path(raw).stem.lower()


def _classify_existing_orca_worker(command_argv: Sequence[str]) -> str:
    program_name = _command_program_name(command_argv)
    if (
        program_name == "chemstack"
        or _command_invokes_module(command_argv, "chemstack.orca.cli")
        or _command_invokes_module(command_argv, "chemstack.orca._internal_cli")
        or _command_invokes_module(command_argv, "chemstack.cli")
    ):
        return "chemstack"
    return "unknown"


def _format_command_argv(command_argv: Sequence[str]) -> str:
    if not command_argv:
        return "<unavailable>"
    return _quoted_command(command_argv)


def _quoted_command(command_argv: Sequence[str]) -> str:
    return " ".join(shlex.quote(part) for part in command_argv)


def _detect_existing_orca_worker_conflict(
    specs: Sequence[WorkerSpec],
    *,
    args: argparse.Namespace,
    deps: Any | None = None,
) -> _ExistingWorkerConflict | None:
    if not any(spec.app == "orca" for spec in specs):
        return None

    discover_shared_config_path = _dependency(
        deps, "_discover_shared_config_path", _discover_shared_config_path
    )
    effective_shared_config_text = _dependency(
        deps, "_effective_shared_config_text", _effective_shared_config_text
    )
    config_path = discover_shared_config_path(effective_shared_config_text(args))
    if not normalize_text(config_path):
        return None

    try:
        from chemstack.orca.config import load_config as _load_orca_config
        from chemstack.orca.queue_worker import read_worker_pid as _read_orca_worker_pid

        cfg = _load_orca_config(str(config_path))
    except Exception:
        return None

    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    existing_pid = _read_orca_worker_pid(allowed_root)
    if existing_pid is None:
        return None

    command_argv = _read_process_command(existing_pid)
    source = _classify_existing_orca_worker(command_argv)
    return _ExistingWorkerConflict(
        app="orca",
        pid=existing_pid,
        allowed_root=str(allowed_root),
        source=source,
        command=_format_command_argv(command_argv),
    )


def _emit_existing_orca_worker_conflict(
    conflict: _ExistingWorkerConflict,
    *,
    command_name: str,
) -> int:
    print(
        f"error: existing ORCA queue worker detected for allowed_root {conflict.allowed_root} "
        f"(pid={conflict.pid})."
    )
    if conflict.source == "chemstack":
        print("source: chemstack queue worker")
        print("This queue root is already being managed by a running chemstack worker.")
    else:
        print("source: existing queue worker")
    print(f"command: {conflict.command}")
    if conflict.source == "chemstack":
        print("Stop the existing queue-worker service before starting another worker.")
    else:
        print("Stop the existing worker before starting another worker.")
    return 1


def _selected_worker_apps(values: Sequence[str] | None) -> list[str]:
    selected = list(values or [])
    if not selected:
        return list(_DEFAULT_WORKER_APPS)

    ordered: list[str] = []
    seen: set[str] = set()
    for value in selected:
        text = normalize_text(value).lower()
        if not text or text in seen:
            continue
        if text not in _KNOWN_WORKER_APPS:
            raise ValueError(f"Unsupported worker app: {text}")
        seen.add(text)
        ordered.append(text)
    return ordered


def _engine_worker_tail_argv(*, app: str, args: argparse.Namespace) -> list[str]:
    tail_argv = ["queue", "worker"]
    if bool(getattr(args, "auto_organize", False)):
        tail_argv.append("--auto-organize")
    elif bool(getattr(args, "no_auto_organize", False)):
        tail_argv.append("--no-auto-organize")
    return tail_argv


def _engine_worker_spec(
    *,
    app: str,
    config_path: str,
    args: argparse.Namespace,
    deps: Any | None = None,
) -> WorkerSpec:
    module_name = {
        "orca": CHEMSTACK_ORCA_INTERNAL_MODULE,
        "xtb": CHEMSTACK_XTB_MODULE,
        "crest": CHEMSTACK_CREST_MODULE,
    }[app]
    build_sibling_command = _dependency(deps, "sibling_app_command", sibling_app_command)
    repo_root_for_subprocess = _dependency(
        deps, "_repo_root_for_subprocess", _repo_root_for_subprocess
    )
    argv, cwd, env = build_sibling_command(
        executable="",
        config_path=config_path,
        repo_root=repo_root_for_subprocess(),
        module_name=module_name,
        tail_argv=_engine_worker_tail_argv(app=app, args=args),
    )
    env_payload = dict(env) if isinstance(env, dict) else dict(os.environ)
    env_payload[_DIRECT_ENGINE_WORKER_ENV_VAR] = "1"
    return WorkerSpec(app=app, argv=tuple(argv), cwd=cwd, env=env_payload)


def _workflow_worker_spec(
    *,
    workflow_root: str,
    config_path: str | None,
    args: argparse.Namespace,
) -> WorkerSpec:
    argv = [
        sys.executable,
        "-m",
        CHEMSTACK_FLOW_MODULE,
        "workflow",
        "worker",
        "--workflow-root",
        str(Path(workflow_root).expanduser().resolve()),
    ]
    if normalize_text(config_path):
        argv.extend(["--chemstack-config", str(Path(str(config_path)).expanduser().resolve())])
    if bool(getattr(args, "no_submit", False)):
        argv.append("--no-submit")
    if bool(getattr(args, "once", False)):
        argv.append("--once")
    if bool(getattr(args, "refresh_registry", False)):
        argv.append("--refresh-registry")
    if bool(getattr(args, "refresh_each_cycle", False)):
        argv.append("--refresh-each-cycle")

    max_cycles = int(getattr(args, "max_cycles", 0) or 0)
    if max_cycles > 0:
        argv.extend(["--max-cycles", str(max_cycles)])

    interval_seconds = float(getattr(args, "interval_seconds", 0.0) or 0.0)
    if interval_seconds > 0:
        argv.extend(["--interval-seconds", str(interval_seconds)])

    lock_timeout_seconds = float(getattr(args, "lock_timeout_seconds", 0.0) or 0.0)
    if lock_timeout_seconds > 0:
        argv.extend(["--lock-timeout-seconds", str(lock_timeout_seconds)])
    return WorkerSpec(app="workflow", argv=tuple(argv))


def _worker_engine_apps(apps: Sequence[str], *, workflow_enabled: bool) -> list[str]:
    engine_apps = [app for app in apps if app in _ENGINE_APPS]
    if workflow_enabled:
        for app in _WORKFLOW_ENGINE_APPS:
            if app not in engine_apps:
                engine_apps.append(app)
    return engine_apps


def _validate_engine_worker_config(engine_apps: Sequence[str], config_path: str | None) -> None:
    if engine_apps and not normalize_text(config_path):
        raise ValueError(
            "Could not discover chemstack.yaml for engine workers. Pass --chemstack-config or set CHEMSTACK_CONFIG."
        )


def _workflow_only_worker_flag_error(args: Any) -> str | None:
    if any(
        bool(getattr(args, attr, False))
        for attr in ("no_submit", "refresh_registry", "refresh_each_cycle")
    ):
        raise ValueError("workflow-only worker flags require --app workflow")
    numeric_flags = (
        ("max_cycles", int, "--max-cycles"),
        ("interval_seconds", float, "--interval-seconds"),
        ("lock_timeout_seconds", float, "--lock-timeout-seconds"),
    )
    for attr, caster, option in numeric_flags:
        if caster(getattr(args, attr, 0) or 0) > 0:
            return f"{option} requires --app workflow"
    return None


def _add_workflow_worker_spec(
    specs: list[WorkerSpec],
    *,
    apps: Sequence[str],
    explicit_app_selection: bool,
    workflow_root: str | None,
    config_path: str | None,
    args: argparse.Namespace,
) -> None:
    if "workflow" in apps and not workflow_root:
        raise ValueError("workflow worker requires workflow.root in chemstack.yaml")

    should_add_workflow = "workflow" in apps or (not explicit_app_selection and bool(workflow_root))
    if should_add_workflow and workflow_root:
        specs.append(
            _workflow_worker_spec(workflow_root=workflow_root, config_path=config_path, args=args)
        )
        return

    flag_error = _workflow_only_worker_flag_error(args)
    if flag_error:
        raise ValueError(flag_error)


def _build_worker_specs(args: Any, *, deps: Any | None = None) -> list[WorkerSpec]:
    explicit_apps = list(getattr(args, "app", None) or [])
    apps = _selected_worker_apps(explicit_apps)
    explicit_app_selection = bool(explicit_apps)
    discover_shared_config_path = _dependency(
        deps, "_discover_shared_config_path", _discover_shared_config_path
    )
    effective_shared_config_text = _dependency(
        deps, "_effective_shared_config_text", _effective_shared_config_text
    )
    workflow_root_for_args = _dependency(deps, "_workflow_root_for_args", _workflow_root_for_args)
    config_path = discover_shared_config_path(effective_shared_config_text(args))
    workflow_root = workflow_root_for_args(args)
    workflow_enabled = "workflow" in apps or (not explicit_app_selection and bool(workflow_root))
    engine_apps = _worker_engine_apps(apps, workflow_enabled=workflow_enabled)
    _validate_engine_worker_config(engine_apps, config_path)

    specs = [
        _engine_worker_spec(app=app, config_path=str(config_path), args=args, deps=deps)
        for app in engine_apps
    ]
    _add_workflow_worker_spec(
        specs,
        apps=apps,
        explicit_app_selection=explicit_app_selection,
        workflow_root=workflow_root,
        config_path=config_path,
        args=args,
    )
    return specs


def _terminate_process(proc: subprocess.Popen[Any], *, deps: Any | None = None) -> None:
    timer = _dependency(deps, "time", time)
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        return

    deadline = timer.monotonic() + 10.0
    while proc.poll() is None and timer.monotonic() < deadline:
        timer.sleep(0.1)
    if proc.poll() is not None:
        return

    try:
        proc.kill()
    except Exception:
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
        print("error: no workers selected")
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
        print(f"error: {exc}")
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
