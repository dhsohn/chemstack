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

from chemstack import activity_rendering as _activity_rendering
from chemstack.activity_view import (
    activity_with_parent_hint,
    count_global_active_simulations,
    queue_list_default_visible_items,
    queue_list_display_rows,
)
from chemstack.core.app_ids import (
    CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_CREST_MODULE,
    CHEMSTACK_FLOW_MODULE,
    CHEMSTACK_ORCA_INTERNAL_MODULE,
    CHEMSTACK_XTB_MODULE,
)
from chemstack.core.config.files import shared_workflow_root_from_config
from chemstack.flow.operations import cancel_activity, clear_activities, list_activities
from chemstack.flow.run_dir_layout import inspect_workflow_run_dir
from chemstack.flow.submitters.common import normalize_text, sibling_app_command

_WORKFLOW_ENGINE_APPS = ("crest", "xtb")
_ENGINE_APPS = ("orca",)
_KNOWN_WORKER_APPS = (*_ENGINE_APPS, "workflow")
_DEFAULT_WORKER_APPS = _ENGINE_APPS
_WORKER_POLL_INTERVAL_SECONDS = 1.0
_WORKER_STARTUP_FAILURE_WINDOW_SECONDS = 5.0
_WORKER_MAX_STARTUP_FAILURES = 2
_DIRECT_ENGINE_WORKER_ENV_VAR = "CHEMSTACK_QUEUE_WORKER_DIRECT"
_WORKFLOW_SCAFFOLD_SHORTCUTS = (
    ("ts_search", "reaction_ts_search", "Create a reaction TS-search scaffold."),
    ("conformer_search", "conformer_screening", "Create a conformer-screening scaffold."),
)
_DEFAULT_QUEUE_TABLE_NOW = _activity_rendering._queue_table_now


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


@dataclass(frozen=True)
class _QueueListRequest:
    shared_config: str | None
    limit: int
    engine_values: tuple[str, ...]
    status_values: tuple[str, ...]
    kind_values: tuple[str, ...]
    json_output: bool

    @property
    def default_combined_text_view(self) -> bool:
        return (
            not self.json_output
            and not self.engine_values
            and not self.status_values
            and not self.kind_values
        )


@dataclass
class _SupervisorShutdown:
    requested: bool = False


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _repo_root_for_subprocess() -> str | None:
    root = _repo_root()
    if (root / "src" / "chemstack").is_dir():
        return str(root)
    return None


def _discover_shared_config_path(explicit: str | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())

    env_text = normalize_text(os.getenv(CHEMSTACK_CONFIG_ENV_VAR))
    if env_text:
        return str(Path(env_text).expanduser().resolve())

    candidates = [
        _repo_root() / "config" / "chemstack.yaml",
        Path.home() / "chemstack" / "config" / "chemstack.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate.expanduser().resolve())
    return None


def _discover_workflow_root(explicit: str | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    return None


def _workflow_root_for_args(args: Any) -> str | None:
    explicit_root = _discover_workflow_root(getattr(args, "workflow_root", None))
    if explicit_root:
        return explicit_root
    config_path = _discover_shared_config_path(_effective_shared_config_text(args))
    return shared_workflow_root_from_config(config_path)


def _effective_shared_config_text(args: argparse.Namespace) -> str:
    return (
        normalize_text(getattr(args, "chemstack_config", None))
        or normalize_text(getattr(args, "config", None))
        or normalize_text(getattr(args, "global_config", None))
    )


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
    return " ".join(shlex.quote(part) for part in command_argv)


def _detect_existing_orca_worker_conflict(
    specs: Sequence[WorkerSpec],
    *,
    args: argparse.Namespace,
) -> _ExistingWorkerConflict | None:
    if not any(spec.app == "orca" for spec in specs):
        return None

    config_path = _discover_shared_config_path(_effective_shared_config_text(args))
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


def _normalize_filter_values(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = normalize_text(value).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _filter_activity_items(
    items: Sequence[dict[str, Any]],
    *,
    engines: Sequence[str] | None = None,
    statuses: Sequence[str] | None = None,
    kinds: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    engine_filter = set(_normalize_filter_values(engines))
    status_filter = set(_normalize_filter_values(statuses))
    kind_filter = set(_normalize_filter_values(kinds))

    filtered: list[dict[str, Any]] = []
    for item in items:
        engine = normalize_text(item.get("engine")).lower()
        status = normalize_text(item.get("status")).lower()
        kind = normalize_text(item.get("kind")).lower()
        if engine_filter and engine not in engine_filter:
            continue
        if status_filter and status not in status_filter:
            continue
        if kind_filter and kind not in kind_filter:
            continue
        filtered.append(dict(item))
    return filtered


def _activity_counter_config_path(
    *,
    payload: dict[str, Any],
    config_hint: str | None,
) -> str | None:
    config_text = normalize_text(config_hint)
    if config_text:
        return config_text
    sources = payload.get("sources")
    if not isinstance(sources, dict):
        return None
    for key in ("orca_auto_config", "crest_auto_config", "xtb_auto_config"):
        source_text = normalize_text(sources.get(key))
        if source_text:
            return source_text
    return None


def _queue_table_now() -> Any:
    return _DEFAULT_QUEUE_TABLE_NOW()


def _queue_elapsed_text(item: dict[str, Any], *, now: Any | None = None) -> str:
    return _activity_rendering._queue_elapsed_text(item, now=now)


def _queue_display_width(value: str) -> int:
    return _activity_rendering._queue_display_width(value)


def _queue_table_lines(rows: Sequence[tuple[int, dict[str, Any]]]) -> list[str]:
    original = _activity_rendering._queue_table_now
    _activity_rendering._queue_table_now = _queue_table_now
    try:
        return _activity_rendering.queue_table_lines(rows)
    finally:
        _activity_rendering._queue_table_now = original


def _queue_clear_lines(payload: dict[str, Any]) -> list[str]:
    return _activity_rendering.queue_clear_lines(payload)


def _queue_list_request(args: Any) -> _QueueListRequest:
    return _QueueListRequest(
        shared_config=_effective_shared_config_text(args) or None,
        limit=int(getattr(args, "limit", 0) or 0),
        engine_values=_normalize_filter_values(getattr(args, "engine", None)),
        status_values=_normalize_filter_values(getattr(args, "status", None)),
        kind_values=_normalize_filter_values(getattr(args, "kind", None)),
        json_output=bool(getattr(args, "json", False)),
    )


def _cmd_queue_list_clear(args: Any, request: _QueueListRequest) -> int:
    if (
        any(getattr(args, field, None) for field in ("engine", "status", "kind"))
        or request.limit > 0
    ):
        print(
            "error: `chemstack queue list clear` does not support --engine/--status/--kind/--limit filters."
        )
        return 1
    payload = clear_activities(
        workflow_root=_workflow_root_for_args(args),
        crest_auto_config=request.shared_config,
        xtb_auto_config=request.shared_config,
        orca_auto_config=request.shared_config,
    )
    if request.json_output:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    for line in _queue_clear_lines(payload):
        print(line)
    return 0


def _queue_list_payload(args: Any, request: _QueueListRequest) -> dict[str, Any]:
    payload = list_activities(
        workflow_root=_workflow_root_for_args(args),
        limit=0,
        refresh=bool(getattr(args, "refresh", False)),
        crest_auto_config=request.shared_config,
        xtb_auto_config=request.shared_config,
        orca_auto_config=request.shared_config,
        child_job_engines=() if request.default_combined_text_view else None,
    )
    return payload


def _filtered_queue_payload(
    payload: dict[str, Any],
    request: _QueueListRequest,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    activities = _filter_activity_items(
        payload.get("activities", []),
        engines=request.engine_values,
        statuses=request.status_values,
        kinds=request.kind_values,
    )
    limited_activities = activities[: request.limit] if request.limit > 0 else list(activities)
    active_simulations = count_global_active_simulations(
        payload.get("activities", []),
        config_path=_activity_counter_config_path(
            payload=payload, config_hint=request.shared_config
        ),
    )
    return {
        "count": len(limited_activities),
        "active_simulations": active_simulations,
        "activities": [activity_with_parent_hint(item) for item in limited_activities],
        "sources": dict(payload.get("sources", {})),
    }, activities


def _queue_list_display_rows(
    *,
    payload: dict[str, Any],
    filtered_activities: Sequence[dict[str, Any]],
    request: _QueueListRequest,
) -> list[tuple[int, dict[str, Any]]]:
    display_items = list(filtered_activities)
    if request.default_combined_text_view:
        display_items = queue_list_default_visible_items(display_items)
    if request.limit > 0:
        display_items = display_items[: request.limit]
    show_workflow_context = set(request.kind_values) != {"job"}
    return queue_list_display_rows(
        all_items=payload.get("activities", []),
        visible_items=display_items,
        show_workflow_context=show_workflow_context,
        visible_workflow_child_engines=("orca",) if request.default_combined_text_view else None,
    )


def _print_queue_list_text(
    *,
    payload: dict[str, Any],
    filtered_payload: dict[str, Any],
    filtered_activities: Sequence[dict[str, Any]],
    request: _QueueListRequest,
) -> int:
    display_rows = _queue_list_display_rows(
        payload=payload,
        filtered_activities=filtered_activities,
        request=request,
    )
    print(f"active_simulations: {filtered_payload['active_simulations']}")
    if not display_rows:
        print("No matching activities.")
        return 0
    for line in _queue_table_lines(display_rows):
        print(line)
    return 0


def cmd_queue_list(args: Any) -> int:
    request = _queue_list_request(args)
    if normalize_text(getattr(args, "action", None)).lower() == "clear":
        return _cmd_queue_list_clear(args, request)

    payload = _queue_list_payload(args, request)
    filtered_payload, filtered_activities = _filtered_queue_payload(payload, request)
    if request.json_output:
        print(json.dumps(filtered_payload, ensure_ascii=True, indent=2))
        return 0
    return _print_queue_list_text(
        payload=payload,
        filtered_payload=filtered_payload,
        filtered_activities=filtered_activities,
        request=request,
    )


def cmd_queue_cancel(args: Any) -> int:
    shared_config = _effective_shared_config_text(args) or None
    try:
        payload = cancel_activity(
            target=getattr(args, "target"),
            workflow_root=_workflow_root_for_args(args),
            crest_auto_config=shared_config,
            xtb_auto_config=shared_config,
            orca_auto_config=shared_config,
        )
    except (LookupError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}")
        return 1

    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"activity_id: {payload.get('activity_id', '-')}")
    print(f"kind: {payload.get('kind', '-')}")
    print(f"engine: {payload.get('engine', '-')}")
    print(f"source: {payload.get('source', '-')}")
    print(f"label: {payload.get('label', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"cancel_target: {payload.get('cancel_target', '-')}")
    return 0


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


def _engine_worker_spec(*, app: str, config_path: str, args: argparse.Namespace) -> WorkerSpec:
    module_name = {
        "orca": CHEMSTACK_ORCA_INTERNAL_MODULE,
        "xtb": CHEMSTACK_XTB_MODULE,
        "crest": CHEMSTACK_CREST_MODULE,
    }[app]
    argv, cwd, env = sibling_app_command(
        executable="",
        config_path=config_path,
        repo_root=_repo_root_for_subprocess(),
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


def _build_worker_specs(args: Any) -> list[WorkerSpec]:
    explicit_apps = list(getattr(args, "app", None) or [])
    apps = _selected_worker_apps(explicit_apps)
    explicit_app_selection = bool(explicit_apps)
    config_path = _discover_shared_config_path(_effective_shared_config_text(args))
    workflow_root = _workflow_root_for_args(args)
    workflow_enabled = "workflow" in apps or (not explicit_app_selection and bool(workflow_root))
    engine_apps = _worker_engine_apps(apps, workflow_enabled=workflow_enabled)
    _validate_engine_worker_config(engine_apps, config_path)

    specs = [
        _engine_worker_spec(app=app, config_path=str(config_path), args=args) for app in engine_apps
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


def _terminate_process(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        return

    deadline = time.monotonic() + 10.0
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    if proc.poll() is not None:
        return

    try:
        proc.kill()
    except Exception:
        return

    deadline = time.monotonic() + 5.0
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)


def _spawn_supervised_worker(spec: WorkerSpec, *, restart: bool = False) -> _SupervisedWorker:
    command_text = " ".join(shlex.quote(part) for part in spec.argv)
    action = "restarting" if restart else "starting"
    print(f"{action} worker[{spec.app}]: {command_text}")
    return _SupervisedWorker(
        spec=spec,
        process=subprocess.Popen(spec.argv, cwd=spec.cwd, env=spec.env),
        started_at_monotonic=time.monotonic(),
    )


def _install_supervisor_signal_handlers(shutdown: _SupervisorShutdown) -> dict[signal.Signals, Any]:
    def _request_shutdown(signum: int, frame: Any) -> None:
        del signum, frame
        shutdown.requested = True

    previous_handlers: dict[signal.Signals, Any] = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _request_shutdown)
        except Exception:
            continue
    return previous_handlers


def _restore_signal_handlers(previous_handlers: dict[signal.Signals, Any]) -> None:
    for sig, handler in previous_handlers.items():
        try:
            signal.signal(sig, handler)
        except Exception:
            continue


def _reset_stable_startup_failure_count(managed: _SupervisedWorker, current_time: float) -> None:
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

    restarted = _spawn_supervised_worker(spec, restart=True)
    restarted.startup_failure_count = managed.startup_failure_count
    processes[index] = restarted
    return None


def _poll_supervised_workers(
    processes: list[_SupervisedWorker],
    shutdown: _SupervisorShutdown,
) -> int | None:
    current_time = time.monotonic()
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
        )
        if exit_code is not None:
            shutdown.requested = True
            return exit_code
    return None


def _supervise_worker_processes(
    processes: list[_SupervisedWorker],
    shutdown: _SupervisorShutdown,
) -> int:
    exit_code = 0
    while True:
        failure_exit_code = _poll_supervised_workers(processes, shutdown)
        if failure_exit_code is not None:
            exit_code = failure_exit_code
        if shutdown.requested:
            return exit_code
        time.sleep(_WORKER_POLL_INTERVAL_SECONDS)


def _terminate_supervised_workers(processes: Sequence[_SupervisedWorker]) -> None:
    for managed in processes:
        _terminate_process(managed.process)


def _run_worker_supervisor(specs: Sequence[WorkerSpec]) -> int:
    if not specs:
        print("error: no workers selected")
        return 1

    processes: list[_SupervisedWorker] = []
    shutdown = _SupervisorShutdown()
    previous_handlers = _install_supervisor_signal_handlers(shutdown)
    try:
        for spec in specs:
            processes.append(_spawn_supervised_worker(spec))
        return _supervise_worker_processes(processes, shutdown)
    finally:
        _terminate_supervised_workers(processes)
        _restore_signal_handlers(previous_handlers)


def _emit_supervisor_specs_json(*, key: str, specs: Sequence[WorkerSpec]) -> int:
    print(json.dumps({key: [spec.to_dict() for spec in specs]}, ensure_ascii=True, indent=2))
    return 0


def cmd_queue_worker(args: Any) -> int:
    try:
        specs = _build_worker_specs(args)
    except ValueError as exc:
        print(f"error: {exc}")
        return 1

    if bool(getattr(args, "json", False)):
        return _emit_supervisor_specs_json(key="workers", specs=specs)

    conflict = _detect_existing_orca_worker_conflict(specs, args=args)
    if conflict is not None:
        return _emit_existing_orca_worker_conflict(conflict, command_name="queue worker")

    return _run_worker_supervisor(specs)


def _engine_config_for_command(args: argparse.Namespace) -> str | None:
    config_path = _discover_shared_config_path(_effective_shared_config_text(args))
    if not config_path:
        return None
    return str(Path(config_path).expanduser().resolve())


def _configure_orca_logging(args: argparse.Namespace) -> None:
    from chemstack.orca.cli import _configure_logging as _configure_orca_logging_impl

    _configure_orca_logging_impl(
        argparse.Namespace(
            verbose=bool(getattr(args, "verbose", False)),
            log_file=getattr(args, "log_file", None),
        )
    )


def cmd_init(args: argparse.Namespace) -> int:
    from chemstack.orca.commands.init import cmd_init as _cmd_orca_init

    _configure_orca_logging(args)
    args.config = _engine_config_for_command(args)
    return int(_cmd_orca_init(args))


def cmd_orca_run_dir(args: argparse.Namespace) -> int:
    from chemstack.orca.commands.run_inp import cmd_run_inp as _cmd_orca_run_dir

    _configure_orca_logging(args)
    args.config = _engine_config_for_command(args)
    return int(_cmd_orca_run_dir(args))


def cmd_orca_organize(args: argparse.Namespace) -> int:
    from chemstack.orca.commands.organize import cmd_organize as _cmd_orca_organize

    _configure_orca_logging(args)
    args.config = _engine_config_for_command(args)
    return int(_cmd_orca_organize(args))


def cmd_orca_summary(args: argparse.Namespace) -> int:
    from chemstack.orca.commands.summary import cmd_summary as _cmd_orca_summary

    _configure_orca_logging(args)
    args.config = _engine_config_for_command(args)
    return int(_cmd_orca_summary(args))


def cmd_summary(args: argparse.Namespace) -> int:
    summary_app = normalize_text(getattr(args, "summary_app", None)).lower() or "combined"
    if summary_app == "orca":
        return int(cmd_orca_summary(args))

    from chemstack.summary import cmd_summary as _cmd_combined_summary

    _configure_orca_logging(args)
    args.config = _engine_config_for_command(args)
    return int(_cmd_combined_summary(args))


def cmd_workflow_scaffold(args: argparse.Namespace) -> int:
    from chemstack.flow.scaffold import cmd_scaffold as _cmd_workflow_scaffold

    return int(_cmd_workflow_scaffold(args))


def _detect_run_dir_app(args: argparse.Namespace) -> str:
    raw_path = normalize_text(getattr(args, "path", None))
    if not raw_path:
        raise ValueError("run-dir requires a target directory path")

    target = Path(raw_path).expanduser().resolve()
    if not target.exists():
        raise ValueError(f"run-dir target not found: {target}")
    if not target.is_dir():
        raise ValueError(f"run-dir target is not a directory: {target}")

    if (target / "workflow.json").is_file():
        return "workflow"

    workflow_layout = inspect_workflow_run_dir(target)
    orca_input_present = any(candidate.is_file() for candidate in target.glob("*.inp"))

    if workflow_layout.has_manifest:
        return "workflow"
    if orca_input_present:
        return "orca"

    raise ValueError(
        "Could not infer run-dir target type from directory. "
        "Expected flow.yaml for workflow inputs, or *.inp for ORCA."
    )


def cmd_run_dir(args: Any) -> int:
    try:
        run_dir_app = _detect_run_dir_app(args)
    except ValueError as exc:
        print(f"error: {exc}")
        return 1

    args.run_dir_app = run_dir_app
    if run_dir_app == "workflow":
        args.workflow_dir = getattr(args, "path")
        return int(cmd_workflow_run_dir(args))
    if getattr(args, "priority", None) is None:
        args.priority = 10
    return int(cmd_orca_run_dir(args))


def cmd_workflow_run_dir(args: argparse.Namespace) -> int:
    from chemstack.flow.cli import cmd_run_dir as _cmd_workflow_run_dir

    shared_config = _engine_config_for_command(args)
    if shared_config:
        args.chemstack_config = shared_config
    return int(_cmd_workflow_run_dir(args))


def build_parser() -> argparse.ArgumentParser:
    from chemstack.cli_parsers import build_parser as _build_parser

    return _build_parser()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
