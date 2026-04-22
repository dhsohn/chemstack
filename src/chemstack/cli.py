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

from chemstack.activity_view import (
    activity_display_fields,
    activity_with_parent_hint,
    count_global_active_simulations,
    queue_list_display_rows,
)
from chemstack.core.app_ids import (
    CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_CREST_MODULE,
    CHEMSTACK_FLOW_MODULE,
    CHEMSTACK_ORCA_INTERNAL_MODULE,
    CHEMSTACK_ORCA_MODULE,
    CHEMSTACK_XTB_MODULE,
)
from chemstack.core.config.files import shared_workflow_root_from_config
from chemstack.flow.operations import cancel_activity, clear_activities, list_activities
from chemstack.flow.run_dir_layout import inspect_workflow_run_dir
from chemstack.flow.submitters.common import normalize_text, sibling_app_command

_WORKFLOW_INTERNAL_ENGINE_APPS = ("crest", "xtb")
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
    if program_name == "chemstack" or _command_invokes_module(command_argv, "chemstack.orca.cli") or _command_invokes_module(
        command_argv, "chemstack.orca._internal_cli"
    ) or _command_invokes_module(
        command_argv, "chemstack.cli"
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


def _queue_clear_lines(payload: dict[str, Any]) -> list[str]:
    total_cleared = int(payload.get("total_cleared", 0) or 0)
    if total_cleared <= 0:
        return ["Nothing to clear."]

    lines = [f"Cleared {total_cleared} completed/failed/cancelled entries."]
    cleared = payload.get("cleared")
    if not isinstance(cleared, dict):
        return lines

    labels = (
        ("workflows", "workflows"),
        ("xtb_queue_entries", "xTB queue entries"),
        ("crest_queue_entries", "CREST queue entries"),
        ("orca_queue_entries", "ORCA queue entries"),
        ("orca_run_states", "ORCA run states"),
    )
    for key, label in labels:
        count = int(cleared.get(key, 0) or 0)
        if count > 0:
            lines.append(f"  {label}: {count}")
    return lines


def cmd_queue_list(args: Any) -> int:
    shared_config = _effective_shared_config_text(args) or None
    if normalize_text(getattr(args, "action", None)).lower() == "clear":
        if any(getattr(args, field, None) for field in ("engine", "status", "kind")) or int(getattr(args, "limit", 0) or 0) > 0:
            print("error: `chemstack queue list clear` does not support --engine/--status/--kind/--limit filters.")
            return 1
        payload = clear_activities(
            workflow_root=_workflow_root_for_args(args),
            crest_auto_config=shared_config,
            xtb_auto_config=shared_config,
            orca_auto_config=shared_config,
        )
        if bool(getattr(args, "json", False)):
            print(json.dumps(payload, ensure_ascii=True, indent=2))
            return 0
        for line in _queue_clear_lines(payload):
            print(line)
        return 0

    limit = int(getattr(args, "limit", 0) or 0)
    payload = list_activities(
        workflow_root=_workflow_root_for_args(args),
        limit=0,
        refresh=bool(getattr(args, "refresh", False)),
        crest_auto_config=shared_config,
        xtb_auto_config=shared_config,
        orca_auto_config=shared_config,
    )
    activities = _filter_activity_items(
        payload.get("activities", []),
        engines=getattr(args, "engine", None),
        statuses=getattr(args, "status", None),
        kinds=getattr(args, "kind", None),
    )
    if limit > 0:
        activities = activities[:limit]
    active_simulations = count_global_active_simulations(
        payload.get("activities", []),
        config_path=_activity_counter_config_path(payload=payload, config_hint=shared_config),
    )
    enriched_activities = [activity_with_parent_hint(item) for item in activities]
    filtered_payload = {
        "count": len(activities),
        "active_simulations": active_simulations,
        "activities": enriched_activities,
        "sources": dict(payload.get("sources", {})),
    }
    if bool(getattr(args, "json", False)):
        print(json.dumps(filtered_payload, ensure_ascii=True, indent=2))
        return 0

    kind_filter = set(_normalize_filter_values(getattr(args, "kind", None)))
    show_workflow_context = kind_filter != {"job"}
    display_rows = queue_list_display_rows(
        all_items=payload.get("activities", []),
        visible_items=activities,
        show_workflow_context=show_workflow_context,
    )

    print(f"active_simulations: {filtered_payload['active_simulations']}")
    for indent, item in display_rows:
        prefix = "  " * max(0, int(indent))
        detail_suffix = "".join(f" {key}={value}" for key, value in activity_display_fields(item))
        print(
            f"{prefix}- {item.get('activity_id', '-')}"
            f" kind={item.get('kind', '-')}"
            f" engine={item.get('engine', '-')}"
            f" status={item.get('status', '-')}"
            f" label={item.get('label', '-')}"
            f" source={item.get('source', '-')}"
            f"{detail_suffix}"
        )
    return 0


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
    except (LookupError, ValueError) as exc:
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
    if bool(getattr(args, "once", False)):
        if app == "orca":
            raise ValueError("--once is not supported for app=orca")
        tail_argv.append("--once")
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


def _build_worker_specs(args: Any) -> list[WorkerSpec]:
    explicit_apps = list(getattr(args, "app", None) or [])
    apps = _selected_worker_apps(explicit_apps)
    explicit_app_selection = bool(explicit_apps)
    config_path = _discover_shared_config_path(_effective_shared_config_text(args))
    workflow_root = _workflow_root_for_args(args)
    workflow_enabled = "workflow" in apps or (not explicit_app_selection and bool(workflow_root))

    engine_apps = [app for app in apps if app in _ENGINE_APPS]
    if workflow_enabled:
        for app in _WORKFLOW_INTERNAL_ENGINE_APPS:
            if app not in engine_apps:
                engine_apps.append(app)
    if engine_apps and not normalize_text(config_path):
        raise ValueError(
            "Could not discover chemstack.yaml for engine workers. Pass --chemstack-config or set CHEMSTACK_CONFIG."
        )

    specs: list[WorkerSpec] = []
    for app in engine_apps:
        specs.append(_engine_worker_spec(app=app, config_path=str(config_path), args=args))

    if "workflow" in apps:
        if not workflow_root:
            raise ValueError("workflow worker requires workflow.root in chemstack.yaml")
        specs.append(_workflow_worker_spec(workflow_root=workflow_root, config_path=config_path, args=args))
    elif not explicit_app_selection and workflow_root:
        specs.append(_workflow_worker_spec(workflow_root=workflow_root, config_path=config_path, args=args))
    elif any(
        bool(getattr(args, attr, False))
        for attr in ("no_submit", "refresh_registry", "refresh_each_cycle")
    ):
        raise ValueError("workflow-only worker flags require --app workflow")
    elif int(getattr(args, "max_cycles", 0) or 0) > 0:
        raise ValueError("--max-cycles requires --app workflow")
    elif float(getattr(args, "interval_seconds", 0.0) or 0.0) > 0:
        raise ValueError("--interval-seconds requires --app workflow")
    elif float(getattr(args, "lock_timeout_seconds", 0.0) or 0.0) > 0:
        raise ValueError("--lock-timeout-seconds requires --app workflow")
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


def _run_worker_supervisor(specs: Sequence[WorkerSpec]) -> int:
    if not specs:
        print("error: no workers selected")
        return 1

    processes: list[_SupervisedWorker] = []
    shutdown_requested = False
    exit_code = 0

    def _spawn_worker(spec: WorkerSpec, *, restart: bool = False) -> _SupervisedWorker:
        command_text = " ".join(shlex.quote(part) for part in spec.argv)
        action = "restarting" if restart else "starting"
        print(f"{action} worker[{spec.app}]: {command_text}")
        return _SupervisedWorker(
            spec=spec,
            process=subprocess.Popen(spec.argv, cwd=spec.cwd, env=spec.env),
            started_at_monotonic=time.monotonic(),
        )

    def _request_shutdown(signum: int, frame: Any) -> None:
        del signum, frame
        nonlocal shutdown_requested
        shutdown_requested = True

    previous_handlers: dict[signal.Signals, Any] = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _request_shutdown)
        except Exception:
            continue

    try:
        for spec in specs:
            processes.append(_spawn_worker(spec))

        while True:
            current_time = time.monotonic()
            for index, managed in enumerate(processes):
                spec = managed.spec
                process = managed.process
                returncode = process.poll()
                if returncode is None:
                    if (
                        managed.startup_failure_count > 0
                        and current_time - managed.started_at_monotonic >= _WORKER_STARTUP_FAILURE_WINDOW_SECONDS
                    ):
                        managed.startup_failure_count = 0
                    continue
                print(f"worker[{spec.app}] exited with code {returncode}")
                if shutdown_requested:
                    continue
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
                        exit_code = returncode if returncode > 0 else 1
                        shutdown_requested = True
                        break
                else:
                    managed.startup_failure_count = 0

                restarted = _spawn_worker(spec, restart=True)
                restarted.startup_failure_count = managed.startup_failure_count
                processes[index] = restarted
            if shutdown_requested:
                break
            time.sleep(_WORKER_POLL_INTERVAL_SECONDS)
    finally:
        for managed in processes:
            _terminate_process(managed.process)
        for sig, handler in previous_handlers.items():
            try:
                signal.signal(sig, handler)
            except Exception:
                continue
    return exit_code


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
    config_text = _effective_shared_config_text(args)
    if not config_text:
        return None
    return str(Path(config_text).expanduser().resolve())


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

    workflow_layout = inspect_workflow_run_dir(target)
    orca_input_present = any(candidate.is_file() for candidate in target.glob("*.inp"))

    if workflow_layout.is_workflow_dir:
        return "workflow"
    if orca_input_present:
        return "orca"

    raise ValueError(
        "Could not infer run-dir target type from directory. "
        "Expected flow.yaml, reactant.xyz + product.xyz, input.xyz, or *.inp."
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


def _add_workflow_scaffold_shortcut(
    scaffold_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    name: str,
    workflow_type: str,
    help_text: str,
) -> None:
    parser = scaffold_subparsers.add_parser(name, help=help_text)
    parser.add_argument("root", help="Workflow input directory to create")
    parser.set_defaults(
        func=cmd_workflow_scaffold,
        workflow_type=workflow_type,
    )


def cmd_workflow_run_dir(args: argparse.Namespace) -> int:
    from chemstack.flow.cli import cmd_run_dir as _cmd_workflow_run_dir

    shared_config = _engine_config_for_command(args)
    if shared_config:
        args.chemstack_config = shared_config
    return int(_cmd_workflow_run_dir(args))


def _add_engine_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--chemstack-config",
        "--config",
        dest="config",
        default=None,
        help="Path to shared chemstack.yaml",
    )


def _add_orca_logging_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument("--log-file", default=None, help="Write logs to file (with rotation, max 10MB x 5)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chemstack")
    parser.add_argument(
        "--chemstack-config",
        "--config",
        dest="global_config",
        default=None,
        help=argparse.SUPPRESS,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    queue_parser = subparsers.add_parser(
        "queue",
        help="Unified queue and worker commands across ORCA, workflow-managed internal engines, and workflows.",
    )
    queue_subparsers = queue_parser.add_subparsers(dest="queue_command", required=True)

    list_parser = queue_subparsers.add_parser(
        "list",
        help="List workflows and engine activities together.",
    )
    list_parser.add_argument(
        "action",
        nargs="?",
        choices=["clear"],
        help="Remove completed/failed/cancelled entries from the unified activity list",
    )
    list_parser.add_argument("--workflow-root", help=argparse.SUPPRESS)
    list_parser.add_argument("--chemstack-config", "--config", dest="chemstack_config", help="Path to shared chemstack.yaml")
    list_parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of activities to print")
    list_parser.add_argument("--refresh", action="store_true", help="Refresh workflow registry before listing")
    list_parser.add_argument(
        "--engine",
        action="append",
        choices=["orca", "xtb", "crest", "workflow"],
        help="Filter by engine; may be passed more than once",
    )
    list_parser.add_argument(
        "--status",
        action="append",
        help="Filter by status; may be passed more than once",
    )
    list_parser.add_argument(
        "--kind",
        action="append",
        choices=["job", "workflow"],
        help="Filter by activity kind; may be passed more than once",
    )
    list_parser.add_argument("--json", action="store_true", help="Print JSON output")
    list_parser.set_defaults(func=cmd_queue_list)

    cancel_parser = queue_subparsers.add_parser(
        "cancel",
        help="Cancel a workflow or engine activity.",
    )
    cancel_parser.add_argument("target", help="Activity id, workflow id, queue id, run id, or known path alias")
    cancel_parser.add_argument("--workflow-root", help=argparse.SUPPRESS)
    cancel_parser.add_argument("--chemstack-config", "--config", dest="chemstack_config", help="Path to shared chemstack.yaml")
    cancel_parser.add_argument("--json", action="store_true", help="Print JSON output")
    cancel_parser.set_defaults(func=cmd_queue_cancel)

    run_dir_parser = subparsers.add_parser(
        "run-dir",
        help="Submit an ORCA job directory or workflow input directory through the unified CLI.",
    )
    _add_engine_config_argument(run_dir_parser)
    _add_orca_logging_arguments(run_dir_parser)
    run_dir_parser.add_argument("path", help="ORCA job directory or workflow input directory")
    run_dir_parser.add_argument("--force", action="store_true", help="Force re-run even if existing ORCA output is completed")
    run_dir_parser.add_argument("--priority", type=int, default=None, help="Queue priority when submission is enqueued (lower = higher)")
    run_dir_parser.add_argument("--max-cores", type=int, default=None, help="Override max cores recorded for this queued run or workflow")
    run_dir_parser.add_argument("--max-memory-gb", type=int, default=None, help="Override max memory (GB) recorded for this queued run or workflow")
    run_dir_parser.add_argument("--json", action="store_true", help="Print JSON output for workflow submission")
    run_dir_parser.set_defaults(func=cmd_run_dir)

    init_parser = subparsers.add_parser(
        "init",
        help="Interactively create or update the shared chemstack.yaml config.",
    )
    _add_engine_config_argument(init_parser)
    _add_orca_logging_arguments(init_parser)
    init_parser.add_argument("--force", action="store_true", help="Overwrite existing config without confirmation")
    init_parser.set_defaults(func=cmd_init)

    scaffold_parser = subparsers.add_parser(
        "scaffold",
        help="Create raw input workflow scaffold directories.",
    )
    scaffold_subparsers = scaffold_parser.add_subparsers(dest="scaffold_app", required=True)

    for name, workflow_type, help_text in _WORKFLOW_SCAFFOLD_SHORTCUTS:
        _add_workflow_scaffold_shortcut(
            scaffold_subparsers,
            name=name,
            workflow_type=workflow_type,
            help_text=help_text,
        )

    organize_parser = subparsers.add_parser(
        "organize",
        help="Plan or apply organization of terminal engine outputs.",
    )
    organize_subparsers = organize_parser.add_subparsers(dest="organize_app", required=True)

    orca_organize_parser = organize_subparsers.add_parser("orca", help="Plan or apply organization into orca_outputs")
    _add_engine_config_argument(orca_organize_parser)
    _add_orca_logging_arguments(orca_organize_parser)
    orca_organize_parser.add_argument("--reaction-dir", default=None, help="Single job directory to organize")
    orca_organize_parser.add_argument("--root", default=None, help="Root directory to scan (mutually exclusive with --reaction-dir)")
    orca_organize_parser.add_argument("--apply", action="store_true", default=False, help="Actually move files (default is dry-run)")
    orca_organize_parser.add_argument("--rebuild-index", action="store_true", default=False, help="Rebuild JSONL index from organized directories")
    orca_organize_parser.set_defaults(func=cmd_orca_organize)

    summary_parser = subparsers.add_parser(
        "summary",
        help="Show engine summaries or send ORCA Telegram digests through the unified CLI.",
    )
    summary_subparsers = summary_parser.add_subparsers(dest="summary_app", required=True)

    orca_summary_parser = summary_subparsers.add_parser("orca", help="Send or print the ORCA Telegram digest")
    _add_engine_config_argument(orca_summary_parser)
    _add_orca_logging_arguments(orca_summary_parser)
    orca_summary_parser.add_argument("--no-send", action="store_true", default=False, help="Print summary without sending Telegram")
    orca_summary_parser.set_defaults(func=cmd_orca_summary)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
