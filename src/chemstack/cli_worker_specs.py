from __future__ import annotations

import argparse
import os
import sys
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
    CHEMSTACK_WORKFLOW_WORKER_MODULE,
)
from chemstack.core.utils import normalize_text

_WORKFLOW_ENGINE_APPS = ("crest", "xtb")
_ENGINE_APPS = ("orca",)
_ENGINE_WORKER_MODULES = {
    "orca": "chemstack.core.engines.queue_worker",
    "crest": "chemstack.core.engines.queue_worker",
    "xtb": "chemstack.core.engines.queue_worker",
}
_KNOWN_WORKER_APPS = (*_ENGINE_APPS, "workflow")
_DEFAULT_WORKER_APPS = _ENGINE_APPS


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
    tail_argv: list[str] = ["--engine", app]
    if app != "orca":
        return tail_argv
    if bool(getattr(args, "auto_organize", False)):
        tail_argv.append("--auto-organize")
    elif bool(getattr(args, "no_auto_organize", False)):
        tail_argv.append("--no-auto-organize")
    return tail_argv


def worker_module_command(
    *,
    config_path: str,
    repo_root: str | None,
    module_name: str,
    tail_argv: list[str],
) -> tuple[list[str], str | None, dict[str, str] | None]:
    argv = [sys.executable, "-m", module_name, "--config", config_path, *tail_argv]
    if repo_root is None:
        return argv, None, None

    root_path = Path(repo_root).expanduser().resolve()
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    candidates = [str(root_path)]
    src_root = root_path / "src"
    if src_root.is_dir():
        candidates.insert(0, str(src_root))
    pythonpath = ":".join(candidates)
    env["PYTHONPATH"] = pythonpath if not existing else f"{pythonpath}:{existing}"
    return argv, str(root_path), env


def _engine_worker_spec(
    *,
    app: str,
    config_path: str,
    args: argparse.Namespace,
    deps: Any | None = None,
) -> WorkerSpec:
    build_worker_command = _dependency(deps, "worker_module_command", worker_module_command)
    repo_root_for_subprocess = _dependency(
        deps, "_repo_root_for_subprocess", _repo_root_for_subprocess
    )
    engine_worker_tail_argv = _dependency(
        deps, "_engine_worker_tail_argv", _engine_worker_tail_argv
    )
    argv, cwd, env = build_worker_command(
        config_path=config_path,
        repo_root=repo_root_for_subprocess(),
        module_name=_ENGINE_WORKER_MODULES[app],
        tail_argv=engine_worker_tail_argv(app=app, args=args),
    )
    env_payload = dict(env) if isinstance(env, dict) else None
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
        CHEMSTACK_WORKFLOW_WORKER_MODULE,
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
    deps: Any | None = None,
) -> None:
    if "workflow" in apps and not workflow_root:
        raise ValueError("workflow worker requires workflow.root in chemstack.yaml")

    should_add_workflow = "workflow" in apps or (not explicit_app_selection and bool(workflow_root))
    if should_add_workflow and workflow_root:
        workflow_worker_spec = _dependency(deps, "_workflow_worker_spec", _workflow_worker_spec)
        specs.append(
            workflow_worker_spec(workflow_root=workflow_root, config_path=config_path, args=args)
        )
        return

    workflow_only_worker_flag_error = _dependency(
        deps, "_workflow_only_worker_flag_error", _workflow_only_worker_flag_error
    )
    flag_error = workflow_only_worker_flag_error(args)
    if flag_error:
        raise ValueError(flag_error)


def _build_worker_specs(args: Any, *, deps: Any | None = None) -> list[WorkerSpec]:
    explicit_apps = list(getattr(args, "app", None) or [])
    selected_worker_apps = _dependency(deps, "_selected_worker_apps", _selected_worker_apps)
    apps = selected_worker_apps(explicit_apps)
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
    worker_engine_apps = _dependency(deps, "_worker_engine_apps", _worker_engine_apps)
    engine_apps = worker_engine_apps(apps, workflow_enabled=workflow_enabled)
    validate_engine_worker_config = _dependency(
        deps, "_validate_engine_worker_config", _validate_engine_worker_config
    )
    validate_engine_worker_config(engine_apps, config_path)

    engine_worker_spec = _dependency(deps, "_engine_worker_spec", _engine_worker_spec)
    specs = [
        engine_worker_spec(app=app, config_path=str(config_path), args=args, deps=deps)
        for app in engine_apps
    ]
    add_workflow_worker_spec = _dependency(
        deps, "_add_workflow_worker_spec", _add_workflow_worker_spec
    )
    add_workflow_worker_spec(
        specs,
        apps=apps,
        explicit_app_selection=explicit_app_selection,
        workflow_root=workflow_root,
        config_path=config_path,
        args=args,
    )
    return specs
