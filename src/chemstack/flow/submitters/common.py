from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from chemstack.core.config.files import default_shared_admission_root, engine_config_mapping


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_key_value_lines(text: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key_text = normalize_text(key)
        if not key_text:
            continue
        payload[key_text] = value.strip()
    return payload


def sibling_app_command(
    *,
    executable: str,
    config_path: str,
    repo_root: str | None,
    module_name: str,
    tail_argv: list[str],
) -> tuple[list[str], str | None, dict[str, str] | None]:
    del executable

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
    return [sys.executable, "-m", module_name, "--config", config_path, *tail_argv], str(root_path), env


def run_sibling_app(
    *,
    executable: str,
    config_path: str,
    repo_root: str | None,
    module_name: str,
    tail_argv: list[str],
) -> subprocess.CompletedProcess[str]:
    argv, cwd, env = sibling_app_command(
        executable=executable,
        config_path=config_path,
        repo_root=repo_root,
        module_name=module_name,
        tail_argv=tail_argv,
    )
    return subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def sibling_allowed_root(config_path: str, *, engine: str | None = None) -> Path:
    import yaml  # type: ignore[import-untyped]

    path = Path(config_path).expanduser().resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid sibling app config file: {path}")
    if engine:
        raw = engine_config_mapping(raw, engine)
    runtime = raw.get("runtime")
    if not isinstance(runtime, dict):
        raise ValueError(f"Missing runtime section in config: {path}")
    allowed_root = normalize_text(runtime.get("allowed_root"))
    if not allowed_root:
        raise ValueError(f"Missing runtime.allowed_root in config: {path}")
    return Path(allowed_root).expanduser().resolve()


def sibling_runtime_paths(config_path: str, *, engine: str | None = None) -> dict[str, Path]:
    import yaml  # type: ignore[import-untyped]

    path = Path(config_path).expanduser().resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid sibling app config file: {path}")
    if engine:
        raw = engine_config_mapping(raw, engine, inherit_keys=("scheduler",))
    runtime = raw.get("runtime")
    if not isinstance(runtime, dict):
        raise ValueError(f"Missing runtime section in config: {path}")
    scheduler = raw.get("scheduler")
    if not isinstance(scheduler, dict):
        scheduler = {}

    resolved: dict[str, Path] = {}
    for key in ("allowed_root", "organized_root"):
        value = normalize_text(runtime.get(key))
        if not value:
            continue
        resolved[key] = Path(value).expanduser().resolve()

    if "allowed_root" not in resolved:
        raise ValueError(f"Missing runtime.allowed_root in config: {path}")

    admission_root = normalize_text(runtime.get("admission_root"))
    if not admission_root:
        admission_root = normalize_text(scheduler.get("admission_root"))
    if not admission_root and scheduler:
        admission_root = default_shared_admission_root(path)
    if admission_root:
        resolved["admission_root"] = Path(admission_root).expanduser().resolve()
    return resolved


__all__ = [
    "normalize_text",
    "parse_key_value_lines",
    "run_sibling_app",
    "sibling_allowed_root",
    "sibling_runtime_paths",
    "sibling_app_command",
]
