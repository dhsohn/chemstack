from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import yaml

from .files import default_shared_admission_root, workflow_root_from_mapping
from .schema import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig


def as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def mapping_section(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key, {})
    return value if isinstance(value, dict) else {}


def load_workflow_engine_config(
    config_path: str | None,
    *,
    default_config_path_fn: Callable[[], str],
    executable_key: str,
    paths_cls: Callable[..., Any],
    behavior_cls: Callable[..., Any],
    app_config_cls: Callable[..., Any],
) -> Any:
    path = Path(config_path or default_config_path_fn()).expanduser().resolve()
    if not path.exists():
        raise ValueError(
            f"Config file not found: {path}. Copy config/chemstack.yaml.example to this path and edit the workflow section."
        )

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Config file is invalid: {path}")

    scheduler_raw = mapping_section(raw, "scheduler")
    workflow_raw = mapping_section(raw, "workflow")
    workflow_paths_raw = mapping_section(workflow_raw, "paths")
    behavior_raw = mapping_section(raw, "behavior")
    resources_raw = mapping_section(raw, "resources")
    telegram_raw = mapping_section(raw, "telegram")

    workflow_root = workflow_root_from_mapping(raw)
    if not workflow_root:
        raise ValueError(f"Config is missing workflow.root: {path}")

    max_active = max(1, as_int(scheduler_raw.get("max_active_simulations"), 4))
    admission_root = as_str(
        scheduler_raw.get("admission_root"),
        default_shared_admission_root(path),
    )

    return app_config_cls(
        runtime=CommonRuntimeConfig(
            allowed_root=workflow_root,
            organized_root=workflow_root,
            max_concurrent=max_active,
            admission_root=admission_root,
            admission_limit=max_active,
        ),
        workflow_root=workflow_root,
        paths=paths_cls(
            **{executable_key: as_str(workflow_paths_raw.get(executable_key))},
        ),
        behavior=behavior_cls(
            auto_organize_on_terminal=as_bool(behavior_raw.get("auto_organize_on_terminal"), False),
        ),
        resources=CommonResourceConfig(
            max_cores_per_task=max(1, as_int(resources_raw.get("max_cores_per_task"), 8)),
            max_memory_gb_per_task=max(1, as_int(resources_raw.get("max_memory_gb_per_task"), 32)),
        ),
        telegram=TelegramConfig(
            bot_token=as_str(telegram_raw.get("bot_token")),
            chat_id=as_str(telegram_raw.get("chat_id")),
            timeout_seconds=max(0.1, as_float(telegram_raw.get("timeout_seconds"), TelegramConfig.timeout_seconds)),
            max_attempts=max(1, as_int(telegram_raw.get("max_attempts"), TelegramConfig.max_attempts)),
            retry_backoff_seconds=max(
                0.0,
                as_float(telegram_raw.get("retry_backoff_seconds"), TelegramConfig.retry_backoff_seconds),
            ),
        ),
    )
