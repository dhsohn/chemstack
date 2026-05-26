from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generic, TypeVar

import yaml

from .files import (
    default_config_path_from_repo_root,
    default_shared_admission_root,
    workflow_root_from_mapping,
)
from .schema import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig

_AppConfigT = TypeVar("_AppConfigT")


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


def positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def mapping_section(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key, {})
    return value if isinstance(value, dict) else {}


def positive_int_mapping(raw: object) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, int] = {}
    for key, value in raw.items():
        key_text = str(key).strip()
        if not key_text:
            continue
        parsed = positive_int(value)
        if parsed is not None:
            result[key_text] = parsed
    return result


def default_workflow_engine_config_path(module_file: str, *, env_var: str) -> str:
    repo_root = Path(module_file).resolve().parents[3]
    return default_config_path_from_repo_root(repo_root, env_var=env_var)


def resource_request_from_manifest(cfg: Any, manifest: dict[str, Any]) -> dict[str, int]:
    resources = manifest.get("resources")
    resource_overrides = dict(resources) if isinstance(resources, dict) else {}
    default_cores = max(1, int(cfg.resources.max_cores_per_task))
    default_memory = max(1, int(cfg.resources.max_memory_gb_per_task))
    max_cores = (
        positive_int(resource_overrides.get("max_cores"))
        or positive_int(resource_overrides.get("max_cores_per_task"))
        or positive_int(manifest.get("max_cores"))
        or positive_int(manifest.get("max_cores_per_task"))
        or default_cores
    )
    max_memory_gb = (
        positive_int(resource_overrides.get("max_memory_gb"))
        or positive_int(resource_overrides.get("max_memory_gb_per_task"))
        or positive_int(manifest.get("max_memory_gb"))
        or positive_int(manifest.get("max_memory_gb_per_task"))
        or default_memory
    )
    return {
        "max_cores": max_cores,
        "max_memory_gb": max_memory_gb,
    }


def resource_actual_from_request(resource_request: dict[str, int]) -> dict[str, int]:
    cores = max(1, int(resource_request.get("max_cores", 1)))
    memory_gb = max(1, int(resource_request.get("max_memory_gb", 1)))
    return {
        "assigned_cores": cores,
        "memory_limit_gb": memory_gb,
        "omp_num_threads": cores,
        "openblas_num_threads": cores,
        "mkl_num_threads": cores,
        "numexpr_num_threads": cores,
    }


def _load_config_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(
            f"Config file not found: {path}. Copy config/chemstack.yaml.example to this path and edit the workflow section."
        )

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Config file is invalid: {path}")
    return raw


def _required_workflow_root(raw: dict[str, Any], path: Path) -> str:
    workflow_root = workflow_root_from_mapping(raw)
    if not workflow_root:
        raise ValueError(f"Config is missing workflow.root: {path}")
    return workflow_root


def _runtime_config_from_scheduler(
    path: Path,
    scheduler_raw: dict[str, Any],
    workflow_root: str,
) -> CommonRuntimeConfig:
    max_active = max(1, as_int(scheduler_raw.get("max_active_simulations"), 4))
    admission_root = as_str(
        scheduler_raw.get("admission_root"),
        default_shared_admission_root(path),
    )
    return CommonRuntimeConfig(
        allowed_root=workflow_root,
        organized_root=workflow_root,
        max_concurrent=max_active,
        admission_root=admission_root,
        admission_limit=max_active,
    )


def _resource_config(resources_raw: dict[str, Any]) -> CommonResourceConfig:
    return CommonResourceConfig(
        max_cores_per_task=max(1, as_int(resources_raw.get("max_cores_per_task"), 8)),
        max_memory_gb_per_task=max(1, as_int(resources_raw.get("max_memory_gb_per_task"), 32)),
    )


def _telegram_config(telegram_raw: dict[str, Any]) -> TelegramConfig:
    return TelegramConfig(
        bot_token=as_str(telegram_raw.get("bot_token")),
        chat_id=as_str(telegram_raw.get("chat_id")),
        timeout_seconds=max(
            0.1,
            as_float(telegram_raw.get("timeout_seconds"), TelegramConfig.timeout_seconds),
        ),
        max_attempts=max(1, as_int(telegram_raw.get("max_attempts"), TelegramConfig.max_attempts)),
        retry_backoff_seconds=max(
            0.0,
            as_float(
                telegram_raw.get("retry_backoff_seconds"),
                TelegramConfig.retry_backoff_seconds,
            ),
        ),
    )


def load_workflow_engine_config(
    config_path: str | None,
    *,
    default_config_path_fn: Callable[[], str],
    executable_key: str,
    paths_cls: Callable[..., Any],
    behavior_cls: Callable[..., Any],
    app_config_cls: Callable[..., _AppConfigT],
) -> _AppConfigT:
    path = Path(config_path or default_config_path_fn()).expanduser().resolve()
    raw = _load_config_mapping(path)

    scheduler_raw = mapping_section(raw, "scheduler")
    workflow_raw = mapping_section(raw, "workflow")
    workflow_paths_raw = mapping_section(workflow_raw, "paths")
    resources_raw = mapping_section(raw, "resources")
    telegram_raw = mapping_section(raw, "telegram")
    workflow_root = _required_workflow_root(raw, path)

    return app_config_cls(
        runtime=_runtime_config_from_scheduler(path, scheduler_raw, workflow_root),
        workflow_root=workflow_root,
        paths=paths_cls(
            **{executable_key: as_str(workflow_paths_raw.get(executable_key))},
        ),
        behavior=behavior_cls(),
        resources=_resource_config(resources_raw),
        telegram=_telegram_config(telegram_raw),
    )


@dataclass(frozen=True)
class WorkflowEngineConfigSpec(Generic[_AppConfigT]):
    module_file: str
    env_var: str
    executable_key: str
    paths_cls: Callable[..., Any]
    behavior_cls: Callable[..., Any]
    app_config_cls: Callable[..., _AppConfigT]

    def default_config_path(self) -> str:
        return default_workflow_engine_config_path(self.module_file, env_var=self.env_var)

    def load_config(
        self,
        config_path: str | None = None,
        *,
        default_config_path_fn: Callable[[], str] | None = None,
    ) -> _AppConfigT:
        return load_workflow_engine_config(
            config_path,
            default_config_path_fn=default_config_path_fn or self.default_config_path,
            executable_key=self.executable_key,
            paths_cls=self.paths_cls,
            behavior_cls=self.behavior_cls,
            app_config_cls=self.app_config_cls,
        )


def workflow_engine_config_spec(
    *,
    module_file: str,
    executable_key: str,
    paths_cls: Callable[..., Any],
    behavior_cls: Callable[..., Any],
    app_config_cls: Callable[..., _AppConfigT],
    env_var: str = "CHEMSTACK_CONFIG",
) -> WorkflowEngineConfigSpec[_AppConfigT]:
    return WorkflowEngineConfigSpec(
        module_file=module_file,
        env_var=env_var,
        executable_key=executable_key,
        paths_cls=paths_cls,
        behavior_cls=behavior_cls,
        app_config_cls=app_config_cls,
    )
