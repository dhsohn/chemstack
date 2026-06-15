from __future__ import annotations

from importlib import import_module
from typing import Any

from .artifacts import (
    ENGINE_ARTIFACT_SCHEMA_VERSION,
    EngineArtifactInput,
    EngineArtifactJob,
    EngineArtifactProcess,
    EngineArtifactRecovery,
    EngineArtifactResources,
    EngineArtifactSchema,
    EngineArtifactStatus,
    EngineArtifactTimestamps,
    build_engine_artifact_payload,
    build_engine_report_markdown,
    load_engine_artifact_payload,
)
from .definitions import (
    EngineArtifactAdapter,
    EngineContextBuilder,
    EngineDefinition,
    EngineNotificationHooks,
    EngineQueueFunctions,
    EngineRunnerCallbacks,
)
from .registry import get_engine_definition, known_engine_ids

_LAZY_EXPORTS = {
    "EngineQueueWorker": (".queue_worker", "EngineQueueWorker"),
    "EngineWorkerChild": (".worker_child", "EngineWorkerChild"),
    "build_engine_runtime_roots": (".definition_builder", "build_engine_runtime_roots"),
    "build_lazy_queue_worker_runner": (
        ".definition_builder",
        "build_lazy_queue_worker_runner",
    ),
    "build_lazy_worker_child_runner": (
        ".definition_builder",
        "build_lazy_worker_child_runner",
    ),
    "build_queue_engine_definition": (".definition_builder", "build_queue_engine_definition"),
    "build_queue_entry_by_id": (".definition_builder", "build_queue_entry_by_id"),
    "build_worker_child_command": (".worker_child", "build_worker_child_command"),
    "build_worker_child_command_for_engine": (
        ".worker_child",
        "build_worker_child_command_for_engine",
    ),
    "run_engine_worker_child_job": (".worker_child", "run_engine_worker_child_job"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(name)
    module_name, attr_name = _LAZY_EXPORTS[name]
    module = import_module(module_name, __name__)
    return getattr(module, attr_name)


__all__ = [
    "ENGINE_ARTIFACT_SCHEMA_VERSION",
    "EngineArtifactSchema",
    "EngineArtifactAdapter",
    "EngineContextBuilder",
    "EngineArtifactInput",
    "EngineArtifactJob",
    "EngineArtifactProcess",
    "EngineArtifactRecovery",
    "EngineArtifactResources",
    "EngineArtifactStatus",
    "EngineArtifactTimestamps",
    "EngineDefinition",
    "EngineNotificationHooks",
    "EngineQueueFunctions",
    "EngineQueueWorker",
    "EngineRunnerCallbacks",
    "EngineWorkerChild",
    "build_engine_artifact_payload",
    "build_engine_report_markdown",
    "build_engine_runtime_roots",
    "build_lazy_queue_worker_runner",
    "build_lazy_worker_child_runner",
    "build_queue_engine_definition",
    "build_queue_entry_by_id",
    "build_worker_child_command",
    "build_worker_child_command_for_engine",
    "get_engine_definition",
    "known_engine_ids",
    "load_engine_artifact_payload",
    "run_engine_worker_child_job",
]
