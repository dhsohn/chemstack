from __future__ import annotations

from importlib import import_module

from orca_auto.flow.orchestration.dep_types import OrchestrationDeps


def orchestration_context(deps: OrchestrationDeps | None = None) -> OrchestrationDeps:
    if deps is not None:
        return deps
    deps_module = import_module("orca_auto.flow.orchestration.deps")
    return deps_module.orchestration_deps()


__all__ = ["orchestration_context"]
