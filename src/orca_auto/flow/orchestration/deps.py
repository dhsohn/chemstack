from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import orca_auto.flow.orchestration.dep_builders as _dep_builders
from orca_auto.flow.orchestration.dep_types import OrchestrationDeps


def orchestration_deps(overrides: Mapping[str, Any] | None = None) -> OrchestrationDeps:
    deps_provider = _dep_builders._LazyOrchestrationDeps(overrides, factory=orchestration_deps)
    deps = OrchestrationDeps(
        contracts=_dep_builders._build_contract_deps(overrides),
        persistence=_dep_builders._build_persistence_deps(overrides),
        engines=_dep_builders._build_engine_deps(overrides),
        stages=_dep_builders._build_stage_deps(overrides, deps_provider=deps_provider),
        advance=_dep_builders._build_advance_deps(overrides, deps_provider=deps_provider),
    )
    deps_provider.resolve_to(deps)
    return deps


def orchestration_context(deps: OrchestrationDeps | None = None) -> OrchestrationDeps:
    if deps is not None:
        return deps
    return orchestration_deps()


__all__ = ["orchestration_context", "orchestration_deps"]
