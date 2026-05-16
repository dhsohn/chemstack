from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class OrchestrationDeps(Protocol):
    def __getattr__(self, name: str) -> Any: ...


@dataclass(frozen=True)
class FacadeOrchestrationDeps:
    """Resolve helper dependencies through the public orchestration facade.

    The extracted helper modules still need to honor tests and callers that
    monkeypatch ``chemstack.flow.orchestration``. Resolving attributes lazily
    keeps that compatibility while making the dependency boundary explicit.
    """

    def __getattr__(self, name: str) -> Any:
        from . import orchestration as facade

        return getattr(facade, name)


def orchestration_deps() -> OrchestrationDeps:
    return FacadeOrchestrationDeps()
