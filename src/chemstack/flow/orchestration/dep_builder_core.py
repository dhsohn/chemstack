from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields
from functools import wraps
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from chemstack.flow.orchestration.deps import (
        OrchestrationDeps,
        _OrchestrationStageDepGroup,
    )

AnyCallable = Callable[..., Any]


class _LazyOrchestrationDeps:
    def __init__(self, overrides: Mapping[str, Any] | None) -> None:
        self._overrides = overrides
        self._deps: OrchestrationDeps | None = None

    def resolve_to(self, deps: OrchestrationDeps) -> None:
        self._deps = deps

    def get(self) -> OrchestrationDeps:
        if self._deps is None:
            from chemstack.flow.orchestration.deps import orchestration_deps

            self._deps = orchestration_deps(overrides=self._overrides)
        return self._deps


def _override(overrides: Mapping[str, Any] | None, name: str, fallback: Any) -> Any:
    if overrides is not None and name in overrides:
        return overrides[name]
    return fallback


@dataclass(frozen=True)
class _StageDepFallbackGroup:
    dep_group: _OrchestrationStageDepGroup
    fallbacks: Mapping[str, Any]

    def build(self, overrides: Mapping[str, Any] | None) -> Any:
        return _build_dep_dataclass(
            self.dep_group.deps_type,
            overrides,
            self.fallbacks,
            label=f"stage dependency group {self.dep_group.name!r}",
        )


def _apply_overrides(
    overrides: Mapping[str, Any] | None,
    items: Mapping[str, Any],
) -> dict[str, Any]:
    return {name: _override(overrides, name, fallback) for name, fallback in items.items()}


def _validate_dep_fallbacks(
    deps_type: type[Any],
    fallbacks: Mapping[str, Any],
    *,
    label: str | None = None,
) -> None:
    expected = tuple(field.name for field in fields(deps_type))
    expected_names = set(expected)
    fallback_names = tuple(fallbacks)
    actual_names = set(fallback_names)
    if actual_names == expected_names:
        return

    missing = tuple(name for name in expected if name not in actual_names)
    unexpected = tuple(name for name in fallback_names if name not in expected_names)
    deps_label = label or deps_type.__name__
    raise ValueError(
        f"{deps_label} fallback mismatch: missing={missing!r} unexpected={unexpected!r}"
    )


def _build_dep_dataclass(
    deps_type: type[Any],
    overrides: Mapping[str, Any] | None,
    fallbacks: Mapping[str, Any],
    *,
    label: str | None = None,
) -> Any:
    _validate_dep_fallbacks(deps_type, fallbacks, label=label)
    return deps_type(**_apply_overrides(overrides, fallbacks))


def _deps_provider(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps | None,
) -> _LazyOrchestrationDeps:
    return deps_provider or _LazyOrchestrationDeps(overrides)


def _bind_with_deps(deps_provider: _LazyOrchestrationDeps, func: AnyCallable) -> AnyCallable:
    @wraps(func)
    def call(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("deps") is None:
            kwargs["deps"] = deps_provider.get()
        return func(*args, **kwargs)

    return call


def _bind_many_with_deps(
    deps_provider: _LazyOrchestrationDeps,
    items: Mapping[str, AnyCallable],
) -> dict[str, AnyCallable]:
    return {name: _bind_with_deps(deps_provider, fallback) for name, fallback in items.items()}


__all__ = [
    "AnyCallable",
    "_LazyOrchestrationDeps",
    "_StageDepFallbackGroup",
    "_apply_overrides",
    "_bind_many_with_deps",
    "_bind_with_deps",
    "_build_dep_dataclass",
    "_deps_provider",
    "_override",
    "_validate_dep_fallbacks",
]
