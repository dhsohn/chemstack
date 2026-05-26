from __future__ import annotations

from collections.abc import Callable
from typing import Any


def delegate(module: object, name: str) -> Callable[..., Any]:
    target = getattr(module, name)

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        return getattr(module, name)(*args, **kwargs)

    _wrapped.__name__ = name
    _wrapped.__doc__ = getattr(target, "__doc__", None)
    return _wrapped


def delegate_with_deps(
    module: object,
    name: str,
    deps_factory: Callable[[], Any],
) -> Callable[..., Any]:
    target = getattr(module, name)

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        return getattr(module, name)(*args, **kwargs, deps=deps_factory())

    _wrapped.__name__ = name
    _wrapped.__doc__ = getattr(target, "__doc__", None)
    return _wrapped


def resolve_grouped_attr(name: str, groups: tuple[object, ...]) -> Any:
    for group in groups:
        if hasattr(group, name):
            return getattr(group, name)
    raise AttributeError(name)


def resolve_dependency(deps: object | None, explicit: Any, name: str) -> Any:
    if explicit is not None:
        return explicit
    if deps is not None:
        return getattr(deps, name)
    raise TypeError(f"missing required dependency: {name}")


__all__ = [
    "delegate",
    "delegate_with_deps",
    "resolve_dependency",
    "resolve_grouped_attr",
]
