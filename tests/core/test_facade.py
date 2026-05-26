from __future__ import annotations

from types import SimpleNamespace

import pytest

from chemstack.core.facade import (
    delegate,
    delegate_with_deps,
    resolve_dependency,
    resolve_grouped_attr,
)
from chemstack.core.queue.types import QueueStatus as CoreQueueStatus
from chemstack.orca.statuses import QueueStatus as OrcaQueueStatus


def test_delegate_resolves_target_dynamically() -> None:
    calls: list[str] = []
    module = SimpleNamespace(target=lambda value: calls.append(f"old:{value}"))
    wrapped = delegate(module, "target")

    module.target = lambda value: calls.append(f"new:{value}")

    wrapped("value")

    assert calls == ["new:value"]


def test_delegate_with_deps_builds_fresh_dependencies() -> None:
    deps_seen: list[object] = []
    module = SimpleNamespace(target=lambda value, *, deps: deps_seen.append((value, deps.token)))
    counter = iter(("first", "second"))
    wrapped = delegate_with_deps(
        module,
        "target",
        lambda: SimpleNamespace(token=next(counter)),
    )

    wrapped("a")
    wrapped("b")

    assert deps_seen == [("a", "first"), ("b", "second")]


def test_dependency_helpers_resolve_explicit_grouped_and_missing_values() -> None:
    assert resolve_dependency(None, "explicit", "name") == "explicit"
    assert resolve_dependency(SimpleNamespace(name="from-deps"), None, "name") == "from-deps"
    assert resolve_grouped_attr("target", (SimpleNamespace(), SimpleNamespace(target=3))) == 3

    with pytest.raises(TypeError, match="missing required dependency"):
        resolve_dependency(None, None, "name")
    with pytest.raises(AttributeError, match="missing"):
        resolve_grouped_attr("missing", (SimpleNamespace(),))


def test_orca_queue_status_reuses_core_queue_status() -> None:
    assert OrcaQueueStatus is CoreQueueStatus
    assert OrcaQueueStatus.RUNNING.value == "running"
