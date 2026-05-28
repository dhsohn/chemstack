from __future__ import annotations

from chemstack.core.utils import process as process_utils


def test_memory_limit_preexec_applies_address_space_limit() -> None:
    calls: list[tuple[int, tuple[int, int]]] = []

    process_utils.memory_limit_preexec(
        3,
        setrlimit_fn=lambda kind, limits: calls.append((kind, limits)),
        limit_resource=9,
    )()

    assert calls == [(9, (3 * 1024 * 1024 * 1024, 3 * 1024 * 1024 * 1024))]
