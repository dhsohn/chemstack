from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


from chemstack.flow import orchestration
from chemstack.flow import _orchestration_dep_builders as dep_builders
from chemstack.flow._orchestration_builders import _copy_input_impl
from chemstack.flow._orchestration_deps import orchestration_deps


def test_copy_input_impl_copies_file_and_raises_for_missing_source(tmp_path: Path) -> None:
    source = tmp_path / "source.xyz"
    source.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.7\n", encoding="utf-8")
    target = tmp_path / "nested" / "copied.xyz"

    copied = _copy_input_impl(str(source), target)

    assert copied == str(target.resolve())
    assert target.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Input XYZ not found"):
        _copy_input_impl(str(tmp_path / "missing.xyz"), tmp_path / "other.xyz")


def test_orchestration_deps_use_explicit_overrides_not_public_module_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_now() -> str:
        return "fake"

    monkeypatch.setattr(orchestration, "now_utc_iso", fake_now)

    assert orchestration_deps().persistence.now_utc_iso is not fake_now
    assert orchestration_deps(overrides={"now_utc_iso": fake_now}).persistence.now_utc_iso is fake_now


def test_orchestration_stage_deps_passthroughs_delegate_to_grouped_deps() -> None:
    def fake_normalize(value: object) -> str:
        return f"normalized:{value}"

    deps = orchestration_deps(overrides={"_normalize_text": fake_normalize})

    assert deps.stages._normalize_text is deps.stages.support._normalize_text
    assert deps.stages._normalize_text("x") == "normalized:x"
    assert deps.stages._append_unique_artifact is deps.stages.runtime._append_unique_artifact
    with pytest.raises(AttributeError, match="OrchestrationStageDeps"):
        getattr(deps.stages, "_not_a_stage_dep")


def test_bound_stage_deps_reuse_lazy_orchestration_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    build_contract_deps = dep_builders._build_contract_deps

    def count_builds(overrides: dict[str, Any] | None) -> Any:
        nonlocal calls
        calls += 1
        return build_contract_deps(overrides)

    monkeypatch.setattr(dep_builders, "_build_contract_deps", count_builds)

    deps = orchestration_deps()
    rows: list[dict[str, Any]] = []
    deps.stages._append_unique_artifact(rows, kind="xyz", path="a.xyz")
    deps.stages._append_unique_artifact(rows, kind="log", path="b.log")

    assert calls == 1
    assert [row["path"] for row in rows] == ["a.xyz", "b.log"]
