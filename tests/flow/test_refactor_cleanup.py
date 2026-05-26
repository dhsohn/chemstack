from __future__ import annotations

from pathlib import Path

import pytest


from chemstack.flow import orchestration
from chemstack.flow._orchestration_builders import _copy_input_impl
from chemstack.flow._orchestration_deps import orchestration_deps
from chemstack.flow.contracts import WorkflowStage


def test_copy_input_impl_copies_file_and_raises_for_missing_source(tmp_path: Path) -> None:
    source = tmp_path / "source.xyz"
    source.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.7\n", encoding="utf-8")
    target = tmp_path / "nested" / "copied.xyz"

    copied = _copy_input_impl(str(source), target)

    assert copied == str(target.resolve())
    assert target.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Input XYZ not found"):
        _copy_input_impl(str(tmp_path / "missing.xyz"), tmp_path / "other.xyz")


def test_stage_dict_returns_stage_dictionary() -> None:
    stage = WorkflowStage(stage_id="stage_01", stage_kind="crest_stage", status="planned")

    assert orchestration._stage_dict(stage) == {
        "stage_id": "stage_01",
        "stage_kind": "crest_stage",
        "status": "planned",
        "input_artifacts": [],
        "output_artifacts": [],
        "task": None,
        "metadata": {},
    }


def test_orchestration_deps_use_explicit_overrides_not_public_module_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_now() -> str:
        return "fake"

    monkeypatch.setattr(orchestration, "now_utc_iso", fake_now)

    assert orchestration_deps().persistence.now_utc_iso is not fake_now
    assert orchestration_deps(overrides={"now_utc_iso": fake_now}).persistence.now_utc_iso is fake_now
