from __future__ import annotations

import errno
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import core.result_organizer as organizer
from core.result_organizer import OrganizePlan


def _write_state(reaction_dir: Path, payload: dict[str, object]) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    (reaction_dir / "run_state.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def _plan(tmp_path: Path) -> OrganizePlan:
    source_dir = tmp_path / "runs" / "run_1"
    target_abs_path = tmp_path / "organized" / "opt" / "H2" / "run_1"
    return OrganizePlan(
        reaction_dir=source_dir,
        run_id="run_1",
        job_type="opt",
        molecule_key="H2",
        selected_inp=str(source_dir / "calc.inp"),
        last_out_path=str(source_dir / "calc.out"),
        attempt_count=1,
        status="completed",
        analyzer_status="completed",
        reason="normal_termination",
        completed_at="2026-03-22T00:00:00+00:00",
        source_dir=source_dir,
        target_rel_path="opt/H2/run_1",
        target_abs_path=target_abs_path,
    )


@pytest.mark.parametrize("status", ["", 123])
def test_check_eligibility_rejects_blank_or_invalid_status_as_schema_invalid(
    tmp_path: Path,
    status: object,
) -> None:
    reaction_dir = tmp_path / "rxn"
    _write_state(
        reaction_dir,
        {
            "run_id": "run_test",
            "status": status,
            "final_result": {},
        },
    )

    state, skip = organizer.check_eligibility(reaction_dir)

    assert state is None
    assert skip is not None
    assert skip.reason == "state_schema_invalid"


def test_last_successful_attempt_inp_path_returns_none_when_no_attempt_is_usable(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    failed_inp = reaction_dir / "failed.inp"
    failed_inp.write_text("! Opt\n", encoding="utf-8")

    state = {
        "attempts": [
            "not-a-dict",
            {"inp_path": "   ", "out_path": str(reaction_dir / "blank.out"), "return_code": 0},
            {"inp_path": str(reaction_dir / "missing.inp"), "out_path": str(reaction_dir / "missing.out")},
            {"inp_path": str(failed_inp), "return_code": 1},
        ],
        "final_result": {},
    }

    assert organizer._last_successful_attempt_inp_path(state, reaction_dir) is None


def test_select_organize_metadata_uses_successful_retry_when_selected_input_is_missing(
    tmp_path: Path,
) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    retry_inp = reaction_dir / "retry.inp"
    retry_out = reaction_dir / "retry.out"
    retry_inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 1\n*\n", encoding="utf-8")
    retry_out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

    state = {
        "selected_inp": str(reaction_dir / "missing_selected.inp"),
        "attempts": [
            {
                "inp_path": str(retry_inp),
                "out_path": str(retry_out),
                "return_code": 0,
            }
        ],
        "final_result": {"last_out_path": str(retry_out)},
    }

    with patch(
        "core.result_organizer.resolve_molecule_key",
        return_value=SimpleNamespace(source="input_file", key="H2"),
    ):
        assert organizer.select_organize_metadata_inp_path(state, reaction_dir) == retry_inp.resolve()


def test_last_successful_attempt_returns_successful_retry_without_final_output_match(
    tmp_path: Path,
) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    retry_inp = reaction_dir / "retry.inp"
    retry_inp.write_text("! Opt\n", encoding="utf-8")

    state = {
        "attempts": [
            {
                "inp_path": str(retry_inp),
                "return_code": 0,
            }
        ],
        "final_result": {},
    }

    assert organizer._last_successful_attempt_inp_path(state, reaction_dir) == retry_inp.resolve()


def test_compute_organize_plan_requires_non_empty_string_run_id(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()

    with pytest.raises(RuntimeError, match="missing run_id"):
        organizer.compute_organize_plan(reaction_dir, {"run_id": ""}, tmp_path / "organized")

    with pytest.raises(RuntimeError, match="missing run_id"):
        organizer.compute_organize_plan(reaction_dir, {"run_id": 123}, tmp_path / "organized")


def test_rollback_move_reraises_non_exdev_oserror(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    plan.target_abs_path.mkdir(parents=True, exist_ok=True)

    with patch("core.result_organizer.os.rename", side_effect=OSError(errno.EPERM, "permission denied")):
        with pytest.raises(OSError) as exc_info:
            organizer.rollback_move(plan)

    assert exc_info.value.errno == errno.EPERM


def test_sync_state_after_move_and_rollback_delegate_to_relocation_helper(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    moved_state = {"reaction_dir": str(plan.target_abs_path)}
    rolled_back_state = {"reaction_dir": str(plan.source_dir)}

    with patch(
        "core.result_organizer._sync_state_after_relocation",
        return_value=moved_state,
    ) as sync_state:
        assert organizer.sync_state_after_move(plan) == moved_state
        sync_state.assert_called_once_with(
            state_dir=plan.target_abs_path,
            source_dir=plan.source_dir,
            target_dir=plan.target_abs_path,
        )

    with patch(
        "core.result_organizer._sync_state_after_relocation",
        return_value=rolled_back_state,
    ) as sync_state:
        assert organizer.sync_state_after_rollback(plan) == rolled_back_state
        sync_state.assert_called_once_with(
            state_dir=plan.source_dir,
            source_dir=plan.target_abs_path,
            target_dir=plan.source_dir,
        )
