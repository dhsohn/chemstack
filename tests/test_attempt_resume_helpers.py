from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from chemstack.orca import attempt_resume
from chemstack.orca.state_store import new_state
from chemstack.orca.statuses import AnalyzerStatus, RunStatus


def test_attempt_resume_text_and_patch_action_helpers_cover_existing_and_missing_values() -> None:
    attempt = {"patch_actions": ["existing"]}
    assert attempt_resume._ensure_patch_actions_list(attempt) == ["existing"]

    attempt = {}
    actions = attempt_resume._ensure_patch_actions_list(attempt)
    assert actions == []
    assert attempt["patch_actions"] == []

    assert attempt_resume._as_non_empty_text(" hello ") == "hello"
    assert attempt_resume._as_non_empty_text("   ") is None
    assert attempt_resume._as_non_empty_text(123) is None


def test_recover_missing_retry_input_covers_missing_attempt_shapes_and_sources(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    selected_inp = reaction_dir / "calc.inp"
    current_inp = reaction_dir / "calc.retry01.inp"
    selected_inp.write_text("! Opt\n", encoding="utf-8")

    base_kwargs = {
        "reaction_dir": reaction_dir,
        "selected_inp": selected_inp,
        "current_inp": current_inp,
        "retries_used": 1,
        "retry_recipe_step": lambda retry_number: retry_number,
        "to_resolved_local": lambda raw: Path(raw),
        "save_state": lambda _reaction_dir, _state: reaction_dir / "run_state.json",
    }

    assert attempt_resume.recover_missing_retry_input(
        state={},
        **base_kwargs,
    ) == (False, "resume_attempts_missing")
    assert attempt_resume.recover_missing_retry_input(
        state={"attempts": ["bad"]},
        **base_kwargs,
    ) == (False, "resume_last_attempt_invalid")
    assert attempt_resume.recover_missing_retry_input(
        state={"attempts": [{}]},
        **base_kwargs,
    ) == (False, "resume_source_input_missing")

    missing_selected = reaction_dir / "missing_selected.inp"
    assert attempt_resume.recover_missing_retry_input(
        state={"attempts": [{"inp_path": str(current_inp)}]},
        reaction_dir=reaction_dir,
        selected_inp=missing_selected,
        current_inp=current_inp,
        retries_used=1,
        retry_recipe_step=lambda retry_number: retry_number,
        to_resolved_local=lambda raw: Path(raw),
        save_state=lambda _reaction_dir, _state: reaction_dir / "run_state.json",
    ) == (False, "resume_fallback_source_missing")

    assert attempt_resume.recover_missing_retry_input(
        state={"attempts": [{"inp_path": str(reaction_dir / "missing_source.inp")}]},
        **base_kwargs,
    ) == (False, "resume_source_input_not_found")


def test_recover_missing_retry_input_success_creates_patch_actions_and_saves_state(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    selected_inp = reaction_dir / "calc.inp"
    current_inp = reaction_dir / "calc.retry01.inp"
    selected_inp.write_text("! Opt\n", encoding="utf-8")
    source_inp = reaction_dir / "calc.prev.inp"
    source_inp.write_text("! Retry\n", encoding="utf-8")
    state = {"attempts": [{"inp_path": str(source_inp), "patch_actions": "bad"}]}

    with patch("chemstack.orca.attempt_resume.rewrite_for_retry", return_value=["patch_one"]) as rewrite_mock:
        saved_paths: list[Path] = []
        recovered, reason = attempt_resume.recover_missing_retry_input(
            reaction_dir=reaction_dir,
            state=state,
            selected_inp=selected_inp,
            current_inp=current_inp,
            retries_used=1,
            retry_recipe_step=lambda retry_number: retry_number + 1,
            to_resolved_local=lambda raw: Path(raw).resolve(),
            save_state=lambda reaction_dir_arg, _state: saved_paths.append(reaction_dir_arg) or reaction_dir / "run_state.json",
        )

    assert recovered
    assert reason == "resume_recovered"
    rewrite_mock.assert_called_once_with(
        source_inp=source_inp.resolve(),
        target_inp=current_inp,
        reaction_dir=reaction_dir,
        step=2,
    )
    assert state["attempts"][-1]["patch_actions"] == [
        "resume_recreated_missing_input:calc.retry01.inp",
        "resume_patch_one",
    ]
    assert saved_paths == [reaction_dir]


def test_resolve_execution_input_covers_existing_retry_recovery_exception_and_success(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    selected_inp = reaction_dir / "calc.inp"
    selected_inp.write_text("! Opt\n", encoding="utf-8")
    retry_path = reaction_dir / "calc.retry01.inp"
    retry_path.write_text("! Retry\n", encoding="utf-8")

    current_inp, reason = attempt_resume.resolve_execution_input(
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        state={"attempts": []},
        execution_index=2,
        retries_used=1,
        retry_inp_path=lambda inp, retry_number: inp.with_name(f"{inp.stem}.retry{retry_number:02d}.inp"),
        retry_recipe_step=lambda retry_number: retry_number,
        to_resolved_local=lambda raw: Path(raw),
        save_state=lambda _reaction_dir, _state: reaction_dir / "run_state.json",
    )
    assert current_inp == retry_path
    assert reason is None

    retry_path.unlink()
    with patch(
        "chemstack.orca.attempt_resume.recover_missing_retry_input",
        side_effect=RuntimeError("boom"),
    ):
        current_inp, reason = attempt_resume.resolve_execution_input(
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            state={"attempts": []},
            execution_index=2,
            retries_used=1,
            retry_inp_path=lambda inp, retry_number: inp.with_name(f"{inp.stem}.retry{retry_number:02d}.inp"),
            retry_recipe_step=lambda retry_number: retry_number,
            to_resolved_local=lambda raw: Path(raw),
            save_state=lambda _reaction_dir, _state: reaction_dir / "run_state.json",
        )
    assert current_inp is None
    assert reason == "missing_input_for_attempt_2:resume_recovery_exception"

    def _recover_and_create(**_kwargs: object) -> tuple[bool, str]:
        retry_path.write_text("! Retry\n", encoding="utf-8")
        return True, "resume_recovered"

    with patch("chemstack.orca.attempt_resume.recover_missing_retry_input", side_effect=_recover_and_create):
        current_inp, reason = attempt_resume.resolve_execution_input(
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            state={"attempts": []},
            execution_index=2,
            retries_used=1,
            retry_inp_path=lambda inp, retry_number: inp.with_name(f"{inp.stem}.retry{retry_number:02d}.inp"),
            retry_recipe_step=lambda retry_number: retry_number,
            to_resolved_local=lambda raw: Path(raw),
            save_state=lambda _reaction_dir, _state: reaction_dir / "run_state.json",
        )
    assert current_inp == retry_path
    assert reason is None


def test_resume_terminal_decision_covers_non_resumed_malformed_and_defaulted_terminal_paths(
    tmp_path: Path,
) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    selected_inp = reaction_dir / "calc.inp"
    selected_inp.write_text("! Opt\n", encoding="utf-8")

    state = new_state(reaction_dir, selected_inp, max_retries=2)
    assert (
        attempt_resume.resume_terminal_decision(
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            state=state,
            resumed=False,
            max_retries=2,
            last_out_path_from_state=lambda current_state: current_state.get("selected_inp"),
            exit_with_result=lambda *args, **kwargs: 0,
            emit=lambda _payload: None,
        )
        is None
    )
    assert (
        attempt_resume.resume_terminal_decision(
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            state={"attempts": "bad"},
            resumed=True,
            max_retries=2,
            last_out_path_from_state=lambda current_state: None,
            exit_with_result=lambda *args, **kwargs: 0,
            emit=lambda _payload: None,
        )
        is None
    )
    assert (
        attempt_resume.resume_terminal_decision(
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            state={"attempts": ["bad"]},
            resumed=True,
            max_retries=2,
            last_out_path_from_state=lambda current_state: None,
            exit_with_result=lambda *args, **kwargs: 0,
            emit=lambda _payload: None,
        )
        is None
    )

    non_terminal_state = {
        "attempts": [
            {
                "analyzer_status": AnalyzerStatus.INCOMPLETE.value,
                "analyzer_reason": "still_running",
            }
        ]
    }
    assert (
        attempt_resume.resume_terminal_decision(
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            state=non_terminal_state,
            resumed=True,
            max_retries=3,
            last_out_path_from_state=lambda current_state: None,
            exit_with_result=lambda *args, **kwargs: 0,
            emit=lambda _payload: None,
        )
        is None
    )

    terminal_state = {
        "attempts": [
            {"analyzer_status": "completed", "analyzer_reason": "normal_termination"},
            {"analyzer_status": " ", "analyzer_reason": " ", "out_path": " "},
        ]
    }
    exit_calls: list[dict[str, object]] = []

    def notify_finished(payload: object) -> None:
        del payload

    result = attempt_resume.resume_terminal_decision(
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        state=terminal_state,
        resumed=True,
        max_retries=1,
        last_out_path_from_state=lambda current_state: "state.out",
        exit_with_result=lambda *args, **kwargs: exit_calls.append(kwargs) or 7,
        emit=lambda _payload: None,
        notify_finished=notify_finished,
    )

    assert result == 7
    assert len(exit_calls) == 1
    assert exit_calls[0]["status"] == RunStatus.FAILED
    assert exit_calls[0]["analyzer_status"] == AnalyzerStatus.INCOMPLETE.value
    assert exit_calls[0]["reason"] == "retry_limit_reached"
    assert exit_calls[0]["last_out_path"] == "state.out"
    assert exit_calls[0]["notify_finished"] is notify_finished
