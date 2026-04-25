from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.contracts import WorkflowStageInput
from chemstack.flow.orchestration import (
    _append_unique_artifact,
    _clear_reaction_xtb_handoff_error_if_recovering,
    _completed_crest_roles,
    _completed_crest_stage,
    _completed_orca_stage,
    _downstream_terminal_result,
    _effective_stage_status,
    _latest_child_stage_summary,
    _load_config_organized_root,
    _load_config_root,
    _reaction_orca_allows_next_candidate,
    _reaction_ts_guess_error,
    _recompute_workflow_status,
    _stage_failure_is_recoverable,
    _stage_has_xtb_candidates,
    _submission_target,
    _workflow_has_active_children,
    _workflow_sync_only,
    _xtb_handoff_status,
)
from chemstack.flow import orchestration


def test_workflow_sync_only_and_active_children_cover_stage_task_and_downstream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _workflow_sync_only({"status": "completed"}) is True
    assert _workflow_sync_only({"status": "planned"}) is False

    monkeypatch.setattr(orchestration, "workflow_has_active_downstream", lambda payload: True)
    assert _workflow_has_active_children({"stages": [{"status": "completed", "task": {"status": "completed"}}]}) is True
    monkeypatch.setattr(orchestration, "workflow_has_active_downstream", lambda payload: False)
    assert _workflow_has_active_children({"stages": [{"status": "running"}]}) is True
    assert _workflow_has_active_children({"stages": [{"status": "completed", "task": {"status": "submitted"}}]}) is True
    assert _workflow_has_active_children({"stages": [{"status": "completed", "task": {"status": "completed"}}]}) is False


def test_latest_child_stage_summary_and_terminal_result_extract_relevant_fields() -> None:
    stage_summaries = [
        {"stage_id": "s1", "status": "planned", "task_status": "planned", "completed_at": ""},
        {
            "stage_id": "s2",
            "stage_kind": "orca_stage",
            "engine": "orca",
            "task_kind": "opt",
            "status": "running",
            "task_status": "completed",
            "analyzer_status": "running",
            "reason": "working",
            "queue_id": "q_1",
            "run_id": "run_1",
            "latest_known_path": "/tmp/rxn",
            "organized_output_dir": "/tmp/out",
            "completed_at": "2026-04-19T00:10:00+00:00",
        },
        {"stage_id": "s3", "status": "queued", "task_status": "queued", "completed_at": "2026-04-19T00:05:00+00:00"},
    ]

    summary = _latest_child_stage_summary(stage_summaries)

    assert summary == {
        "stage_id": "s2",
        "stage_kind": "orca_stage",
        "engine": "orca",
        "task_kind": "opt",
        "status": "running",
        "task_status": "completed",
        "analyzer_status": "running",
        "reason": "working",
        "queue_id": "q_1",
        "run_id": "run_1",
        "latest_known_path": "/tmp/rxn",
        "organized_output_dir": "/tmp/out",
        "completed_at": "2026-04-19T00:10:00+00:00",
    }

    terminal = _downstream_terminal_result(
        {"metadata": {"workflow_error": {"reason": "boom", "scope": "orca"}}},
        {"status": "failed", "stage_summaries": stage_summaries},
    )
    assert terminal == {
        "status": "failed",
        "completed_at": "2026-04-19T00:05:00+00:00",
        "failure_reason": "boom",
        "failure_scope": "orca",
    }
    assert _downstream_terminal_result({}, {"status": "running", "stage_summaries": []}) == {}


def test_submission_target_and_config_roots_follow_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    stage = {
        "metadata": {"queue_id": "q_meta"},
        "task": {"submission_result": {"parsed_stdout": {"job_id": "job_stdout", "queue_id": "q_stdout"}}},
    }
    assert _submission_target(stage) == "q_meta"
    assert _submission_target({"task": {"submission_result": {"parsed_stdout": {"job_id": "job_stdout"}}}}) == "job_stdout"
    assert _submission_target({}) == ""

    monkeypatch.setattr(orchestration, "sibling_runtime_paths", lambda path: {"allowed_root": Path("/tmp/allowed"), "organized_root": Path("/tmp/organized")})
    assert _load_config_root("/tmp/config.yaml") == Path("/tmp/allowed")
    assert _load_config_organized_root("/tmp/config.yaml") == Path("/tmp/organized")
    monkeypatch.setattr(orchestration, "sibling_runtime_paths", lambda path: {"allowed_root": Path("/tmp/allowed")})
    assert _load_config_organized_root("/tmp/config.yaml") == Path("/tmp/allowed")
    monkeypatch.setattr(orchestration, "sibling_runtime_paths", lambda path: (_ for _ in ()).throw(ValueError("bad")))
    assert _load_config_root("/tmp/config.yaml") is None
    assert _load_config_organized_root("/tmp/config.yaml") is None
    assert _load_config_root(None) is None


def test_xtb_handoff_status_and_ts_guess_error_cover_ready_and_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    ready_input = WorkflowStageInput(
        source_job_id="xtb_job",
        source_job_type="path_search",
        reaction_key="rxn",
        selected_input_xyz="/tmp/ts.xyz",
        rank=1,
        kind="ts_guess",
        artifact_path="/tmp/ts.xyz",
        selected=True,
    )
    contract = SimpleNamespace(candidate_details=())

    monkeypatch.setattr(orchestration, "select_xtb_downstream_inputs", lambda contract, policy, require_geometry: (ready_input,))
    ready = _xtb_handoff_status(contract)
    assert ready == {
        "status": "ready",
        "reason": "",
        "message": "",
        "artifact_path": "/tmp/ts.xyz",
    }

    missing_contract = SimpleNamespace(candidate_details=())
    monkeypatch.setattr(orchestration, "select_xtb_downstream_inputs", lambda contract, policy, require_geometry: ())
    assert _xtb_handoff_status(missing_contract) == {
        "status": "failed",
        "reason": "xtb_ts_guess_missing",
        "message": "xTB path_search did not produce a ts_guess candidate (xtbpath_ts.xyz); refusing ORCA handoff.",
        "artifact_path": "",
    }

    invalid_contract = SimpleNamespace(candidate_details=(SimpleNamespace(kind="ts_guess", path="/tmp/xtbpath_ts.xyz", rank=1),))
    monkeypatch.setattr(orchestration, "choose_orca_geometry_frame", lambda path, candidate_kind: ("", {"selection_reason": "ts_guess_requires_single_frame"}))
    assert _reaction_ts_guess_error(invalid_contract) == {
        "reason": "xtb_ts_guess_not_single_geometry",
        "message": "xTB produced xtbpath_ts.xyz but it is not a single-geometry TS guess; refusing ORCA handoff.",
    }


def test_stage_candidate_and_failure_helpers_cover_recoverable_paths() -> None:
    assert _stage_has_xtb_candidates({"output_artifacts": [{"kind": "xtb_candidate", "path": "/tmp/candidate.xyz"}]}) is True
    assert _stage_has_xtb_candidates({"output_artifacts": [{"kind": "xtb_candidate", "path": ""}]}) is False

    xtb_stage = {"status": "failed", "task": {"engine": "xtb"}, "metadata": {"reaction_handoff_status": "ready"}}
    orca_stage = {"status": "cancel_failed", "task": {"engine": "orca"}, "metadata": {"reaction_candidate_status": "superseded"}}
    plain_stage = {"status": "failed", "task": {"engine": "crest"}, "metadata": {}}
    assert _stage_failure_is_recoverable(xtb_stage) is True
    assert _stage_failure_is_recoverable(orca_stage) is True
    assert _stage_failure_is_recoverable(plain_stage) is False
    assert _effective_stage_status(xtb_stage) == "completed"
    assert _effective_stage_status({"status": "running"}) == "running"

    failing_orca: dict[str, Any] = {
        "status": "failed",
        "metadata": {"analyzer_status": "ts_not_found"},
    }
    assert _reaction_orca_allows_next_candidate(failing_orca) is True
    failing_orca["metadata"]["reaction_candidate_status"] = "superseded"
    assert _reaction_orca_allows_next_candidate(failing_orca) is False


def test_clear_reaction_xtb_handoff_error_and_unique_artifact_helpers() -> None:
    payload = {
        "metadata": {
            "workflow_error": {
                "status": "failed",
                "scope": "reaction_ts_search_xtb_handoff",
            }
        },
        "stages": [
            {
                "status": "planned",
                "task": {"engine": "xtb"},
                "metadata": {"reaction_handoff_status": "retrying"},
            }
        ],
    }

    _clear_reaction_xtb_handoff_error_if_recovering(payload)
    assert "workflow_error" not in payload["metadata"]

    rows = [{"kind": "artifact", "path": "/tmp/a.xyz"}]
    _append_unique_artifact(rows, kind="artifact", path="/tmp/a.xyz")
    _append_unique_artifact(rows, kind="artifact", path="/tmp/b.xyz", selected=True, metadata={"rank": 2})
    assert rows == [
        {"kind": "artifact", "path": "/tmp/a.xyz"},
        {"kind": "artifact", "path": "/tmp/b.xyz", "selected": True, "metadata": {"rank": 2}},
    ]


def test_completed_role_and_contract_helpers_use_expected_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "stages": [
            {"status": "completed", "metadata": {"input_role": "reactant"}, "task": {"engine": "crest"}},
            {"status": "running", "metadata": {"input_role": "product"}, "task": {"engine": "crest"}},
            {"status": "completed", "metadata": {"input_role": "product"}, "task": {"engine": "crest"}},
        ]
    }
    assert set(_completed_crest_roles(payload).keys()) == {"reactant", "product"}

    crest_calls: list[dict[str, Any]] = []

    def fake_load_crest_artifact_contract(*, crest_index_root: Path, target: str) -> str:
        crest_calls.append({"crest_index_root": crest_index_root, "target": target})
        return "crest_contract"

    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: Path("/tmp/crest_allowed"))
    monkeypatch.setattr(orchestration, "load_crest_artifact_contract", fake_load_crest_artifact_contract)
    crest_stage = {
        "task": {"payload": {"job_dir": "/tmp/crest_job"}},
        "metadata": {"queue_id": "q_ignore"},
    }
    assert _completed_crest_stage(crest_stage, crest_auto_config="/tmp/crest.yaml") == "crest_contract"
    assert crest_calls == [{"crest_index_root": Path("/tmp/crest_allowed"), "target": "/tmp/crest_job"}]

    orca_calls: list[dict[str, Any]] = []

    def fake_load_orca_artifact_contract(**kwargs: Any) -> str:
        orca_calls.append(kwargs)
        return "orca_contract"

    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: Path("/tmp/orca_allowed"))
    monkeypatch.setattr(orchestration, "_load_config_organized_root", lambda path: Path("/tmp/orca_organized"))
    monkeypatch.setattr(orchestration, "load_orca_artifact_contract", fake_load_orca_artifact_contract)
    orca_stage = {
        "metadata": {"run_id": "run_1", "queue_id": "q_1"},
        "task": {
            "payload": {"reaction_dir": "/tmp/reaction_dir"},
            "enqueue_payload": {"reaction_dir": "/tmp/enqueue_dir"},
        },
    }
    assert _completed_orca_stage(orca_stage, orca_auto_config="/tmp/orca.yaml") == "orca_contract"
    assert orca_calls == [
        {
            "target": "run_1",
            "orca_allowed_root": Path("/tmp/orca_allowed"),
            "orca_organized_root": Path("/tmp/orca_organized"),
            "queue_id": "q_1",
            "run_id": "run_1",
            "reaction_dir": "/tmp/reaction_dir",
        }
    ]


def test_completed_crest_roles_ignore_stale_completed_stage_when_newer_stage_is_active() -> None:
    payload = {
        "stages": [
            {"status": "completed", "metadata": {"input_role": "reactant"}, "task": {"engine": "crest", "status": "completed"}},
            {"status": "completed", "metadata": {"input_role": "product"}, "task": {"engine": "crest", "status": "completed"}},
            {"status": "running", "metadata": {"input_role": "product"}, "task": {"engine": "crest", "status": "running"}},
        ]
    }

    assert set(_completed_crest_roles(payload).keys()) == {"reactant"}


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"metadata": {"workflow_error": {"status": "failed"}}, "stages": []}, "failed"),
        ({"stages": [{"status": "submission_failed"}]}, "failed"),
        ({"status": "cancel_requested", "stages": [{"status": "running"}]}, "cancel_requested"),
        ({"status": "cancel_requested", "stages": [{"status": "completed"}]}, "cancelled"),
        ({"stages": [{"status": "queued"}]}, "running"),
        ({"stages": [{"status": "completed"}, {"status": "completed"}]}, "completed"),
        ({"stages": [{"status": "completed"}, {"status": "planned"}]}, "running"),
        ({"stages": [{"status": "cancelled"}]}, "completed"),
        ({"stages": []}, "planned"),
    ],
)
def test_recompute_workflow_status_covers_major_branches(payload: dict[str, Any], expected: str) -> None:
    assert _recompute_workflow_status(payload) == expected


def test_recompute_workflow_status_treats_child_failures_by_engine_role() -> None:
    assert _recompute_workflow_status(
        {
            "template_name": "reaction_ts_search",
            "stages": [
                {"status": "failed", "task": {"engine": "crest"}},
                {"status": "running", "task": {"engine": "xtb"}},
            ],
        }
    ) == "failed"

    assert _recompute_workflow_status(
        {
            "template_name": "reaction_ts_search",
            "stages": [
                {"status": "failed", "task": {"engine": "xtb"}},
                {"status": "planned", "task": {"engine": "orca"}},
            ],
        }
    ) == "running"

    assert _recompute_workflow_status(
        {
            "template_name": "reaction_ts_search",
            "stages": [
                {"status": "failed", "task": {"engine": "xtb"}},
                {"status": "failed", "task": {"engine": "orca"}},
            ],
        }
    ) == "completed"

    assert _recompute_workflow_status(
        {
            "template_name": "conformer_screening",
            "stages": [
                {"status": "cancel_requested", "task": {"engine": "orca"}},
                {"status": "completed", "task": {"engine": "orca"}},
            ],
        }
    ) == "running"

    assert _recompute_workflow_status(
        {
            "template_name": "conformer_screening",
            "stages": [
                {"status": "cancelled", "task": {"engine": "orca"}},
                {"status": "completed", "task": {"engine": "orca"}},
            ],
        }
    ) == "completed"

    assert _recompute_workflow_status(
        {
            "template_name": "conformer_screening",
            "stages": [
                {"status": "completed", "task": {"engine": "orca"}},
                {"status": "running", "task": {"engine": "orca"}},
            ],
        }
    ) == "running"

    assert _recompute_workflow_status(
        {
            "template_name": "conformer_screening",
            "stages": [
                {"status": "failed", "task": {"engine": "orca"}},
                {"status": "completed", "task": {"engine": "orca"}},
            ],
        }
    ) == "completed"
