from __future__ import annotations

import sys
from collections import UserDict
from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import orchestration
from chemstack.flow.contracts import WorkflowStageInput


class _FakeMaterializedStage:
    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def to_dict(self) -> dict[str, object]:
        return dict(self._data)


def _candidate(path: str, *, rank: int = 1, kind: str = "ts_guess", metadata: dict[str, object] | None = None) -> WorkflowStageInput:
    return WorkflowStageInput(
        source_job_id="job_01",
        source_job_type="path_search",
        reaction_key="rxn_01",
        selected_input_xyz=path,
        rank=rank,
        kind=kind,
        artifact_path=path,
        selected=True,
        score=float(rank),
        metadata=metadata or {},
    )


def test_misc_helper_edges_cover_missing_inputs_and_non_dict_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(FileNotFoundError, match="Input XYZ not found"):
        orchestration._copy_input(str(tmp_path / "missing.xyz"), tmp_path / "out.xyz")

    monkeypatch.setattr(orchestration, "workflow_has_active_downstream", lambda payload: False)
    assert orchestration._workflow_has_active_children({"stages": ["skip", {"status": "completed", "task": "bad"}]}) is False
    assert orchestration._latest_child_stage_summary([]) == {}

    terminal = orchestration._downstream_terminal_result(
        {},
        {"status": "completed", "stage_summaries": ["skip", {"completed_at": "2026-04-19T00:00:00+00:00"}]},
    )
    assert terminal["status"] == "completed"
    assert terminal["completed_at"] == "2026-04-19T00:00:00+00:00"


def test_create_reaction_ts_search_standard_and_barrier_sequence_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes: list[dict[str, object]] = []
    syncs: list[dict[str, object]] = []

    monkeypatch.setattr(orchestration, "_workflow_id", lambda prefix: f"{prefix}_001")
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T00:00:00+00:00")
    monkeypatch.setattr(
        orchestration,
        "load_xyz_atom_sequence",
        lambda path: ("H", "H") if Path(path).name != "ts_guess.xyz" else ("H", "O"),
    )
    monkeypatch.setattr(orchestration, "_copy_input", lambda source, target: str(target))
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, payload: writes.append(deepcopy(payload)))
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, payload: syncs.append(deepcopy(payload)))

    payload = orchestration.create_reaction_ts_search_workflow(
        reactant_xyz=str(tmp_path / "reactant.xyz"),
        product_xyz=str(tmp_path / "product.xyz"),
        workflow_root=tmp_path,
        crest_mode="standard",
    )

    assert [stage["stage_id"] for stage in payload["stages"]] == ["crest_reactant_01", "crest_product_01"]
    assert writes and syncs

def test_metadata_payload_and_retry_helper_edges() -> None:
    stage: dict[str, object] = {}
    task: dict[str, object] = {}

    assert orchestration._stage_metadata(stage) == {}
    assert stage["metadata"] == {}
    assert orchestration._task_payload_dict(task) == {}
    assert task["payload"] == {}
    assert orchestration._xtb_retry_recipe(0)["recipe_id"] == "baseline"
    assert orchestration._xtb_path_retry_limit({"task": None}) == 2


def test_helper_edges_cover_invalid_candidates_and_non_recoverable_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert orchestration._stage_has_xtb_candidates({"output_artifacts": None}) is False
    assert orchestration._stage_has_xtb_candidates({"output_artifacts": ["skip", {"kind": "other", "path": "/tmp/other.xyz"}]}) is False
    assert orchestration._stage_failure_is_recoverable({"status": "failed", "task": None}) is False

    monkeypatch.setattr(orchestration, "choose_orca_geometry_frame", lambda path, candidate_kind: ("", {"selection_reason": "invalid_or_empty_xyz"}))
    error = orchestration._reaction_ts_guess_error(
        SimpleNamespace(candidate_details=(SimpleNamespace(kind="ts_guess", path="/tmp/xtbpath_ts.xyz", rank=1),))
    )
    assert error == {
        "reason": "xtb_ts_guess_invalid",
        "message": "xTB produced xtbpath_ts.xyz but it is empty or not a valid XYZ geometry; refusing ORCA handoff.",
    }

    assert orchestration._reaction_orca_allows_next_candidate({"status": "completed"}) is False

    payload: dict[str, Any] = {
        "metadata": {"workflow_error": {"scope": "reaction_ts_search_xtb_handoff"}},
        "stages": ["skip"],
    }
    orchestration._clear_reaction_xtb_handoff_error_if_recovering(payload)
    assert payload["metadata"]["workflow_error"]["scope"] == "reaction_ts_search_xtb_handoff"


def test_sync_xtb_stage_returns_early_without_target_or_on_contract_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_target_stage = {
        "status": "planned",
        "metadata": {},
        "task": {
            "engine": "xtb",
            "task_kind": "path_search",
            "status": "completed",
            "payload": {},
            "enqueue_payload": {},
        },
    }

    load_calls: list[dict[str, Any]] = []

    def fake_load_xtb_artifact_contract(**kwargs: Any) -> None:
        load_calls.append(kwargs)
        return None

    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", fake_load_xtb_artifact_contract)
    orchestration._sync_xtb_stage(
        no_target_stage,
        xtb_auto_config=None,
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root=None,
        submit_ready=False,
        workflow_id="wf_01",
    )
    assert load_calls == []

    failing_stage = {
        "status": "completed",
        "metadata": {},
        "task": {
            "engine": "xtb",
            "task_kind": "path_search",
            "status": "completed",
            "payload": {"job_dir": "/tmp/xtb_job"},
            "enqueue_payload": {},
        },
    }

    monkeypatch.setattr(
        orchestration,
        "load_xtb_artifact_contract",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    orchestration._sync_xtb_stage(
        failing_stage,
        xtb_auto_config=None,
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root=None,
        submit_ready=False,
        workflow_id="wf_01",
    )
    assert failing_stage["status"] == "completed"


def test_completed_contract_helpers_cover_missing_targets_and_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "stages": [
            "skip",
            {"status": "completed", "task": {"engine": "xtb"}},
            {"status": "completed", "task": {"engine": "crest"}, "metadata": {"input_role": "reactant"}},
        ]
    }
    assert set(orchestration._completed_crest_roles(payload).keys()) == {"reactant"}

    assert orchestration._completed_crest_stage({"task": None}, crest_auto_config=None) is None
    assert orchestration._completed_crest_stage({"task": {"payload": {}}, "metadata": {}}, crest_auto_config=None) is None

    monkeypatch.setattr(
        orchestration,
        "load_crest_artifact_contract",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("bad crest")),
    )
    assert (
        orchestration._completed_crest_stage(
            {"task": {"payload": {"job_dir": "/tmp/crest_job"}}, "metadata": {}},
            crest_auto_config=None,
        )
        is None
    )

    assert orchestration._completed_orca_stage({"task": None}, orca_auto_config=None) is None
    assert (
        orchestration._completed_orca_stage(
            {"task": {"payload": {}, "enqueue_payload": {}}, "metadata": {}},
            orca_auto_config=None,
        )
        is None
    )

    monkeypatch.setattr(
        orchestration,
        "load_orca_artifact_contract",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("bad orca")),
    )
    assert (
        orchestration._completed_orca_stage(
            {"task": {"payload": {"reaction_dir": "/tmp/rxn"}, "enqueue_payload": {}}, "metadata": {}},
            orca_auto_config=None,
        )
        is None
    )


def test_append_reaction_xtb_stages_false_branches_and_zero_created(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert orchestration._append_reaction_xtb_stages(
        {"stages": [{"task": {"engine": "xtb"}}]},
        workspace_dir=tmp_path,
        crest_auto_config=None,
    ) is False

    monkeypatch.setattr(orchestration, "_completed_crest_roles", lambda payload: {"reactant": {}})
    assert orchestration._append_reaction_xtb_stages(
        {"stages": [], "metadata": {}, "workflow_id": "wf_xtb", "reaction_key": "rxn"},
        workspace_dir=tmp_path,
        crest_auto_config=None,
    ) is False

    monkeypatch.setattr(orchestration, "_completed_crest_roles", lambda payload: {"reactant": {}, "product": {}})
    monkeypatch.setattr(
        orchestration,
        "_completed_crest_stage",
        lambda stage, **kwargs: None if stage == {} else SimpleNamespace(),
    )
    payload: dict[str, Any] = {"stages": [], "metadata": {}, "workflow_id": "wf_xtb", "reaction_key": "rxn"}
    assert orchestration._append_reaction_xtb_stages(payload, workspace_dir=tmp_path, crest_auto_config=None) is False

    monkeypatch.setattr(orchestration, "_completed_crest_stage", lambda stage, **kwargs: SimpleNamespace())
    monkeypatch.setattr(orchestration, "select_crest_downstream_inputs", lambda contract, policy: ())
    assert orchestration._append_reaction_xtb_stages(payload, workspace_dir=tmp_path, crest_auto_config=None) is False


def test_append_reaction_orca_stages_covers_skip_and_first_candidate_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        orchestration,
        "_load_config_root",
        lambda config: tmp_path / ("xtb_allowed" if config == "xtb.yaml" else "orca_allowed"),
    )

    def fake_load_xtb_artifact_contract(*, target: str, **kwargs: object) -> SimpleNamespace:
        if target == "/tmp/broken_job":
            raise RuntimeError("broken")
        return SimpleNamespace(job_id=f"job:{Path(target).name}", job_type="path_search", candidate_details=())

    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", fake_load_xtb_artifact_contract)

    def fake_select_xtb_downstream_inputs(contract: SimpleNamespace, policy: object, require_geometry: bool) -> tuple[WorkflowStageInput, ...]:
        if contract.job_id == "job:ok_job":
            return (
                _candidate("", rank=1),
                _candidate("/tmp/candidate_a.xyz", rank=2),
                _candidate("/tmp/candidate_a.xyz", rank=3),
                _candidate("/tmp/candidate_b.xyz", rank=4),
            )
        return ()

    monkeypatch.setattr(orchestration, "select_xtb_downstream_inputs", fake_select_xtb_downstream_inputs)
    monkeypatch.setattr(
        orchestration,
        "build_materialized_orca_stage",
        lambda **kwargs: _FakeMaterializedStage(
            {
                "stage_id": kwargs["stage_id"],
                "status": "planned",
                "metadata": {},
                "task": {"engine": "orca", "status": "planned", "metadata": {"source_candidate_path": kwargs["candidate"].artifact_path}},
            }
        ),
    )

    payload: dict[str, Any] = {
        "workflow_id": "wf_orca",
        "status": "running",
        "stages": [
            {
                "stage_id": "userdict_task",
                "status": "completed",
                "task": UserDict({"engine": "xtb"}),
                "metadata": {},
            },
            {
                "stage_id": "missing_target",
                "status": "completed",
                "task": {"engine": "xtb", "payload": {}},
                "metadata": {},
            },
            {
                "stage_id": "broken_lookup",
                "status": "completed",
                "task": {"engine": "xtb", "payload": {"job_dir": "/tmp/broken_job"}},
                "metadata": {},
            },
            {
                "stage_id": "ok_lookup",
                "status": "completed",
                "task": {"engine": "xtb", "payload": {"job_dir": "/tmp/ok_job"}},
                "metadata": {},
            },
        ],
        "metadata": {
            "workflow_error": {"scope": "reaction_ts_search_xtb_handoff"},
            "request": {"parameters": {"max_orca_stages": 2}},
        },
    }

    created = orchestration._append_reaction_orca_stages(
        payload,
        workspace_dir=tmp_path,
        xtb_auto_config="xtb.yaml",
        orca_auto_config="orca.yaml",
    )

    assert created is True
    assert "workflow_error" not in payload["metadata"]
    appended = payload["stages"][-1]
    assert appended["stage_id"] == "orca_optts_freq_01"
    assert appended["metadata"]["reaction_candidate_pool_size"] == 2
    assert appended["metadata"]["reaction_remaining_candidates_after_this"] == 1
    assert payload["stages"][3]["metadata"]["reaction_handoff_status"] == "ready"


def test_append_reaction_orca_stages_false_and_exhaustion_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert orchestration._append_reaction_orca_stages(
        {"stages": [], "metadata": {}},
        workspace_dir=tmp_path,
        xtb_auto_config="xtb.yaml",
        orca_auto_config="orca.yaml",
    ) is False

    base_xtb_stage = {
        "stage_id": "xtb_01",
        "status": "completed",
        "task": {"engine": "xtb", "payload": {"job_dir": "/tmp/xtb_job"}},
        "metadata": {},
    }
    payload: dict[str, Any] = {"stages": [deepcopy(base_xtb_stage)], "metadata": {"request": {"parameters": {}}}}
    monkeypatch.setattr(orchestration, "_load_config_root", lambda config: None if config == "xtb.yaml" else tmp_path / "orca")
    assert orchestration._append_reaction_orca_stages(payload, workspace_dir=tmp_path, xtb_auto_config="xtb.yaml", orca_auto_config="orca.yaml") is False

    monkeypatch.setattr(orchestration, "_load_config_root", lambda config: None if config == "orca.yaml" else tmp_path / "xtb")
    assert orchestration._append_reaction_orca_stages(payload, workspace_dir=tmp_path, xtb_auto_config="xtb.yaml", orca_auto_config="orca.yaml") is False

    monkeypatch.setattr(orchestration, "_load_config_root", lambda config: tmp_path / ("xtb" if config == "xtb.yaml" else "orca"))
    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", lambda **kwargs: SimpleNamespace(job_id="xtb_job", job_type="path_search", candidate_details=()))
    monkeypatch.setattr(orchestration, "select_xtb_downstream_inputs", lambda contract, policy, require_geometry: (_candidate("/tmp/already.xyz"),))

    active_payload = {
        "stages": [
            deepcopy(base_xtb_stage),
            {"status": "queued", "task": {"engine": "orca", "metadata": {"source_candidate_path": "/tmp/already.xyz"}}, "metadata": {}},
        ],
        "metadata": {"request": {"parameters": {}}},
    }
    assert orchestration._append_reaction_orca_stages(active_payload, workspace_dir=tmp_path, xtb_auto_config="xtb.yaml", orca_auto_config="orca.yaml") is False

    completed_payload = {
        "stages": [
            deepcopy(base_xtb_stage),
            {"status": "completed", "task": {"engine": "orca", "metadata": {"source_candidate_path": "/tmp/already.xyz"}}, "metadata": {}},
        ],
        "metadata": {"request": {"parameters": {}}},
    }
    assert orchestration._append_reaction_orca_stages(completed_payload, workspace_dir=tmp_path, xtb_auto_config="xtb.yaml", orca_auto_config="orca.yaml") is False

    blocked_payload = {
        "stages": [
            deepcopy(base_xtb_stage),
            {"stage_id": "orca_failed", "status": "failed", "task": {"engine": "orca", "metadata": {"source_candidate_path": "/tmp/other.xyz"}}, "metadata": {}},
        ],
        "metadata": {"request": {"parameters": {}}},
    }
    monkeypatch.setattr(orchestration, "_reaction_orca_allows_next_candidate", lambda stage: False)
    assert orchestration._append_reaction_orca_stages(blocked_payload, workspace_dir=tmp_path, xtb_auto_config="xtb.yaml", orca_auto_config="orca.yaml") is False

    exhausted_payload: dict[str, Any] = {
        "stages": [
            deepcopy(base_xtb_stage),
            {
                "stage_id": "orca_failed",
                "status": "failed",
                "task": {"engine": "orca", "metadata": {"source_candidate_path": "/tmp/already.xyz"}},
                "metadata": {},
            },
        ],
        "metadata": {"request": {"parameters": {}}},
    }
    monkeypatch.setattr(orchestration, "_reaction_orca_allows_next_candidate", lambda stage: True)
    created = orchestration._append_reaction_orca_stages(
        exhausted_payload,
        workspace_dir=tmp_path,
        xtb_auto_config="xtb.yaml",
        orca_auto_config="orca.yaml",
    )
    assert created is False
    exhausted_stages = cast(list[dict[str, Any]], exhausted_payload["stages"])
    exhausted_metadata = cast(dict[str, Any], exhausted_payload["metadata"])
    assert exhausted_stages[1]["metadata"]["reaction_candidate_status"] == "exhausted"
    assert exhausted_metadata["workflow_error"]["scope"] == "reaction_ts_search_orca_candidate_exhausted"


def test_append_crest_orca_stage_false_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {"stages": [{"task": {"engine": "orca"}}], "metadata": {}}
    assert (
        orchestration._append_crest_orca_stages(
            payload,
            template_name="conformer_screening",
            crest_auto_config=None,
            orca_auto_config=None,
            stage_id_prefix="orca_conformer",
            xyz_filename="guess.xyz",
            inp_filename="guess.inp",
        )
        is False
    )

    payload = {"stages": [], "metadata": {}}
    assert (
        orchestration._append_crest_orca_stages(
            payload,
            template_name="conformer_screening",
            crest_auto_config=None,
            orca_auto_config=None,
            stage_id_prefix="orca_conformer",
            xyz_filename="guess.xyz",
            inp_filename="guess.inp",
        )
        is False
    )

    payload = {"stages": [{"status": "completed", "task": {"engine": "crest"}}], "metadata": {}}
    monkeypatch.setattr(orchestration, "_completed_crest_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_load_config_root", lambda config: tmp_path / "orca")
    assert (
        orchestration._append_crest_orca_stages(
            payload,
            template_name="conformer_screening",
            crest_auto_config="crest.yaml",
            orca_auto_config="orca.yaml",
            stage_id_prefix="orca_conformer",
            xyz_filename="guess.xyz",
            inp_filename="guess.inp",
        )
        is False
    )

    monkeypatch.setattr(orchestration, "_completed_crest_stage", lambda stage, **kwargs: SimpleNamespace())
    monkeypatch.setattr(orchestration, "_load_config_root", lambda config: None)
    assert (
        orchestration._append_crest_orca_stages(
            payload,
            template_name="conformer_screening",
            crest_auto_config="crest.yaml",
            orca_auto_config="orca.yaml",
            stage_id_prefix="orca_conformer",
            xyz_filename="guess.xyz",
            inp_filename="guess.inp",
        )
        is False
    )


def test_recompute_workflow_status_covers_cancelled_and_cancel_requested_edges() -> None:
    assert orchestration._recompute_workflow_status({"status": "cancelled", "stages": []}) == "cancelled"
    assert orchestration._recompute_workflow_status({"status": "planned", "stages": [{"status": "cancel_requested", "task": {"engine": "crest"}}]}) == "cancel_requested"
    assert (
        orchestration._recompute_workflow_status(
            {
                "status": "planned",
                "stages": [
                    {"status": "cancelled", "task": {"engine": "crest"}},
                    {"status": "planned", "task": {"engine": "crest"}},
                ],
            }
        )
        == "cancelled"
    )


@pytest.mark.parametrize(
    ("template_name", "expected_call"),
    [
        ("conformer_screening", "append_crest_orca"),
    ],
)
def test_advance_workflow_routes_template_specific_appenders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    template_name: str,
    expected_call: str,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_template",
        "template_name": template_name,
        "status": "planned",
        "stages": ["skip", {"stage_id": "stage_01", "status": "planned", "task": {"engine": "crest", "status": "planned"}, "metadata": {}}],
        "metadata": {},
    }
    calls: list[str] = []

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: deepcopy(payload))
    monkeypatch.setattr(orchestration, "_workflow_sync_only", lambda current_payload: False)
    def record_sync_crest(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append("sync_crest")

    def record_sync_xtb(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append("sync_xtb")

    def record_clear(current_payload: dict[str, Any]) -> None:
        calls.append("clear")

    def record_sync_orca(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append("sync_orca")

    def record_append_crest_orca(*args: object, **kwargs: object) -> bool:
        calls.append("append_crest_orca")
        return False

    monkeypatch.setattr(orchestration, "_sync_crest_stage", record_sync_crest)
    monkeypatch.setattr(orchestration, "_sync_xtb_stage", record_sync_xtb)
    monkeypatch.setattr(orchestration, "_clear_reaction_xtb_handoff_error_if_recovering", record_clear)
    monkeypatch.setattr(orchestration, "_sync_orca_stage", record_sync_orca)
    monkeypatch.setattr(orchestration, "_append_crest_orca_stages", record_append_crest_orca)
    monkeypatch.setattr(orchestration, "_recompute_workflow_status", lambda current_payload: "planned")
    monkeypatch.setattr(orchestration, "_workflow_has_active_children", lambda current_payload: False)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T03:00:00+00:00")
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: None)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    orchestration.advance_workflow(target="wf_template", workflow_root=tmp_path, submit_ready=True)

    assert expected_call in calls


def test_cancel_materialized_workflow_skips_invalid_rows_and_uses_xtb_remote_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_cancel_xtb",
        "status": "running",
        "stages": [
            "skip",
            {"stage_id": "missing_task", "status": "running", "task": None},
            {
                "stage_id": "xtb_remote",
                "status": "submitted",
                "metadata": {"queue_id": "q_xtb"},
                "task": {"engine": "xtb", "status": "submitted"},
            },
        ],
    }

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "xtb_cancel_target", lambda **kwargs: {"status": "cancel_requested", "queue_id": kwargs["target"]})
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: None)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    result = orchestration.cancel_materialized_workflow(
        target="wf_cancel_xtb",
        workflow_root=tmp_path,
        xtb_auto_config="xtb.yaml",
    )

    assert result["status"] == "cancel_requested"
    assert result["cancelled"] == [{"stage_id": "xtb_remote", "status": "cancel_requested"}]
