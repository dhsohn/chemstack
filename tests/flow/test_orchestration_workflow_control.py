from __future__ import annotations

import sys
from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import orchestration


def _write_xyz_ensemble(path: Path, comments: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for comment in comments:
        lines.extend(
            [
                "2",
                comment,
                "H 0 0 0",
                "H 0 0 0.74",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_xtb_retry_helpers_and_job_writer_materialize_attempt_files(tmp_path: Path) -> None:
    reactant_xyz = tmp_path / "inputs" / "reactant.xyz"
    product_xyz = tmp_path / "inputs" / "product.xyz"
    reactant_xyz.parent.mkdir(parents=True)
    reactant_xyz.write_text("2\nreactant\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    product_xyz.write_text("2\nproduct\nH 0 0 0\nH 0 0 0.80\n", encoding="utf-8")

    stage: dict[str, Any] = {
        "stage_id": "xtb_path_search_01",
        "metadata": {},
        "task": {
            "resource_request": {"max_cores": 12, "max_memory_gb": 36},
            "payload": {
                "reaction_key": "rxn_01",
                "reactant_source": {"artifact_path": str(reactant_xyz)},
                "product_source": {"artifact_path": str(product_xyz)},
                "max_handoff_retries": "3",
            },
            "metadata": {"max_handoff_retries": "5"},
            "enqueue_payload": {},
        },
    }

    assert orchestration._xtb_path_retry_limit(stage) == 3
    assert orchestration._xtb_current_attempt_number(stage) == 0
    assert orchestration._xtb_retry_recipe(1)["recipe_id"] == "path_input_recommended"
    assert orchestration._xtb_retry_recipe(2)["xcontrol_name"] == "path_retry_02.inp"

    job_dir = orchestration._write_xtb_path_job(
        stage,
        xtb_allowed_root=tmp_path / "xtb_allowed",
        workflow_id="wf_01",
        attempt_number=2,
    )

    job_path = Path(job_dir)
    payload = cast(dict[str, Any], stage["task"])["payload"]
    metadata = cast(dict[str, Any], stage["metadata"])
    attempt = cast(list[dict[str, Any]], metadata["xtb_attempts"])[0]

    assert job_path == tmp_path / "xtb_allowed" / "xtb_path_search_01" / "retry_attempt_02"
    assert (job_path / "reactants" / "r1.xyz").exists()
    assert (job_path / "products" / "p1.xyz").exists()
    assert (job_path / "path_retry_02.inp").read_text(encoding="utf-8").startswith("$path")
    assert "namespace: retry_02" in (job_path / "xtb_job.yaml").read_text(encoding="utf-8")
    assert payload["job_dir"] == str(job_path)
    assert payload["selected_input_xyz"] == str((job_path / "reactants" / "r1.xyz"))
    assert payload["secondary_input_xyz"] == str((job_path / "products" / "p1.xyz"))
    assert payload["xtb_active_attempt_number"] == 2
    assert payload["xtb_retry_recipe_id"] == "path_input_refined"
    assert metadata["xtb_active_attempt_number"] == 2
    assert metadata["xtb_retry_recipe_label"] == "refined_path_input"
    assert attempt["attempt_number"] == 2
    assert attempt["recipe_id"] == "path_input_refined"
    assert attempt["job_dir"] == str(job_path)
    assert attempt["namespace"] == "retry_02"

    metadata["xtb_active_attempt_number"] = 4
    assert orchestration._xtb_current_attempt_number(stage) == 4


def test_xtb_job_writer_materializes_ranked_multiframe_inputs(tmp_path: Path) -> None:
    reactant_xyz = tmp_path / "inputs" / "crest_reactant_conformers.xyz"
    product_xyz = tmp_path / "inputs" / "crest_product_conformers.xyz"
    _write_xyz_ensemble(reactant_xyz, ("energy: -3.0", "energy: -2.5", "energy: -2.2"))
    _write_xyz_ensemble(product_xyz, ("energy: -1.0", "energy: -0.8", "energy: -0.6"))

    stage: dict[str, Any] = {
        "stage_id": "xtb_path_search_02",
        "metadata": {},
        "task": {
            "resource_request": {"max_cores": 8, "max_memory_gb": 24},
            "payload": {
                "reaction_key": "rxn_ranked",
                "reactant_source": {
                    "artifact_path": str(reactant_xyz),
                    "rank": 2,
                    "metadata": {"source_frame_index": 2},
                },
                "product_source": {
                    "artifact_path": str(product_xyz),
                    "rank": 3,
                    "metadata": {"source_frame_index": 3},
                },
            },
            "enqueue_payload": {},
        },
    }

    job_dir = orchestration._write_xtb_path_job(
        stage,
        xtb_allowed_root=tmp_path / "xtb_ranked",
        workflow_id="wf_ranked",
        attempt_number=0,
    )

    job_path = Path(job_dir)
    reactant_target = job_path / "reactants" / "r2.xyz"
    product_target = job_path / "products" / "p3.xyz"
    manifest = yaml.safe_load((job_path / "xtb_job.yaml").read_text(encoding="utf-8"))

    assert reactant_target.exists()
    assert product_target.exists()
    assert reactant_target.read_text(encoding="utf-8").splitlines()[1] == "energy: -2.5"
    assert product_target.read_text(encoding="utf-8").splitlines()[1] == "energy: -0.6"
    assert manifest["reactant_xyz"] == "r2.xyz"
    assert manifest["product_xyz"] == "p3.xyz"


def test_job_dir_writers_apply_manifest_overrides(tmp_path: Path) -> None:
    input_xyz = tmp_path / "crest_input.xyz"
    input_xyz.write_text("2\ncrest\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    crest_stage: dict[str, Any] = {
        "stage_id": "crest_conformer_01",
        "task": {
            "resource_request": {"max_cores": 7, "max_memory_gb": 28},
            "payload": {
                "source_input_xyz": str(input_xyz),
                "mode": "standard",
                "job_manifest_overrides": {"speed": "mquick", "solvent_model": "alpb", "solvent": "water"},
            },
            "enqueue_payload": {},
        },
    }
    crest_job_dir = orchestration._ensure_crest_job_dir(
        crest_stage,
        crest_allowed_root=tmp_path / "crest_allowed",
        workflow_id="wf_crest",
    )
    crest_manifest = yaml.safe_load((Path(crest_job_dir) / "crest_job.yaml").read_text(encoding="utf-8"))
    assert crest_manifest == {
        "mode": "standard",
        "speed": "mquick",
        "gfn": 2,
        "solvent_model": "alpb",
        "solvent": "water",
        "resources": {"max_cores": 7, "max_memory_gb": 28},
        "input_xyz": "input.xyz",
    }

    reactant_xyz = tmp_path / "reactant_override.xyz"
    product_xyz = tmp_path / "product_override.xyz"
    xcontrol_file = tmp_path / "path_override.inp"
    reactant_xyz.write_text("2\nreactant\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    product_xyz.write_text("2\nproduct\nH 0 0 0\nH 0 0 0.80\n", encoding="utf-8")
    xcontrol_file.write_text("$path\nnrun=4\n$end\n", encoding="utf-8")
    xtb_stage: dict[str, Any] = {
        "stage_id": "xtb_path_search_01",
        "metadata": {},
        "task": {
            "resource_request": {"max_cores": 9, "max_memory_gb": 30},
            "payload": {
                "reaction_key": "rxn_override",
                "reactant_source": {"artifact_path": str(reactant_xyz)},
                "product_source": {"artifact_path": str(product_xyz)},
                "job_manifest_overrides": {
                    "gfn": 1,
                    "solvent_model": "alpb",
                    "solvent": "water",
                    "namespace": "baseline_ns",
                    "xcontrol_file": str(xcontrol_file),
                },
            },
            "enqueue_payload": {},
        },
    }
    xtb_job_dir = orchestration._write_xtb_path_job(
        xtb_stage,
        xtb_allowed_root=tmp_path / "xtb_allowed_override",
        workflow_id="wf_xtb",
        attempt_number=0,
    )
    xtb_job_path = Path(xtb_job_dir)
    xtb_manifest = yaml.safe_load((xtb_job_path / "xtb_job.yaml").read_text(encoding="utf-8"))
    assert xtb_manifest == {
        "job_type": "path_search",
        "gfn": 1,
        "charge": 0,
        "uhf": 0,
        "solvent_model": "alpb",
        "solvent": "water",
        "resources": {"max_cores": 9, "max_memory_gb": 30},
        "reaction_key": "rxn_override",
        "reactant_xyz": "r1.xyz",
        "product_xyz": "p1.xyz",
        "namespace": "baseline_ns",
        "xcontrol": "workflow_xcontrol.inp",
    }
    assert (xtb_job_path / "workflow_xcontrol.inp").read_text(encoding="utf-8") == "$path\nnrun=4\n$end\n"


def test_advance_workflow_reaction_ts_search_runs_append_sequence_and_sets_child_sync_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_reaction_01",
        "template_name": "reaction_ts_search",
        "status": "planned",
        "stages": [
            {
                "stage_id": "crest_stage_01",
                "status": "completed",
                "task": {"engine": "crest", "status": "completed"},
                "metadata": {},
            }
        ],
        "metadata": {},
    }
    calls: list[tuple[str, str, bool]] = []
    written: list[dict[str, Any]] = []
    synced: list[dict[str, Any]] = []

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T12:00:00+00:00")

    def fake_sync_crest_stage(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append(("crest", str(stage.get("stage_id", "")), bool(kwargs["submit_ready"])))

    def fake_append_reaction_xtb_stages(current_payload: dict[str, Any], **kwargs: object) -> bool:
        calls.append(("append_xtb", str(kwargs["workspace_dir"]), False))
        cast(list[dict[str, Any]], current_payload.setdefault("stages", [])).append(
            {
                "stage_id": "xtb_stage_01",
                "status": "planned",
                "task": {"engine": "xtb", "status": "planned"},
                "metadata": {},
            }
        )
        return True

    def fake_sync_xtb_stage(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append(("xtb", str(stage.get("stage_id", "")), bool(kwargs["submit_ready"])))

    def fake_clear(current_payload: dict[str, Any]) -> None:
        calls.append(("clear_xtb_error", str(current_payload.get("workflow_id", "")), False))

    def fake_append_reaction_orca_stages(current_payload: dict[str, Any], **kwargs: object) -> bool:
        calls.append(("append_orca", str(kwargs["workspace_dir"]), False))
        cast(list[dict[str, Any]], current_payload.setdefault("stages", [])).append(
            {
                "stage_id": "orca_stage_01",
                "status": "planned",
                "task": {"engine": "orca", "status": "planned"},
                "metadata": {},
            }
        )
        return True

    def fake_sync_orca_stage(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append(("orca", str(stage.get("stage_id", "")), bool(kwargs["submit_ready"])))

    monkeypatch.setattr(orchestration, "_sync_crest_stage", fake_sync_crest_stage)
    monkeypatch.setattr(orchestration, "_append_reaction_xtb_stages", fake_append_reaction_xtb_stages)
    monkeypatch.setattr(orchestration, "_sync_xtb_stage", fake_sync_xtb_stage)
    monkeypatch.setattr(orchestration, "_clear_reaction_xtb_handoff_error_if_recovering", fake_clear)
    monkeypatch.setattr(orchestration, "_append_reaction_orca_stages", fake_append_reaction_orca_stages)
    monkeypatch.setattr(orchestration, "_sync_orca_stage", fake_sync_orca_stage)
    monkeypatch.setattr(orchestration, "_recompute_workflow_status", lambda current_payload: "failed")
    monkeypatch.setattr(orchestration, "_workflow_has_active_children", lambda current_payload: True)
    def fake_write_workflow_payload(workspace_dir: Path, current_payload: dict[str, Any]) -> None:
        written.append(deepcopy(current_payload))

    def fake_sync_workflow_registry(workflow_root: Path, workspace_dir: Path, current_payload: dict[str, Any]) -> None:
        synced.append(deepcopy(current_payload))

    monkeypatch.setattr(orchestration, "write_workflow_payload", fake_write_workflow_payload)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", fake_sync_workflow_registry)

    result = orchestration.advance_workflow(
        target="wf_reaction_01",
        workflow_root=tmp_path,
        submit_ready=True,
    )

    assert result["status"] == "failed"
    assert result["metadata"]["last_advanced_at"] == "2026-04-19T12:00:00+00:00"
    assert result["metadata"]["sync_only"] is False
    assert result["metadata"]["final_child_sync_pending"] is True
    assert result["metadata"]["final_child_sync_completed_at"] == ""
    assert [entry[:2] for entry in calls] == [
        ("crest", "crest_stage_01"),
        ("append_xtb", str(tmp_path / "workspace")),
        ("xtb", "crest_stage_01"),
        ("xtb", "xtb_stage_01"),
        ("clear_xtb_error", "wf_reaction_01"),
        ("append_orca", str(tmp_path / "workspace")),
        ("orca", "crest_stage_01"),
        ("orca", "xtb_stage_01"),
        ("orca", "orca_stage_01"),
    ]
    assert {entry[2] for entry in calls if entry[0] in {"crest", "xtb", "orca"}} == {True}
    assert written and written[-1]["metadata"]["final_child_sync_pending"] is True
    assert synced and synced[-1]["metadata"]["sync_only"] is False


def test_advance_workflow_checkpoints_completed_crest_before_xtb_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_reaction_checkpoint",
        "template_name": "reaction_ts_search",
        "status": "planned",
        "stages": [
            {
                "stage_id": "crest_reactant_01",
                "status": "running",
                "task": {"engine": "crest", "status": "running"},
                "metadata": {"input_role": "reactant"},
            },
            {
                "stage_id": "crest_product_01",
                "status": "running",
                "task": {"engine": "crest", "status": "running"},
                "metadata": {"input_role": "product"},
            },
        ],
        "metadata": {},
    }
    writes: list[dict[str, Any]] = []

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-24T06:00:00+00:00")

    def fake_sync_crest_stage(stage: dict[str, Any], **kwargs: object) -> None:
        stage["status"] = "completed"
        cast(dict[str, Any], stage["task"])["status"] = "completed"

    def fake_append_reaction_xtb_stages(current_payload: dict[str, Any], **kwargs: object) -> bool:
        cast(list[dict[str, Any]], current_payload.setdefault("stages", [])).append(
            {
                "stage_id": "xtb_path_search_01",
                "status": "planned",
                "task": {"engine": "xtb", "status": "planned"},
                "metadata": {},
            }
        )
        return True

    monkeypatch.setattr(orchestration, "_sync_crest_stage", fake_sync_crest_stage)
    monkeypatch.setattr(orchestration, "_append_reaction_xtb_stages", fake_append_reaction_xtb_stages)
    monkeypatch.setattr(orchestration, "_sync_xtb_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_clear_reaction_xtb_handoff_error_if_recovering", lambda current_payload: None)
    monkeypatch.setattr(orchestration, "_append_reaction_orca_stages", lambda current_payload, **kwargs: False)
    monkeypatch.setattr(orchestration, "_sync_orca_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_recompute_workflow_status", lambda current_payload: "running")
    monkeypatch.setattr(orchestration, "_workflow_has_active_children", lambda current_payload: False)
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: writes.append(deepcopy(current_payload)))
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    orchestration.advance_workflow(
        target="wf_reaction_checkpoint",
        workflow_root=tmp_path,
        submit_ready=True,
    )

    assert len(writes) >= 3
    first_stage_ids = [stage["stage_id"] for stage in writes[0]["stages"]]
    second_stage_ids = [stage["stage_id"] for stage in writes[1]["stages"]]
    assert first_stage_ids == ["crest_reactant_01", "crest_product_01"]
    assert all(stage["status"] == "completed" for stage in writes[0]["stages"])
    assert second_stage_ids == ["crest_reactant_01", "crest_product_01", "xtb_path_search_01"]
    assert writes[-1]["metadata"]["last_advanced_at"] == "2026-04-24T06:00:00+00:00"


def test_advance_workflow_reaction_ts_search_queues_orca_without_waiting_for_all_xtb_children(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_reaction_incremental",
        "template_name": "reaction_ts_search",
        "status": "running",
        "stages": [
            {
                "stage_id": "xtb_path_search_01",
                "status": "completed",
                "task": {"engine": "xtb", "status": "completed"},
                "metadata": {},
            },
            {
                "stage_id": "xtb_path_search_02",
                "status": "queued",
                "task": {"engine": "xtb", "status": "queued"},
                "metadata": {},
            },
        ],
        "metadata": {},
    }
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-22T09:00:00+00:00")
    monkeypatch.setattr(orchestration, "_sync_crest_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_append_reaction_xtb_stages", lambda current_payload, **kwargs: False)

    def fake_sync_xtb_stage(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append(("sync_xtb", str(stage.get("stage_id", ""))))

    def fake_append_reaction_orca_stages(current_payload: dict[str, Any], **kwargs: object) -> bool:
        xtb_statuses = {
            str(stage.get("stage_id", "")): str(stage.get("status", ""))
            for stage in cast(list[dict[str, Any]], current_payload.get("stages", []))
            if isinstance(stage, dict) and str((stage.get("task") or {}).get("engine", "")) == "xtb"
        }
        assert xtb_statuses == {
            "xtb_path_search_01": "completed",
            "xtb_path_search_02": "queued",
        }
        calls.append(("append_orca", "while_xtb_02_still_queued"))
        cast(list[dict[str, Any]], current_payload.setdefault("stages", [])).append(
            {
                "stage_id": "orca_optts_freq_01",
                "status": "planned",
                "task": {"engine": "orca", "status": "planned"},
                "metadata": {"source_xtb_stage_id": "xtb_path_search_01"},
            }
        )
        return True

    def fake_sync_orca_stage(stage: dict[str, Any], **kwargs: object) -> None:
        calls.append(("sync_orca", str(stage.get("stage_id", ""))))

    monkeypatch.setattr(orchestration, "_sync_xtb_stage", fake_sync_xtb_stage)
    monkeypatch.setattr(orchestration, "_clear_reaction_xtb_handoff_error_if_recovering", lambda current_payload: None)
    monkeypatch.setattr(orchestration, "_append_reaction_orca_stages", fake_append_reaction_orca_stages)
    monkeypatch.setattr(orchestration, "_sync_orca_stage", fake_sync_orca_stage)
    monkeypatch.setattr(orchestration, "_recompute_workflow_status", lambda current_payload: "running")
    monkeypatch.setattr(orchestration, "_workflow_has_active_children", lambda current_payload: True)
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: None)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    result = orchestration.advance_workflow(
        target="wf_reaction_incremental",
        workflow_root=tmp_path,
        submit_ready=True,
    )

    assert result["status"] == "running"
    assert ("append_orca", "while_xtb_02_still_queued") in calls
    assert ("sync_orca", "orca_optts_freq_01") in calls
    assert result["stages"][-1]["stage_id"] == "orca_optts_freq_01"
    assert result["stages"][-1]["metadata"]["source_xtb_stage_id"] == "xtb_path_search_01"


def test_advance_workflow_conformer_screening_queues_twenty_orca_children_after_crest_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_conformer_incremental",
        "template_name": "conformer_screening",
        "status": "running",
        "stages": [
            {
                "stage_id": "crest_conformer_01",
                "status": "completed",
                "task": {"engine": "crest", "status": "completed"},
                "metadata": {},
            }
        ],
        "metadata": {},
    }
    synced_orca_stage_ids: list[str] = []

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-22T11:00:00+00:00")
    monkeypatch.setattr(orchestration, "_sync_crest_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_sync_xtb_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_clear_reaction_xtb_handoff_error_if_recovering", lambda current_payload: None)

    def fake_append_crest_orca_stages(current_payload: dict[str, Any], **kwargs: object) -> bool:
        for index in range(1, 21):
            cast(list[dict[str, Any]], current_payload.setdefault("stages", [])).append(
                {
                    "stage_id": f"orca_conformer_{index:02d}",
                    "status": "planned",
                    "task": {"engine": "orca", "status": "planned"},
                    "metadata": {"source_crest_stage_id": "crest_conformer_01"},
                }
            )
        return True

    def fake_sync_orca_stage(stage: dict[str, Any], **kwargs: object) -> None:
        if str((stage.get("task") or {}).get("engine", "")) == "orca":
            synced_orca_stage_ids.append(str(stage.get("stage_id", "")))

    monkeypatch.setattr(orchestration, "_append_crest_orca_stages", fake_append_crest_orca_stages)
    monkeypatch.setattr(orchestration, "_sync_orca_stage", fake_sync_orca_stage)
    monkeypatch.setattr(orchestration, "_recompute_workflow_status", lambda current_payload: "running")
    monkeypatch.setattr(orchestration, "_workflow_has_active_children", lambda current_payload: True)
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: None)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    result = orchestration.advance_workflow(
        target="wf_conformer_incremental",
        workflow_root=tmp_path,
        submit_ready=True,
    )

    assert result["status"] == "running"
    assert len(synced_orca_stage_ids) == 20
    assert synced_orca_stage_ids[0] == "orca_conformer_01"
    assert synced_orca_stage_ids[-1] == "orca_conformer_20"


def test_advance_workflow_auto_cancels_active_siblings_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_failed_cancel",
        "template_name": "reaction_ts_search",
        "status": "running",
        "stages": [
            {
                "stage_id": "crest_product",
                "status": "failed",
                "task": {"engine": "crest", "status": "failed"},
                "metadata": {},
            },
            {
                "stage_id": "crest_reactant",
                "status": "running",
                "task": {"engine": "crest", "status": "running"},
                "metadata": {"queue_id": "q_reactant"},
            },
            {
                "stage_id": "xtb_pending",
                "status": "planned",
                "task": {"engine": "xtb", "status": "planned"},
                "metadata": {},
            },
        ],
        "metadata": {},
    }
    crest_cancel_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-24T01:00:00+00:00")
    monkeypatch.setattr(orchestration, "_sync_crest_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_append_reaction_xtb_stages", lambda current_payload, **kwargs: False)
    monkeypatch.setattr(orchestration, "_sync_xtb_stage", lambda stage, **kwargs: None)
    monkeypatch.setattr(orchestration, "_clear_reaction_xtb_handoff_error_if_recovering", lambda current_payload: None)
    monkeypatch.setattr(orchestration, "_append_reaction_orca_stages", lambda current_payload, **kwargs: False)
    monkeypatch.setattr(orchestration, "_sync_orca_stage", lambda stage, **kwargs: None)

    def fake_crest_cancel_target(**kwargs: Any) -> dict[str, Any]:
        crest_cancel_calls.append(dict(kwargs))
        return {"status": "cancel_requested", "queue_id": kwargs["target"]}

    monkeypatch.setattr(
        orchestration,
        "crest_cancel_target",
        fake_crest_cancel_target,
    )
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: None)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    result = orchestration.advance_workflow(
        target="wf_failed_cancel",
        workflow_root=tmp_path,
        crest_auto_config="crest.yaml",
        submit_ready=True,
    )

    assert result["status"] == "failed"
    assert crest_cancel_calls == [
        {
            "target": "q_reactant",
            "config_path": "crest.yaml",
            "executable": "crest_auto",
            "repo_root": None,
        }
    ]
    assert result["stages"][1]["status"] == "cancel_requested"
    assert result["stages"][1]["task"]["status"] == "cancel_requested"
    assert result["stages"][1]["task"]["cancel_result"]["status"] == "cancel_requested"
    assert result["stages"][2]["status"] == "cancelled"
    assert result["stages"][2]["task"]["status"] == "cancelled"
    assert result["metadata"]["final_child_sync_pending"] is True
    assert result["metadata"]["final_child_sync_completed_at"] == ""


def test_cancel_materialized_workflow_mixes_local_remote_and_failed_cancellations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_cancel_01",
        "status": "running",
        "stages": [
            {
                "stage_id": "stage_completed",
                "status": "completed",
                "task": {"engine": "crest", "status": "completed"},
            },
            {
                "stage_id": "stage_local",
                "status": "planned",
                "task": {"engine": "crest", "status": "planned"},
            },
            {
                "stage_id": "stage_crest_remote",
                "status": "queued",
                "metadata": {"queue_id": "q_crest"},
                "task": {"engine": "crest", "status": "queued"},
            },
            {
                "stage_id": "stage_xtb_missing_config",
                "status": "running",
                "metadata": {"queue_id": "q_xtb"},
                "task": {"engine": "xtb", "status": "running"},
            },
            {
                "stage_id": "stage_orca_remote",
                "status": "submitted",
                "metadata": {"queue_id": "q_orca"},
                "task": {"engine": "orca", "status": "submitted"},
            },
        ],
    }

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "crest_cancel_target", lambda **kwargs: {"status": "cancel_requested", "queue_id": kwargs["target"]})
    monkeypatch.setattr(orchestration, "orca_cancel_target", lambda **kwargs: {"status": "cancelled", "queue_id": kwargs["target"]})
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: None)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    result = orchestration.cancel_materialized_workflow(
        target="wf_cancel_01",
        workflow_root=tmp_path,
        crest_auto_config="/tmp/crest.yaml",
        orca_auto_config="/tmp/orca.yaml",
    )

    assert result["status"] == "cancel_requested"
    assert result["cancelled"] == [
        {"stage_id": "stage_local", "mode": "local"},
        {"stage_id": "stage_crest_remote", "status": "cancel_requested"},
        {"stage_id": "stage_orca_remote", "status": "cancelled"},
    ]
    assert result["failed"] == [
        {"stage_id": "stage_xtb_missing_config", "reason": "missing_engine_config"},
    ]
    assert payload["stages"][1]["status"] == "cancelled"
    assert payload["stages"][1]["task"]["status"] == "cancelled"
    assert payload["stages"][2]["task"]["cancel_result"]["status"] == "cancel_requested"
    assert payload["stages"][3]["task"]["cancel_result"]["reason"] == "missing_engine_config"
    assert payload["stages"][4]["task"]["cancel_result"]["status"] == "cancelled"


def test_cancel_materialized_workflow_reports_cancelled_when_no_remote_request_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_cancel_02",
        "status": "running",
        "stages": [
            {
                "stage_id": "stage_local",
                "status": "queued",
                "task": {"engine": "crest", "status": "queued"},
            }
        ],
    }

    monkeypatch.setattr(orchestration, "resolve_workflow_workspace", lambda target, workflow_root: tmp_path / "workspace")
    monkeypatch.setattr(orchestration, "acquire_workflow_lock", lambda workspace_dir: nullcontext())
    monkeypatch.setattr(orchestration, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(orchestration, "write_workflow_payload", lambda workspace_dir, current_payload: None)
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, current_payload: None)

    result = orchestration.cancel_materialized_workflow(
        target="wf_cancel_02",
        workflow_root=tmp_path,
    )

    assert result["status"] == "cancelled"
    assert result["cancelled"] == [{"stage_id": "stage_local", "mode": "local"}]
    assert result["failed"] == []


def test_sync_xtb_stage_submits_initial_attempt_and_records_handoff_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = SimpleNamespace(
        status="completed",
        job_id="xtb_job_01",
        reason="ok",
        latest_known_path="/tmp/xtb_done",
        organized_output_dir="/tmp/xtb_outputs/run_01",
        selected_input_xyz="/tmp/xtb_done/reactant.xyz",
        candidate_details=(
            SimpleNamespace(path="/tmp/xtb_done/ts_guess.xyz", selected=True, rank=1, kind="ts_guess", score=-12.3, metadata={"source": "xtb"}),
        ),
        selected_candidate_paths=["/tmp/xtb_done/ts_guess.xyz"],
        analysis_summary={"completed_at": "2026-04-19T00:10:00+00:00"},
    )
    stage: dict[str, Any] = {
        "stage_id": "xtb_path_search_01",
        "status": "planned",
        "metadata": {},
        "task": {
            "engine": "xtb",
            "task_kind": "path_search",
            "status": "planned",
            "payload": {"job_dir": "", "selected_input_xyz": ""},
            "enqueue_payload": {"priority": 7},
        },
    }

    monkeypatch.setattr(orchestration, "sibling_allowed_root", lambda path: tmp_path / "xtb_allowed")
    monkeypatch.setattr(orchestration, "_load_config_root", lambda config_path: tmp_path / "xtb_allowed")
    monkeypatch.setattr(orchestration, "_ensure_xtb_job_dir", lambda stage, **kwargs: str(tmp_path / "xtb_allowed" / "wf_01" / "job_01"))
    monkeypatch.setattr(
        orchestration,
        "submit_xtb_job_dir",
        lambda **kwargs: {"status": "submitted", "queue_id": "q_xtb_01", "job_id": "xtb_job_01"},
    )
    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(
        orchestration,
        "_xtb_handoff_status",
        lambda current_contract: {
            "status": "ready",
            "reason": "",
            "message": "",
            "artifact_path": "/tmp/xtb_done/ts_guess.xyz",
        },
    )
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T14:00:00+00:00")

    orchestration._sync_xtb_stage(
        stage,
        xtb_auto_config="/tmp/xtb.yaml",
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root="/tmp/xtb_repo",
        submit_ready=True,
        workflow_id="wf_01",
        workspace_dir=tmp_path / "workspace" / "wf_01",
    )

    metadata = stage["metadata"]
    task = stage["task"]
    attempt = metadata["xtb_attempts"][0]

    assert stage["status"] == "completed"
    assert task["status"] == "completed"
    assert task["submission_result"]["queue_id"] == "q_xtb_01"
    assert task["submission_result"]["submitted_at"] == "2026-04-19T14:00:00+00:00"
    assert task["payload"]["selected_input_xyz"] == "/tmp/xtb_done/reactant.xyz"
    assert metadata["queue_id"] == "q_xtb_01"
    assert metadata["child_job_id"] == "xtb_job_01"
    assert metadata["reaction_handoff_status"] == "ready"
    assert metadata["reaction_handoff_artifact_path"] == "/tmp/xtb_done/ts_guess.xyz"
    assert metadata["xtb_handoff_retry_limit"] == 2
    assert metadata["xtb_handoff_retries_used"] == 0
    assert attempt["submission_status"] == "submitted"
    assert attempt["queue_id"] == "q_xtb_01"
    assert attempt["status"] == "completed"
    assert attempt["handoff_status"] == "ready"
    assert stage["output_artifacts"] == [
        {
            "kind": "xtb_candidate",
            "path": "/tmp/xtb_done/ts_guess.xyz",
            "selected": True,
            "metadata": {"rank": 1, "kind": "ts_guess", "score": -12.3, "source": "xtb"},
        }
    ]


def test_sync_xtb_stage_retries_failed_handoff_when_retry_budget_remains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = SimpleNamespace(
        status="completed",
        job_id="xtb_job_02",
        reason="ts_missing",
        latest_known_path="/tmp/xtb_done",
        organized_output_dir="/tmp/xtb_outputs/run_02",
        selected_input_xyz="/tmp/xtb_done/reactant.xyz",
        candidate_details=(),
        selected_candidate_paths=[],
        analysis_summary={"completed_at": "2026-04-19T00:20:00+00:00"},
    )
    stage: dict[str, Any] = {
        "stage_id": "xtb_path_search_02",
        "status": "completed",
        "metadata": {"xtb_handoff_retries_used": 0},
        "task": {
            "engine": "xtb",
            "task_kind": "path_search",
            "status": "completed",
            "payload": {"job_dir": "/tmp/original_job", "max_handoff_retries": 2},
            "enqueue_payload": {"priority": 9},
        },
    }
    submissions: list[dict[str, Any]] = []

    monkeypatch.setattr(orchestration, "_load_config_root", lambda config_path: tmp_path / "xtb_allowed")
    monkeypatch.setattr(orchestration, "sibling_allowed_root", lambda path: tmp_path / "xtb_allowed")
    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(
        orchestration,
        "_xtb_handoff_status",
        lambda current_contract: {
            "status": "failed",
            "reason": "xtb_ts_guess_missing",
            "message": "missing ts guess",
            "artifact_path": "",
        },
    )
    monkeypatch.setattr(orchestration, "_write_xtb_path_job", lambda stage, **kwargs: str(tmp_path / "xtb_allowed" / "wf_02" / "retry_attempt_01"))
    def fake_submit_xtb_job_dir(**kwargs: Any) -> dict[str, str]:
        submissions.append(kwargs)
        return {"status": "submitted", "queue_id": "q_retry_01", "job_id": "xtb_job_retry"}

    monkeypatch.setattr(orchestration, "submit_xtb_job_dir", fake_submit_xtb_job_dir)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T14:10:00+00:00")

    orchestration._sync_xtb_stage(
        stage,
        xtb_auto_config="/tmp/xtb.yaml",
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root="/tmp/xtb_repo",
        submit_ready=True,
        workflow_id="wf_02",
        workspace_dir=tmp_path / "workspace" / "wf_02",
    )

    metadata = stage["metadata"]
    retry_attempt = next(item for item in cast(list[dict[str, Any]], metadata["xtb_attempts"]) if item["attempt_number"] == 1)

    assert submissions and submissions[0]["job_dir"].endswith("retry_attempt_01")
    assert stage["status"] == "queued"
    assert stage["task"]["status"] == "submitted"
    assert stage["task"]["submission_result"]["queue_id"] == "q_retry_01"
    assert metadata["queue_id"] == "q_retry_01"
    assert metadata["xtb_handoff_status"] == "retrying"
    assert metadata["reaction_handoff_status"] == "retrying"
    assert metadata["xtb_handoff_retries_used"] == 1
    assert metadata["xtb_handoff_retry_limit"] == 2
    assert retry_attempt["submission_status"] == "submitted"
    assert retry_attempt["trigger_reason"] == "xtb_ts_guess_missing"
    assert retry_attempt["trigger_message"] == "missing ts guess"


def test_sync_xtb_stage_stops_retrying_after_limit_and_materializes_empty_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = SimpleNamespace(
        status="failed",
        job_id="xtb_job_03",
        reason="ts_missing",
        latest_known_path="/tmp/xtb_failed",
        organized_output_dir="/tmp/xtb_outputs/run_03",
        selected_input_xyz="/tmp/xtb_failed/reactant.xyz",
        candidate_details=(),
        selected_candidate_paths=[],
        analysis_summary={"completed_at": "2026-04-19T00:30:00+00:00"},
    )
    stage: dict[str, Any] = {
        "stage_id": "xtb_path_search_03",
        "status": "failed",
        "metadata": {"xtb_handoff_retries_used": 2},
        "task": {
            "engine": "xtb",
            "task_kind": "path_search",
            "status": "failed",
            "payload": {"job_dir": "/tmp/original_job", "max_handoff_retries": 2},
            "enqueue_payload": {"priority": 9},
        },
    }

    monkeypatch.setattr(orchestration, "_load_config_root", lambda config_path: tmp_path / "xtb_allowed")
    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(
        orchestration,
        "_xtb_handoff_status",
        lambda current_contract: {
            "status": "failed",
            "reason": "xtb_ts_guess_missing",
            "message": "missing ts guess",
            "artifact_path": "",
        },
    )
    monkeypatch.setattr(
        orchestration,
        "submit_xtb_job_dir",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not resubmit once retry limit is exhausted")),
    )

    orchestration._sync_xtb_stage(
        stage,
        xtb_auto_config="/tmp/xtb.yaml",
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root="/tmp/xtb_repo",
        submit_ready=True,
        workflow_id="wf_03",
        workspace_dir=tmp_path / "workspace" / "wf_03",
    )

    metadata = stage["metadata"]
    assert stage["status"] == "failed"
    assert stage["task"]["status"] == "failed"
    assert metadata["reaction_handoff_status"] == "failed"
    assert metadata["reaction_handoff_reason"] == "xtb_ts_guess_missing"
    assert metadata["xtb_handoff_retries_used"] == 2
    assert metadata["xtb_handoff_retry_limit"] == 2
    assert stage["output_artifacts"] == []
