from __future__ import annotations

from chemstack.flow.orchestration.stage_builders import (
    new_crest_stage_impl,
    new_xtb_stage_impl,
)


def test_new_crest_stage_applies_manifest_overrides_to_payload_and_metadata() -> None:
    stage = new_crest_stage_impl(
        workflow_id="wf-1",
        template_name="reaction_ts_search",
        stage_id="crest_reactant_01",
        source_path="/tmp/reactant.xyz",
        input_role="reactant",
        mode="standard",
        priority=3,
        max_cores=8,
        max_memory_gb=32,
        manifest_overrides={"rthr": 0.3},
    )

    task = stage["task"]
    assert task["payload"]["job_manifest_overrides"] == {"rthr": 0.3}
    assert task["metadata"]["job_manifest_overrides"] == {"rthr": 0.3}
    assert stage["metadata"]["job_manifest_overrides"] == {"rthr": 0.3}
    assert task["enqueue_payload"]["config_argument_placeholder"] == "<crest_config>"


def test_new_xtb_stage_builds_handoff_payload_and_clamps_retry_limit() -> None:
    stage = new_xtb_stage_impl(
        workflow_id="wf-1",
        stage_id="xtb_path_search_01",
        reaction_key="rxn-1",
        reactant_input={
            "artifact_path": "/tmp/reactant_conformer.xyz",
            "source_job_id": "crest-reactant",
        },
        product_input={
            "artifact_path": "/tmp/product_conformer.xyz",
            "source_job_id": "crest-product",
        },
        priority=5,
        max_cores=4,
        max_memory_gb=16,
        max_handoff_retries=-10,
        manifest_overrides={"gfn": 1},
    )

    task = stage["task"]
    assert stage["stage_kind"] == "xtb_stage"
    assert task["engine"] == "xtb"
    assert task["payload"]["max_handoff_retries"] == 0
    assert task["metadata"]["max_handoff_retries"] == 0
    assert task["enqueue_payload"]["reaction_key"] == "rxn-1"
    assert task["enqueue_payload"]["config_argument_placeholder"] == "<xtb_config>"
    assert task["payload"]["job_manifest_overrides"] == {"gfn": 1}
    assert [artifact["metadata"]["role"] for artifact in stage["input_artifacts"]] == [
        "reactant",
        "product",
    ]
