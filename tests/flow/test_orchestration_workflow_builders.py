from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from chemstack.flow._orchestration_requests import (
    ReactionTsSearchWorkflowRequest,
    WorkflowCreationContext,
    WorkflowPersistenceContext,
)
from chemstack.flow._orchestration_workflow_builders import (
    _ReactionWorkflowInputs,
    _WorkflowWorkspace,
    _copy_reaction_inputs,
    _merge_manifest_defaults,
    _persist_workflow,
    _workflow_workspace,
)
from chemstack.flow.contracts import (
    WorkflowArtifactRef,
    WorkflowStagePayload,
    WorkflowTemplateRequest,
)


def _workflow_context(
    *,
    copy_input_fn: Any | None = None,
    write_workflow_payload_fn: Any | None = None,
    sync_workflow_registry_fn: Any | None = None,
) -> WorkflowCreationContext:
    return WorkflowCreationContext(
        workflow_id_factory=lambda prefix: f"{prefix}_generated",
        copy_input_fn=copy_input_fn or (lambda source, target: str(target)),
        now_utc_iso_fn=lambda: "2026-05-29T00:00:00+00:00",
        new_crest_stage_fn=lambda **_kwargs: cast(Any, {}),
        write_workflow_payload_fn=write_workflow_payload_fn
        or (lambda _workspace_dir, _payload: None),
        sync_workflow_registry_fn=sync_workflow_registry_fn
        or (lambda _root, _workspace_dir, _payload: None),
    )


def test_merge_manifest_defaults_trims_keys_and_removes_blank_overrides() -> None:
    assert _merge_manifest_defaults(
        {"rthr": 0.3, "keep": "yes"},
        {
            " rthr ": 0.5,
            "keep": "",
            "drop": None,
            "   ": "ignored",
        },
    ) == {"rthr": 0.5}


def test_workflow_workspace_generates_id_and_requested_at(tmp_path: Path) -> None:
    workspace = _workflow_workspace(
        workflow_id="   ",
        workflow_root=tmp_path / "workflows",
        default_id_prefix="wf_demo",
        context=_workflow_context(),
    )

    assert workspace.workflow_id == "wf_demo_generated"
    assert workspace.workflow_root_path == (tmp_path / "workflows").resolve()
    assert workspace.workspace_dir == (tmp_path / "workflows" / "wf_demo_generated").resolve()
    assert workspace.requested_at == "2026-05-29T00:00:00+00:00"


def test_copy_reaction_inputs_uses_role_directories_and_reaction_key(tmp_path: Path) -> None:
    copied: list[tuple[str, Path]] = []

    def copy_input(source: str, target: Path) -> str:
        copied.append((source, target))
        return str(target)

    workspace = _WorkflowWorkspace(
        workflow_id="wf_rxn",
        workflow_root_path=tmp_path,
        workspace_dir=tmp_path / "wf_rxn",
        requested_at="2026-05-29T00:00:00+00:00",
    )

    inputs = _copy_reaction_inputs(
        ReactionTsSearchWorkflowRequest(
            reactant_xyz="/inputs/reactant.xyz",
            product_xyz="/inputs/product.xyz",
            workflow_root=tmp_path,
        ),
        workspace,
        _workflow_context(copy_input_fn=copy_input),
    )

    assert isinstance(inputs, _ReactionWorkflowInputs)
    assert inputs.reaction_key == "reactant_to_product"
    assert copied == [
        ("/inputs/reactant.xyz", tmp_path / "wf_rxn" / "inputs" / "reactants" / "reactant.xyz"),
        ("/inputs/product.xyz", tmp_path / "wf_rxn" / "inputs" / "products" / "product.xyz"),
    ]


def test_persist_workflow_writes_payload_and_syncs_registry(tmp_path: Path) -> None:
    writes: list[tuple[Path, dict[str, Any]]] = []
    syncs: list[tuple[Path, Path, dict[str, Any]]] = []

    def write_payload(workspace_dir: Path, payload: dict[str, Any]) -> None:
        writes.append((workspace_dir, payload))

    def sync_registry(root: Path, workspace_dir: Path, payload: dict[str, Any]) -> None:
        syncs.append((root, workspace_dir, payload))

    workspace_dir = tmp_path / "workflows" / "wf_1"
    request = WorkflowTemplateRequest(
        workflow_id="wf_1",
        template_name="conformer_screening",
        source_job_id="",
        source_job_type="raw_xyz",
        reaction_key="mol",
        status="planned",
        requested_at="2026-05-29T00:00:00+00:00",
        parameters={"crest_mode": "standard"},
        source_artifacts=(WorkflowArtifactRef(kind="input_xyz", path="/inputs/mol.xyz"),),
    )
    stages = cast(list[WorkflowStagePayload], [{"stage_id": "crest_conformer_01"}])

    payload = _persist_workflow(
        persistence_context=WorkflowPersistenceContext(
            workflow_root_path=tmp_path / "workflows",
            workspace_dir=workspace_dir,
            workflow_id="wf_1",
            template_name="conformer_screening",
            source_job_id="",
            source_job_type="raw_xyz",
            reaction_key="mol",
            requested_at="2026-05-29T00:00:00+00:00",
        ),
        request=request,
        stages=stages,
        creation_context=_workflow_context(
            write_workflow_payload_fn=write_payload,
            sync_workflow_registry_fn=sync_registry,
        ),
    )

    assert payload["metadata"]["request"]["parameters"] == {"crest_mode": "standard"}
    assert payload["metadata"]["workspace_dir"] == str(workspace_dir)
    assert payload["stages"] == stages
    assert writes == [(workspace_dir, cast(dict[str, Any], payload))]
    assert syncs == [
        (tmp_path / "workflows", workspace_dir, cast(dict[str, Any], payload)),
    ]
