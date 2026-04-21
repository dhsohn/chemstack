from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable

from chemstack.core.app_ids import (
    CHEMSTACK_CREST_COMMAND,
    CHEMSTACK_CREST_MODULE,
    CHEMSTACK_XTB_COMMAND,
    CHEMSTACK_XTB_MODULE,
)

from .contracts import WorkflowArtifactRef, WorkflowPlan, WorkflowStage, WorkflowTask, WorkflowTemplateRequest


def new_crest_stage_impl(
    *,
    workflow_id: str,
    template_name: str,
    stage_id: str,
    source_path: str,
    input_role: str,
    mode: str,
    priority: int,
    max_cores: int,
    max_memory_gb: int,
    manifest_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resource_request = {"max_cores": int(max_cores), "max_memory_gb": int(max_memory_gb)}
    config_placeholder = "<crest_auto_config>"
    resolved_overrides = dict(manifest_overrides or {})
    payload_dict: dict[str, Any] = {
        "workflow_id": workflow_id,
        "template_name": template_name,
        "source_input_xyz": source_path,
        "selected_input_xyz": "",
        "job_dir": "",
        "mode": mode,
        "input_role": input_role,
    }
    metadata_dict: dict[str, Any] = {
        "input_role": input_role,
        "mode": mode,
    }
    stage_metadata: dict[str, Any] = {"input_role": input_role, "mode": mode}
    if resolved_overrides:
        payload_dict["job_manifest_overrides"] = resolved_overrides
        metadata_dict["job_manifest_overrides"] = resolved_overrides
        stage_metadata["job_manifest_overrides"] = resolved_overrides
    task = WorkflowTask.from_raw(
        task_id=f"{workflow_id}:{stage_id}",
        engine="crest",
        task_kind="conformer_search",
        resource_request=resource_request,
        payload=payload_dict,
        enqueue_payload={
            "submitter": "crest_auto_cli",
            "app_name": "crest_auto",
            "command": f"{CHEMSTACK_CREST_COMMAND} --config {config_placeholder} run-dir '<job_dir>' --priority {int(priority)}",
            "command_argv": [
                "python",
                "-m",
                CHEMSTACK_CREST_MODULE,
                "--config",
                config_placeholder,
                "run-dir",
                "<job_dir>",
                "--priority",
                str(int(priority)),
            ],
            "requires_config": True,
            "config_argument_placeholder": config_placeholder,
            "job_dir": "",
            "priority": int(priority),
        },
        metadata=metadata_dict,
    )
    stage = WorkflowStage(
        stage_id=stage_id,
        stage_kind="crest_stage",
        status="planned",
        input_artifacts=(WorkflowArtifactRef(kind="input_xyz", path=source_path, selected=True, metadata={"input_role": input_role}),),
        output_artifacts=(),
        task=task,
        metadata=stage_metadata,
    )
    return stage.to_dict()


def new_xtb_stage_impl(
    *,
    workflow_id: str,
    stage_id: str,
    reaction_key: str,
    reactant_input: dict[str, Any],
    product_input: dict[str, Any],
    priority: int,
    max_cores: int,
    max_memory_gb: int,
    max_handoff_retries: int = 2,
    manifest_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resource_request = {"max_cores": int(max_cores), "max_memory_gb": int(max_memory_gb)}
    config_placeholder = "<xtb_auto_config>"
    resolved_overrides = dict(manifest_overrides or {})
    payload_dict: dict[str, Any] = {
        "workflow_id": workflow_id,
        "job_dir": "",
        "reaction_key": reaction_key,
        "reactant_source": dict(reactant_input),
        "product_source": dict(product_input),
        "selected_input_xyz": "",
        "secondary_input_xyz": "",
        "max_handoff_retries": max(0, int(max_handoff_retries)),
    }
    metadata_dict: dict[str, Any] = {
        "reaction_key": reaction_key,
        "max_handoff_retries": max(0, int(max_handoff_retries)),
    }
    stage_metadata: dict[str, Any] = {
        "reaction_key": reaction_key,
        "max_handoff_retries": max(0, int(max_handoff_retries)),
    }
    if resolved_overrides:
        payload_dict["job_manifest_overrides"] = resolved_overrides
        metadata_dict["job_manifest_overrides"] = resolved_overrides
        stage_metadata["job_manifest_overrides"] = resolved_overrides
    task = WorkflowTask.from_raw(
        task_id=f"{workflow_id}:{stage_id}",
        engine="xtb",
        task_kind="path_search",
        resource_request=resource_request,
        payload=payload_dict,
        enqueue_payload={
            "submitter": "xtb_auto_cli",
            "app_name": "xtb_auto",
            "command": f"{CHEMSTACK_XTB_COMMAND} --config {config_placeholder} run-dir '<job_dir>' --priority {int(priority)}",
            "command_argv": [
                "python",
                "-m",
                CHEMSTACK_XTB_MODULE,
                "--config",
                config_placeholder,
                "run-dir",
                "<job_dir>",
                "--priority",
                str(int(priority)),
            ],
            "requires_config": True,
            "config_argument_placeholder": config_placeholder,
            "job_dir": "",
            "priority": int(priority),
            "reaction_key": reaction_key,
        },
        metadata=metadata_dict,
    )
    stage = WorkflowStage(
        stage_id=stage_id,
        stage_kind="xtb_stage",
        status="planned",
        input_artifacts=(
            WorkflowArtifactRef(kind="crest_conformer", path=str(reactant_input["artifact_path"]), selected=True, metadata={"role": "reactant", "source_job_id": reactant_input["source_job_id"]}),
            WorkflowArtifactRef(kind="crest_conformer", path=str(product_input["artifact_path"]), selected=True, metadata={"role": "product", "source_job_id": product_input["source_job_id"]}),
        ),
        output_artifacts=(),
        task=task,
        metadata=stage_metadata,
    )
    return stage.to_dict()


def _copy_input_impl(source: str, target: Path) -> str:
    src = Path(source).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Input XYZ not found: {src}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, target)
    return str(target.resolve())


def _persist_workflow(
    *,
    workflow_root_path: Path,
    workspace_dir: Path,
    workflow_id: str,
    template_name: str,
    source_job_id: str,
    source_job_type: str,
    reaction_key: str,
    requested_at: str,
    request: WorkflowTemplateRequest,
    stages: list[dict[str, Any]],
    write_workflow_payload_fn: Callable[[Path, dict[str, Any]], None],
    sync_workflow_registry_fn: Callable[[Path, Path, dict[str, Any]], None],
) -> dict[str, Any]:
    plan = WorkflowPlan(
        workflow_id=workflow_id,
        template_name=template_name,
        status="planned",
        source_job_id=source_job_id,
        source_job_type=source_job_type,
        reaction_key=reaction_key,
        requested_at=requested_at,
        stages=(),
        metadata={
            "request": request.to_dict(),
            "workspace_dir": str(workspace_dir),
        },
    )
    payload = plan.to_dict()
    payload["stages"] = list(stages)
    write_workflow_payload_fn(workspace_dir, payload)
    sync_workflow_registry_fn(workflow_root_path, workspace_dir, payload)
    return payload


def create_reaction_ts_search_workflow_impl(
    *,
    reactant_xyz: str,
    product_xyz: str,
    workflow_root: str | Path,
    crest_mode: str = "standard",
    priority: int = 10,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    max_crest_candidates: int = 3,
    max_xtb_stages: int = 3,
    max_xtb_handoff_retries: int = 2,
    max_orca_stages: int = 3,
    orca_route_line: str = "! r2scan-3c OptTS Freq TightSCF",
    charge: int = 0,
    multiplicity: int = 1,
    crest_job_manifest: dict[str, Any] | None = None,
    xtb_job_manifest: dict[str, Any] | None = None,
    source_job_id: str = "",
    source_job_type: str = "",
    workflow_id_factory: Callable[[str], str],
    copy_input_fn: Callable[[str, Path], str],
    now_utc_iso_fn: Callable[[], str],
    load_xyz_atom_sequence_fn: Callable[[str], Any],
    new_crest_stage_fn: Callable[..., dict[str, Any]],
    write_workflow_payload_fn: Callable[[Path, dict[str, Any]], None],
    sync_workflow_registry_fn: Callable[[Path, Path, dict[str, Any]], None],
) -> dict[str, Any]:
    workflow_id = workflow_id_factory("wf_reaction_ts")
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    workspace_dir = workflow_root_path / "workflows" / workflow_id
    reactant_sequence = load_xyz_atom_sequence_fn(reactant_xyz)
    product_sequence = load_xyz_atom_sequence_fn(product_xyz)
    if reactant_sequence != product_sequence:
        raise ValueError(
            "reaction_ts_search requires identical reactant/product atom order for xTB path search; "
            f"reactant sequence={list(reactant_sequence)}, product sequence={list(product_sequence)}"
        )
    input_reactant = copy_input_fn(reactant_xyz, workspace_dir / "inputs" / "reactants" / Path(reactant_xyz).name)
    input_product = copy_input_fn(product_xyz, workspace_dir / "inputs" / "products" / Path(product_xyz).name)
    requested_at = now_utc_iso_fn()
    stages: list[dict[str, Any]] = [
        new_crest_stage_fn(
            workflow_id=workflow_id,
            template_name="reaction_ts_search",
            stage_id="crest_reactant_01",
            source_path=input_reactant,
            input_role="reactant",
            mode=crest_mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            manifest_overrides=crest_job_manifest,
        ),
        new_crest_stage_fn(
            workflow_id=workflow_id,
            template_name="reaction_ts_search",
            stage_id="crest_product_01",
            source_path=input_product,
            input_role="product",
            mode=crest_mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            manifest_overrides=crest_job_manifest,
        ),
    ]
    resolved_source_job_type = source_job_type or "raw_xyz"
    reaction_key = f"{Path(input_reactant).stem}_to_{Path(input_product).stem}"
    request = WorkflowTemplateRequest(
        workflow_id=workflow_id,
        template_name="reaction_ts_search",
        source_job_id=source_job_id,
        source_job_type=resolved_source_job_type,
        reaction_key=reaction_key,
        status="planned",
        requested_at=requested_at,
        parameters={
            "crest_mode": crest_mode,
            "priority": int(priority),
            "max_cores": int(max_cores),
            "max_memory_gb": int(max_memory_gb),
            "max_crest_candidates": int(max_crest_candidates),
            "max_xtb_stages": int(max_xtb_stages),
            "max_xtb_handoff_retries": int(max_xtb_handoff_retries),
            "max_orca_stages": int(max_orca_stages),
            "orca_route_line": str(orca_route_line),
            "charge": int(charge),
            "multiplicity": int(multiplicity),
            **({"crest_job_manifest": dict(crest_job_manifest)} if crest_job_manifest else {}),
            **({"xtb_job_manifest": dict(xtb_job_manifest)} if xtb_job_manifest else {}),
        },
        source_artifacts=(
            WorkflowArtifactRef(kind="reactant_xyz", path=input_reactant, selected=True),
            WorkflowArtifactRef(kind="product_xyz", path=input_product, selected=True),
        ),
    )
    return _persist_workflow(
        workflow_root_path=workflow_root_path,
        workspace_dir=workspace_dir,
        workflow_id=workflow_id,
        template_name="reaction_ts_search",
        source_job_id=source_job_id,
        source_job_type=resolved_source_job_type,
        reaction_key=reaction_key,
        requested_at=requested_at,
        request=request,
        stages=stages,
        write_workflow_payload_fn=write_workflow_payload_fn,
        sync_workflow_registry_fn=sync_workflow_registry_fn,
    )


def create_conformer_screening_workflow_impl(
    *,
    input_xyz: str,
    workflow_root: str | Path,
    crest_mode: str = "standard",
    priority: int = 10,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    max_orca_stages: int = 20,
    orca_route_line: str = "! r2scan-3c Opt TightSCF",
    charge: int = 0,
    multiplicity: int = 1,
    crest_job_manifest: dict[str, Any] | None = None,
    workflow_id_factory: Callable[[str], str],
    copy_input_fn: Callable[[str, Path], str],
    now_utc_iso_fn: Callable[[], str],
    new_crest_stage_fn: Callable[..., dict[str, Any]],
    write_workflow_payload_fn: Callable[[Path, dict[str, Any]], None],
    sync_workflow_registry_fn: Callable[[Path, Path, dict[str, Any]], None],
) -> dict[str, Any]:
    workflow_id = workflow_id_factory("wf_conformer_screening")
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    workspace_dir = workflow_root_path / "workflows" / workflow_id
    copied_input = copy_input_fn(input_xyz, workspace_dir / "inputs" / Path(input_xyz).name)
    requested_at = now_utc_iso_fn()
    stages = [
        new_crest_stage_fn(
            workflow_id=workflow_id,
            template_name="conformer_screening",
            stage_id="crest_conformer_01",
            source_path=copied_input,
            input_role="molecule",
            mode=crest_mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            manifest_overrides=crest_job_manifest,
        ),
    ]
    request = WorkflowTemplateRequest(
        workflow_id=workflow_id,
        template_name="conformer_screening",
        source_job_id="",
        source_job_type="raw_xyz",
        reaction_key=Path(copied_input).stem,
        status="planned",
        requested_at=requested_at,
        parameters={
            "crest_mode": crest_mode,
            "priority": int(priority),
            "max_cores": int(max_cores),
            "max_memory_gb": int(max_memory_gb),
            "max_orca_stages": int(max_orca_stages),
            "orca_route_line": str(orca_route_line),
            "charge": int(charge),
            "multiplicity": int(multiplicity),
            **({"crest_job_manifest": dict(crest_job_manifest)} if crest_job_manifest else {}),
        },
        source_artifacts=(WorkflowArtifactRef(kind="input_xyz", path=copied_input, selected=True),),
    )
    return _persist_workflow(
        workflow_root_path=workflow_root_path,
        workspace_dir=workspace_dir,
        workflow_id=workflow_id,
        template_name="conformer_screening",
        source_job_id="",
        source_job_type="raw_xyz",
        reaction_key=request.reaction_key,
        requested_at=requested_at,
        request=request,
        stages=stages,
        write_workflow_payload_fn=write_workflow_payload_fn,
        sync_workflow_registry_fn=sync_workflow_registry_fn,
    )

__all__ = [
    "_copy_input_impl",
    "create_conformer_screening_workflow_impl",
    "create_reaction_ts_search_workflow_impl",
    "new_crest_stage_impl",
    "new_xtb_stage_impl",
]
