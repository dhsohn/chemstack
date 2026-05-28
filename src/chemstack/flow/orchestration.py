from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.utils import (
    normalize_text,
    now_utc_iso,
    timestamped_token,
)

from ._orchestration_advance import (
    advance_workflow,
    cancel_materialized_workflow,
)
from . import _orchestration_builders as _stage_builders
from . import orchestration_factories as _workflow_factories
from .orchestration_factories import WorkflowFactoryDeps
from ._orchestration_requests import (
    ConformerScreeningWorkflowRequest,
    ReactionTsSearchWorkflowRequest,
)
from .registry import sync_workflow_registry
from .state import write_workflow_payload
from .xyz_utils import load_xyz_atom_sequence


def _workflow_factory_deps() -> WorkflowFactoryDeps:
    return WorkflowFactoryDeps(
        normalize_text=normalize_text,
        workflow_id_factory=timestamped_token,
        copy_input_fn=_stage_builders._copy_input_impl,
        now_utc_iso_fn=now_utc_iso,
        new_crest_stage_fn=_stage_builders.new_crest_stage_impl,
        write_workflow_payload_fn=write_workflow_payload,
        sync_workflow_registry_fn=sync_workflow_registry,
        load_xyz_atom_sequence_fn=load_xyz_atom_sequence,
    )


def create_reaction_ts_search_workflow_from_request(
    request: ReactionTsSearchWorkflowRequest,
) -> dict[str, Any]:
    return _workflow_factories.create_reaction_ts_search_workflow_from_request(
        request,
        deps=_workflow_factory_deps(),
    )


def create_conformer_screening_workflow_from_request(
    request: ConformerScreeningWorkflowRequest,
) -> dict[str, Any]:
    return _workflow_factories.create_conformer_screening_workflow_from_request(
        request,
        deps=_workflow_factory_deps(),
    )


def create_reaction_ts_search_workflow(
    *,
    reactant_xyz: str,
    product_xyz: str,
    workflow_root: str | Path,
    workflow_id: str | None = None,
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
    endpoint_pairing: dict[str, Any] | None = None,
    source_job_id: str = "",
    source_job_type: str = "",
) -> dict[str, Any]:
    return create_reaction_ts_search_workflow_from_request(
        ReactionTsSearchWorkflowRequest(
            reactant_xyz=reactant_xyz,
            product_xyz=product_xyz,
            workflow_root=workflow_root,
            workflow_id=workflow_id,
            crest_mode=crest_mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            max_crest_candidates=max_crest_candidates,
            max_xtb_stages=max_xtb_stages,
            max_xtb_handoff_retries=max_xtb_handoff_retries,
            max_orca_stages=max_orca_stages,
            orca_route_line=orca_route_line,
            charge=charge,
            multiplicity=multiplicity,
            crest_job_manifest=crest_job_manifest,
            xtb_job_manifest=xtb_job_manifest,
            endpoint_pairing=endpoint_pairing,
            source_job_id=source_job_id,
            source_job_type=source_job_type,
        )
    )


def create_conformer_screening_workflow(
    *,
    input_xyz: str,
    workflow_root: str | Path,
    workflow_id: str | None = None,
    crest_mode: str = "standard",
    priority: int = 10,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    max_orca_stages: int = 20,
    orca_route_line: str = "! r2scan-3c Opt TightSCF",
    charge: int = 0,
    multiplicity: int = 1,
    crest_job_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return create_conformer_screening_workflow_from_request(
        ConformerScreeningWorkflowRequest(
            input_xyz=input_xyz,
            workflow_root=workflow_root,
            workflow_id=workflow_id,
            crest_mode=crest_mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            max_orca_stages=max_orca_stages,
            orca_route_line=orca_route_line,
            charge=charge,
            multiplicity=multiplicity,
            crest_job_manifest=crest_job_manifest,
        )
    )


__all__ = [
    "ConformerScreeningWorkflowRequest",
    "ReactionTsSearchWorkflowRequest",
    "advance_workflow",
    "cancel_materialized_workflow",
    "create_conformer_screening_workflow",
    "create_conformer_screening_workflow_from_request",
    "create_reaction_ts_search_workflow",
    "create_reaction_ts_search_workflow_from_request",
    "load_xyz_atom_sequence",
]
