from __future__ import annotations

from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .requests import ConformerScreeningWorkflowRequest, ReactionTsSearchWorkflowRequest

_EXPORTS: dict[str, tuple[str, str]] = {
    "ConformerScreeningWorkflowRequest": (
        ".requests",
        "ConformerScreeningWorkflowRequest",
    ),
    "ReactionTsSearchWorkflowRequest": (".requests", "ReactionTsSearchWorkflowRequest"),
    "WorkflowFactoryDeps": (".factories", "WorkflowFactoryDeps"),
    "advance_workflow": (".advance", "advance_workflow"),
    "cancel_materialized_workflow": (".advance", "cancel_materialized_workflow"),
    "load_xyz_atom_sequence": ("chemstack.flow.xyz_utils", "load_xyz_atom_sequence"),
    "normalize_text": ("chemstack.core.utils", "normalize_text"),
    "now_utc_iso": ("chemstack.core.utils", "now_utc_iso"),
    "sync_workflow_registry": ("chemstack.flow.registry", "sync_workflow_registry"),
    "timestamped_token": ("chemstack.core.utils", "timestamped_token"),
    "write_workflow_payload": ("chemstack.flow.state", "write_workflow_payload"),
}

_SUBMODULES: dict[str, str] = {
    "advance": ".advance",
    "advance_phases": ".advance_phases",
    "builders": ".builders",
    "crest_orca_materialization": ".crest_orca_materialization",
    "dep_builder_core": ".dep_builder_core",
    "dep_builder_fallbacks": ".dep_builder_fallbacks",
    "dep_builder_factories": ".dep_builder_factories",
    "dep_builders": ".dep_builders",
    "deps": ".deps",
    "factories": ".factories",
    "lifecycle": ".lifecycle",
    "materialization": ".materialization",
    "reaction_materialization": ".reaction_materialization",
    "reaction_orca_materialization": ".reaction_orca_materialization",
    "requests": ".requests",
    "stage_builders": ".stage_builders",
    "stage_runtime": ".stage_runtime",
    "stage_view_mutators": ".stage_view_mutators",
    "stage_views": ".stage_views",
    "steps": ".steps",
    "support": ".support",
    "template_builders": ".template_builders",
    "workflow_builders": ".workflow_builders",
    "workflow_cancellation": ".workflow_cancellation",
}

_MISSING = object()


def _import_from(module_name: str, attr_name: str) -> Any:
    module = import_module(module_name, __name__)
    return getattr(module, attr_name)


def _load_export(name: str) -> Any:
    value = _import_from(*_EXPORTS[name])
    globals()[name] = value
    return value


def _load_submodule(name: str) -> ModuleType:
    module = import_module(_SUBMODULES[name], __name__)
    globals()[name] = module
    return module


def _resolve(name: str) -> Any:
    value = globals().get(name, _MISSING)
    if value is _MISSING:
        return __getattr__(name)
    return value


def _workflow_factory_deps() -> Any:
    factory_deps_type = _resolve("WorkflowFactoryDeps")
    workflow_builders = _load_submodule("workflow_builders")
    stage_builders = _load_submodule("stage_builders")
    return factory_deps_type(
        normalize_text=_resolve("normalize_text"),
        workflow_id_factory=_resolve("timestamped_token"),
        copy_input_fn=workflow_builders._copy_input_impl,
        now_utc_iso_fn=_resolve("now_utc_iso"),
        new_crest_stage_fn=stage_builders.new_crest_stage_impl,
        write_workflow_payload_fn=_resolve("write_workflow_payload"),
        sync_workflow_registry_fn=_resolve("sync_workflow_registry"),
        load_xyz_atom_sequence_fn=_resolve("load_xyz_atom_sequence"),
    )


def create_reaction_ts_search_workflow_from_request(
    request: ReactionTsSearchWorkflowRequest,
) -> dict[str, Any]:
    factory = _import_from(
        ".factories",
        "create_reaction_ts_search_workflow_from_request",
    )
    return factory(request, deps=_workflow_factory_deps())


def create_conformer_screening_workflow_from_request(
    request: ConformerScreeningWorkflowRequest,
) -> dict[str, Any]:
    factory = _import_from(
        ".factories",
        "create_conformer_screening_workflow_from_request",
    )
    return factory(request, deps=_workflow_factory_deps())


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
    request_type = _resolve("ReactionTsSearchWorkflowRequest")
    return create_reaction_ts_search_workflow_from_request(
        request_type(
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
    request_type = _resolve("ConformerScreeningWorkflowRequest")
    return create_conformer_screening_workflow_from_request(
        request_type(
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


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        return _load_export(name)
    if name in _SUBMODULES:
        return _load_submodule(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS) | set(_SUBMODULES))


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
