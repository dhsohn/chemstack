from __future__ import annotations

from pathlib import Path
from typing import Any

from orca_auto.cli_common import _dependency, _normalize_workflow_type
from orca_auto.core.utils.coercion import normalize_text

from .manifest import (
    FLOW_MANIFEST_FILENAMES as WORKFLOW_MANIFEST_FILENAMES,
)
from .manifest import (
    load_flow_manifest as _shared_load_flow_manifest,
)
from .manifest import (
    manifest_mapping as _shared_manifest_mapping,
)
from .manifest import (
    resolve_endpoint_pairing_manifest as _shared_resolve_endpoint_pairing_manifest,
)
from .manifest import (
    resolve_engine_manifest as _shared_resolve_engine_manifest,
)
from .run_dir_layout import (
    STANDARD_CONFORMER_INPUT_FILENAME,
    STANDARD_REACTION_PRODUCT_FILENAME,
    STANDARD_REACTION_REACTANT_FILENAME,
    inspect_workflow_run_dir,
)
from .run_dir_options import RunDirManifestSections, RunDirWorkflowConfig


def _load_run_dir_manifest(workflow_dir: Path, *, deps: Any | None = None) -> dict[str, Any]:
    manifest_filenames = _dependency(
        deps, "WORKFLOW_MANIFEST_FILENAMES", WORKFLOW_MANIFEST_FILENAMES
    )
    return _shared_load_flow_manifest(
        workflow_dir,
        filenames=tuple(manifest_filenames),
        description="Run directory manifest",
    )


def _resolve_run_dir_path(
    workflow_dir: Path,
    *,
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    default_names: tuple[str, ...],
    deps: Any | None = None,
) -> str:
    normalize = _dependency(deps, "_normalize_text", normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    candidate_text = normalize(explicit)
    if not candidate_text:
        candidate_text = normalize(manifest.get(key))
    if candidate_text:
        candidate = path_cls(candidate_text).expanduser()
        if not candidate.is_absolute():
            candidate = workflow_dir / candidate
        return str(candidate.resolve())

    for name in default_names:
        candidate = workflow_dir / name
        if candidate.exists():
            return str(candidate.resolve())
    return ""


def _resolve_run_dir_workflow_type(
    args: Any, manifest: dict[str, Any], workflow_layout: Any, *, deps: Any | None = None
) -> str:
    normalize = _dependency(deps, "_normalize_text", normalize_text)
    normalize_workflow_type = _dependency(
        deps, "_normalize_workflow_type", _normalize_workflow_type
    )

    workflow_type_text = normalize(getattr(args, "workflow_type", None))
    if not workflow_type_text:
        workflow_type_text = normalize(manifest.get("workflow_type"))
    if workflow_type_text:
        return normalize_workflow_type(workflow_type_text)
    if workflow_layout.is_ambiguous:
        raise ValueError(
            "Ambiguous workflow_dir: found both reaction inputs and conformer input. "
            "Pass --workflow-type to choose one."
        )
    inferred_workflow_type = workflow_layout.inferred_workflow_type
    if inferred_workflow_type:
        return inferred_workflow_type
    raise ValueError(
        "Could not infer workflow type from workflow_dir. "
        "Expected reactant.xyz + product.xyz or input.xyz."
    )


def _resolve_run_dir_manifest_sections(
    workflow_dir: Path, manifest: dict[str, Any], *, deps: Any | None = None
) -> RunDirManifestSections:
    del deps
    xtb_manifest = _shared_resolve_engine_manifest(workflow_dir, manifest, "xtb")
    return RunDirManifestSections(
        resources=_shared_manifest_mapping(manifest.get("resources")),
        crest=_shared_resolve_engine_manifest(workflow_dir, manifest, "crest"),
        xtb=xtb_manifest,
        endpoint_pairing=_shared_resolve_endpoint_pairing_manifest(manifest, xtb_manifest),
        orca=_shared_resolve_engine_manifest(workflow_dir, manifest, "orca"),
    )


def _load_run_dir_workflow_config(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> RunDirWorkflowConfig:
    inspect_run_dir = _dependency(deps, "inspect_workflow_run_dir", inspect_workflow_run_dir)
    load_run_dir_manifest = _dependency(deps, "_load_run_dir_manifest", _load_run_dir_manifest)
    resolve_run_dir_manifest_sections = _dependency(
        deps, "_resolve_run_dir_manifest_sections", _resolve_run_dir_manifest_sections
    )
    resolve_run_dir_path = _dependency(deps, "_resolve_run_dir_path", _resolve_run_dir_path)
    resolve_run_dir_workflow_type = _dependency(
        deps, "_resolve_run_dir_workflow_type", _resolve_run_dir_workflow_type
    )
    reaction_reactant_filename = _dependency(
        deps, "STANDARD_REACTION_REACTANT_FILENAME", STANDARD_REACTION_REACTANT_FILENAME
    )
    reaction_product_filename = _dependency(
        deps, "STANDARD_REACTION_PRODUCT_FILENAME", STANDARD_REACTION_PRODUCT_FILENAME
    )
    conformer_input_filename = _dependency(
        deps, "STANDARD_CONFORMER_INPUT_FILENAME", STANDARD_CONFORMER_INPUT_FILENAME
    )

    workflow_layout = inspect_run_dir(workflow_dir)
    if not workflow_layout.has_manifest:
        raise ValueError("workflow run-dir requires flow.yaml in workflow_dir.")

    manifest = load_run_dir_manifest(workflow_dir)
    sections = resolve_run_dir_manifest_sections(workflow_dir, manifest)
    return RunDirWorkflowConfig(
        workflow_dir=workflow_dir,
        manifest=manifest,
        sections=sections,
        reactant_xyz=resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "reactant_xyz", None),
            manifest=manifest,
            key="reactant_xyz",
            default_names=(reaction_reactant_filename,),
        ),
        product_xyz=resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "product_xyz", None),
            manifest=manifest,
            key="product_xyz",
            default_names=(reaction_product_filename,),
        ),
        input_xyz=resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "input_xyz", None),
            manifest=manifest,
            key="input_xyz",
            default_names=(conformer_input_filename,),
        ),
        workflow_type=resolve_run_dir_workflow_type(args, manifest, workflow_layout),
    )


__all__ = [
    "WORKFLOW_MANIFEST_FILENAMES",
]
