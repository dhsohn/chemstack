from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .cli_common import (
    _dependency,
    _normalize_text,
    _normalize_workflow_type,
)
from . import cli_workflow_output as _workflow_output
from .operations import create_conformer_screening_workflow, create_reaction_workflow
from .restart import restart_failed_workflow
from .run_dir_layout import (
    STANDARD_CONFORMER_INPUT_FILENAME,
    STANDARD_REACTION_PRODUCT_FILENAME,
    STANDARD_REACTION_REACTANT_FILENAME,
    WORKFLOW_MANIFEST_FILENAMES,
    inspect_workflow_run_dir,
)


def _load_run_dir_manifest(workflow_dir: Path, *, deps: Any | None = None) -> dict[str, Any]:
    manifest_filenames = _dependency(
        deps, "WORKFLOW_MANIFEST_FILENAMES", WORKFLOW_MANIFEST_FILENAMES
    )
    for name in manifest_filenames:
        candidate = workflow_dir / name
        if not candidate.exists():
            continue
        if candidate.suffix == ".json":
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        else:
            import yaml  # type: ignore[import-untyped]

            payload = yaml.safe_load(candidate.read_text(encoding="utf-8"))
        if payload is None:
            return {}
        if not isinstance(payload, dict):
            raise ValueError(f"Run directory manifest must contain a mapping: {candidate}")
        return dict(payload)
    return {}


def _manifest_mapping(value: Any, *, deps: Any | None = None) -> dict[str, Any]:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if normalize_text(key)}


def _resolve_manifest_file_value(workflow_dir: Path, value: Any, *, deps: Any | None = None) -> str:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    text = normalize_text(value)
    if not text:
        return ""
    candidate = path_cls(text).expanduser()
    if not candidate.is_absolute():
        candidate = workflow_dir / candidate
    return str(candidate.resolve())


def _resolve_engine_manifest(
    workflow_dir: Path, manifest: dict[str, Any], key: str, *, deps: Any | None = None
) -> dict[str, Any]:
    manifest_mapping = _dependency(deps, "_manifest_mapping", _manifest_mapping)
    resolve_manifest_file_value = _dependency(
        deps, "_resolve_manifest_file_value", _resolve_manifest_file_value
    )

    section = manifest_mapping(manifest.get(key))
    if not section:
        return {}
    resolved = dict(section)
    if "xcontrol_file" in resolved:
        resolved["xcontrol_file"] = resolve_manifest_file_value(
            workflow_dir, resolved.get("xcontrol_file")
        )
    return resolved


def _resolve_endpoint_pairing_manifest(
    manifest: dict[str, Any],
    xtb_manifest: dict[str, Any],
    *,
    deps: Any | None = None,
) -> dict[str, Any]:
    manifest_mapping = _dependency(deps, "_manifest_mapping", _manifest_mapping)

    xtb_section = manifest_mapping(xtb_manifest.pop("endpoint_pairing", None))
    top_level = manifest_mapping(manifest.get("endpoint_pairing"))
    resolved = dict(xtb_section)
    resolved.update(top_level)
    return resolved


def _resolve_run_dir_path(
    workflow_dir: Path,
    *,
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    default_names: tuple[str, ...],
    deps: Any | None = None,
) -> str:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    candidate_text = normalize_text(explicit)
    if not candidate_text:
        candidate_text = normalize_text(manifest.get(key))
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


def _resolve_text_option_with_section(
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    section: dict[str, Any],
    section_key: str,
    default: str,
    *,
    deps: Any | None = None,
) -> str:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)

    explicit_text = normalize_text(explicit)
    if explicit_text:
        return explicit_text
    manifest_text = normalize_text(manifest.get(key))
    if manifest_text:
        return manifest_text
    section_text = normalize_text(section.get(section_key))
    if section_text:
        return section_text
    return default


def _resolve_int_option(
    explicit: Any, manifest: dict[str, Any], key: str, default: int, *, deps: Any | None = None
) -> int:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)

    if explicit is not None:
        return int(explicit)
    manifest_value = manifest.get(key)
    if manifest_value is None or normalize_text(manifest_value) == "":
        return default
    return int(manifest_value)


def _resolve_int_option_with_section(
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    section: dict[str, Any],
    section_key: str,
    default: int,
    *,
    deps: Any | None = None,
) -> int:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)

    if explicit is not None:
        return int(explicit)
    manifest_value = manifest.get(key)
    if manifest_value is not None and normalize_text(manifest_value) != "":
        return int(manifest_value)
    section_value = section.get(section_key)
    if section_value is None or normalize_text(section_value) == "":
        return default
    return int(section_value)


def _resolve_required_workflow_root(
    args: Any, manifest: dict[str, Any], *, deps: Any | None = None
) -> str:
    discover_workflow_root = _dependency(deps, "_discover_workflow_root", None)
    if discover_workflow_root is None:
        from .cli_common import _discover_workflow_root

        discover_workflow_root = _discover_workflow_root

    resolved_workflow_root = discover_workflow_root(
        getattr(args, "workflow_root", None) or manifest.get("workflow_root")
    )
    if not resolved_workflow_root:
        raise ValueError("workflow_root is not configured. Set workflow.root in chemstack.yaml.")
    return resolved_workflow_root


def _safe_workflow_name(value: Any, *, fallback: str, deps: Any | None = None) -> str:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)

    cleaned = "".join(
        ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in normalize_text(value)
    )
    cleaned = cleaned.strip("._-").lower()
    return cleaned or fallback


def _preferred_run_dir_workflow_id(
    workflow_dir: Path, *, workflow_type: str, deps: Any | None = None
) -> str:
    safe_workflow_name = _dependency(deps, "_safe_workflow_name", _safe_workflow_name)

    stem = safe_workflow_name(workflow_dir.name, fallback="workflow")
    prefix = "wf_reaction_ts" if workflow_type == "reaction_ts_search" else "wf_conformer_screening"
    if stem.startswith(prefix):
        return stem
    return f"{prefix}_{stem}"


def _unique_run_dir_workflow_id(
    workflow_dir: Path,
    *,
    workflow_root: str | Path,
    workflow_type: str,
    deps: Any | None = None,
) -> str:
    path_cls = _dependency(deps, "Path", Path)
    preferred_run_dir_workflow_id = _dependency(
        deps, "_preferred_run_dir_workflow_id", _preferred_run_dir_workflow_id
    )

    workflow_root_path = path_cls(workflow_root).expanduser().resolve()
    if workflow_dir.parent == workflow_root_path and not (workflow_dir / "workflow.json").exists():
        return workflow_dir.name

    preferred = preferred_run_dir_workflow_id(workflow_dir, workflow_type=workflow_type)
    candidate = preferred
    suffix = 2
    while (workflow_root_path / candidate).exists():
        candidate = f"{preferred}_{suffix:02d}"
        suffix += 1
    return candidate


def _resolve_run_dir_common_workflow_kwargs(
    args: Any,
    manifest: dict[str, Any],
    *,
    resources_manifest: dict[str, Any],
    crest_manifest: dict[str, Any],
    orca_manifest: dict[str, Any],
    default_orca_route_line: str,
    default_max_orca_stages: int,
    deps: Any | None = None,
) -> dict[str, Any]:
    resolve_run_dir_workflow_options = _dependency(
        deps, "_resolve_run_dir_workflow_options", _resolve_run_dir_workflow_options
    )
    workflow_options_to_common_kwargs = _dependency(
        deps, "_workflow_options_to_common_kwargs", _workflow_options_to_common_kwargs
    )
    sections = _RunDirManifestSections(
        resources=resources_manifest,
        crest=crest_manifest,
        xtb={},
        endpoint_pairing={},
        orca=orca_manifest,
    )
    options = resolve_run_dir_workflow_options(
        args,
        manifest,
        sections,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
    )
    return workflow_options_to_common_kwargs(options)


_print_created_workflow = _workflow_output.emit_created_workflow


def _workflow_root_for_existing_run_dir(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> Path:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    raw_root = normalize_text(getattr(args, "workflow_root", None))
    if raw_root:
        return path_cls(raw_root).expanduser().resolve()
    return workflow_dir.parent


_print_restarted_workflow = _workflow_output.emit_restarted_workflow


@dataclass(frozen=True)
class _RunDirManifestSections:
    resources: dict[str, Any]
    crest: dict[str, Any]
    xtb: dict[str, Any]
    endpoint_pairing: dict[str, Any]
    orca: dict[str, Any]


@dataclass(frozen=True)
class _RunDirWorkflowOptions:
    workflow_root: str
    crest_mode: str
    priority: int
    max_cores: int
    max_memory_gb: int
    max_orca_stages: int
    orca_route_line: str
    charge: int
    multiplicity: int
    max_crest_candidates: int
    max_xtb_stages: int


@dataclass(frozen=True)
class _RunDirWorkflowConfig:
    workflow_dir: Path
    manifest: dict[str, Any]
    sections: _RunDirManifestSections
    reactant_xyz: str
    product_xyz: str
    input_xyz: str
    workflow_type: str

    @property
    def resources_manifest(self) -> dict[str, Any]:
        return self.sections.resources

    @property
    def crest_manifest(self) -> dict[str, Any]:
        return self.sections.crest

    @property
    def xtb_manifest(self) -> dict[str, Any]:
        return self.sections.xtb

    @property
    def endpoint_pairing(self) -> dict[str, Any]:
        return self.sections.endpoint_pairing

    @property
    def orca_manifest(self) -> dict[str, Any]:
        return self.sections.orca


def _resolve_run_dir_workflow_type(
    args: Any, manifest: dict[str, Any], workflow_layout: Any, *, deps: Any | None = None
) -> str:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    normalize_workflow_type = _dependency(
        deps, "_normalize_workflow_type", _normalize_workflow_type
    )

    workflow_type_text = normalize_text(getattr(args, "workflow_type", None))
    if not workflow_type_text:
        workflow_type_text = normalize_text(manifest.get("workflow_type"))
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
) -> _RunDirManifestSections:
    manifest_mapping = _dependency(deps, "_manifest_mapping", _manifest_mapping)
    resolve_engine_manifest = _dependency(
        deps, "_resolve_engine_manifest", _resolve_engine_manifest
    )
    resolve_endpoint_pairing_manifest = _dependency(
        deps, "_resolve_endpoint_pairing_manifest", _resolve_endpoint_pairing_manifest
    )

    xtb_manifest = resolve_engine_manifest(workflow_dir, manifest, "xtb")
    return _RunDirManifestSections(
        resources=manifest_mapping(manifest.get("resources")),
        crest=resolve_engine_manifest(workflow_dir, manifest, "crest"),
        xtb=xtb_manifest,
        endpoint_pairing=resolve_endpoint_pairing_manifest(manifest, xtb_manifest),
        orca=resolve_engine_manifest(workflow_dir, manifest, "orca"),
    )


def _resolve_run_dir_workflow_options(
    args: Any,
    manifest: dict[str, Any],
    sections: _RunDirManifestSections,
    *,
    default_orca_route_line: str,
    default_max_orca_stages: int,
    default_max_crest_candidates: int = 3,
    default_max_xtb_stages: int = 3,
    workflow_root: str | None = None,
    deps: Any | None = None,
) -> _RunDirWorkflowOptions:
    resolve_required_workflow_root = _dependency(
        deps, "_resolve_required_workflow_root", _resolve_required_workflow_root
    )
    resolve_text_option_with_section = _dependency(
        deps, "_resolve_text_option_with_section", _resolve_text_option_with_section
    )
    resolve_int_option = _dependency(deps, "_resolve_int_option", _resolve_int_option)
    resolve_int_option_with_section = _dependency(
        deps, "_resolve_int_option_with_section", _resolve_int_option_with_section
    )

    return _RunDirWorkflowOptions(
        workflow_root=workflow_root or resolve_required_workflow_root(args, manifest),
        crest_mode=resolve_text_option_with_section(
            getattr(args, "crest_mode", None),
            manifest,
            "crest_mode",
            sections.crest,
            "mode",
            "standard",
        ),
        priority=resolve_int_option(getattr(args, "priority", None), manifest, "priority", 10),
        max_cores=resolve_int_option_with_section(
            getattr(args, "max_cores", None),
            manifest,
            "max_cores",
            sections.resources,
            "max_cores",
            8,
        ),
        max_memory_gb=resolve_int_option_with_section(
            getattr(args, "max_memory_gb", None),
            manifest,
            "max_memory_gb",
            sections.resources,
            "max_memory_gb",
            32,
        ),
        max_orca_stages=resolve_int_option(
            getattr(args, "max_orca_stages", None),
            manifest,
            "max_orca_stages",
            default_max_orca_stages,
        ),
        orca_route_line=resolve_text_option_with_section(
            getattr(args, "orca_route_line", None),
            manifest,
            "orca_route_line",
            sections.orca,
            "route_line",
            default_orca_route_line,
        ),
        charge=resolve_int_option_with_section(
            getattr(args, "charge", None), manifest, "charge", sections.orca, "charge", 0
        ),
        multiplicity=resolve_int_option_with_section(
            getattr(args, "multiplicity", None),
            manifest,
            "multiplicity",
            sections.orca,
            "multiplicity",
            1,
        ),
        max_crest_candidates=resolve_int_option(
            getattr(args, "max_crest_candidates", None),
            manifest,
            "max_crest_candidates",
            default_max_crest_candidates,
        ),
        max_xtb_stages=resolve_int_option(
            getattr(args, "max_xtb_stages", None),
            manifest,
            "max_xtb_stages",
            default_max_xtb_stages,
        ),
    )


def _workflow_options_to_common_kwargs(options: _RunDirWorkflowOptions) -> dict[str, Any]:
    return {
        "workflow_root": options.workflow_root,
        "crest_mode": options.crest_mode,
        "priority": options.priority,
        "max_cores": options.max_cores,
        "max_memory_gb": options.max_memory_gb,
        "max_orca_stages": options.max_orca_stages,
        "orca_route_line": options.orca_route_line,
        "charge": options.charge,
        "multiplicity": options.multiplicity,
    }


def _load_run_dir_workflow_config(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> _RunDirWorkflowConfig:
    inspect_run_dir = _dependency(deps, "inspect_workflow_run_dir", inspect_workflow_run_dir)
    load_run_dir_manifest = _dependency(deps, "_load_run_dir_manifest", _load_run_dir_manifest)
    resolve_run_dir_manifest_sections = _dependency(
        deps, "_resolve_run_dir_manifest_sections", _resolve_run_dir_manifest_sections
    )
    resolve_run_dir_path = _dependency(deps, "_resolve_run_dir_path", _resolve_run_dir_path)
    resolve_run_dir_workflow_type = _dependency(
        deps, "_resolve_run_dir_workflow_type", _resolve_run_dir_workflow_type
    )

    workflow_layout = inspect_run_dir(workflow_dir)
    if not workflow_layout.has_manifest:
        raise ValueError("workflow run-dir requires flow.yaml in workflow_dir.")

    manifest = load_run_dir_manifest(workflow_dir)
    sections = resolve_run_dir_manifest_sections(workflow_dir, manifest)
    return _RunDirWorkflowConfig(
        workflow_dir=workflow_dir,
        manifest=manifest,
        sections=sections,
        reactant_xyz=resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "reactant_xyz", None),
            manifest=manifest,
            key="reactant_xyz",
            default_names=(STANDARD_REACTION_REACTANT_FILENAME,),
        ),
        product_xyz=resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "product_xyz", None),
            manifest=manifest,
            key="product_xyz",
            default_names=(STANDARD_REACTION_PRODUCT_FILENAME,),
        ),
        input_xyz=resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "input_xyz", None),
            manifest=manifest,
            key="input_xyz",
            default_names=(STANDARD_CONFORMER_INPUT_FILENAME,),
        ),
        workflow_type=resolve_run_dir_workflow_type(args, manifest, workflow_layout),
    )


def _run_dir_workflow_id(
    config: _RunDirWorkflowConfig, workflow_root: str, *, deps: Any | None = None
) -> str:
    unique_run_dir_workflow_id = _dependency(
        deps, "_unique_run_dir_workflow_id", _unique_run_dir_workflow_id
    )
    return unique_run_dir_workflow_id(
        config.workflow_dir,
        workflow_root=workflow_root,
        workflow_type=config.workflow_type,
    )


def _common_run_dir_workflow_kwargs(
    args: Any,
    config: _RunDirWorkflowConfig,
    *,
    workflow_root: str,
    default_orca_route_line: str,
    default_max_orca_stages: int,
    deps: Any | None = None,
) -> dict[str, Any]:
    resolve_run_dir_workflow_options = _dependency(
        deps, "_resolve_run_dir_workflow_options", _resolve_run_dir_workflow_options
    )
    workflow_options_to_common_kwargs = _dependency(
        deps, "_workflow_options_to_common_kwargs", _workflow_options_to_common_kwargs
    )

    options = resolve_run_dir_workflow_options(
        args,
        config.manifest,
        config.sections,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        workflow_root=workflow_root,
    )
    return workflow_options_to_common_kwargs(options)


def _create_reaction_run_dir_workflow(
    args: Any, config: _RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    resolve_required_workflow_root = _dependency(
        deps, "_resolve_required_workflow_root", _resolve_required_workflow_root
    )
    run_dir_workflow_id = _dependency(deps, "_run_dir_workflow_id", _run_dir_workflow_id)
    resolve_run_dir_workflow_options = _dependency(
        deps, "_resolve_run_dir_workflow_options", _resolve_run_dir_workflow_options
    )
    workflow_options_to_common_kwargs = _dependency(
        deps, "_workflow_options_to_common_kwargs", _workflow_options_to_common_kwargs
    )
    create_workflow = _dependency(deps, "create_reaction_workflow", create_reaction_workflow)

    if not config.reactant_xyz or not config.product_xyz:
        raise ValueError(
            "reaction_ts_search requires both reactant.xyz and product.xyz "
            "(or manifest/CLI overrides)."
        )
    workflow_root = resolve_required_workflow_root(args, config.manifest)
    options = resolve_run_dir_workflow_options(
        args,
        config.manifest,
        config.sections,
        default_orca_route_line="! r2scan-3c OptTS Freq TightSCF",
        default_max_orca_stages=3,
        workflow_root=workflow_root,
    )
    reaction_kwargs: dict[str, Any] = {
        "reactant_xyz": config.reactant_xyz,
        "product_xyz": config.product_xyz,
        "workflow_id": run_dir_workflow_id(config, workflow_root),
        **workflow_options_to_common_kwargs(options),
        "max_crest_candidates": options.max_crest_candidates,
        "max_xtb_stages": options.max_xtb_stages,
    }
    if config.crest_manifest:
        reaction_kwargs["crest_job_manifest"] = config.crest_manifest
    if config.xtb_manifest:
        reaction_kwargs["xtb_job_manifest"] = config.xtb_manifest
    if config.endpoint_pairing:
        reaction_kwargs["endpoint_pairing"] = config.endpoint_pairing
    return create_workflow(**reaction_kwargs)


def _create_conformer_run_dir_workflow(
    args: Any, config: _RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    resolve_required_workflow_root = _dependency(
        deps, "_resolve_required_workflow_root", _resolve_required_workflow_root
    )
    run_dir_workflow_id = _dependency(deps, "_run_dir_workflow_id", _run_dir_workflow_id)
    common_run_dir_workflow_kwargs = _dependency(
        deps, "_common_run_dir_workflow_kwargs", _common_run_dir_workflow_kwargs
    )
    create_workflow = _dependency(
        deps, "create_conformer_screening_workflow", create_conformer_screening_workflow
    )

    if not config.input_xyz:
        raise ValueError("conformer_screening requires input.xyz (or manifest/CLI override).")
    workflow_root = resolve_required_workflow_root(args, config.manifest)
    conformer_kwargs: dict[str, Any] = {
        "input_xyz": config.input_xyz,
        "workflow_id": run_dir_workflow_id(config, workflow_root),
        **common_run_dir_workflow_kwargs(
            args,
            config,
            workflow_root=workflow_root,
            default_orca_route_line="! r2scan-3c Opt TightSCF",
            default_max_orca_stages=20,
        ),
    }
    if config.crest_manifest:
        conformer_kwargs["crest_job_manifest"] = config.crest_manifest
    return create_workflow(**conformer_kwargs)


def _create_run_dir_workflow(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> dict[str, Any]:
    load_run_dir_workflow_config = _dependency(
        deps, "_load_run_dir_workflow_config", _load_run_dir_workflow_config
    )
    create_reaction_run_dir_workflow = _dependency(
        deps, "_create_reaction_run_dir_workflow", _create_reaction_run_dir_workflow
    )
    create_conformer_run_dir_workflow = _dependency(
        deps, "_create_conformer_run_dir_workflow", _create_conformer_run_dir_workflow
    )

    config = load_run_dir_workflow_config(args, workflow_dir)
    if config.workflow_type == "reaction_ts_search":
        return create_reaction_run_dir_workflow(args, config)
    return create_conformer_run_dir_workflow(args, config)


def _restart_existing_run_dir_workflow(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> dict[str, Any]:
    restart_workflow = _dependency(deps, "restart_failed_workflow", restart_failed_workflow)
    workflow_root_for_existing_run_dir = _dependency(
        deps, "_workflow_root_for_existing_run_dir", _workflow_root_for_existing_run_dir
    )
    return restart_workflow(
        workspace_dir=workflow_dir,
        workflow_root=workflow_root_for_existing_run_dir(args, workflow_dir),
        force=bool(getattr(args, "force", False)),
    )


def cmd_run_dir(args: Any, *, deps: Any | None = None) -> int:
    path_cls = _dependency(deps, "Path", Path)
    restart_existing_run_dir_workflow = _dependency(
        deps, "_restart_existing_run_dir_workflow", _restart_existing_run_dir_workflow
    )
    create_run_dir_workflow = _dependency(
        deps, "_create_run_dir_workflow", _create_run_dir_workflow
    )
    print_restarted_workflow = _dependency(
        deps, "_print_restarted_workflow", _print_restarted_workflow
    )
    print_created_workflow = _dependency(deps, "_print_created_workflow", _print_created_workflow)

    try:
        workflow_dir = path_cls(getattr(args, "workflow_dir")).expanduser().resolve()
        if not workflow_dir.is_dir():
            raise ValueError(f"workflow_dir does not exist or is not a directory: {workflow_dir}")

        if (workflow_dir / "workflow.json").is_file():
            payload = restart_existing_run_dir_workflow(args, workflow_dir)
            return print_restarted_workflow(payload, json_mode=bool(getattr(args, "json", False)))

        payload = create_run_dir_workflow(args, workflow_dir)
    except ValueError as exc:
        print(f"error: {exc}")
        return 1

    return print_created_workflow(payload, json_mode=bool(getattr(args, "json", False)))
