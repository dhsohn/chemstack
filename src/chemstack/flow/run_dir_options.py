from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.cli_common import (
    _dependency,
    _normalize_text,
    _workflow_root_from_args as _cli_workflow_root_from_args,
)

RUN_DIR_COMMON_WORKFLOW_OPTION_FIELDS = (
    "workflow_root",
    "crest_mode",
    "priority",
    "max_cores",
    "max_memory_gb",
    "max_orca_stages",
    "orca_route_line",
    "charge",
    "multiplicity",
)


@dataclass(frozen=True)
class RunDirManifestSections:
    resources: dict[str, Any]
    crest: dict[str, Any]
    xtb: dict[str, Any]
    endpoint_pairing: dict[str, Any]
    orca: dict[str, Any]


@dataclass(frozen=True)
class RunDirWorkflowOptions:
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

    def common_kwargs(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in RUN_DIR_COMMON_WORKFLOW_OPTION_FIELDS}


@dataclass(frozen=True)
class RunDirWorkflowConfig:
    workflow_dir: Path
    manifest: dict[str, Any]
    sections: RunDirManifestSections
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
    del manifest
    discover_workflow_root = _dependency(deps, "_discover_workflow_root", None)
    if discover_workflow_root is None:
        from chemstack.cli_common import _discover_workflow_root

        discover_workflow_root = _discover_workflow_root

    resolved_workflow_root = discover_workflow_root(getattr(args, "workflow_root", None))
    if not resolved_workflow_root:
        resolve_workflow_root_from_args = _dependency(
            deps, "_workflow_root_from_args", _cli_workflow_root_from_args
        )
        config_path = (
            getattr(args, "chemstack_config", None)
            or getattr(args, "config", None)
            or getattr(args, "global_config", None)
        )
        resolved_workflow_root = resolve_workflow_root_from_args(args, config_path=config_path)
    if not resolved_workflow_root:
        raise ValueError("workflow_root is not configured. Set workflow.root in chemstack.yaml.")
    return resolved_workflow_root


def _resolve_run_dir_workflow_options(
    args: Any,
    manifest: dict[str, Any],
    sections: RunDirManifestSections,
    *,
    default_orca_route_line: str,
    default_max_orca_stages: int,
    default_max_crest_candidates: int = 3,
    default_max_xtb_stages: int = 3,
    workflow_root: str | None = None,
    deps: Any | None = None,
) -> RunDirWorkflowOptions:
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

    return RunDirWorkflowOptions(
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


def _resolve_run_dir_workflow_option_bundle(
    args: Any,
    manifest: dict[str, Any],
    sections: RunDirManifestSections,
    *,
    default_orca_route_line: str,
    default_max_orca_stages: int,
    default_max_crest_candidates: int = 3,
    default_max_xtb_stages: int = 3,
    workflow_root: str | None = None,
    deps: Any | None = None,
) -> tuple[RunDirWorkflowOptions, dict[str, Any]]:
    resolve_run_dir_workflow_options = _dependency(
        deps, "_resolve_run_dir_workflow_options", _resolve_run_dir_workflow_options
    )
    workflow_options_to_common_kwargs = _dependency(
        deps, "_workflow_options_to_common_kwargs", _workflow_options_to_common_kwargs
    )

    options = resolve_run_dir_workflow_options(
        args,
        manifest,
        sections,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        default_max_crest_candidates=default_max_crest_candidates,
        default_max_xtb_stages=default_max_xtb_stages,
        workflow_root=workflow_root,
    )
    return options, workflow_options_to_common_kwargs(options)


def _workflow_options_to_common_kwargs(options: RunDirWorkflowOptions) -> dict[str, Any]:
    common_kwargs = getattr(options, "common_kwargs", None)
    if callable(common_kwargs):
        return common_kwargs()
    return {name: getattr(options, name) for name in RUN_DIR_COMMON_WORKFLOW_OPTION_FIELDS}


__all__ = [
    "RUN_DIR_COMMON_WORKFLOW_OPTION_FIELDS",
    "RunDirManifestSections",
    "RunDirWorkflowConfig",
    "RunDirWorkflowOptions",
    "_resolve_int_option",
    "_resolve_int_option_with_section",
    "_resolve_required_workflow_root",
    "_resolve_run_dir_workflow_option_bundle",
    "_resolve_run_dir_workflow_options",
    "_resolve_text_option_with_section",
    "_workflow_options_to_common_kwargs",
]
