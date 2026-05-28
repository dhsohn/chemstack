from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.cli_common import (
    _dependency,
    _workflow_root_for_args as _cli_workflow_root_for_args,
)
from chemstack.core.utils.coercion import normalize_text

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


@dataclass(frozen=True)
class _RunDirWorkflowOptionDefaults:
    orca_route_line: str
    max_orca_stages: int
    max_crest_candidates: int
    max_xtb_stages: int


@dataclass(frozen=True)
class _RunDirWorkflowOptionResolvers:
    resolve_required_workflow_root: Any
    resolve_text_option_with_section: Any
    resolve_int_option: Any
    resolve_int_option_with_section: Any


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
    normalize = _dependency(deps, "_normalize_text", normalize_text)

    explicit_text = normalize(explicit)
    if explicit_text:
        return explicit_text
    manifest_text = normalize(manifest.get(key))
    if manifest_text:
        return manifest_text
    section_text = normalize(section.get(section_key))
    if section_text:
        return section_text
    return default


def _resolve_int_option(
    explicit: Any, manifest: dict[str, Any], key: str, default: int, *, deps: Any | None = None
) -> int:
    normalize = _dependency(deps, "_normalize_text", normalize_text)

    if explicit is not None:
        return int(explicit)
    manifest_value = manifest.get(key)
    if manifest_value is None or normalize(manifest_value) == "":
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
    normalize = _dependency(deps, "_normalize_text", normalize_text)

    if explicit is not None:
        return int(explicit)
    manifest_value = manifest.get(key)
    if manifest_value is not None and normalize(manifest_value) != "":
        return int(manifest_value)
    section_value = section.get(section_key)
    if section_value is None or normalize(section_value) == "":
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
        resolve_workflow_root_for_args = _dependency(
            deps, "_workflow_root_for_args", _cli_workflow_root_for_args
        )
        config_path = (
            getattr(args, "chemstack_config", None)
            or getattr(args, "config", None)
            or getattr(args, "global_config", None)
        )
        resolved_workflow_root = resolve_workflow_root_for_args(args, config_path=config_path)
    if not resolved_workflow_root:
        raise ValueError("workflow_root is not configured. Set workflow.root in chemstack.yaml.")
    return resolved_workflow_root


def _run_dir_workflow_option_defaults(
    *,
    default_orca_route_line: str,
    default_max_orca_stages: int,
    default_max_crest_candidates: int,
    default_max_xtb_stages: int,
) -> _RunDirWorkflowOptionDefaults:
    return _RunDirWorkflowOptionDefaults(
        orca_route_line=default_orca_route_line,
        max_orca_stages=default_max_orca_stages,
        max_crest_candidates=default_max_crest_candidates,
        max_xtb_stages=default_max_xtb_stages,
    )


def _run_dir_workflow_option_resolvers(deps: Any | None) -> _RunDirWorkflowOptionResolvers:
    return _RunDirWorkflowOptionResolvers(
        resolve_required_workflow_root=_dependency(
            deps, "_resolve_required_workflow_root", _resolve_required_workflow_root
        ),
        resolve_text_option_with_section=_dependency(
            deps, "_resolve_text_option_with_section", _resolve_text_option_with_section
        ),
        resolve_int_option=_dependency(deps, "_resolve_int_option", _resolve_int_option),
        resolve_int_option_with_section=_dependency(
            deps, "_resolve_int_option_with_section", _resolve_int_option_with_section
        ),
    )


def _resolve_run_dir_core_options(
    args: Any,
    manifest: dict[str, Any],
    sections: RunDirManifestSections,
    *,
    workflow_root: str | None,
    resolvers: _RunDirWorkflowOptionResolvers,
) -> dict[str, Any]:
    return {
        "workflow_root": workflow_root
        or resolvers.resolve_required_workflow_root(args, manifest),
        "crest_mode": resolvers.resolve_text_option_with_section(
            getattr(args, "crest_mode", None),
            manifest,
            "crest_mode",
            sections.crest,
            "mode",
            "standard",
        ),
        "priority": resolvers.resolve_int_option(
            getattr(args, "priority", None), manifest, "priority", 10
        ),
    }


def _resolve_run_dir_resource_options(
    args: Any,
    manifest: dict[str, Any],
    sections: RunDirManifestSections,
    *,
    resolvers: _RunDirWorkflowOptionResolvers,
) -> dict[str, Any]:
    return {
        "max_cores": resolvers.resolve_int_option_with_section(
            getattr(args, "max_cores", None),
            manifest,
            "max_cores",
            sections.resources,
            "max_cores",
            8,
        ),
        "max_memory_gb": resolvers.resolve_int_option_with_section(
            getattr(args, "max_memory_gb", None),
            manifest,
            "max_memory_gb",
            sections.resources,
            "max_memory_gb",
            32,
        ),
    }


def _resolve_run_dir_orca_options(
    args: Any,
    manifest: dict[str, Any],
    sections: RunDirManifestSections,
    *,
    defaults: _RunDirWorkflowOptionDefaults,
    resolvers: _RunDirWorkflowOptionResolvers,
) -> dict[str, Any]:
    return {
        "max_orca_stages": resolvers.resolve_int_option(
            getattr(args, "max_orca_stages", None),
            manifest,
            "max_orca_stages",
            defaults.max_orca_stages,
        ),
        "orca_route_line": resolvers.resolve_text_option_with_section(
            getattr(args, "orca_route_line", None),
            manifest,
            "orca_route_line",
            sections.orca,
            "route_line",
            defaults.orca_route_line,
        ),
        "charge": resolvers.resolve_int_option_with_section(
            getattr(args, "charge", None),
            manifest,
            "charge",
            sections.orca,
            "charge",
            0,
        ),
        "multiplicity": resolvers.resolve_int_option_with_section(
            getattr(args, "multiplicity", None),
            manifest,
            "multiplicity",
            sections.orca,
            "multiplicity",
            1,
        ),
    }


def _resolve_run_dir_stage_options(
    args: Any,
    manifest: dict[str, Any],
    *,
    defaults: _RunDirWorkflowOptionDefaults,
    resolvers: _RunDirWorkflowOptionResolvers,
) -> dict[str, Any]:
    return {
        "max_crest_candidates": resolvers.resolve_int_option(
            getattr(args, "max_crest_candidates", None),
            manifest,
            "max_crest_candidates",
            defaults.max_crest_candidates,
        ),
        "max_xtb_stages": resolvers.resolve_int_option(
            getattr(args, "max_xtb_stages", None),
            manifest,
            "max_xtb_stages",
            defaults.max_xtb_stages,
        ),
    }


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
    defaults = _run_dir_workflow_option_defaults(
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        default_max_crest_candidates=default_max_crest_candidates,
        default_max_xtb_stages=default_max_xtb_stages,
    )
    resolvers = _run_dir_workflow_option_resolvers(deps)

    return RunDirWorkflowOptions(
        **_resolve_run_dir_core_options(
            args,
            manifest,
            sections,
            workflow_root=workflow_root,
            resolvers=resolvers,
        ),
        **_resolve_run_dir_resource_options(
            args,
            manifest,
            sections,
            resolvers=resolvers,
        ),
        **_resolve_run_dir_orca_options(
            args,
            manifest,
            sections,
            defaults=defaults,
            resolvers=resolvers,
        ),
        **_resolve_run_dir_stage_options(
            args,
            manifest,
            defaults=defaults,
            resolvers=resolvers,
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
]
