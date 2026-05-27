from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.cli_common import (
    _dependency,
    _normalize_text as _normalize_text,
    _normalize_workflow_type as _normalize_workflow_type,
    _workflow_root_from_args as _cli_workflow_root_from_args,
)
from . import cli_workflow_output as _workflow_output
from . import run_dir_manifest as _run_dir_manifest
from . import run_dir_options as _run_dir_options
from .orchestration import create_conformer_screening_workflow, create_reaction_ts_search_workflow
from .restart import restart_failed_workflow
from .run_dir_manifest import WORKFLOW_MANIFEST_FILENAMES as WORKFLOW_MANIFEST_FILENAMES
from .run_dir_options import (
    RUN_DIR_COMMON_WORKFLOW_OPTION_FIELDS as RUN_DIR_COMMON_WORKFLOW_OPTION_FIELDS,
    RunDirManifestSections,
    RunDirWorkflowConfig,
    RunDirWorkflowOptions,
)
from .run_dir_layout import (
    STANDARD_CONFORMER_INPUT_FILENAME as STANDARD_CONFORMER_INPUT_FILENAME,
    STANDARD_REACTION_PRODUCT_FILENAME as STANDARD_REACTION_PRODUCT_FILENAME,
    STANDARD_REACTION_REACTANT_FILENAME as STANDARD_REACTION_REACTANT_FILENAME,
    inspect_workflow_run_dir as inspect_workflow_run_dir,
)


class _CliRunDirDeps:
    def __init__(self, deps: Any | None) -> None:
        self._deps = deps

    def __getattr__(self, name: str) -> Any:
        if self._deps is not None and hasattr(self._deps, name):
            return getattr(self._deps, name)
        if name == "_workflow_root_from_args":
            return _cli_workflow_root_from_args
        try:
            return globals()[name]
        except KeyError:
            raise AttributeError(name) from None


def _cli_run_dir_deps(deps: Any | None) -> _CliRunDirDeps:
    return _CliRunDirDeps(deps)


def _load_run_dir_manifest(workflow_dir: Path, *, deps: Any | None = None) -> dict[str, Any]:
    return _run_dir_manifest._load_run_dir_manifest(
        workflow_dir, deps=_cli_run_dir_deps(deps)
    )


def _manifest_mapping(value: Any, *, deps: Any | None = None) -> dict[str, Any]:
    return _run_dir_manifest._manifest_mapping(value, deps=_cli_run_dir_deps(deps))


def _resolve_manifest_file_value(
    workflow_dir: Path,
    value: Any,
    *,
    deps: Any | None = None,
) -> str:
    return _run_dir_manifest._resolve_manifest_file_value(
        workflow_dir, value, deps=_cli_run_dir_deps(deps)
    )


def _resolve_engine_manifest(
    workflow_dir: Path,
    manifest: dict[str, Any],
    key: str,
    *,
    deps: Any | None = None,
) -> dict[str, Any]:
    return _run_dir_manifest._resolve_engine_manifest(
        workflow_dir, manifest, key, deps=_cli_run_dir_deps(deps)
    )


def _resolve_endpoint_pairing_manifest(
    manifest: dict[str, Any],
    xtb_manifest: dict[str, Any],
    *,
    deps: Any | None = None,
) -> dict[str, Any]:
    return _run_dir_manifest._resolve_endpoint_pairing_manifest(
        manifest, xtb_manifest, deps=_cli_run_dir_deps(deps)
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
    return _run_dir_manifest._resolve_run_dir_path(
        workflow_dir,
        explicit=explicit,
        manifest=manifest,
        key=key,
        default_names=default_names,
        deps=_cli_run_dir_deps(deps),
    )


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
    return _run_dir_options._resolve_text_option_with_section(
        explicit,
        manifest,
        key,
        section,
        section_key,
        default,
        deps=_cli_run_dir_deps(deps),
    )


def _resolve_int_option(
    explicit: Any, manifest: dict[str, Any], key: str, default: int, *, deps: Any | None = None
) -> int:
    return _run_dir_options._resolve_int_option(
        explicit, manifest, key, default, deps=_cli_run_dir_deps(deps)
    )


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
    return _run_dir_options._resolve_int_option_with_section(
        explicit,
        manifest,
        key,
        section,
        section_key,
        default,
        deps=_cli_run_dir_deps(deps),
    )


def _resolve_required_workflow_root(
    args: Any, manifest: dict[str, Any], *, deps: Any | None = None
) -> str:
    return _run_dir_options._resolve_required_workflow_root(
        args, manifest, deps=_cli_run_dir_deps(deps)
    )


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


def _workflow_root_for_existing_run_dir(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> Path:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    raw_root = normalize_text(getattr(args, "workflow_root", None))
    if raw_root:
        return path_cls(raw_root).expanduser().resolve()
    return workflow_dir.parent


def _resolve_run_dir_workflow_type(
    args: Any, manifest: dict[str, Any], workflow_layout: Any, *, deps: Any | None = None
) -> str:
    return _run_dir_manifest._resolve_run_dir_workflow_type(
        args, manifest, workflow_layout, deps=_cli_run_dir_deps(deps)
    )


def _resolve_run_dir_manifest_sections(
    workflow_dir: Path, manifest: dict[str, Any], *, deps: Any | None = None
) -> RunDirManifestSections:
    return _run_dir_manifest._resolve_run_dir_manifest_sections(
        workflow_dir, manifest, deps=_cli_run_dir_deps(deps)
    )


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
    return _run_dir_options._resolve_run_dir_workflow_options(
        args,
        manifest,
        sections,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        default_max_crest_candidates=default_max_crest_candidates,
        default_max_xtb_stages=default_max_xtb_stages,
        workflow_root=workflow_root,
        deps=_cli_run_dir_deps(deps),
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
    return _run_dir_options._resolve_run_dir_workflow_option_bundle(
        args,
        manifest,
        sections,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        default_max_crest_candidates=default_max_crest_candidates,
        default_max_xtb_stages=default_max_xtb_stages,
        workflow_root=workflow_root,
        deps=_cli_run_dir_deps(deps),
    )


def _workflow_options_to_common_kwargs(options: RunDirWorkflowOptions) -> dict[str, Any]:
    return _run_dir_options._workflow_options_to_common_kwargs(options)


def _update_present_kwargs(kwargs: dict[str, Any], values: dict[str, Any]) -> None:
    for key, value in values.items():
        if value:
            kwargs[key] = value


def _load_run_dir_workflow_config(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> RunDirWorkflowConfig:
    return _run_dir_manifest._load_run_dir_workflow_config(
        args, workflow_dir, deps=_cli_run_dir_deps(deps)
    )


def _run_dir_workflow_id(
    config: RunDirWorkflowConfig, workflow_root: str, *, deps: Any | None = None
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
    config: RunDirWorkflowConfig,
    *,
    workflow_root: str,
    default_orca_route_line: str,
    default_max_orca_stages: int,
    deps: Any | None = None,
) -> dict[str, Any]:
    resolve_run_dir_workflow_option_bundle = _dependency(
        deps,
        "_resolve_run_dir_workflow_option_bundle",
        _resolve_run_dir_workflow_option_bundle,
    )

    _, common_kwargs = resolve_run_dir_workflow_option_bundle(
        args,
        config.manifest,
        config.sections,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        workflow_root=workflow_root,
    )
    return common_kwargs


def _create_reaction_run_dir_workflow(
    args: Any, config: RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    resolve_required_workflow_root = _dependency(
        deps, "_resolve_required_workflow_root", _resolve_required_workflow_root
    )
    run_dir_workflow_id = _dependency(deps, "_run_dir_workflow_id", _run_dir_workflow_id)
    resolve_run_dir_workflow_option_bundle = _dependency(
        deps,
        "_resolve_run_dir_workflow_option_bundle",
        _resolve_run_dir_workflow_option_bundle,
    )
    update_present_kwargs = _dependency(deps, "_update_present_kwargs", _update_present_kwargs)
    create_workflow = _dependency(
        deps, "create_reaction_ts_search_workflow", create_reaction_ts_search_workflow
    )

    if not config.reactant_xyz or not config.product_xyz:
        raise ValueError(
            "reaction_ts_search requires both reactant.xyz and product.xyz "
            "(or manifest/CLI overrides)."
        )
    workflow_root = resolve_required_workflow_root(args, config.manifest)
    options, common_kwargs = resolve_run_dir_workflow_option_bundle(
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
        **common_kwargs,
        "max_crest_candidates": options.max_crest_candidates,
        "max_xtb_stages": options.max_xtb_stages,
    }
    update_present_kwargs(
        reaction_kwargs,
        {
            "crest_job_manifest": config.crest_manifest,
            "xtb_job_manifest": config.xtb_manifest,
            "endpoint_pairing": config.endpoint_pairing,
        },
    )
    return create_workflow(**reaction_kwargs)


def _create_conformer_run_dir_workflow(
    args: Any, config: RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    resolve_required_workflow_root = _dependency(
        deps, "_resolve_required_workflow_root", _resolve_required_workflow_root
    )
    run_dir_workflow_id = _dependency(deps, "_run_dir_workflow_id", _run_dir_workflow_id)
    common_run_dir_workflow_kwargs = _dependency(
        deps, "_common_run_dir_workflow_kwargs", _common_run_dir_workflow_kwargs
    )
    update_present_kwargs = _dependency(deps, "_update_present_kwargs", _update_present_kwargs)
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
    update_present_kwargs(conformer_kwargs, {"crest_job_manifest": config.crest_manifest})
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
        deps, "_print_restarted_workflow", _workflow_output.emit_restarted_workflow
    )
    print_created_workflow = _dependency(
        deps, "_print_created_workflow", _workflow_output.emit_created_workflow
    )

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
