from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.cli_common import _dependency
from chemstack.core.utils.coercion import normalize_text
from . import cli_workflow_output as _workflow_output
from . import run_dir_manifest as _run_dir_manifest
from . import run_dir_options as _run_dir_options
from .orchestration import create_conformer_screening_workflow, create_reaction_ts_search_workflow
from .restart import restart_failed_workflow


def _safe_workflow_name(value: Any, *, fallback: str, deps: Any | None = None) -> str:
    normalize = _dependency(deps, "_normalize_text", normalize_text)

    cleaned = "".join(
        ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in normalize(value)
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
    normalize = _dependency(deps, "_normalize_text", normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    raw_root = normalize(getattr(args, "workflow_root", None))
    if raw_root:
        return path_cls(raw_root).expanduser().resolve()
    return workflow_dir.parent


def _update_present_kwargs(kwargs: dict[str, Any], values: dict[str, Any]) -> None:
    for key, value in values.items():
        if value:
            kwargs[key] = value


def _create_reaction_run_dir_workflow(
    args: Any, config: _run_dir_options.RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    resolve_required_workflow_root = _dependency(
        deps,
        "_resolve_required_workflow_root",
        _run_dir_options._resolve_required_workflow_root,
    )
    unique_run_dir_workflow_id = _dependency(
        deps, "_unique_run_dir_workflow_id", _unique_run_dir_workflow_id
    )
    resolve_run_dir_workflow_option_bundle = _dependency(
        deps,
        "_resolve_run_dir_workflow_option_bundle",
        _run_dir_options._resolve_run_dir_workflow_option_bundle,
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
        "workflow_id": unique_run_dir_workflow_id(
            config.workflow_dir,
            workflow_root=workflow_root,
            workflow_type=config.workflow_type,
        ),
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
    args: Any, config: _run_dir_options.RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    resolve_required_workflow_root = _dependency(
        deps,
        "_resolve_required_workflow_root",
        _run_dir_options._resolve_required_workflow_root,
    )
    unique_run_dir_workflow_id = _dependency(
        deps, "_unique_run_dir_workflow_id", _unique_run_dir_workflow_id
    )
    resolve_run_dir_workflow_option_bundle = _dependency(
        deps,
        "_resolve_run_dir_workflow_option_bundle",
        _run_dir_options._resolve_run_dir_workflow_option_bundle,
    )
    update_present_kwargs = _dependency(deps, "_update_present_kwargs", _update_present_kwargs)
    create_workflow = _dependency(
        deps, "create_conformer_screening_workflow", create_conformer_screening_workflow
    )

    if not config.input_xyz:
        raise ValueError("conformer_screening requires input.xyz (or manifest/CLI override).")
    workflow_root = resolve_required_workflow_root(args, config.manifest)
    _, common_kwargs = resolve_run_dir_workflow_option_bundle(
        args,
        config.manifest,
        config.sections,
        default_orca_route_line="! r2scan-3c Opt TightSCF",
        default_max_orca_stages=20,
        workflow_root=workflow_root,
    )
    conformer_kwargs: dict[str, Any] = {
        "input_xyz": config.input_xyz,
        "workflow_id": unique_run_dir_workflow_id(
            config.workflow_dir,
            workflow_root=workflow_root,
            workflow_type=config.workflow_type,
        ),
        **common_kwargs,
    }
    update_present_kwargs(conformer_kwargs, {"crest_job_manifest": config.crest_manifest})
    return create_workflow(**conformer_kwargs)


def _create_run_dir_workflow(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> dict[str, Any]:
    load_run_dir_workflow_config = _dependency(
        deps, "_load_run_dir_workflow_config", _run_dir_manifest._load_run_dir_workflow_config
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
