from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.cli_common import _dependency
from chemstack.core.utils.coercion import normalize_text
from . import cli_workflow_output as _workflow_output
from . import run_dir_manifest as _run_dir_manifest
from . import run_dir_options as _run_dir_options
from chemstack.flow.orchestration.requests import (
    ConformerScreeningWorkflowRequest,
    ReactionTsSearchWorkflowRequest,
)
from .orchestration import (
    create_conformer_screening_workflow,
    create_conformer_screening_workflow_from_request,
    create_reaction_ts_search_workflow,
    create_reaction_ts_search_workflow_from_request,
)
from .restart import restart_failed_workflow

_DEFAULT_CREATE_REACTION_TS_SEARCH_WORKFLOW = create_reaction_ts_search_workflow
_DEFAULT_CREATE_CONFORMER_SCREENING_WORKFLOW = create_conformer_screening_workflow


@dataclass(frozen=True)
class _RunDirWorkflowCreationSpec:
    workflow_type: str
    required_input_kwargs: tuple[tuple[str, str], ...]
    missing_inputs_error: str
    request_type_name: str
    create_workflow_name: str
    create_workflow_from_request_name: str
    default_orca_route_line: str
    default_max_orca_stages: int
    option_kwargs: tuple[tuple[str, str], ...] = ()
    manifest_kwargs: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class _RunDirWorkflowCreationBinding:
    request_type: Any
    create_workflow: Any
    default_create_workflow: Any
    create_workflow_from_request: Any


@dataclass(frozen=True)
class _NormalizedRunDirWorkflowCreationSpec:
    spec: _RunDirWorkflowCreationSpec
    create_workflow: Any | None
    create_workflow_from_request: Any
    request_type: Any


@dataclass(frozen=True)
class _RunDirWorkflowCreationPlan:
    normalized_spec: _NormalizedRunDirWorkflowCreationSpec
    workflow_kwargs: dict[str, Any]


@dataclass(frozen=True)
class _RunDirWorkflowCreationRegistry:
    bindings: dict[str, _RunDirWorkflowCreationBinding]

    def resolve(self, workflow_type: str) -> _RunDirWorkflowCreationBinding:
        try:
            return self.bindings[workflow_type]
        except KeyError as exc:
            raise ValueError(f"unsupported workflow_type: {workflow_type}") from exc


def _run_dir_workflow_creation_registry() -> _RunDirWorkflowCreationRegistry:
    return _RunDirWorkflowCreationRegistry(
        bindings={
            "reaction_ts_search": _RunDirWorkflowCreationBinding(
                request_type=ReactionTsSearchWorkflowRequest,
                create_workflow=create_reaction_ts_search_workflow,
                default_create_workflow=_DEFAULT_CREATE_REACTION_TS_SEARCH_WORKFLOW,
                create_workflow_from_request=create_reaction_ts_search_workflow_from_request,
            ),
            "conformer_screening": _RunDirWorkflowCreationBinding(
                request_type=ConformerScreeningWorkflowRequest,
                create_workflow=create_conformer_screening_workflow,
                default_create_workflow=_DEFAULT_CREATE_CONFORMER_SCREENING_WORKFLOW,
                create_workflow_from_request=create_conformer_screening_workflow_from_request,
            ),
        }
    )


_REACTION_RUN_DIR_WORKFLOW_SPEC = _RunDirWorkflowCreationSpec(
    workflow_type="reaction_ts_search",
    required_input_kwargs=(
        ("reactant_xyz", "reactant_xyz"),
        ("product_xyz", "product_xyz"),
    ),
    missing_inputs_error=(
        "reaction_ts_search requires both reactant.xyz and product.xyz (or manifest/CLI overrides)."
    ),
    request_type_name="ReactionTsSearchWorkflowRequest",
    create_workflow_name="create_reaction_ts_search_workflow",
    create_workflow_from_request_name="create_reaction_ts_search_workflow_from_request",
    default_orca_route_line="! r2scan-3c OptTS Freq TightSCF",
    default_max_orca_stages=3,
    option_kwargs=(
        ("max_crest_candidates", "max_crest_candidates"),
        ("max_xtb_stages", "max_xtb_stages"),
    ),
    manifest_kwargs=(
        ("crest_job_manifest", "crest_manifest"),
        ("xtb_job_manifest", "xtb_manifest"),
        ("endpoint_pairing", "endpoint_pairing"),
    ),
)

_CONFORMER_RUN_DIR_WORKFLOW_SPEC = _RunDirWorkflowCreationSpec(
    workflow_type="conformer_screening",
    required_input_kwargs=(("input_xyz", "input_xyz"),),
    missing_inputs_error="conformer_screening requires input.xyz (or manifest/CLI override).",
    request_type_name="ConformerScreeningWorkflowRequest",
    create_workflow_name="create_conformer_screening_workflow",
    create_workflow_from_request_name="create_conformer_screening_workflow_from_request",
    default_orca_route_line="! r2scan-3c Opt TightSCF",
    default_max_orca_stages=20,
    manifest_kwargs=(("crest_job_manifest", "crest_manifest"),),
)


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


def _classify_run_dir_workflow_spec(
    config: _run_dir_options.RunDirWorkflowConfig,
) -> _RunDirWorkflowCreationSpec:
    if config.workflow_type == _REACTION_RUN_DIR_WORKFLOW_SPEC.workflow_type:
        return _REACTION_RUN_DIR_WORKFLOW_SPEC
    return _CONFORMER_RUN_DIR_WORKFLOW_SPEC


def _normalize_run_dir_workflow_creation_spec(
    spec: _RunDirWorkflowCreationSpec, *, deps: Any | None = None
) -> _NormalizedRunDirWorkflowCreationSpec:
    workflow_creation_registry = _dependency(
        deps,
        "_run_dir_workflow_creation_registry",
        _run_dir_workflow_creation_registry,
    )

    binding = workflow_creation_registry().resolve(spec.workflow_type)
    create_workflow = _dependency(deps, spec.create_workflow_name, None)
    if create_workflow is None and binding.create_workflow is not binding.default_create_workflow:
        create_workflow = binding.create_workflow

    return _NormalizedRunDirWorkflowCreationSpec(
        spec=spec,
        create_workflow=create_workflow,
        create_workflow_from_request=_dependency(
            deps,
            spec.create_workflow_from_request_name,
            binding.create_workflow_from_request,
        ),
        request_type=_dependency(deps, spec.request_type_name, binding.request_type),
    )


def _run_dir_required_input_kwargs(
    config: _run_dir_options.RunDirWorkflowConfig,
    spec: _RunDirWorkflowCreationSpec,
) -> dict[str, Any]:
    workflow_kwargs: dict[str, Any] = {}
    for kwarg_name, config_attr in spec.required_input_kwargs:
        value = getattr(config, config_attr)
        if not value:
            raise ValueError(spec.missing_inputs_error)
        workflow_kwargs[kwarg_name] = value
    return workflow_kwargs


def _run_dir_option_kwargs(
    options: _run_dir_options.RunDirWorkflowOptions,
    spec: _RunDirWorkflowCreationSpec,
) -> dict[str, Any]:
    return {
        kwarg_name: getattr(options, option_attr)
        for kwarg_name, option_attr in spec.option_kwargs
    }


def _run_dir_manifest_kwargs(
    config: _run_dir_options.RunDirWorkflowConfig,
    spec: _RunDirWorkflowCreationSpec,
) -> dict[str, Any]:
    return {
        kwarg_name: getattr(config, config_attr)
        for kwarg_name, config_attr in spec.manifest_kwargs
    }


def _build_run_dir_workflow_creation_plan(
    args: Any,
    config: _run_dir_options.RunDirWorkflowConfig,
    normalized_spec: _NormalizedRunDirWorkflowCreationSpec,
    *,
    deps: Any | None = None,
) -> _RunDirWorkflowCreationPlan:
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

    spec = normalized_spec.spec
    workflow_kwargs = _run_dir_required_input_kwargs(config, spec)
    workflow_root = resolve_required_workflow_root(args, config.manifest)
    options, common_kwargs = resolve_run_dir_workflow_option_bundle(
        args,
        config.manifest,
        config.sections,
        default_orca_route_line=spec.default_orca_route_line,
        default_max_orca_stages=spec.default_max_orca_stages,
        workflow_root=workflow_root,
    )

    workflow_kwargs.update(
        {
            "workflow_id": unique_run_dir_workflow_id(
                config.workflow_dir,
                workflow_root=workflow_root,
                workflow_type=config.workflow_type,
            ),
            **common_kwargs,
        }
    )
    workflow_kwargs.update(_run_dir_option_kwargs(options, spec))
    update_present_kwargs(workflow_kwargs, _run_dir_manifest_kwargs(config, spec))
    return _RunDirWorkflowCreationPlan(
        normalized_spec=normalized_spec,
        workflow_kwargs=workflow_kwargs,
    )


def _invoke_run_dir_workflow_creation(plan: _RunDirWorkflowCreationPlan) -> dict[str, Any]:
    normalized_spec = plan.normalized_spec
    if normalized_spec.create_workflow is not None:
        return normalized_spec.create_workflow(**plan.workflow_kwargs)
    return normalized_spec.create_workflow_from_request(
        normalized_spec.request_type(**plan.workflow_kwargs)
    )


def _create_run_dir_workflow_from_spec(
    args: Any,
    config: _run_dir_options.RunDirWorkflowConfig,
    spec: _RunDirWorkflowCreationSpec,
    *,
    deps: Any | None = None,
) -> dict[str, Any]:
    normalized_spec = _normalize_run_dir_workflow_creation_spec(spec, deps=deps)
    creation_plan = _build_run_dir_workflow_creation_plan(
        args,
        config,
        normalized_spec,
        deps=deps,
    )
    return _invoke_run_dir_workflow_creation(creation_plan)


def _create_reaction_run_dir_workflow(
    args: Any, config: _run_dir_options.RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    return _create_run_dir_workflow_from_spec(
        args,
        config,
        _REACTION_RUN_DIR_WORKFLOW_SPEC,
        deps=deps,
    )


def _create_conformer_run_dir_workflow(
    args: Any, config: _run_dir_options.RunDirWorkflowConfig, *, deps: Any | None = None
) -> dict[str, Any]:
    return _create_run_dir_workflow_from_spec(
        args,
        config,
        _CONFORMER_RUN_DIR_WORKFLOW_SPEC,
        deps=deps,
    )


def _create_run_dir_workflow(
    args: Any, workflow_dir: Path, *, deps: Any | None = None
) -> dict[str, Any]:
    load_run_dir_workflow_config = _dependency(
        deps, "_load_run_dir_workflow_config", _run_dir_manifest._load_run_dir_workflow_config
    )
    classify_run_dir_workflow_spec = _dependency(
        deps, "_classify_run_dir_workflow_spec", _classify_run_dir_workflow_spec
    )
    create_reaction_run_dir_workflow = _dependency(
        deps, "_create_reaction_run_dir_workflow", _create_reaction_run_dir_workflow
    )
    create_conformer_run_dir_workflow = _dependency(
        deps, "_create_conformer_run_dir_workflow", _create_conformer_run_dir_workflow
    )

    config = load_run_dir_workflow_config(args, workflow_dir)
    spec = classify_run_dir_workflow_spec(config)
    if spec.workflow_type == _REACTION_RUN_DIR_WORKFLOW_SPEC.workflow_type:
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
        _workflow_output.emit_error(exc)
        return 1

    return print_created_workflow(payload, json_mode=bool(getattr(args, "json", False)))
