from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE
from chemstack.core.config.files import default_config_path_from_repo_root, shared_workflow_root_from_config
from chemstack.core.utils import file_lock, now_utc_iso, timestamped_token

from .adapters import load_crest_artifact_contract, load_xtb_artifact_contract, select_xtb_downstream_inputs
from .contracts import XtbDownstreamPolicy
from .operations import (
    advance_materialized_workflow,
    cancel_activity,
    cancel_workflow,
    create_conformer_screening_workflow,
    create_reaction_workflow,
    get_workflow,
    get_workflow_artifacts,
    get_workflow_journal,
    get_workflow_runtime_status,
    get_workflow_telemetry,
    list_activities,
    list_workflows,
)
from .registry import (
    append_workflow_journal_event,
    reindex_workflow_registry,
    write_workflow_worker_state,
)
from .runtime import advance_workflow_registry_once, workflow_worker_lock_path
from .run_dir_layout import (
    STANDARD_CONFORMER_INPUT_FILENAME,
    STANDARD_REACTION_PRODUCT_FILENAME,
    STANDARD_REACTION_REACTANT_FILENAME,
    WORKFLOW_MANIFEST_FILENAMES,
    inspect_workflow_run_dir,
)
from .restart import restart_failed_workflow
from .submitters import submit_reaction_ts_search_workflow
from .workflows import (
    build_conformer_screening_plan_from_target,
    build_reaction_ts_search_plan_from_target,
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_existing_path(path_text: str) -> Path | None:
    text = _normalize_text(path_text)
    if not text:
        return None
    try:
        candidate = Path(text).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() else None


def _discover_workflow_root(
    explicit: str | Path | None,
    *,
    config_path: str | Path | None = None,
) -> str | None:
    explicit_text = _normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    config_text = _normalize_text(config_path)
    if config_text:
        return shared_workflow_root_from_config(config_text)
    return shared_workflow_root_from_config(default_config_path_from_repo_root(_project_root()))


def _shared_chemstack_config(args: Any) -> str | None:
    explicit = (
        _normalize_text(getattr(args, "chemstack_config", None))
        or _normalize_text(getattr(args, "orca_auto_config", None))
    )
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    default_config = _resolve_existing_path(default_config_path_from_repo_root(_project_root()))
    return str(default_config) if default_config is not None else None


def _workflow_root_from_args(args: Any, *, config_path: str | None = None) -> str | None:
    return _discover_workflow_root(getattr(args, "workflow_root", None), config_path=config_path)


def _normalize_workflow_type(value: Any) -> str:
    text = _normalize_text(value).lower().replace("-", "_")
    aliases = {
        "reaction": "reaction_ts_search",
        "reaction_ts": "reaction_ts_search",
        "reaction_ts_search": "reaction_ts_search",
        "reaction_tssearch": "reaction_ts_search",
        "conformer": "conformer_screening",
        "conformer_screening": "conformer_screening",
        "conformer_screen": "conformer_screening",
        "screening": "conformer_screening",
    }
    normalized = aliases.get(text, "")
    if normalized:
        return normalized
    raise ValueError(
        "workflow_type must be one of: reaction_ts_search, conformer_screening"
    )


def _load_run_dir_manifest(workflow_dir: Path) -> dict[str, Any]:
    for name in WORKFLOW_MANIFEST_FILENAMES:
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


def _manifest_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if _normalize_text(key)}


def _resolve_manifest_file_value(workflow_dir: Path, value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = workflow_dir / candidate
    return str(candidate.resolve())


def _resolve_engine_manifest(workflow_dir: Path, manifest: dict[str, Any], key: str) -> dict[str, Any]:
    section = _manifest_mapping(manifest.get(key))
    if not section:
        return {}
    resolved = dict(section)
    if "xcontrol_file" in resolved:
        resolved["xcontrol_file"] = _resolve_manifest_file_value(workflow_dir, resolved.get("xcontrol_file"))
    return resolved


def _resolve_run_dir_path(
    workflow_dir: Path,
    *,
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    default_names: tuple[str, ...],
) -> str:
    candidate_text = _normalize_text(explicit)
    if not candidate_text:
        candidate_text = _normalize_text(manifest.get(key))
    if candidate_text:
        candidate = Path(candidate_text).expanduser()
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
) -> str:
    explicit_text = _normalize_text(explicit)
    if explicit_text:
        return explicit_text
    manifest_text = _normalize_text(manifest.get(key))
    if manifest_text:
        return manifest_text
    section_text = _normalize_text(section.get(section_key))
    if section_text:
        return section_text
    return default


def _resolve_int_option(explicit: Any, manifest: dict[str, Any], key: str, default: int) -> int:
    if explicit is not None:
        return int(explicit)
    manifest_value = manifest.get(key)
    if manifest_value is None or _normalize_text(manifest_value) == "":
        return default
    return int(manifest_value)


def _resolve_int_option_with_section(
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    section: dict[str, Any],
    section_key: str,
    default: int,
) -> int:
    if explicit is not None:
        return int(explicit)
    manifest_value = manifest.get(key)
    if manifest_value is not None and _normalize_text(manifest_value) != "":
        return int(manifest_value)
    section_value = section.get(section_key)
    if section_value is None or _normalize_text(section_value) == "":
        return default
    return int(section_value)


def _resolve_required_workflow_root(args: Any, manifest: dict[str, Any]) -> str:
    resolved_workflow_root = _discover_workflow_root(
        getattr(args, "workflow_root", None) or manifest.get("workflow_root")
    )
    if not resolved_workflow_root:
        raise ValueError("workflow_root is not configured. Set workflow.root in chemstack.yaml.")
    return resolved_workflow_root


def _safe_workflow_name(value: Any, *, fallback: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in _normalize_text(value))
    cleaned = cleaned.strip("._-").lower()
    return cleaned or fallback


def _preferred_run_dir_workflow_id(workflow_dir: Path, *, workflow_type: str) -> str:
    stem = _safe_workflow_name(workflow_dir.name, fallback="workflow")
    prefix = "wf_reaction_ts" if workflow_type == "reaction_ts_search" else "wf_conformer_screening"
    if stem.startswith(prefix):
        return stem
    return f"{prefix}_{stem}"


def _unique_run_dir_workflow_id(
    workflow_dir: Path,
    *,
    workflow_root: str | Path,
    workflow_type: str,
) -> str:
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    if workflow_dir.parent == workflow_root_path and not (workflow_dir / "workflow.json").exists():
        return workflow_dir.name

    preferred = _preferred_run_dir_workflow_id(workflow_dir, workflow_type=workflow_type)
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
) -> dict[str, Any]:
    return {
        "workflow_root": _resolve_required_workflow_root(args, manifest),
        "crest_mode": _resolve_text_option_with_section(
            getattr(args, "crest_mode", None),
            manifest,
            "crest_mode",
            crest_manifest,
            "mode",
            "standard",
        ),
        "priority": _resolve_int_option(getattr(args, "priority", None), manifest, "priority", 10),
        "max_cores": _resolve_int_option_with_section(
            getattr(args, "max_cores", None), manifest, "max_cores", resources_manifest, "max_cores", 8
        ),
        "max_memory_gb": _resolve_int_option_with_section(
            getattr(args, "max_memory_gb", None),
            manifest,
            "max_memory_gb",
            resources_manifest,
            "max_memory_gb",
            32,
        ),
        "max_orca_stages": _resolve_int_option(
            getattr(args, "max_orca_stages", None),
            manifest,
            "max_orca_stages",
            default_max_orca_stages,
        ),
        "orca_route_line": _resolve_text_option_with_section(
            getattr(args, "orca_route_line", None),
            manifest,
            "orca_route_line",
            orca_manifest,
            "route_line",
            default_orca_route_line,
        ),
        "charge": _resolve_int_option_with_section(
            getattr(args, "charge", None), manifest, "charge", orca_manifest, "charge", 0
        ),
        "multiplicity": _resolve_int_option_with_section(
            getattr(args, "multiplicity", None),
            manifest,
            "multiplicity",
            orca_manifest,
            "multiplicity",
            1,
        ),
    }


def _print_created_workflow(payload: dict[str, Any], *, json_mode: bool) -> int:
    if json_mode:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"template_name: {payload.get('template_name', '-')}")
    print(f"workspace_dir: {(payload.get('metadata') or {}).get('workspace_dir', '-')}")
    print(f"stage_count: {len(payload.get('stages', []))}")
    return 0


def _workflow_root_for_existing_run_dir(args: Any, workflow_dir: Path) -> Path:
    raw_root = _normalize_text(getattr(args, "workflow_root", None))
    if raw_root:
        return Path(raw_root).expanduser().resolve()
    return workflow_dir.parent


def _print_restarted_workflow(payload: dict[str, Any], *, json_mode: bool) -> int:
    if json_mode:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"workflow_status: {payload.get('workflow_status', '-')}")
    print(f"previous_status: {payload.get('previous_status', '-')}")
    print(f"workspace_dir: {payload.get('workspace_dir', '-')}")
    print(f"restarted_count: {payload.get('restarted_count', 0)}")
    for item in payload.get("restarted_stages", []):
        print(
            f"- restarted {item.get('stage_id', '-')}"
            f" previous_status={item.get('previous_status', '-')}"
            f" previous_task_status={item.get('previous_task_status', '-')}"
        )
    return 0


def cmd_run_dir(args: Any) -> int:
    try:
        workflow_dir = Path(getattr(args, "workflow_dir")).expanduser().resolve()
        if not workflow_dir.is_dir():
            raise ValueError(f"workflow_dir does not exist or is not a directory: {workflow_dir}")

        if (workflow_dir / "workflow.json").is_file():
            payload = restart_failed_workflow(
                workspace_dir=workflow_dir,
                workflow_root=_workflow_root_for_existing_run_dir(args, workflow_dir),
                force=bool(getattr(args, "force", False)),
            )
            return _print_restarted_workflow(payload, json_mode=bool(getattr(args, "json", False)))

        workflow_layout = inspect_workflow_run_dir(workflow_dir)
        if not workflow_layout.has_manifest:
            raise ValueError(
                "workflow run-dir requires flow.yaml in workflow_dir."
            )

        manifest = _load_run_dir_manifest(workflow_dir)
        resources_manifest = _manifest_mapping(manifest.get("resources"))
        crest_manifest = _resolve_engine_manifest(workflow_dir, manifest, "crest")
        xtb_manifest = _resolve_engine_manifest(workflow_dir, manifest, "xtb")
        orca_manifest = _resolve_engine_manifest(workflow_dir, manifest, "orca")
        reactant_xyz = _resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "reactant_xyz", None),
            manifest=manifest,
            key="reactant_xyz",
            default_names=(STANDARD_REACTION_REACTANT_FILENAME,),
        )
        product_xyz = _resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "product_xyz", None),
            manifest=manifest,
            key="product_xyz",
            default_names=(STANDARD_REACTION_PRODUCT_FILENAME,),
        )
        input_xyz = _resolve_run_dir_path(
            workflow_dir,
            explicit=getattr(args, "input_xyz", None),
            manifest=manifest,
            key="input_xyz",
            default_names=(STANDARD_CONFORMER_INPUT_FILENAME,),
        )

        workflow_type_text = _normalize_text(getattr(args, "workflow_type", None))
        if not workflow_type_text:
            workflow_type_text = _normalize_text(manifest.get("workflow_type"))

        if workflow_type_text:
            workflow_type = _normalize_workflow_type(workflow_type_text)
        else:
            if workflow_layout.is_ambiguous:
                raise ValueError(
                    "Ambiguous workflow_dir: found both reaction inputs and conformer input. "
                    "Pass --workflow-type to choose one."
                )
            inferred_workflow_type = workflow_layout.inferred_workflow_type
            if inferred_workflow_type:
                workflow_type = inferred_workflow_type
            else:
                raise ValueError(
                    "Could not infer workflow type from workflow_dir. "
                    "Expected reactant.xyz + product.xyz or input.xyz."
                )

        if workflow_type == "reaction_ts_search":
            if not reactant_xyz or not product_xyz:
                raise ValueError(
                    "reaction_ts_search requires both reactant.xyz and product.xyz "
                    "(or manifest/CLI overrides)."
                )
            workflow_root = _resolve_required_workflow_root(args, manifest)
            reaction_kwargs: dict[str, Any] = {
                "reactant_xyz": reactant_xyz,
                "product_xyz": product_xyz,
                "workflow_id": _unique_run_dir_workflow_id(
                    workflow_dir,
                    workflow_root=workflow_root,
                    workflow_type=workflow_type,
                ),
                **_resolve_run_dir_common_workflow_kwargs(
                    args,
                    manifest,
                    resources_manifest=resources_manifest,
                    crest_manifest=crest_manifest,
                    orca_manifest=orca_manifest,
                    default_orca_route_line="! r2scan-3c OptTS Freq TightSCF",
                    default_max_orca_stages=3,
                ),
                "max_crest_candidates": _resolve_int_option(
                    getattr(args, "max_crest_candidates", None), manifest, "max_crest_candidates", 3
                ),
                "max_xtb_stages": _resolve_int_option(
                    getattr(args, "max_xtb_stages", None), manifest, "max_xtb_stages", 3
                ),
            }
            if crest_manifest:
                reaction_kwargs["crest_job_manifest"] = crest_manifest
            if xtb_manifest:
                reaction_kwargs["xtb_job_manifest"] = xtb_manifest
            payload = create_reaction_workflow(**reaction_kwargs)
        else:
            if not input_xyz:
                raise ValueError(
                    "conformer_screening requires input.xyz (or manifest/CLI override)."
                )
            workflow_root = _resolve_required_workflow_root(args, manifest)
            conformer_kwargs: dict[str, Any] = {
                "input_xyz": input_xyz,
                "workflow_id": _unique_run_dir_workflow_id(
                    workflow_dir,
                    workflow_root=workflow_root,
                    workflow_type=workflow_type,
                ),
                **_resolve_run_dir_common_workflow_kwargs(
                    args,
                    manifest,
                    resources_manifest=resources_manifest,
                    crest_manifest=crest_manifest,
                    orca_manifest=orca_manifest,
                    default_orca_route_line="! r2scan-3c Opt TightSCF",
                    default_max_orca_stages=20,
                ),
            }
            if crest_manifest:
                conformer_kwargs["crest_job_manifest"] = crest_manifest
            payload = create_conformer_screening_workflow(**conformer_kwargs)
    except ValueError as exc:
        print(f"error: {exc}")
        return 1

    return _print_created_workflow(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_xtb_inspect(args: Any) -> int:
    contract = load_xtb_artifact_contract(
        xtb_index_root=getattr(args, "xtb_index_root"),
        target=getattr(args, "target"),
    )
    payload = contract.to_dict()
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"job_id: {contract.job_id}")
    print(f"job_type: {contract.job_type}")
    print(f"status: {contract.status}")
    print(f"reason: {contract.reason or '-'}")
    print(f"job_dir: {contract.job_dir}")
    print(f"latest_known_path: {contract.latest_known_path}")
    print(f"organized_output_dir: {contract.organized_output_dir or '-'}")
    print(f"reaction_key: {contract.reaction_key or '-'}")
    print(f"selected_input_xyz: {contract.selected_input_xyz or '-'}")
    print(f"candidate_count: {len(contract.candidate_details)}")
    if contract.selected_candidate_paths:
        print(f"selected_candidate_paths: {list(contract.selected_candidate_paths)}")
    if contract.analysis_summary:
        print(f"analysis_summary: {contract.analysis_summary}")
    return 0


def cmd_xtb_candidates(args: Any) -> int:
    contract = load_xtb_artifact_contract(
        xtb_index_root=getattr(args, "xtb_index_root"),
        target=getattr(args, "target"),
    )
    policy = XtbDownstreamPolicy.build(
        preferred_kinds=getattr(args, "preferred_kinds", None),
        max_candidates=int(getattr(args, "max_candidates", 3) or 3),
        selected_only=not bool(getattr(args, "include_unselected", False)),
    )
    candidates = select_xtb_downstream_inputs(contract, policy=policy)
    payload = {
        "source_job_id": contract.job_id,
        "source_job_type": contract.job_type,
        "reaction_key": contract.reaction_key,
        "candidate_count": len(candidates),
        "candidates": [item.to_dict() for item in candidates],
    }
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"source_job_id: {contract.job_id}")
    print(f"source_job_type: {contract.job_type}")
    print(f"reaction_key: {contract.reaction_key or '-'}")
    print(f"candidate_count: {len(candidates)}")
    for candidate in candidates:
        print(
            f"- rank={candidate.rank} kind={candidate.kind} selected={candidate.selected} "
            f"path={candidate.artifact_path}"
        )
    return 0


def cmd_crest_inspect(args: Any) -> int:
    contract = load_crest_artifact_contract(
        crest_index_root=getattr(args, "crest_index_root"),
        target=getattr(args, "target"),
    )
    payload = contract.to_dict()
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"job_id: {contract.job_id}")
    print(f"mode: {contract.mode}")
    print(f"status: {contract.status}")
    print(f"reason: {contract.reason or '-'}")
    print(f"job_dir: {contract.job_dir}")
    print(f"latest_known_path: {contract.latest_known_path}")
    print(f"organized_output_dir: {contract.organized_output_dir or '-'}")
    print(f"molecule_key: {contract.molecule_key or '-'}")
    print(f"selected_input_xyz: {contract.selected_input_xyz or '-'}")
    print(f"retained_conformer_count: {contract.retained_conformer_count}")
    if contract.retained_conformer_paths:
        print(f"retained_conformer_paths: {list(contract.retained_conformer_paths)}")
    return 0


def cmd_workflow_reaction_ts_search(args: Any) -> int:
    payload = build_reaction_ts_search_plan_from_target(
        xtb_index_root=getattr(args, "xtb_index_root"),
        target=getattr(args, "target"),
        max_orca_stages=int(getattr(args, "max_orca_stages", 3) or 3),
        selected_only=not bool(getattr(args, "include_unselected", False)),
        workspace_root=getattr(args, "workspace_root", None),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        priority=int(getattr(args, "priority", 10) or 10),
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_id: {payload['workflow_id']}")
    print(f"template_name: {payload['template_name']}")
    print(f"status: {payload['status']}")
    print(f"source_job_id: {payload['source_job_id']}")
    print(f"reaction_key: {payload['reaction_key']}")
    workspace_dir = str((payload.get("metadata") or {}).get("workspace_dir", "")).strip()
    print(f"workspace_dir: {workspace_dir or '-'}")
    print(f"stage_count: {len(payload.get('stages', []))}")
    for stage in payload.get("stages", []):
        task = stage.get("task") or {}
        task_payload = task.get("payload", {})
        enqueue_payload = task.get("enqueue_payload") or {}
        print(
            f"- {stage.get('stage_id')} {task.get('engine', '-')}/{task.get('task_kind', '-')}"
            f" input={task_payload.get('selected_input_xyz', '-')}"
        )
        if task_payload.get("reaction_dir"):
            print(f"  reaction_dir={task_payload.get('reaction_dir')}")
        if enqueue_payload.get("command"):
            print(f"  enqueue_command={enqueue_payload.get('command')}")
        elif task_payload.get("suggested_command"):
            print(f"  suggested_command={task_payload.get('suggested_command')}")
    return 0


def cmd_workflow_conformer_screening(args: Any) -> int:
    payload = build_conformer_screening_plan_from_target(
        crest_index_root=getattr(args, "crest_index_root"),
        target=getattr(args, "target"),
        max_orca_stages=int(getattr(args, "max_orca_stages", 20) or 20),
        workspace_root=getattr(args, "workspace_root", None),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        priority=int(getattr(args, "priority", 10) or 10),
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_id: {payload['workflow_id']}")
    print(f"template_name: {payload['template_name']}")
    print(f"status: {payload['status']}")
    print(f"source_job_id: {payload['source_job_id']}")
    print(f"reaction_key: {payload['reaction_key']}")
    workspace_dir = str((payload.get("metadata") or {}).get("workspace_dir", "")).strip()
    print(f"workspace_dir: {workspace_dir or '-'}")
    print(f"stage_count: {len(payload.get('stages', []))}")
    for stage in payload.get("stages", []):
        task = stage.get("task") or {}
        task_payload = task.get("payload", {})
        print(
            f"- {stage.get('stage_id')} {task.get('engine', '-')}/{task.get('task_kind', '-')}"
            f" input={task_payload.get('selected_input_xyz', '-')}"
        )
        if task_payload.get("reaction_dir"):
            print(f"  reaction_dir={task_payload.get('reaction_dir')}")
    return 0

def cmd_workflow_create_reaction_ts_search(args: Any) -> int:
    payload = create_reaction_workflow(
        reactant_xyz=getattr(args, "reactant_xyz"),
        product_xyz=getattr(args, "product_xyz"),
        workflow_root=getattr(args, "workflow_root"),
        crest_mode=str(getattr(args, "crest_mode", "standard") or "standard"),
        priority=int(getattr(args, "priority", 10) or 10),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        max_crest_candidates=int(getattr(args, "max_crest_candidates", 3) or 3),
        max_xtb_stages=int(getattr(args, "max_xtb_stages", 3) or 3),
        max_orca_stages=int(getattr(args, "max_orca_stages", 3) or 3),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
    )
    return _print_created_workflow(payload, json_mode=bool(getattr(args, "json", False)))


def cmd_workflow_create_conformer_screening(args: Any) -> int:
    payload = create_conformer_screening_workflow(
        input_xyz=getattr(args, "input_xyz"),
        workflow_root=getattr(args, "workflow_root"),
        crest_mode=str(getattr(args, "crest_mode", "standard") or "standard"),
        priority=int(getattr(args, "priority", 10) or 10),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        max_orca_stages=int(getattr(args, "max_orca_stages", 20) or 20),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
    )
    return _print_created_workflow(payload, json_mode=bool(getattr(args, "json", False)))

def cmd_workflow_advance(args: Any) -> int:
    shared_config = _shared_chemstack_config(args)
    payload = advance_materialized_workflow(
        target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root"),
        crest_auto_config=shared_config,
        crest_auto_executable=getattr(args, "crest_auto_executable", "crest_auto"),
        crest_auto_repo_root=getattr(args, "crest_auto_repo_root", None),
        xtb_auto_config=shared_config,
        xtb_auto_executable=getattr(args, "xtb_auto_executable", "xtb_auto"),
        xtb_auto_repo_root=getattr(args, "xtb_auto_repo_root", None),
        orca_auto_config=shared_config,
        orca_auto_executable=getattr(args, "orca_auto_executable", CHEMSTACK_EXECUTABLE),
        orca_auto_repo_root=getattr(args, "orca_auto_repo_root", None),
        submit_ready=not bool(getattr(args, "no_submit", False)),
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"stage_count: {len(payload.get('stages', []))}")
    return 0


def _emit_worker_payload(payload: dict[str, Any], *, json_mode: bool, single_cycle: bool) -> None:
    if json_mode:
        if single_cycle:
            print(json.dumps(payload, ensure_ascii=True, indent=2))
        else:
            print(json.dumps(payload, ensure_ascii=True))
        return

    print(
        f"cycle_started_at: {payload.get('cycle_started_at', '-')}"
        f" worker_session_id={payload.get('worker_session_id', '-')}"
        f" discovered={payload.get('discovered_count', 0)}"
        f" advanced={payload.get('advanced_count', 0)}"
        f" skipped={payload.get('skipped_count', 0)}"
        f" failed={payload.get('failed_count', 0)}"
    )
    for item in payload.get("workflow_results", []):
        print(
            f"- {item.get('workflow_id', '-')} template={item.get('template_name', '-')}"
            f" previous={item.get('previous_status', '-')}"
            f" status={item.get('status', '-')}"
            f" advanced={'yes' if item.get('advanced') else 'no'}"
        )
        if item.get("reason"):
            print(f"  reason={item.get('reason')}")


def cmd_workflow_worker(args: Any) -> int:
    once = bool(getattr(args, "once", False))
    max_cycles = int(getattr(args, "max_cycles", 0) or 0)
    if once:
        max_cycles = 1
    if max_cycles < 0:
        print("error: --max-cycles must be >= 0")
        return 1
    interval_seconds = float(getattr(args, "interval_seconds", 30.0) or 30.0)
    lock_timeout_seconds = float(getattr(args, "lock_timeout_seconds", 5.0) or 5.0)
    refresh_registry = bool(getattr(args, "refresh_registry", False))
    refresh_each_cycle = bool(getattr(args, "refresh_each_cycle", False))
    service_mode = bool(getattr(args, "service_mode", False))
    json_mode = bool(getattr(args, "json", False))
    shared_config = _shared_chemstack_config(args)
    workflow_root = _workflow_root_from_args(args, config_path=shared_config)
    if not workflow_root:
        print("error: workflow_root is not configured. Pass --workflow-root or set workflow.root in chemstack.yaml.")
        return 1
    workflow_root_text = str(workflow_root)
    cycle_count = 0
    worker_session_id = _normalize_text(getattr(args, "worker_session_id", "")) or timestamped_token("wf_worker")
    lease_seconds = max(float(getattr(args, "lease_seconds", 60.0) or 60.0), interval_seconds * 2.5)

    try:
        with file_lock(workflow_worker_lock_path(workflow_root), timeout_seconds=lock_timeout_seconds):
            started_at = now_utc_iso()
            write_workflow_worker_state(
                workflow_root_text,
                worker_session_id=worker_session_id,
                status="starting",
                workflow_root_path=workflow_root_text,
                last_heartbeat_at=started_at,
                interval_seconds=interval_seconds,
                submit_ready=not bool(getattr(args, "no_submit", False)),
            )
            append_workflow_journal_event(
                workflow_root_text,
                event_type="worker_started",
                worker_session_id=worker_session_id,
                metadata={"started_at": started_at, "service_mode": service_mode},
            )
            while True:
                cycle_count += 1
                payload = advance_workflow_registry_once(
                    workflow_root=workflow_root_text,
                    crest_auto_config=shared_config,
                    crest_auto_executable=getattr(args, "crest_auto_executable", "crest_auto"),
                    crest_auto_repo_root=getattr(args, "crest_auto_repo_root", None),
                    xtb_auto_config=shared_config,
                    xtb_auto_executable=getattr(args, "xtb_auto_executable", "xtb_auto"),
                    xtb_auto_repo_root=getattr(args, "xtb_auto_repo_root", None),
                    orca_auto_config=shared_config,
                    orca_auto_executable=getattr(args, "orca_auto_executable", CHEMSTACK_EXECUTABLE),
                    orca_auto_repo_root=getattr(args, "orca_auto_repo_root", None),
                    submit_ready=not bool(getattr(args, "no_submit", False)),
                    refresh_registry=refresh_each_cycle or (refresh_registry and cycle_count == 1),
                    worker_session_id=worker_session_id,
                    interval_seconds=interval_seconds,
                    lease_seconds=lease_seconds,
                )
                _emit_worker_payload(payload, json_mode=json_mode, single_cycle=max_cycles == 1)
                if max_cycles > 0 and cycle_count >= max_cycles:
                    stopped_at = now_utc_iso()
                    write_workflow_worker_state(
                        workflow_root_text,
                        worker_session_id=worker_session_id,
                        status="stopped",
                        workflow_root_path=workflow_root_text,
                        last_cycle_finished_at=stopped_at,
                        last_heartbeat_at=stopped_at,
                        interval_seconds=interval_seconds,
                        submit_ready=not bool(getattr(args, "no_submit", False)),
                        metadata={"stop_reason": "max_cycles_reached", "cycle_count": cycle_count, "service_mode": service_mode},
                    )
                    append_workflow_journal_event(
                        workflow_root_text,
                        event_type="worker_stopped",
                        worker_session_id=worker_session_id,
                        metadata={"stopped_at": stopped_at, "reason": "max_cycles_reached", "cycle_count": cycle_count},
                    )
                    return 0
                time.sleep(max(0.0, interval_seconds))
    except KeyboardInterrupt:
        stopped_at = now_utc_iso()
        write_workflow_worker_state(
            workflow_root_text,
            worker_session_id=worker_session_id,
            status="interrupted",
            workflow_root_path=workflow_root_text,
            last_heartbeat_at=stopped_at,
            interval_seconds=interval_seconds,
            submit_ready=not bool(getattr(args, "no_submit", False)),
            metadata={"stop_reason": "keyboard_interrupt", "cycle_count": cycle_count, "service_mode": service_mode},
        )
        append_workflow_journal_event(
            workflow_root_text,
            event_type="worker_interrupted",
            worker_session_id=worker_session_id,
            metadata={"stopped_at": stopped_at, "cycle_count": cycle_count},
        )
        return 130
    except TimeoutError as exc:
        stopped_at = now_utc_iso()
        write_workflow_worker_state(
            workflow_root_text,
            worker_session_id=worker_session_id,
            status="lock_error",
            workflow_root_path=workflow_root_text,
            last_heartbeat_at=stopped_at,
            interval_seconds=interval_seconds,
            submit_ready=not bool(getattr(args, "no_submit", False)),
            metadata={"stop_reason": "worker_lock_error", "error": str(exc), "service_mode": service_mode},
        )
        append_workflow_journal_event(
            workflow_root_text,
            event_type="worker_lock_error",
            worker_session_id=worker_session_id,
            reason=str(exc),
            metadata={"stopped_at": stopped_at},
        )
        print(f"worker_lock_error: {exc}")
        return 1


def cmd_workflow_runtime_status(args: Any) -> int:
    payload = get_workflow_runtime_status(workflow_root=getattr(args, "workflow_root"))
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    state = payload["worker_state"] or {}
    print(f"worker_session_id: {state.get('worker_session_id', '-')}")
    print(f"status: {state.get('status', '-')}")
    print(f"pid: {state.get('pid', '-')}")
    print(f"hostname: {state.get('hostname', '-')}")
    print(f"last_heartbeat_at: {state.get('last_heartbeat_at', '-')}")
    print(f"lease_expires_at: {state.get('lease_expires_at', '-')}")
    print(f"last_cycle_started_at: {state.get('last_cycle_started_at', '-')}")
    print(f"last_cycle_finished_at: {state.get('last_cycle_finished_at', '-')}")
    return 0


def cmd_workflow_journal(args: Any) -> int:
    payload = get_workflow_journal(
        workflow_root=getattr(args, "workflow_root"),
        limit=int(getattr(args, "limit", 50) or 0),
    )
    events = payload["events"]
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"event_count: {len(events)}")
    for item in events:
        print(
            f"- {item.get('occurred_at', '-')} {item.get('event_type', '-')}"
            f" workflow_id={item.get('workflow_id', '-') or '-'}"
            f" status={item.get('status', '-') or '-'}"
        )
        if item.get("reason"):
            print(f"  reason={item.get('reason')}")
    return 0


def cmd_workflow_telemetry(args: Any) -> int:
    payload = get_workflow_telemetry(
        workflow_root=getattr(args, "workflow_root"),
        limit=int(getattr(args, "limit", 200) or 0),
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_root: {payload.get('workflow_root', '-')}")
    worker_state = payload.get("worker_state") or {}
    print(f"worker_status: {worker_state.get('status', '-')}")
    print(f"worker_session_id: {worker_state.get('worker_session_id', '-')}")
    print(f"registry_count: {payload.get('registry_count', 0)}")
    print(f"journal_event_count: {payload.get('journal_event_count', 0)}")
    print(f"workflow_status_counts: {payload.get('workflow_status_counts', {})}")
    print(f"template_counts: {payload.get('template_counts', {})}")
    print(f"journal_event_type_counts: {payload.get('journal_event_type_counts', {})}")
    recent_failures = payload.get("recent_failures") or []
    if recent_failures:
        print("recent_failures:")
        for item in recent_failures:
            print(
                f"- {item.get('occurred_at', '-')} workflow={item.get('workflow_id', '-') or '-'}"
                f" reason={item.get('reason', '-') or '-'}"
            )
    recent_status_changes = payload.get("recent_status_changes") or []
    if recent_status_changes:
        print("recent_status_changes:")
        for item in recent_status_changes:
            print(
                f"- {item.get('occurred_at', '-')} workflow={item.get('workflow_id', '-') or '-'}"
                f" {item.get('previous_status', '-') or '-'}->{item.get('status', '-') or '-'}"
            )
    return 0


def cmd_workflow_submit_reaction_ts_search(args: Any) -> int:
    shared_config = _shared_chemstack_config(args) or default_config_path_from_repo_root(_project_root())
    payload = submit_reaction_ts_search_workflow(
        workflow_target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root", None),
        orca_auto_config=shared_config,
        orca_auto_executable=getattr(args, "orca_auto_executable", CHEMSTACK_EXECUTABLE),
        orca_auto_repo_root=getattr(args, "orca_auto_repo_root", None),
        skip_submitted=not bool(getattr(args, "resubmit", False)),
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"workspace_dir: {payload.get('workspace_dir', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"submitted_count: {len(payload.get('submitted', []))}")
    for item in payload.get("submitted", []):
        print(f"- submitted {item.get('stage_id', '-')} queue_id={item.get('queue_id', '-')}")
    if payload.get("skipped"):
        print(f"skipped_count: {len(payload.get('skipped', []))}")
        for item in payload.get("skipped", []):
            print(f"- skipped {item.get('stage_id', '-')} reason={item.get('reason', '-')}")
    if payload.get("failed"):
        print(f"failed_count: {len(payload.get('failed', []))}")
        for item in payload.get("failed", []):
            print(f"- failed {item.get('stage_id', '-')} returncode={item.get('returncode', '-')}")
    return 0


def cmd_activity_list(args: Any) -> int:
    shared_config = _shared_chemstack_config(args)
    payload = list_activities(
        workflow_root=getattr(args, "workflow_root", None),
        limit=int(getattr(args, "limit", 0) or 0),
        refresh=bool(getattr(args, "refresh", False)),
        crest_auto_config=shared_config,
        xtb_auto_config=shared_config,
        orca_auto_config=shared_config,
        orca_auto_repo_root=getattr(args, "orca_auto_repo_root", None),
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"activity_count: {payload.get('count', 0)}")
    for item in payload.get("activities", []):
        print(
            f"- {item.get('activity_id', '-')}"
            f" engine={item.get('engine', '-')}"
            f" status={item.get('status', '-')}"
            f" label={item.get('label', '-')}"
            f" source={item.get('source', '-')}"
        )
    return 0


def cmd_activity_cancel(args: Any) -> int:
    shared_config = _shared_chemstack_config(args)
    try:
        payload = cancel_activity(
            target=getattr(args, "target"),
            workflow_root=getattr(args, "workflow_root", None),
            crest_auto_config=shared_config,
            crest_auto_executable=getattr(args, "crest_auto_executable", "crest_auto"),
            crest_auto_repo_root=getattr(args, "crest_auto_repo_root", None),
            xtb_auto_config=shared_config,
            xtb_auto_executable=getattr(args, "xtb_auto_executable", "xtb_auto"),
            xtb_auto_repo_root=getattr(args, "xtb_auto_repo_root", None),
            orca_auto_config=shared_config,
            orca_auto_executable=getattr(args, "orca_auto_executable", CHEMSTACK_EXECUTABLE),
            orca_auto_repo_root=getattr(args, "orca_auto_repo_root", None),
        )
    except (LookupError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"activity_id: {payload.get('activity_id', '-')}")
    print(f"engine: {payload.get('engine', '-')}")
    print(f"source: {payload.get('source', '-')}")
    print(f"label: {payload.get('label', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"cancel_target: {payload.get('cancel_target', '-')}")
    return 0


def cmd_bot(args: Any) -> int:
    from .telegram_bot import run_bot

    return int(run_bot())


def cmd_workflow_list(args: Any) -> int:
    payload = list_workflows(
        workflow_root=getattr(args, "workflow_root"),
        limit=int(getattr(args, "limit", 0) or 0),
        refresh=bool(getattr(args, "refresh", False)),
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_count: {payload.get('count', 0)}")
    for item in payload.get("workflows", []):
        submission_summary = item.get("submission_summary") or {}
        submitted_count = int(submission_summary.get("submitted_count", 0) or 0)
        failed_count = int(submission_summary.get("failed_count", 0) or 0)
        print(
            f"- {item.get('workflow_id', '-')} template={item.get('template_name', '-')}"
            f" status={item.get('status', '-')}"
            f" stages={item.get('stage_count', 0)}"
            f" submitted={submitted_count}"
            f" failed={failed_count}"
        )
    return 0


def cmd_workflow_get(args: Any) -> int:
    response = get_workflow(
        target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root", None),
        sync_registry=True,
    )
    summary = response["summary"]
    if bool(getattr(args, "json", False)):
        print(json.dumps(response, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_id: {summary.get('workflow_id', '-')}")
    print(f"template_name: {summary.get('template_name', '-')}")
    print(f"status: {summary.get('status', '-')}")
    print(f"source_job_id: {summary.get('source_job_id', '-')}")
    print(f"reaction_key: {summary.get('reaction_key', '-')}")
    print(f"workspace_dir: {summary.get('workspace_dir', '-')}")
    print(f"stage_count: {summary.get('stage_count', 0)}")
    downstream = summary.get("downstream_reaction_workflow") or {}
    if downstream:
        print(
            f"downstream_reaction: {downstream.get('workflow_id', '-')} "
            f"status={downstream.get('status', '-')}"
        )
    submission_summary = summary.get("submission_summary") or {}
    if submission_summary:
        print(
            f"submission_summary: submitted={submission_summary.get('submitted_count', 0)} "
            f"skipped={submission_summary.get('skipped_count', 0)} "
            f"failed={submission_summary.get('failed_count', 0)}"
        )
    for stage in summary.get("stage_summaries", []):
        print(
            f"- {stage.get('stage_id', '-')} {stage.get('engine', '-')}/{stage.get('task_kind', '-')}"
            f" stage_status={stage.get('status', '-')}"
            f" task_status={stage.get('task_status', '-')}"
        )
        if stage.get("queue_id"):
            print(f"  queue_id={stage.get('queue_id')}")
        if stage.get("selected_input_xyz"):
            print(f"  selected_input_xyz={stage.get('selected_input_xyz')}")
        if stage.get("selected_inp"):
            print(f"  selected_inp={stage.get('selected_inp')}")
    return 0


def cmd_workflow_artifacts(args: Any) -> int:
    response = get_workflow_artifacts(
        target=getattr(args, "target"),
        workflow_root=getattr(args, "workflow_root", None),
        sync_registry=True,
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(response, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_id: {response.get('workflow_id', '-')}")
    print(f"workspace_dir: {response.get('workspace_dir', '-')}")
    print(f"artifact_count: {response.get('artifact_count', 0)}")
    for item in response.get("artifacts", []):
        print(
            f"- {item.get('kind', '-')}"
            f" stage={item.get('stage_id', '-') or '-'}"
            f" exists={'yes' if item.get('exists') else 'no'}"
            f" selected={'yes' if item.get('selected') else 'no'}"
        )
        print(f"  path={item.get('path', '-')}")
    return 0


def cmd_workflow_cancel(args: Any) -> int:
    try:
        payload = cancel_workflow(
            target=getattr(args, "target"),
            workflow_root=getattr(args, "workflow_root", None),
            crest_auto_config=getattr(args, "crest_auto_config", None),
            crest_auto_executable=getattr(args, "crest_auto_executable", "crest_auto"),
            crest_auto_repo_root=getattr(args, "crest_auto_repo_root", None),
            xtb_auto_config=getattr(args, "xtb_auto_config", None),
            xtb_auto_executable=getattr(args, "xtb_auto_executable", "xtb_auto"),
            xtb_auto_repo_root=getattr(args, "xtb_auto_repo_root", None),
            orca_auto_config=getattr(args, "orca_auto_config", None),
            orca_auto_executable=getattr(args, "orca_auto_executable", CHEMSTACK_EXECUTABLE),
            orca_auto_repo_root=getattr(args, "orca_auto_repo_root", None),
        )
    except (ValueError, TimeoutError) as exc:
        print(f"error: {exc}")
        return 1
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"workspace_dir: {payload.get('workspace_dir', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"cancelled_count: {len(payload.get('cancelled', []))}")
    for item in payload.get("cancelled", []):
        print(f"- cancelled {item.get('stage_id', '-')} queue_id={item.get('queue_id', '-')}")
    if payload.get("requested"):
        print(f"requested_count: {len(payload.get('requested', []))}")
        for item in payload.get("requested", []):
            print(f"- cancel_requested {item.get('stage_id', '-')} queue_id={item.get('queue_id', '-')}")
    if payload.get("skipped"):
        print(f"skipped_count: {len(payload.get('skipped', []))}")
        for item in payload.get("skipped", []):
            print(f"- skipped {item.get('stage_id', '-')} reason={item.get('reason', '-')}")
    if payload.get("failed"):
        print(f"failed_count: {len(payload.get('failed', []))}")
        for item in payload.get("failed", []):
            print(f"- failed {item.get('stage_id', '-')} reason={item.get('reason', '-')}")
    return 0


def cmd_workflow_reindex(args: Any) -> int:
    records = reindex_workflow_registry(getattr(args, "workflow_root"))
    payload = {
        "workflow_root": str(getattr(args, "workflow_root")),
        "count": len(records),
        "workflow_ids": [record.workflow_id for record in records],
    }
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_count: {len(records)}")
    for record in records:
        print(f"- {record.workflow_id} status={record.status} template={record.template_name}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.flow.cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_dir_parser = subparsers.add_parser(
        "run-dir",
        help="Create a workflow from an input directory containing reactant/product or input XYZ files.",
    )
    run_dir_parser.add_argument("workflow_dir", help="Directory that contains workflow input XYZ files")
    run_dir_parser.add_argument(
        "--workflow-type",
        help="Optional workflow type override: reaction_ts_search or conformer_screening",
    )
    run_dir_parser.add_argument("--workflow-root", help="Root that directly contains workflow workspaces.")
    run_dir_parser.add_argument("--reactant-xyz", help="Optional reactant XYZ override")
    run_dir_parser.add_argument("--product-xyz", help="Optional product XYZ override")
    run_dir_parser.add_argument("--input-xyz", help="Optional conformer input XYZ override")
    run_dir_parser.add_argument("--crest-mode", help="CREST mode (`standard` or `nci`)")
    run_dir_parser.add_argument("--priority", type=int, default=None)
    run_dir_parser.add_argument("--max-cores", type=int, default=None)
    run_dir_parser.add_argument("--max-memory-gb", type=int, default=None)
    run_dir_parser.add_argument("--max-crest-candidates", type=int, default=None)
    run_dir_parser.add_argument("--max-xtb-stages", type=int, default=None)
    run_dir_parser.add_argument("--max-orca-stages", type=int, default=None)
    run_dir_parser.add_argument("--orca-route-line")
    run_dir_parser.add_argument("--charge", type=int, default=None)
    run_dir_parser.add_argument("--multiplicity", type=int, default=None)
    run_dir_parser.add_argument(
        "--force",
        action="store_true",
        help="Allow restarting an existing workflow workspace outside failed status",
    )
    run_dir_parser.add_argument("--json", action="store_true", help="Print JSON output")
    run_dir_parser.set_defaults(func=cmd_run_dir)

    activity_list_parser = subparsers.add_parser("list", help="List workflows and standalone engine activities together.")
    activity_list_parser.add_argument("--workflow-root", help="Root that directly contains workflow workspaces.")
    activity_list_parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of activities to print")
    activity_list_parser.add_argument("--refresh", action="store_true", help="Refresh workflow registry before listing")
    activity_list_parser.add_argument("--chemstack-config", help="Path to shared chemstack.yaml")
    activity_list_parser.add_argument("--json", action="store_true", help="Print JSON output")
    activity_list_parser.set_defaults(func=cmd_activity_list)

    activity_cancel_parser = subparsers.add_parser("cancel", help="Cancel a workflow or standalone engine activity.")
    activity_cancel_parser.add_argument("target", help="Activity id, workflow id, queue id, run id, or known path alias")
    activity_cancel_parser.add_argument("--workflow-root", help="Root that directly contains workflow workspaces.")
    activity_cancel_parser.add_argument("--chemstack-config", help="Path to shared chemstack.yaml")
    activity_cancel_parser.add_argument("--json", action="store_true", help="Print JSON output")
    activity_cancel_parser.set_defaults(func=cmd_activity_cancel)

    bot_parser = subparsers.add_parser("bot", help="Run the ChemStack flow Telegram bot.")
    bot_parser.set_defaults(func=cmd_bot)

    xtb_parser = subparsers.add_parser("xtb", help="Inspect and adapt xTB artifacts.")
    xtb_subparsers = xtb_parser.add_subparsers(dest="xtb_command", required=True)

    inspect_parser = xtb_subparsers.add_parser("inspect", help="Load a normalized xTB artifact contract.")
    inspect_parser.add_argument("target", help="xTB job_id or job directory")
    inspect_parser.add_argument("--xtb-index-root", required=True, help="xTB index root, usually allowed_root")
    inspect_parser.add_argument("--json", action="store_true", help="Print JSON output")
    inspect_parser.set_defaults(func=cmd_xtb_inspect)

    candidates_parser = xtb_subparsers.add_parser("candidates", help="Select downstream-ready xTB candidate inputs.")
    candidates_parser.add_argument("target", help="xTB job_id or job directory")
    candidates_parser.add_argument("--xtb-index-root", required=True, help="xTB index root, usually allowed_root")
    candidates_parser.add_argument("--max-candidates", type=int, default=3, help="Maximum number of candidates to emit")
    candidates_parser.add_argument(
        "--preferred-kind",
        dest="preferred_kinds",
        action="append",
        help="Preferred candidate kind in priority order; may be passed more than once",
    )
    candidates_parser.add_argument(
        "--include-unselected",
        action="store_true",
        help="Consider non-selected candidate_details when building downstream inputs",
    )
    candidates_parser.add_argument("--json", action="store_true", help="Print JSON output")
    candidates_parser.set_defaults(func=cmd_xtb_candidates)

    crest_parser = subparsers.add_parser("crest", help="Inspect CREST artifacts.")
    crest_subparsers = crest_parser.add_subparsers(dest="crest_command", required=True)
    crest_inspect_parser = crest_subparsers.add_parser("inspect", help="Load a normalized CREST artifact contract.")
    crest_inspect_parser.add_argument("target", help="CREST job_id or job directory")
    crest_inspect_parser.add_argument("--crest-index-root", required=True, help="CREST index root, usually allowed_root")
    crest_inspect_parser.add_argument("--json", action="store_true", help="Print JSON output")
    crest_inspect_parser.set_defaults(func=cmd_crest_inspect)

    workflow_parser = subparsers.add_parser("workflow", help="Build chemistry workflow plans.")
    workflow_subparsers = workflow_parser.add_subparsers(dest="workflow_command", required=True)

    list_parser = workflow_subparsers.add_parser("list", help="List materialized workflows under a workflow root.")
    list_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    list_parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of workflows to print")
    list_parser.add_argument("--refresh", action="store_true", help="Rebuild the registry from workflow workspaces before listing")
    list_parser.add_argument("--json", action="store_true", help="Print JSON output")
    list_parser.set_defaults(func=cmd_workflow_list)

    get_parser = workflow_subparsers.add_parser("get", help="Inspect one materialized workflow.")
    get_parser.add_argument("target", help="workflow_id, workflow workspace directory, or workflow.json path")
    get_parser.add_argument("--workflow-root", help="Root that directly contains workflow workspaces.")
    get_parser.add_argument("--json", action="store_true", help="Print JSON output")
    get_parser.set_defaults(func=cmd_workflow_get)

    artifacts_parser = workflow_subparsers.add_parser(
        "artifacts",
        help="List known materialized artifacts for one workflow.",
    )
    artifacts_parser.add_argument("target", help="workflow_id, workflow workspace directory, or workflow.json path")
    artifacts_parser.add_argument("--workflow-root", help="Root that directly contains workflow workspaces.")
    artifacts_parser.add_argument("--json", action="store_true", help="Print JSON output")
    artifacts_parser.set_defaults(func=cmd_workflow_artifacts)

    cancel_parser = workflow_subparsers.add_parser(
        "cancel",
        help="Cancel a materialized workflow and request queue cancellation for submitted engine stages.",
    )
    cancel_parser.add_argument("target", help="workflow_id, workflow workspace directory, or workflow.json path")
    cancel_parser.add_argument("--workflow-root", help="Root that directly contains workflow workspaces.")
    cancel_parser.add_argument("--chemstack-config", help="Path to shared chemstack.yaml; required if submitted stages exist")
    cancel_parser.add_argument("--json", action="store_true", help="Print JSON output")
    cancel_parser.set_defaults(func=cmd_workflow_cancel)

    reindex_parser = workflow_subparsers.add_parser("reindex", help="Rebuild the workflow registry from workflow workspaces.")
    reindex_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    reindex_parser.add_argument("--json", action="store_true", help="Print JSON output")
    reindex_parser.set_defaults(func=cmd_workflow_reindex)

    runtime_status_parser = workflow_subparsers.add_parser(
        "runtime-status",
        help="Show the current worker heartbeat/state for a workflow root.",
    )
    runtime_status_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    runtime_status_parser.add_argument("--json", action="store_true", help="Print JSON output")
    runtime_status_parser.set_defaults(func=cmd_workflow_runtime_status)

    journal_parser = workflow_subparsers.add_parser(
        "journal",
        help="Show recent append-only orchestration journal events.",
    )
    journal_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    journal_parser.add_argument("--limit", type=int, default=50, help="Maximum number of recent events to show")
    journal_parser.add_argument("--json", action="store_true", help="Print JSON output")
    journal_parser.set_defaults(func=cmd_workflow_journal)

    telemetry_parser = workflow_subparsers.add_parser(
        "telemetry",
        help="Summarize registry status, worker heartbeat, and recent journal activity.",
    )
    telemetry_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    telemetry_parser.add_argument("--limit", type=int, default=200, help="Maximum number of recent journal events to summarize")
    telemetry_parser.add_argument("--json", action="store_true", help="Print JSON output")
    telemetry_parser.set_defaults(func=cmd_workflow_telemetry)

    reaction_ts_parser = workflow_subparsers.add_parser(
        "reaction-ts-search",
        help="Build a reaction_ts_search workflow plan from xTB results.",
    )
    reaction_ts_parser.add_argument("target", help="xTB job_id or job directory")
    reaction_ts_parser.add_argument("--xtb-index-root", required=True, help="xTB index root, usually allowed_root")
    reaction_ts_parser.add_argument(
        "--max-orca-stages",
        type=int,
        default=3,
        help="Maximum number of ORCA stage payloads to emit",
    )
    reaction_ts_parser.add_argument(
        "--include-unselected",
        action="store_true",
        help="Consider non-selected xTB candidate_details when planning",
    )
    reaction_ts_parser.add_argument(
        "--workspace-root",
        help="If provided, materialize a workflow workspace with ORCA reaction directories and workflow.json",
    )
    reaction_ts_parser.add_argument("--charge", type=int, default=0, help="Charge for materialized ORCA inputs")
    reaction_ts_parser.add_argument("--multiplicity", type=int, default=1, help="Multiplicity for materialized ORCA inputs")
    reaction_ts_parser.add_argument("--max-cores", type=int, default=8, help="Maximum cores per planned ORCA task")
    reaction_ts_parser.add_argument("--max-memory-gb", type=int, default=32, help="Maximum memory GiB per planned ORCA task")
    reaction_ts_parser.add_argument(
        "--orca-route-line",
        default="! r2scan-3c OptTS Freq TightSCF",
        help="Route line for materialized ORCA inputs",
    )
    reaction_ts_parser.add_argument("--priority", type=int, default=10, help="Planned queue priority")
    reaction_ts_parser.add_argument("--json", action="store_true", help="Print JSON output")
    reaction_ts_parser.set_defaults(func=cmd_workflow_reaction_ts_search)

    conformer_parser = workflow_subparsers.add_parser(
        "conformer-screening",
        help="Build a conformer_screening workflow plan from CREST results (`standard` or `nci`).",
    )
    conformer_parser.add_argument("target", help="CREST job_id or job directory")
    conformer_parser.add_argument("--crest-index-root", required=True, help="CREST index root, usually allowed_root")
    conformer_parser.add_argument("--max-orca-stages", type=int, default=3, help="Maximum number of ORCA stage payloads to emit")
    conformer_parser.add_argument("--workspace-root", help="If provided, materialize a workflow workspace")
    conformer_parser.add_argument("--charge", type=int, default=0, help="Charge for materialized ORCA inputs")
    conformer_parser.add_argument("--multiplicity", type=int, default=1, help="Multiplicity for materialized ORCA inputs")
    conformer_parser.add_argument("--max-cores", type=int, default=8, help="Maximum cores per planned ORCA task")
    conformer_parser.add_argument("--max-memory-gb", type=int, default=32, help="Maximum memory GiB per planned ORCA task")
    conformer_parser.add_argument("--orca-route-line", default="! r2scan-3c Opt TightSCF", help="Route line for materialized ORCA inputs")
    conformer_parser.add_argument("--priority", type=int, default=10, help="Planned queue priority")
    conformer_parser.add_argument("--json", action="store_true", help="Print JSON output")
    conformer_parser.set_defaults(func=cmd_workflow_conformer_screening)

    create_reaction_parser = workflow_subparsers.add_parser(
        "create-reaction-ts-search",
        help="Create a raw-input reaction_ts_search workflow from reactant/product precomplex XYZ inputs.",
    )
    create_reaction_parser.add_argument(
        "--reactant-xyz",
        dest="reactant_xyz",
        required=True,
        help="Reactant-side precomplex XYZ input",
    )
    create_reaction_parser.add_argument(
        "--product-xyz",
        dest="product_xyz",
        required=True,
        help="Product-side XYZ input",
    )
    create_reaction_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    create_reaction_parser.add_argument("--crest-mode", default="standard", help="CREST mode for initial stages (`standard` or `nci`)")
    create_reaction_parser.add_argument("--priority", type=int, default=10)
    create_reaction_parser.add_argument("--max-cores", type=int, default=8)
    create_reaction_parser.add_argument("--max-memory-gb", type=int, default=32)
    create_reaction_parser.add_argument("--max-crest-candidates", type=int, default=3)
    create_reaction_parser.add_argument("--max-xtb-stages", type=int, default=3)
    create_reaction_parser.add_argument("--max-orca-stages", type=int, default=3)
    create_reaction_parser.add_argument("--orca-route-line", default="! r2scan-3c OptTS Freq TightSCF")
    create_reaction_parser.add_argument("--charge", type=int, default=0)
    create_reaction_parser.add_argument("--multiplicity", type=int, default=1)
    create_reaction_parser.add_argument("--json", action="store_true", help="Print JSON output")
    create_reaction_parser.set_defaults(func=cmd_workflow_create_reaction_ts_search)

    create_conformer_parser = workflow_subparsers.add_parser(
        "create-conformer-screening",
        help="Create a raw-input conformer_screening workflow that can be advanced through CREST and ORCA (`standard` or `nci`).",
    )
    create_conformer_parser.add_argument("--input-xyz", required=True, help="Input XYZ for the molecule to screen")
    create_conformer_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    create_conformer_parser.add_argument("--crest-mode", default="standard", help="CREST mode for the initial stage")
    create_conformer_parser.add_argument("--priority", type=int, default=10)
    create_conformer_parser.add_argument("--max-cores", type=int, default=8)
    create_conformer_parser.add_argument("--max-memory-gb", type=int, default=32)
    create_conformer_parser.add_argument("--max-orca-stages", type=int, default=3)
    create_conformer_parser.add_argument("--orca-route-line", default="! r2scan-3c Opt TightSCF")
    create_conformer_parser.add_argument("--charge", type=int, default=0)
    create_conformer_parser.add_argument("--multiplicity", type=int, default=1)
    create_conformer_parser.add_argument("--json", action="store_true", help="Print JSON output")
    create_conformer_parser.set_defaults(func=cmd_workflow_create_conformer_screening)

    advance_parser = workflow_subparsers.add_parser(
        "advance",
        help="Advance a materialized workflow by syncing/submitting actionable CREST, xTB, and ORCA stages.",
    )
    advance_parser.add_argument("target", help="workflow_id, workflow workspace directory, or workflow.json path")
    advance_parser.add_argument("--workflow-root", required=True, help="Root that directly contains workflow workspaces.")
    advance_parser.add_argument("--chemstack-config", help="Path to shared chemstack.yaml")
    advance_parser.add_argument("--no-submit", action="store_true", help="Only sync and append stages; do not submit newly actionable stages")
    advance_parser.add_argument("--json", action="store_true", help="Print JSON output")
    advance_parser.set_defaults(func=cmd_workflow_advance)

    worker_parser = workflow_subparsers.add_parser(
        "worker",
        help="Continuously advance non-terminal workflows from the registry.",
    )
    worker_parser.add_argument(
        "--workflow-root",
        help="Root that directly contains workflow workspaces. Defaults to workflow.root in chemstack.yaml.",
    )
    worker_parser.add_argument("--chemstack-config", help="Path to shared chemstack.yaml")
    worker_parser.add_argument("--no-submit", action="store_true", help="Only sync/append stages; do not submit newly actionable stages")
    worker_parser.add_argument("--once", action="store_true", help="Run exactly one orchestration cycle")
    worker_parser.add_argument("--max-cycles", type=int, default=0, help="Optional cycle limit; 0 means run forever")
    worker_parser.add_argument("--interval-seconds", type=float, default=30.0, help="Sleep interval between orchestration cycles")
    worker_parser.add_argument("--lock-timeout-seconds", type=float, default=5.0, help="How long to wait for the worker lock")
    worker_parser.add_argument("--refresh-registry", action="store_true", help="Reindex the workflow registry before the first cycle")
    worker_parser.add_argument("--refresh-each-cycle", action="store_true", help="Reindex the workflow registry before every cycle")
    worker_parser.add_argument("--json", action="store_true", help="Print JSON output")
    worker_parser.set_defaults(func=cmd_workflow_worker)

    submit_parser = workflow_subparsers.add_parser(
        "submit-reaction-ts-search",
        help="Submit a materialized reaction_ts_search workflow into chemstack ORCA.",
    )
    submit_parser.add_argument("target", help="workflow_id, workflow workspace directory, or workflow.json path")
    submit_parser.add_argument("--workflow-root", help="Root that directly contains workflow workspaces.")
    submit_parser.add_argument("--chemstack-config", required=True, help="Path to shared chemstack.yaml")
    submit_parser.add_argument("--resubmit", action="store_true", help="Retry stages already marked as submitted")
    submit_parser.add_argument("--json", action="store_true", help="Print JSON output")
    submit_parser.set_defaults(func=cmd_workflow_submit_reaction_ts_search)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
