from __future__ import annotations

import argparse
import json as json
import time as time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE as CHEMSTACK_EXECUTABLE
from chemstack.core.config.files import (
    default_config_path_from_repo_root as default_config_path_from_repo_root,
    shared_workflow_root_from_config as shared_workflow_root_from_config,
)
from chemstack.core.utils import (
    file_lock as file_lock,
    now_utc_iso as now_utc_iso,
    timestamped_token as timestamped_token,
)

from . import cli_activity as _cli_activity
from . import cli_common as _cli_common
from . import cli_inspect as _cli_inspect
from . import cli_run_dir as _cli_run_dir
from . import cli_workflow as _cli_workflow
from . import cli_workflow_output as _cli_workflow_output
from .adapters import (
    load_crest_artifact_contract as load_crest_artifact_contract,
    load_xtb_artifact_contract as load_xtb_artifact_contract,
    select_xtb_downstream_inputs as select_xtb_downstream_inputs,
)
from .contracts import XtbDownstreamPolicy as XtbDownstreamPolicy
from .operations import (
    advance_materialized_workflow as advance_materialized_workflow,
    cancel_activity as cancel_activity,
    cancel_workflow as cancel_workflow,
    create_conformer_screening_workflow as create_conformer_screening_workflow,
    create_reaction_workflow as create_reaction_workflow,
    get_workflow as get_workflow,
    get_workflow_artifacts as get_workflow_artifacts,
    get_workflow_journal as get_workflow_journal,
    get_workflow_runtime_status as get_workflow_runtime_status,
    get_workflow_telemetry as get_workflow_telemetry,
    list_activities as list_activities,
    list_workflows as list_workflows,
)
from .registry import (
    append_workflow_journal_event as append_workflow_journal_event,
    reindex_workflow_registry as reindex_workflow_registry,
    write_workflow_worker_state as write_workflow_worker_state,
)
from .restart import restart_failed_workflow as restart_failed_workflow
from .run_dir_layout import (
    STANDARD_CONFORMER_INPUT_FILENAME as STANDARD_CONFORMER_INPUT_FILENAME,
    STANDARD_REACTION_PRODUCT_FILENAME as STANDARD_REACTION_PRODUCT_FILENAME,
    STANDARD_REACTION_REACTANT_FILENAME as STANDARD_REACTION_REACTANT_FILENAME,
    WORKFLOW_MANIFEST_FILENAMES as WORKFLOW_MANIFEST_FILENAMES,
    inspect_workflow_run_dir as inspect_workflow_run_dir,
)
from .runtime import (
    advance_workflow_registry_once as advance_workflow_registry_once,
    workflow_worker_lock_path as workflow_worker_lock_path,
)
from .submitters import submit_reaction_ts_search_workflow as submit_reaction_ts_search_workflow
from .workflows import (
    build_conformer_screening_plan_from_target as build_conformer_screening_plan_from_target,
    build_reaction_ts_search_plan_from_target as build_reaction_ts_search_plan_from_target,
)

_RunDirWorkflowConfig = _cli_run_dir._RunDirWorkflowConfig


@dataclass(frozen=True)
class _FlowCommonDeps:
    Path: Any
    _normalize_text: Any
    _project_root: Any
    _resolve_existing_path: Any
    _discover_workflow_root: Any
    default_config_path_from_repo_root: Any
    shared_workflow_root_from_config: Any


@dataclass(frozen=True)
class _FlowRunDirDeps:
    Path: Any
    _normalize_text: Any
    _normalize_workflow_type: Any
    _discover_workflow_root: Any
    WORKFLOW_MANIFEST_FILENAMES: Any
    inspect_workflow_run_dir: Any
    _load_run_dir_manifest: Any
    _manifest_mapping: Any
    _resolve_manifest_file_value: Any
    _resolve_engine_manifest: Any
    _resolve_endpoint_pairing_manifest: Any
    _resolve_run_dir_path: Any
    _resolve_text_option_with_section: Any
    _resolve_int_option: Any
    _resolve_int_option_with_section: Any
    _resolve_required_workflow_root: Any
    _safe_workflow_name: Any
    _preferred_run_dir_workflow_id: Any
    _unique_run_dir_workflow_id: Any
    _resolve_run_dir_common_workflow_kwargs: Any
    _print_created_workflow: Any
    _workflow_root_for_existing_run_dir: Any
    _print_restarted_workflow: Any
    _resolve_run_dir_workflow_type: Any
    _load_run_dir_workflow_config: Any
    _run_dir_workflow_id: Any
    _common_run_dir_workflow_kwargs: Any
    _create_reaction_run_dir_workflow: Any
    _create_conformer_run_dir_workflow: Any
    _create_run_dir_workflow: Any
    _restart_existing_run_dir_workflow: Any
    create_reaction_workflow: Any
    create_conformer_screening_workflow: Any
    restart_failed_workflow: Any


@dataclass(frozen=True)
class _FlowInspectDeps:
    load_xtb_artifact_contract: Any
    select_xtb_downstream_inputs: Any
    XtbDownstreamPolicy: Any
    load_crest_artifact_contract: Any


@dataclass(frozen=True)
class _FlowWorkflowDeps:
    _normalize_text: Any
    _project_root: Any
    _shared_chemstack_config: Any
    _workflow_root_from_args: Any
    _print_created_workflow: Any
    build_reaction_ts_search_plan_from_target: Any
    build_conformer_screening_plan_from_target: Any
    create_reaction_workflow: Any
    create_conformer_screening_workflow: Any
    advance_materialized_workflow: Any
    timestamped_token: Any
    file_lock: Any
    workflow_worker_lock_path: Any
    now_utc_iso: Any
    write_workflow_worker_state: Any
    append_workflow_journal_event: Any
    advance_workflow_registry_once: Any
    _emit_worker_payload: Any
    time: Any
    get_workflow_runtime_status: Any
    emit_workflow_runtime_status: Any
    get_workflow_journal: Any
    emit_workflow_journal: Any
    get_workflow_telemetry: Any
    emit_workflow_telemetry: Any
    default_config_path_from_repo_root: Any
    submit_reaction_ts_search_workflow: Any
    list_workflows: Any
    get_workflow: Any
    get_workflow_artifacts: Any
    cancel_workflow: Any
    reindex_workflow_registry: Any
    CHEMSTACK_EXECUTABLE: Any


@dataclass(frozen=True)
class _FlowActivityDeps:
    _shared_chemstack_config: Any
    list_activities: Any
    cancel_activity: Any
    CHEMSTACK_EXECUTABLE: Any


def _common_deps() -> _FlowCommonDeps:
    return _FlowCommonDeps(
        Path=Path,
        _normalize_text=_normalize_text,
        _project_root=_project_root,
        _resolve_existing_path=_resolve_existing_path,
        _discover_workflow_root=_discover_workflow_root,
        default_config_path_from_repo_root=default_config_path_from_repo_root,
        shared_workflow_root_from_config=shared_workflow_root_from_config,
    )


def _run_dir_deps() -> _FlowRunDirDeps:
    return _FlowRunDirDeps(
        Path=Path,
        _normalize_text=_normalize_text,
        _normalize_workflow_type=_normalize_workflow_type,
        _discover_workflow_root=_discover_workflow_root,
        WORKFLOW_MANIFEST_FILENAMES=WORKFLOW_MANIFEST_FILENAMES,
        inspect_workflow_run_dir=inspect_workflow_run_dir,
        _load_run_dir_manifest=_load_run_dir_manifest,
        _manifest_mapping=_manifest_mapping,
        _resolve_manifest_file_value=_resolve_manifest_file_value,
        _resolve_engine_manifest=_resolve_engine_manifest,
        _resolve_endpoint_pairing_manifest=_resolve_endpoint_pairing_manifest,
        _resolve_run_dir_path=_resolve_run_dir_path,
        _resolve_text_option_with_section=_resolve_text_option_with_section,
        _resolve_int_option=_resolve_int_option,
        _resolve_int_option_with_section=_resolve_int_option_with_section,
        _resolve_required_workflow_root=_resolve_required_workflow_root,
        _safe_workflow_name=_safe_workflow_name,
        _preferred_run_dir_workflow_id=_preferred_run_dir_workflow_id,
        _unique_run_dir_workflow_id=_unique_run_dir_workflow_id,
        _resolve_run_dir_common_workflow_kwargs=_resolve_run_dir_common_workflow_kwargs,
        _print_created_workflow=_print_created_workflow,
        _workflow_root_for_existing_run_dir=_workflow_root_for_existing_run_dir,
        _print_restarted_workflow=_print_restarted_workflow,
        _resolve_run_dir_workflow_type=_resolve_run_dir_workflow_type,
        _load_run_dir_workflow_config=_load_run_dir_workflow_config,
        _run_dir_workflow_id=_run_dir_workflow_id,
        _common_run_dir_workflow_kwargs=_common_run_dir_workflow_kwargs,
        _create_reaction_run_dir_workflow=_create_reaction_run_dir_workflow,
        _create_conformer_run_dir_workflow=_create_conformer_run_dir_workflow,
        _create_run_dir_workflow=_create_run_dir_workflow,
        _restart_existing_run_dir_workflow=_restart_existing_run_dir_workflow,
        create_reaction_workflow=create_reaction_workflow,
        create_conformer_screening_workflow=create_conformer_screening_workflow,
        restart_failed_workflow=restart_failed_workflow,
    )


def _inspect_deps() -> _FlowInspectDeps:
    return _FlowInspectDeps(
        load_xtb_artifact_contract=load_xtb_artifact_contract,
        select_xtb_downstream_inputs=select_xtb_downstream_inputs,
        XtbDownstreamPolicy=XtbDownstreamPolicy,
        load_crest_artifact_contract=load_crest_artifact_contract,
    )


def _workflow_deps() -> _FlowWorkflowDeps:
    return _FlowWorkflowDeps(
        _normalize_text=_normalize_text,
        _project_root=_project_root,
        _shared_chemstack_config=_shared_chemstack_config,
        _workflow_root_from_args=_workflow_root_from_args,
        _print_created_workflow=_print_created_workflow,
        build_reaction_ts_search_plan_from_target=build_reaction_ts_search_plan_from_target,
        build_conformer_screening_plan_from_target=build_conformer_screening_plan_from_target,
        create_reaction_workflow=create_reaction_workflow,
        create_conformer_screening_workflow=create_conformer_screening_workflow,
        advance_materialized_workflow=advance_materialized_workflow,
        timestamped_token=timestamped_token,
        file_lock=file_lock,
        workflow_worker_lock_path=workflow_worker_lock_path,
        now_utc_iso=now_utc_iso,
        write_workflow_worker_state=write_workflow_worker_state,
        append_workflow_journal_event=append_workflow_journal_event,
        advance_workflow_registry_once=advance_workflow_registry_once,
        _emit_worker_payload=_emit_worker_payload,
        time=time,
        get_workflow_runtime_status=get_workflow_runtime_status,
        emit_workflow_runtime_status=_cli_workflow_output.emit_workflow_runtime_status,
        get_workflow_journal=get_workflow_journal,
        emit_workflow_journal=_cli_workflow_output.emit_workflow_journal,
        get_workflow_telemetry=get_workflow_telemetry,
        emit_workflow_telemetry=_cli_workflow_output.emit_workflow_telemetry,
        default_config_path_from_repo_root=default_config_path_from_repo_root,
        submit_reaction_ts_search_workflow=submit_reaction_ts_search_workflow,
        list_workflows=list_workflows,
        get_workflow=get_workflow,
        get_workflow_artifacts=get_workflow_artifacts,
        cancel_workflow=cancel_workflow,
        reindex_workflow_registry=reindex_workflow_registry,
        CHEMSTACK_EXECUTABLE=CHEMSTACK_EXECUTABLE,
    )


def _activity_deps() -> _FlowActivityDeps:
    return _FlowActivityDeps(
        _shared_chemstack_config=_shared_chemstack_config,
        list_activities=list_activities,
        cancel_activity=cancel_activity,
        CHEMSTACK_EXECUTABLE=CHEMSTACK_EXECUTABLE,
    )


def _normalize_text(value: Any) -> str:
    return _cli_common._normalize_text(value)


def _project_root() -> Path:
    return _cli_common._project_root(deps=_common_deps())


def _resolve_existing_path(path_text: str) -> Path | None:
    return _cli_common._resolve_existing_path(path_text, deps=_common_deps())


def _discover_workflow_root(
    explicit: str | Path | None,
    *,
    config_path: str | Path | None = None,
) -> str | None:
    return _cli_common._discover_workflow_root(
        explicit, config_path=config_path, deps=_common_deps()
    )


def _shared_chemstack_config(args: Any) -> str | None:
    return _cli_common._shared_chemstack_config(args, deps=_common_deps())


def _workflow_root_from_args(args: Any, *, config_path: str | None = None) -> str | None:
    return _cli_common._workflow_root_from_args(args, config_path=config_path, deps=_common_deps())


def _normalize_workflow_type(value: Any) -> str:
    return _cli_common._normalize_workflow_type(value, deps=_common_deps())


def _load_run_dir_manifest(workflow_dir: Path) -> dict[str, Any]:
    return _cli_run_dir._load_run_dir_manifest(workflow_dir, deps=_run_dir_deps())


def _manifest_mapping(value: Any) -> dict[str, Any]:
    return _cli_run_dir._manifest_mapping(value, deps=_run_dir_deps())


def _resolve_manifest_file_value(workflow_dir: Path, value: Any) -> str:
    return _cli_run_dir._resolve_manifest_file_value(workflow_dir, value, deps=_run_dir_deps())


def _resolve_engine_manifest(
    workflow_dir: Path, manifest: dict[str, Any], key: str
) -> dict[str, Any]:
    return _cli_run_dir._resolve_engine_manifest(workflow_dir, manifest, key, deps=_run_dir_deps())


def _resolve_endpoint_pairing_manifest(
    manifest: dict[str, Any],
    xtb_manifest: dict[str, Any],
) -> dict[str, Any]:
    return _cli_run_dir._resolve_endpoint_pairing_manifest(
        manifest, xtb_manifest, deps=_run_dir_deps()
    )


def _resolve_run_dir_path(
    workflow_dir: Path,
    *,
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    default_names: tuple[str, ...],
) -> str:
    return _cli_run_dir._resolve_run_dir_path(
        workflow_dir,
        explicit=explicit,
        manifest=manifest,
        key=key,
        default_names=default_names,
        deps=_run_dir_deps(),
    )


def _resolve_text_option_with_section(
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    section: dict[str, Any],
    section_key: str,
    default: str,
) -> str:
    return _cli_run_dir._resolve_text_option_with_section(
        explicit, manifest, key, section, section_key, default, deps=_run_dir_deps()
    )


def _resolve_int_option(explicit: Any, manifest: dict[str, Any], key: str, default: int) -> int:
    return _cli_run_dir._resolve_int_option(explicit, manifest, key, default, deps=_run_dir_deps())


def _resolve_int_option_with_section(
    explicit: Any,
    manifest: dict[str, Any],
    key: str,
    section: dict[str, Any],
    section_key: str,
    default: int,
) -> int:
    return _cli_run_dir._resolve_int_option_with_section(
        explicit, manifest, key, section, section_key, default, deps=_run_dir_deps()
    )


def _resolve_required_workflow_root(args: Any, manifest: dict[str, Any]) -> str:
    return _cli_run_dir._resolve_required_workflow_root(args, manifest, deps=_run_dir_deps())


def _safe_workflow_name(value: Any, *, fallback: str) -> str:
    return _cli_run_dir._safe_workflow_name(value, fallback=fallback, deps=_run_dir_deps())


def _preferred_run_dir_workflow_id(workflow_dir: Path, *, workflow_type: str) -> str:
    return _cli_run_dir._preferred_run_dir_workflow_id(
        workflow_dir, workflow_type=workflow_type, deps=_run_dir_deps()
    )


def _unique_run_dir_workflow_id(
    workflow_dir: Path,
    *,
    workflow_root: str | Path,
    workflow_type: str,
) -> str:
    return _cli_run_dir._unique_run_dir_workflow_id(
        workflow_dir,
        workflow_root=workflow_root,
        workflow_type=workflow_type,
        deps=_run_dir_deps(),
    )


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
    return _cli_run_dir._resolve_run_dir_common_workflow_kwargs(
        args,
        manifest,
        resources_manifest=resources_manifest,
        crest_manifest=crest_manifest,
        orca_manifest=orca_manifest,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        deps=_run_dir_deps(),
    )


def _print_created_workflow(payload: dict[str, Any], *, json_mode: bool) -> int:
    return _cli_run_dir._print_created_workflow(payload, json_mode=json_mode)


def _workflow_root_for_existing_run_dir(args: Any, workflow_dir: Path) -> Path:
    return _cli_run_dir._workflow_root_for_existing_run_dir(
        args, workflow_dir, deps=_run_dir_deps()
    )


def _print_restarted_workflow(payload: dict[str, Any], *, json_mode: bool) -> int:
    return _cli_run_dir._print_restarted_workflow(payload, json_mode=json_mode)


def _resolve_run_dir_workflow_type(
    args: Any, manifest: dict[str, Any], workflow_layout: Any
) -> str:
    return _cli_run_dir._resolve_run_dir_workflow_type(
        args, manifest, workflow_layout, deps=_run_dir_deps()
    )


def _load_run_dir_workflow_config(args: Any, workflow_dir: Path) -> _RunDirWorkflowConfig:
    return _cli_run_dir._load_run_dir_workflow_config(args, workflow_dir, deps=_run_dir_deps())


def _run_dir_workflow_id(config: _RunDirWorkflowConfig, workflow_root: str) -> str:
    return _cli_run_dir._run_dir_workflow_id(config, workflow_root, deps=_run_dir_deps())


def _common_run_dir_workflow_kwargs(
    args: Any,
    config: _RunDirWorkflowConfig,
    *,
    workflow_root: str,
    default_orca_route_line: str,
    default_max_orca_stages: int,
) -> dict[str, Any]:
    return _cli_run_dir._common_run_dir_workflow_kwargs(
        args,
        config,
        workflow_root=workflow_root,
        default_orca_route_line=default_orca_route_line,
        default_max_orca_stages=default_max_orca_stages,
        deps=_run_dir_deps(),
    )


def _create_reaction_run_dir_workflow(args: Any, config: _RunDirWorkflowConfig) -> dict[str, Any]:
    return _cli_run_dir._create_reaction_run_dir_workflow(args, config, deps=_run_dir_deps())


def _create_conformer_run_dir_workflow(args: Any, config: _RunDirWorkflowConfig) -> dict[str, Any]:
    return _cli_run_dir._create_conformer_run_dir_workflow(args, config, deps=_run_dir_deps())


def _create_run_dir_workflow(args: Any, workflow_dir: Path) -> dict[str, Any]:
    return _cli_run_dir._create_run_dir_workflow(args, workflow_dir, deps=_run_dir_deps())


def _restart_existing_run_dir_workflow(args: Any, workflow_dir: Path) -> dict[str, Any]:
    return _cli_run_dir._restart_existing_run_dir_workflow(args, workflow_dir, deps=_run_dir_deps())


def cmd_run_dir(args: Any) -> int:
    return _cli_run_dir.cmd_run_dir(args, deps=_run_dir_deps())


def cmd_xtb_inspect(args: Any) -> int:
    return _cli_inspect.cmd_xtb_inspect(args, deps=_inspect_deps())


def cmd_xtb_candidates(args: Any) -> int:
    return _cli_inspect.cmd_xtb_candidates(args, deps=_inspect_deps())


def cmd_crest_inspect(args: Any) -> int:
    return _cli_inspect.cmd_crest_inspect(args, deps=_inspect_deps())


def cmd_workflow_reaction_ts_search(args: Any) -> int:
    return _cli_workflow.cmd_workflow_reaction_ts_search(args, deps=_workflow_deps())


def cmd_workflow_conformer_screening(args: Any) -> int:
    return _cli_workflow.cmd_workflow_conformer_screening(args, deps=_workflow_deps())


def cmd_workflow_create_reaction_ts_search(args: Any) -> int:
    return _cli_workflow.cmd_workflow_create_reaction_ts_search(args, deps=_workflow_deps())


def cmd_workflow_create_conformer_screening(args: Any) -> int:
    return _cli_workflow.cmd_workflow_create_conformer_screening(args, deps=_workflow_deps())


def cmd_workflow_advance(args: Any) -> int:
    return _cli_workflow.cmd_workflow_advance(args, deps=_workflow_deps())


def _emit_worker_payload(payload: dict[str, Any], *, json_mode: bool, single_cycle: bool) -> None:
    return _cli_workflow._emit_worker_payload(
        payload, json_mode=json_mode, single_cycle=single_cycle, deps=_workflow_deps()
    )


def cmd_workflow_worker(args: Any) -> int:
    return _cli_workflow.cmd_workflow_worker(args, deps=_workflow_deps())


def cmd_workflow_runtime_status(args: Any) -> int:
    return _cli_workflow.cmd_workflow_runtime_status(args, deps=_workflow_deps())


def cmd_workflow_journal(args: Any) -> int:
    return _cli_workflow.cmd_workflow_journal(args, deps=_workflow_deps())


def cmd_workflow_telemetry(args: Any) -> int:
    return _cli_workflow.cmd_workflow_telemetry(args, deps=_workflow_deps())


def cmd_workflow_submit_reaction_ts_search(args: Any) -> int:
    return _cli_workflow.cmd_workflow_submit_reaction_ts_search(args, deps=_workflow_deps())


def cmd_activity_list(args: Any) -> int:
    return _cli_activity.cmd_activity_list(args, deps=_activity_deps())


def cmd_activity_cancel(args: Any) -> int:
    return _cli_activity.cmd_activity_cancel(args, deps=_activity_deps())


def cmd_bot(args: Any) -> int:
    return _cli_workflow.cmd_bot(args, deps=_workflow_deps())


def cmd_workflow_list(args: Any) -> int:
    return _cli_workflow.cmd_workflow_list(args, deps=_workflow_deps())


def cmd_workflow_get(args: Any) -> int:
    return _cli_workflow.cmd_workflow_get(args, deps=_workflow_deps())


def cmd_workflow_artifacts(args: Any) -> int:
    return _cli_workflow.cmd_workflow_artifacts(args, deps=_workflow_deps())


def cmd_workflow_cancel(args: Any) -> int:
    return _cli_workflow.cmd_workflow_cancel(args, deps=_workflow_deps())


def cmd_workflow_reindex(args: Any) -> int:
    return _cli_workflow.cmd_workflow_reindex(args, deps=_workflow_deps())


def build_parser() -> argparse.ArgumentParser:
    from chemstack.flow.cli_parsers import build_parser as _build_parser

    return _build_parser()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
