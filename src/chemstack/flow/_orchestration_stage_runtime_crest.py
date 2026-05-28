from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import yaml

from ._orchestration_deps import OrchestrationDeps
from ._orchestration_stage_runtime_shared import (
    _apply_contract_status,
    _apply_submission_result,
    _engine_job_dir_contract_lookup,
    _load_contract_or_none,
    _manifest_override_mapping,
    _orchestration_context,
    _workflow_internal_runs_root,
)
from .state import workflow_workspace_internal_engine_paths


def ensure_crest_job_dir_impl(
    stage: dict[str, Any],
    *,
    crest_allowed_root: Path,
    workflow_id: str,
    deps: OrchestrationDeps | None = None,
) -> str:
    o = _orchestration_context(deps)
    task = stage["task"]
    payload = task["payload"]
    existing = o.stages._normalize_text(payload.get("job_dir"))
    if existing:
        return existing
    stage_id = o.stages._normalize_text(stage.get("stage_id"))
    job_dir = crest_allowed_root / stage_id
    job_dir.mkdir(parents=True, exist_ok=True)
    input_target = job_dir / "input.xyz"
    shutil.copy2(Path(payload["source_input_xyz"]).expanduser().resolve(), input_target)
    overrides = _manifest_override_mapping(payload.get("job_manifest_overrides"))
    task_resource_request = o.stages._coerce_mapping(task.get("resource_request"))
    manifest_payload: dict[str, Any] = {
        "mode": o.stages._normalize_text(payload.get("mode")) or "standard",
        "speed": "quick",
        "gfn": 2,
    }
    for key, value in overrides.items():
        if key == "input_xyz":
            continue
        manifest_payload[key] = value
    manifest_payload["resources"] = {
        "max_cores": o.stages._safe_int(task_resource_request.get("max_cores"), default=8),
        "max_memory_gb": o.stages._safe_int(task_resource_request.get("max_memory_gb"), default=32),
    }
    manifest_payload["input_xyz"] = "input.xyz"
    (job_dir / "crest_job.yaml").write_text(
        yaml.safe_dump(manifest_payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    payload["job_dir"] = str(job_dir)
    payload["selected_input_xyz"] = str(input_target)
    task["enqueue_payload"]["job_dir"] = str(job_dir)
    return str(job_dir)


def _submit_crest_stage(
    o: Any,
    stage: dict[str, Any],
    task: dict[str, Any],
    *,
    crest_runtime_paths: dict[str, Path],
    crest_config: str | None,
    workflow_id: str,
) -> None:
    job_dir = o.stages._ensure_crest_job_dir(
        stage,
        crest_allowed_root=crest_runtime_paths["allowed_root"],
        workflow_id=workflow_id,
    )
    submission = o.engines.submit_crest_job_dir(
        job_dir=job_dir,
        priority=int(task["enqueue_payload"].get("priority", 10) or 10),
        config_path=str(crest_config),
    )
    submission["submitted_at"] = o.persistence.now_utc_iso()
    task["submission_result"] = submission
    stage_metadata = stage.setdefault("metadata", {})
    if not isinstance(stage_metadata, dict):
        return
    _apply_submission_result(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        submission=submission,
        metadata_fields=(("queue_id", "queue_id"), ("child_job_id", "job_id")),
    )


def _load_crest_contract(
    o: Any,
    stage: dict[str, Any],
    task: dict[str, Any],
    *,
    crest_runtime_paths: dict[str, Path],
    crest_config: str | None,
) -> Any | None:
    payload = o.stages._task_payload_dict(task)
    lookup = _engine_job_dir_contract_lookup(
        o,
        stage,
        payload,
        runtime_paths=crest_runtime_paths,
        config_path=crest_config,
        engine="crest",
    )
    if lookup is None:
        return None
    target, index_root = lookup
    return _load_contract_or_none(
        o.engines.load_crest_artifact_contract,
        engine="crest",
        target=target,
        stage=stage,
        crest_index_root=index_root,
    )


def _apply_crest_contract(
    stage: dict[str, Any],
    task: dict[str, Any],
    contract: Any,
) -> None:
    _apply_contract_status(stage, task, contract.status)
    stage_metadata = stage.setdefault("metadata", {})
    if isinstance(stage_metadata, dict):
        stage_metadata["child_job_id"] = contract.job_id
        stage_metadata["latest_known_path"] = contract.latest_known_path
        stage_metadata["organized_output_dir"] = contract.organized_output_dir
    task_payload = task.setdefault("payload", {})
    if isinstance(task_payload, dict):
        task_payload["selected_input_xyz"] = contract.selected_input_xyz
    stage["output_artifacts"] = [
        {
            "kind": "crest_conformer",
            "path": path,
            "selected": index == 1,
            "metadata": {"rank": index, "mode": contract.mode},
        }
        for index, path in enumerate(contract.retained_conformer_paths, start=1)
    ]


def sync_crest_stage_impl(
    stage: dict[str, Any],
    *,
    crest_config: str | None,
    submit_ready: bool,
    workflow_id: str,
    workspace_dir: Path,
    deps: OrchestrationDeps | None = None,
) -> None:
    o = _orchestration_context(deps)
    task = stage.get("task")
    if not isinstance(task, dict) or o.stages._normalize_text(task.get("engine")) != "crest":
        return
    crest_runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="crest")
    if (
        o.stages._normalize_text(task.get("status")) == "planned"
        and submit_ready
        and o.stages._normalize_text(crest_config)
    ):
        _submit_crest_stage(
            o,
            stage,
            task,
            crest_runtime_paths=crest_runtime_paths,
            crest_config=crest_config,
            workflow_id=workflow_id,
        )
    contract = _load_crest_contract(
        o,
        stage,
        task,
        crest_runtime_paths=crest_runtime_paths,
        crest_config=crest_config,
    )
    if contract is not None:
        _apply_crest_contract(stage, task, contract)


def completed_crest_roles_impl(
    payload: dict[str, Any], *, deps: OrchestrationDeps | None = None
) -> dict[str, dict[str, Any]]:
    o = _orchestration_context(deps)
    latest_by_role: dict[str, dict[str, Any]] = {}
    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        task = stage.get("task")
        if not isinstance(task, dict) or o.stages._normalize_text(task.get("engine")) != "crest":
            continue
        task_payload = task.get("payload")
        role = o.stages._normalize_text((stage.get("metadata") or {}).get("input_role")).lower()
        if not role and isinstance(task_payload, dict):
            role = o.stages._normalize_text(task_payload.get("input_role")).lower()
        if role:
            latest_by_role[role] = stage
    rows: dict[str, dict[str, Any]] = {}
    for role, stage in latest_by_role.items():
        stage_status = o.stages._normalize_text(stage.get("status")).lower()
        task = stage.get("task")
        task_status = (
            o.stages._normalize_text((task or {}).get("status")).lower() if isinstance(task, dict) else ""
        )
        if stage_status == "completed" and task_status in {"", "completed"}:
            rows[role] = stage
    return rows


def completed_crest_stage_impl(
    stage: dict[str, Any],
    *,
    crest_config: str | None,
    deps: OrchestrationDeps | None = None,
) -> Any | None:
    o = _orchestration_context(deps)
    task = stage.get("task")
    if not isinstance(task, dict):
        return None
    payload = o.stages._task_payload_dict(task)
    job_dir_target = o.stages._normalize_text(payload.get("job_dir"))
    index_root = (
        _workflow_internal_runs_root(job_dir_target, engine="crest")
        or o.stages._load_config_root(crest_config, engine="crest")
        or (
            Path(job_dir_target).expanduser().resolve().parent
            if job_dir_target
            else Path(".").resolve().parent
        )
    )
    target = job_dir_target or o.stages._submission_target(stage)
    if not target:
        return None
    return _load_contract_or_none(
        o.engines.load_crest_artifact_contract,
        engine="crest",
        target=target,
        stage=stage,
        crest_index_root=index_root,
    )
