from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import yaml

from .state import workflow_workspace_internal_engine_paths
from .xyz_utils import load_xyz_frames


def _orchestration_module():
    from . import orchestration as o

    return o


def _call_engine_aware(func: Any, config_path: str | None, *, engine: str) -> Any:
    try:
        return func(config_path, engine=engine)
    except TypeError as exc:
        if "engine" not in str(exc):
            raise
        return func(config_path)


def _workflow_internal_runs_root(path_text: str, *, engine: str) -> Path | None:
    text = str(path_text).strip()
    if not text:
        return None
    try:
        path = Path(text).expanduser().resolve()
    except OSError:
        return None

    engine_text = str(engine).strip().lower()
    for candidate in (path, *path.parents):
        if (
            candidate.name == "runs"
            and candidate.parent.name == engine_text
            and candidate.parent.parent.name == "internal"
        ):
            return candidate
    return None


def _workflow_internal_organized_root(path_text: str, *, engine: str) -> Path | None:
    runs_root = _workflow_internal_runs_root(path_text, engine=engine)
    if runs_root is None:
        return None
    try:
        return workflow_workspace_internal_engine_paths(runs_root.parents[2], engine=engine)["organized_root"]
    except (IndexError, ValueError):
        return None


def _manifest_override_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if str(key).strip()}


def _materialize_xtb_override_xcontrol(
    job_dir: Path,
    *,
    overrides: dict[str, Any],
    fallback_name: str = "workflow_xcontrol.inp",
) -> str:
    xcontrol_file = str(overrides.get("xcontrol_file", "")).strip()
    xcontrol_text = str(overrides.get("xcontrol_text", "")).strip()
    xcontrol_lines_value = overrides.get("xcontrol_lines")
    target_name = str(overrides.get("xcontrol", "")).strip() or fallback_name

    if xcontrol_file:
        source = Path(xcontrol_file).expanduser().resolve()
        if source.exists() and source.is_file():
            shutil.copy2(source, job_dir / target_name)
            return target_name

    lines: list[str] = []
    if isinstance(xcontrol_lines_value, (list, tuple)):
        lines = [str(item) for item in xcontrol_lines_value]
    elif isinstance(xcontrol_lines_value, str) and xcontrol_lines_value.strip():
        lines = xcontrol_lines_value.splitlines()
    elif xcontrol_text:
        lines = xcontrol_text.splitlines()

    if lines:
        (job_dir / target_name).write_text("\n".join(lines) + "\n", encoding="utf-8")
        return target_name

    return ""


def _stage_input_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _stage_input_rank(source: dict[str, Any]) -> int:
    rank = 1
    try:
        rank = int(source.get("rank", 1))
    except (TypeError, ValueError):
        rank = 1
    return max(1, rank)


def _materialize_xtb_stage_input(source: dict[str, Any], target: Path) -> str:
    source_path = Path(str(source.get("artifact_path", "")).strip()).expanduser().resolve()
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(f"xTB workflow input artifact not found: {source_path}")

    metadata = _stage_input_mapping(source.get("metadata"))
    try:
        frame_index = int(metadata.get("source_frame_index", 0) or 0)
    except (TypeError, ValueError):
        frame_index = 0

    target.parent.mkdir(parents=True, exist_ok=True)
    if frame_index > 0:
        frames = load_xyz_frames(source_path)
        if frame_index > len(frames):
            raise ValueError(
                f"Requested CREST frame {frame_index} is unavailable in retained artifact: {source_path}"
            )
        target.write_text(frames[frame_index - 1].render(), encoding="utf-8")
        return str(target.resolve())

    shutil.copy2(source_path, target)
    return str(target.resolve())


def xtb_attempt_rows_impl(stage: dict[str, Any]) -> list[dict[str, Any]]:
    o = _orchestration_module()
    metadata = o._stage_metadata(stage)
    attempts = metadata.get("xtb_attempts")
    if isinstance(attempts, list):
        filtered = [item for item in attempts if isinstance(item, dict)]
        metadata["xtb_attempts"] = filtered
        return filtered
    metadata["xtb_attempts"] = []
    return metadata["xtb_attempts"]


def xtb_attempt_record_impl(stage: dict[str, Any], *, attempt_number: int) -> dict[str, Any]:
    o = _orchestration_module()
    rows = o._xtb_attempt_rows(stage)
    for row in rows:
        if o._safe_int(row.get("attempt_number"), default=-1) == int(attempt_number):
            return row
    record = {"attempt_number": int(attempt_number)}
    rows.append(record)
    rows.sort(key=lambda item: o._safe_int(item.get("attempt_number"), default=0))
    return record


def xtb_retry_recipe_impl(attempt_number: int) -> dict[str, Any]:
    attempt = max(0, int(attempt_number))
    if attempt <= 0:
        return {
            "attempt_number": 0,
            "recipe_id": "baseline",
            "recipe_label": "baseline",
            "namespace": "",
            "xcontrol_name": "",
            "xcontrol_lines": (),
        }
    if attempt == 1:
        return {
            "attempt_number": 1,
            "recipe_id": "path_input_recommended",
            "recipe_label": "recommended_path_input",
            "namespace": "retry_01",
            "xcontrol_name": "path_retry_01.inp",
            "xcontrol_lines": (
                "$path",
                "   nrun=1",
                "   npoint=25",
                "   anopt=10",
                "   kpush=0.003",
                "   kpull=-0.015",
                "   ppull=0.05",
                "   alp=1.2",
                "$end",
            ),
        }
    return {
        "attempt_number": attempt,
        "recipe_id": "path_input_refined",
        "recipe_label": "refined_path_input",
        "namespace": f"retry_{attempt:02d}",
        "xcontrol_name": f"path_retry_{attempt:02d}.inp",
        "xcontrol_lines": (
            "$path",
            "   nrun=2",
            "   npoint=35",
            "   anopt=15",
            "   kpush=0.003",
            "   kpull=-0.015",
            "   ppull=0.05",
            "   alp=1.2",
            "$end",
        ),
    }


def xtb_path_retry_limit_impl(stage: dict[str, Any]) -> int:
    o = _orchestration_module()
    task = stage.get("task")
    if not isinstance(task, dict):
        return 2
    payload = o._task_payload_dict(task)
    metadata = o._coerce_mapping(task.get("metadata"))
    return max(
        0,
        o._safe_int(
            payload.get("max_handoff_retries", metadata.get("max_handoff_retries", 2)),
            default=2,
        ),
    )


def xtb_current_attempt_number_impl(stage: dict[str, Any]) -> int:
    o = _orchestration_module()
    metadata = o._stage_metadata(stage)
    current = o._safe_int(metadata.get("xtb_active_attempt_number"), default=-1)
    if current >= 0:
        return current
    attempts = o._xtb_attempt_rows(stage)
    if attempts:
        return max(o._safe_int(item.get("attempt_number"), default=0) for item in attempts)
    return 0


def write_xtb_path_job_impl(
    stage: dict[str, Any],
    *,
    xtb_allowed_root: Path,
    workflow_id: str,
    attempt_number: int,
) -> str:
    o = _orchestration_module()
    task = stage["task"]
    payload = o._task_payload_dict(task)
    recipe = o._xtb_retry_recipe(attempt_number)
    stage_id = o._normalize_text(stage.get("stage_id"))
    base_dir = xtb_allowed_root / stage_id
    job_dir = base_dir if attempt_number == 0 else base_dir / f"retry_attempt_{attempt_number:02d}"

    reactants_dir = job_dir / "reactants"
    products_dir = job_dir / "products"
    reactants_dir.mkdir(parents=True, exist_ok=True)
    products_dir.mkdir(parents=True, exist_ok=True)

    reactant_source = _stage_input_mapping(payload.get("reactant_source"))
    product_source = _stage_input_mapping(payload.get("product_source"))
    reactant_name = f"r{_stage_input_rank(reactant_source)}.xyz"
    product_name = f"p{_stage_input_rank(product_source)}.xyz"
    reactant_target = reactants_dir / reactant_name
    product_target = products_dir / product_name
    _materialize_xtb_stage_input(reactant_source, reactant_target)
    _materialize_xtb_stage_input(product_source, product_target)

    xcontrol_name = o._normalize_text(recipe.get("xcontrol_name"))
    if xcontrol_name:
        (job_dir / xcontrol_name).write_text(
            "\n".join(str(line) for line in recipe.get("xcontrol_lines", ())) + "\n",
            encoding="utf-8",
        )

    overrides = _manifest_override_mapping(payload.get("job_manifest_overrides"))
    task_resource_request = o._coerce_mapping(task.get("resource_request"))
    manifest_payload: dict[str, Any] = {
        "job_type": "path_search",
        "gfn": 2,
        "charge": 0,
        "uhf": 0,
    }
    for key, value in overrides.items():
        if key in {
            "job_type",
            "reaction_key",
            "reactant_xyz",
            "product_xyz",
            "xcontrol",
            "xcontrol_file",
            "xcontrol_text",
            "xcontrol_lines",
        }:
            continue
        manifest_payload[key] = value
    manifest_payload["resources"] = {
        "max_cores": o._safe_int(task_resource_request.get("max_cores"), default=8),
        "max_memory_gb": o._safe_int(task_resource_request.get("max_memory_gb"), default=32),
    }

    namespace = o._normalize_text(recipe.get("namespace")) or str(overrides.get("namespace", "")).strip()
    xcontrol_override_name = ""
    if not xcontrol_name:
        xcontrol_override_name = _materialize_xtb_override_xcontrol(job_dir, overrides=overrides)
    selected_xcontrol_name = xcontrol_name or xcontrol_override_name

    manifest_payload["reaction_key"] = o._normalize_text(payload.get("reaction_key")) or stage_id
    manifest_payload["reactant_xyz"] = reactant_target.name
    manifest_payload["product_xyz"] = product_target.name
    if namespace:
        manifest_payload["namespace"] = namespace
    if selected_xcontrol_name:
        manifest_payload["xcontrol"] = selected_xcontrol_name

    (job_dir / "xtb_job.yaml").write_text(
        yaml.safe_dump(manifest_payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    payload["job_dir"] = str(job_dir)
    payload["selected_input_xyz"] = str(reactant_target)
    payload["secondary_input_xyz"] = str(product_target)
    payload["xtb_active_attempt_number"] = int(attempt_number)
    payload["xtb_retry_recipe_id"] = o._normalize_text(recipe.get("recipe_id"))
    task["enqueue_payload"]["job_dir"] = str(job_dir)
    task["enqueue_payload"]["reaction_key"] = o._normalize_text(payload.get("reaction_key"))
    stage_metadata = o._stage_metadata(stage)
    stage_metadata["xtb_active_attempt_number"] = int(attempt_number)
    stage_metadata["xtb_retry_recipe_id"] = o._normalize_text(recipe.get("recipe_id"))
    stage_metadata["xtb_retry_recipe_label"] = o._normalize_text(recipe.get("recipe_label"))

    attempt_record = o._xtb_attempt_record(stage, attempt_number=attempt_number)
    attempt_record.update(
        {
            "attempt_number": int(attempt_number),
            "recipe_id": o._normalize_text(recipe.get("recipe_id")),
            "recipe_label": o._normalize_text(recipe.get("recipe_label")),
            "job_dir": str(job_dir),
            "manifest_path": str((job_dir / "xtb_job.yaml").resolve()),
            "xcontrol_path": str((job_dir / selected_xcontrol_name).resolve()) if selected_xcontrol_name else "",
            "namespace": namespace,
            "reaction_key": o._normalize_text(payload.get("reaction_key")),
        }
    )
    return str(job_dir)


def xtb_handoff_status_impl(contract: Any) -> dict[str, str]:
    o = _orchestration_module()
    inputs = o.select_xtb_downstream_inputs(
        contract,
        policy=o.XtbDownstreamPolicy.build(
            preferred_kinds=("ts_guess",),
            allowed_kinds=("ts_guess",),
            max_candidates=1,
            selected_only=False,
            fallback_to_selected_paths=False,
        ),
        require_geometry=True,
    )
    if inputs:
        return {
            "status": "ready",
            "reason": "",
            "message": "",
            "artifact_path": o._normalize_text(inputs[0].artifact_path),
        }
    error = o._reaction_ts_guess_error(contract)
    return {
        "status": "failed",
        "reason": error["reason"],
        "message": error["message"],
        "artifact_path": "",
    }


def stage_has_xtb_candidates_impl(stage: dict[str, Any]) -> bool:
    o = _orchestration_module()
    artifacts = stage.get("output_artifacts")
    if not isinstance(artifacts, list):
        return False
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        if o._normalize_text(artifact.get("kind")) != "xtb_candidate":
            continue
        if o._normalize_text(artifact.get("path")):
            return True
    return False


def append_unique_artifact_impl(
    rows: list[dict[str, Any]],
    *,
    kind: str,
    path: str,
    selected: bool = False,
    metadata: dict[str, Any] | None = None,
) -> None:
    o = _orchestration_module()
    path_text = o._normalize_text(path)
    if not path_text:
        return
    key = (o._normalize_text(kind), path_text)
    seen = {
        (o._normalize_text(item.get("kind")), o._normalize_text(item.get("path")))
        for item in rows
        if isinstance(item, dict)
    }
    if key in seen:
        return
    rows.append(
        {
            "kind": o._normalize_text(kind) or "artifact",
            "path": path_text,
            "selected": bool(selected),
            "metadata": dict(metadata or {}),
        }
    )


def ensure_crest_job_dir_impl(stage: dict[str, Any], *, crest_allowed_root: Path, workflow_id: str) -> str:
    o = _orchestration_module()
    task = stage["task"]
    payload = task["payload"]
    existing = o._normalize_text(payload.get("job_dir"))
    if existing:
        return existing
    stage_id = o._normalize_text(stage.get("stage_id"))
    job_dir = crest_allowed_root / stage_id
    job_dir.mkdir(parents=True, exist_ok=True)
    input_target = job_dir / "input.xyz"
    shutil.copy2(Path(payload["source_input_xyz"]).expanduser().resolve(), input_target)
    overrides = _manifest_override_mapping(payload.get("job_manifest_overrides"))
    task_resource_request = o._coerce_mapping(task.get("resource_request"))
    manifest_payload: dict[str, Any] = {
        "mode": o._normalize_text(payload.get("mode")) or "standard",
        "speed": "quick",
        "gfn": 2,
    }
    for key, value in overrides.items():
        if key == "input_xyz":
            continue
        manifest_payload[key] = value
    manifest_payload["resources"] = {
        "max_cores": o._safe_int(task_resource_request.get("max_cores"), default=8),
        "max_memory_gb": o._safe_int(task_resource_request.get("max_memory_gb"), default=32),
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


def ensure_xtb_job_dir_impl(stage: dict[str, Any], *, xtb_allowed_root: Path, workflow_id: str) -> str:
    o = _orchestration_module()
    task = stage["task"]
    payload = task["payload"]
    existing = o._normalize_text(payload.get("job_dir"))
    if existing:
        return existing
    return o._write_xtb_path_job(stage, xtb_allowed_root=xtb_allowed_root, workflow_id=workflow_id, attempt_number=0)


def sync_crest_stage_impl(
    stage: dict[str, Any],
    *,
    crest_auto_config: str | None,
    crest_auto_executable: str,
    crest_auto_repo_root: str | None,
    submit_ready: bool,
    workflow_id: str,
    workspace_dir: Path,
) -> None:
    o = _orchestration_module()
    task = stage.get("task")
    if not isinstance(task, dict):
        return
    if o._normalize_text(task.get("engine")) != "crest":
        return
    crest_runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="crest")
    if o._normalize_text(task.get("status")) == "planned" and submit_ready and o._normalize_text(crest_auto_config):
        job_dir = o._ensure_crest_job_dir(
            stage,
            crest_allowed_root=crest_runtime_paths["allowed_root"],
            workflow_id=workflow_id,
        )
        submission = o.submit_crest_job_dir(
            job_dir=job_dir,
            priority=int(task["enqueue_payload"].get("priority", 10) or 10),
            config_path=str(crest_auto_config),
            executable=crest_auto_executable,
            repo_root=crest_auto_repo_root,
        )
        submission["submitted_at"] = o.now_utc_iso()
        task["submission_result"] = submission
        task["status"] = "submitted" if submission["status"] == "submitted" else "submission_failed"
        stage["status"] = "queued" if submission["status"] == "submitted" else "submission_failed"
        stage.setdefault("metadata", {})
        if isinstance(stage["metadata"], dict):
            stage["metadata"]["queue_id"] = submission.get("queue_id", "")
            stage["metadata"]["child_job_id"] = submission.get("job_id", "")
    payload = o._task_payload_dict(task)
    job_dir_target = o._normalize_text(payload.get("job_dir"))
    index_root = (
        crest_runtime_paths["allowed_root"]
        or _call_engine_aware(o._load_config_root, crest_auto_config, engine="crest")
        or Path(job_dir_target or ".").resolve().parent
    )
    target = job_dir_target or o._submission_target(stage)
    if not target:
        return
    try:
        contract = o.load_crest_artifact_contract(crest_index_root=index_root, target=target)
    except Exception:
        return
    if contract.status != "unknown":
        task["status"] = contract.status
        stage["status"] = contract.status
    stage.setdefault("metadata", {})
    if isinstance(stage["metadata"], dict):
        stage["metadata"]["child_job_id"] = contract.job_id
        stage["metadata"]["latest_known_path"] = contract.latest_known_path
        stage["metadata"]["organized_output_dir"] = contract.organized_output_dir
    task.setdefault("payload", {})
    if isinstance(task["payload"], dict):
        task["payload"]["selected_input_xyz"] = contract.selected_input_xyz
    stage["output_artifacts"] = [
        {
            "kind": "crest_conformer",
            "path": path,
            "selected": index == 1,
            "metadata": {"rank": index, "mode": contract.mode},
        }
        for index, path in enumerate(contract.retained_conformer_paths, start=1)
    ]


def sync_xtb_stage_impl(
    stage: dict[str, Any],
    *,
    xtb_auto_config: str | None,
    xtb_auto_executable: str,
    xtb_auto_repo_root: str | None,
    submit_ready: bool,
    workflow_id: str,
    workspace_dir: Path,
) -> None:
    o = _orchestration_module()
    task = stage.get("task")
    if not isinstance(task, dict) or o._normalize_text(task.get("engine")) != "xtb":
        return
    stage_metadata = o._stage_metadata(stage)
    task_payload = o._task_payload_dict(task)
    xtb_runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="xtb")
    if o._normalize_text(task.get("status")) == "planned" and submit_ready and o._normalize_text(xtb_auto_config):
        job_dir = o._ensure_xtb_job_dir(
            stage,
            xtb_allowed_root=xtb_runtime_paths["allowed_root"],
            workflow_id=workflow_id,
        )
        submission = o.submit_xtb_job_dir(
            job_dir=job_dir,
            priority=int(task["enqueue_payload"].get("priority", 10) or 10),
            config_path=str(xtb_auto_config),
            executable=xtb_auto_executable,
            repo_root=xtb_auto_repo_root,
        )
        submission["submitted_at"] = o.now_utc_iso()
        task["submission_result"] = submission
        task["status"] = "submitted" if submission["status"] == "submitted" else "submission_failed"
        stage["status"] = "queued" if submission["status"] == "submitted" else "submission_failed"
        current_attempt = o._xtb_current_attempt_number(stage)
        attempt_record = o._xtb_attempt_record(stage, attempt_number=current_attempt)
        attempt_record["submission_status"] = submission.get("status", "")
        attempt_record["submitted_at"] = submission.get("submitted_at", "")
        attempt_record["queue_id"] = submission.get("queue_id", "")
        stage_metadata["queue_id"] = submission.get("queue_id", "")
        stage_metadata["child_job_id"] = submission.get("job_id", "")
        stage_metadata["xtb_handoff_status"] = "submitted"
    job_dir_target = o._normalize_text(task_payload.get("job_dir"))
    index_root = (
        xtb_runtime_paths["allowed_root"]
        or _call_engine_aware(o._load_config_root, xtb_auto_config, engine="xtb")
        or Path(job_dir_target or ".").resolve().parent
    )
    target = job_dir_target or o._submission_target(stage)
    if not target:
        return
    try:
        contract = o.load_xtb_artifact_contract(xtb_index_root=index_root, target=target)
    except Exception:
        return
    if contract.status != "unknown":
        task["status"] = contract.status
        stage["status"] = contract.status
    stage_metadata["child_job_id"] = contract.job_id
    stage_metadata["latest_known_path"] = contract.latest_known_path
    stage_metadata["organized_output_dir"] = contract.organized_output_dir
    task_payload["selected_input_xyz"] = contract.selected_input_xyz

    current_attempt = o._xtb_current_attempt_number(stage)
    handoff = o._xtb_handoff_status(contract) if o._normalize_text(task.get("task_kind")) == "path_search" else {
        "status": "",
        "reason": "",
        "message": "",
        "artifact_path": "",
    }
    attempt_record = o._xtb_attempt_record(stage, attempt_number=current_attempt)
    attempt_record.update(
        {
            "job_id": contract.job_id,
            "status": contract.status,
            "reason": contract.reason,
            "latest_known_path": contract.latest_known_path,
            "organized_output_dir": contract.organized_output_dir,
            "candidate_count": len(contract.candidate_details),
            "selected_candidate_paths": list(contract.selected_candidate_paths),
            "analysis_summary": dict(contract.analysis_summary),
            "handoff_status": handoff["status"],
            "handoff_reason": handoff["reason"],
            "handoff_message": handoff["message"],
            "completed_at": o._normalize_text(contract.analysis_summary.get("completed_at")),
        }
    )
    if handoff["status"]:
        stage_metadata["reaction_handoff_status"] = handoff["status"]
        if handoff["reason"]:
            stage_metadata["reaction_handoff_reason"] = handoff["reason"]
        else:
            stage_metadata.pop("reaction_handoff_reason", None)
        if handoff["message"]:
            stage_metadata["reaction_handoff_message"] = handoff["message"]
        else:
            stage_metadata.pop("reaction_handoff_message", None)
        if handoff["artifact_path"]:
            stage_metadata["reaction_handoff_artifact_path"] = handoff["artifact_path"]
        else:
            stage_metadata.pop("reaction_handoff_artifact_path", None)

    if (
        submit_ready
        and o._normalize_text(xtb_auto_config)
        and o._normalize_text(task.get("task_kind")) == "path_search"
        and handoff["status"] == "failed"
        and o._normalize_text(stage.get("status")).lower() in {"completed", "failed"}
    ):
        retries_used = o._safe_int(stage_metadata.get("xtb_handoff_retries_used"), default=0)
        retry_limit = o._xtb_path_retry_limit(stage)
        if retries_used < retry_limit:
            next_attempt = retries_used + 1
            retry_job_dir = o._write_xtb_path_job(
                stage,
                xtb_allowed_root=xtb_runtime_paths["allowed_root"],
                workflow_id=workflow_id,
                attempt_number=next_attempt,
            )
            submission = o.submit_xtb_job_dir(
                job_dir=retry_job_dir,
                priority=int(task["enqueue_payload"].get("priority", 10) or 10),
                config_path=str(xtb_auto_config),
                executable=xtb_auto_executable,
                repo_root=xtb_auto_repo_root,
            )
            submission["submitted_at"] = o.now_utc_iso()
            task["submission_result"] = submission
            task["status"] = "submitted" if submission["status"] == "submitted" else "submission_failed"
            stage["status"] = "queued" if submission["status"] == "submitted" else "submission_failed"
            stage_metadata["queue_id"] = submission.get("queue_id", "")
            stage_metadata["xtb_handoff_status"] = "retrying"
            stage_metadata["reaction_handoff_status"] = "retrying"
            stage_metadata["xtb_handoff_retries_used"] = next_attempt
            stage_metadata["xtb_handoff_retry_limit"] = retry_limit
            retry_record = o._xtb_attempt_record(stage, attempt_number=next_attempt)
            retry_record["submission_status"] = submission.get("status", "")
            retry_record["submitted_at"] = submission.get("submitted_at", "")
            retry_record["queue_id"] = submission.get("queue_id", "")
            retry_record["trigger_reason"] = handoff["reason"]
            retry_record["trigger_message"] = handoff["message"]
            return
    stage_metadata["xtb_handoff_retries_used"] = o._safe_int(stage_metadata.get("xtb_handoff_retries_used"), default=0)
    stage_metadata["xtb_handoff_retry_limit"] = o._xtb_path_retry_limit(stage)
    stage["output_artifacts"] = [
        {
            "kind": "xtb_candidate",
            "path": item.path,
            "selected": item.selected,
            "metadata": {"rank": item.rank, "kind": item.kind, "score": item.score, **dict(item.metadata)},
        }
        for item in contract.candidate_details
    ]


def sync_orca_stage_impl(
    stage: dict[str, Any],
    *,
    orca_auto_config: str | None,
    orca_auto_executable: str,
    orca_auto_repo_root: str | None,
    submit_ready: bool,
) -> None:
    o = _orchestration_module()
    task = stage.get("task")
    if not isinstance(task, dict) or o._normalize_text(task.get("engine")) != "orca":
        return
    enqueue_payload = task.get("enqueue_payload")
    if not isinstance(enqueue_payload, dict):
        return
    stage_metadata = o._stage_metadata(stage)
    task_payload = o._task_payload_dict(task)
    reaction_dir_hint = o._normalize_text(task_payload.get("reaction_dir") or enqueue_payload.get("reaction_dir"))
    if o._normalize_text(task.get("status")) == "planned" and submit_ready and o._normalize_text(orca_auto_config):
        resource_kwargs: dict[str, Any] = {}
        max_cores = o._safe_int(enqueue_payload.get("max_cores"), default=0)
        max_memory_gb = o._safe_int(enqueue_payload.get("max_memory_gb"), default=0)
        if max_cores > 0:
            resource_kwargs["max_cores"] = max_cores
        if max_memory_gb > 0:
            resource_kwargs["max_memory_gb"] = max_memory_gb
        submission = o.submit_reaction_dir(
            reaction_dir=str(enqueue_payload.get("reaction_dir", "")),
            priority=int(enqueue_payload.get("priority", 10) or 10),
            config_path=str(orca_auto_config),
            executable=orca_auto_executable,
            repo_root=orca_auto_repo_root,
            **resource_kwargs,
        )
        submission["submitted_at"] = o.now_utc_iso()
        task["submission_result"] = submission
        task["status"] = "submitted" if submission["status"] == "submitted" else "submission_failed"
        stage["status"] = "queued" if submission["status"] == "submitted" else "submission_failed"
        stage_metadata["queue_id"] = submission.get("queue_id", "")
        stage_metadata["submission_status"] = submission.get("status", "")
        stage_metadata["submitted_at"] = submission.get("submitted_at", "")

    allowed_root = _call_engine_aware(o._load_config_root, orca_auto_config, engine="orca")
    organized_root = (
        _workflow_internal_organized_root(reaction_dir_hint, engine="orca")
        or _call_engine_aware(o._load_config_organized_root, orca_auto_config, engine="orca")
    )
    target = (
        o._normalize_text(stage_metadata.get("run_id"))
        or reaction_dir_hint
        or o._normalize_text(stage_metadata.get("queue_id"))
    )
    if not target:
        return
    contract = o.load_orca_artifact_contract(
        target=target,
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
        queue_id=o._normalize_text(stage_metadata.get("queue_id")),
        run_id=o._normalize_text(stage_metadata.get("run_id")),
        reaction_dir=reaction_dir_hint,
    )
    if contract.status != "unknown":
        task["status"] = contract.status
        stage["status"] = contract.status

    task_payload["selected_inp"] = contract.selected_inp or o._normalize_text(task_payload.get("selected_inp"))
    if contract.selected_input_xyz:
        task_payload["selected_input_xyz"] = contract.selected_input_xyz
    if contract.last_out_path:
        task_payload["last_out_path"] = contract.last_out_path
    if contract.optimized_xyz_path:
        task_payload["optimized_xyz_path"] = contract.optimized_xyz_path

    stage_metadata["queue_id"] = contract.queue_id or o._normalize_text(stage_metadata.get("queue_id"))
    stage_metadata["run_id"] = contract.run_id or o._normalize_text(stage_metadata.get("run_id"))
    stage_metadata["queue_status"] = contract.queue_status
    stage_metadata["cancel_requested"] = bool(contract.cancel_requested)
    stage_metadata["latest_known_path"] = contract.latest_known_path
    stage_metadata["organized_output_dir"] = contract.organized_output_dir
    stage_metadata["optimized_xyz_path"] = contract.optimized_xyz_path
    stage_metadata["analyzer_status"] = contract.analyzer_status
    stage_metadata["reason"] = contract.reason
    stage_metadata["completed_at"] = contract.completed_at
    stage_metadata["state_status"] = contract.state_status
    stage_metadata["attempt_count"] = contract.attempt_count
    stage_metadata["max_retries"] = contract.max_retries
    stage_metadata["orca_attempts"] = [dict(item) for item in contract.attempts]
    stage_metadata["orca_final_result"] = dict(contract.final_result)
    if contract.state_status in {"running", "retrying"}:
        stage_metadata["orca_current_attempt_number"] = max(0, contract.attempt_count)
    elif contract.attempts:
        stage_metadata["orca_current_attempt_number"] = contract.attempts[-1].get("attempt_number")
    else:
        stage_metadata.pop("orca_current_attempt_number", None)
    if contract.attempts:
        last_attempt = contract.attempts[-1]
        stage_metadata["orca_latest_attempt_number"] = last_attempt.get("attempt_number")
        stage_metadata["orca_latest_attempt_status"] = last_attempt.get("analyzer_status")
        task_payload["orca_latest_attempt_inp"] = o._normalize_text(last_attempt.get("inp_path"))
        task_payload["orca_latest_attempt_out"] = o._normalize_text(last_attempt.get("out_path"))
    else:
        stage_metadata.pop("orca_latest_attempt_number", None)
        stage_metadata.pop("orca_latest_attempt_status", None)

    artifacts: list[dict[str, Any]] = []
    o._append_unique_artifact(
        artifacts,
        kind="orca_selected_inp",
        path=contract.selected_inp,
        selected=True,
        metadata={"run_id": contract.run_id},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_selected_input_xyz",
        path=contract.selected_input_xyz,
        metadata={"run_id": contract.run_id},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_optimized_xyz",
        path=contract.optimized_xyz_path,
        selected=contract.status == "completed",
        metadata={"run_id": contract.run_id},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_last_out",
        path=contract.last_out_path,
        selected=contract.status == "completed",
        metadata={"analyzer_status": contract.analyzer_status},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_run_state",
        path=contract.run_state_path,
        metadata={"status": contract.status},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_report_json",
        path=contract.report_json_path,
        metadata={"status": contract.status},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_report_md",
        path=contract.report_md_path,
        metadata={"status": contract.status},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_output_dir",
        path=contract.latest_known_path,
        selected=contract.status in {"completed", "failed", "cancelled"},
        metadata={"organized": bool(contract.organized_output_dir)},
    )
    o._append_unique_artifact(
        artifacts,
        kind="orca_organized_output_dir",
        path=contract.organized_output_dir,
        selected=bool(contract.organized_output_dir),
        metadata={"run_id": contract.run_id},
    )
    stage["output_artifacts"] = artifacts


def completed_crest_roles_impl(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    o = _orchestration_module()
    latest_by_role: dict[str, dict[str, Any]] = {}
    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        task = stage.get("task")
        if not isinstance(task, dict) or o._normalize_text(task.get("engine")) != "crest":
            continue
        task_payload = task.get("payload")
        role = o._normalize_text((stage.get("metadata") or {}).get("input_role")).lower()
        if not role and isinstance(task_payload, dict):
            role = o._normalize_text(task_payload.get("input_role")).lower()
        if role:
            latest_by_role[role] = stage
    rows: dict[str, dict[str, Any]] = {}
    for role, stage in latest_by_role.items():
        stage_status = o._normalize_text(stage.get("status")).lower()
        task = stage.get("task")
        task_status = o._normalize_text((task or {}).get("status")).lower() if isinstance(task, dict) else ""
        if stage_status == "completed" and task_status in {"", "completed"}:
            rows[role] = stage
    return rows


def completed_crest_stage_impl(stage: dict[str, Any], *, crest_auto_config: str | None) -> Any | None:
    o = _orchestration_module()
    task = stage.get("task")
    if not isinstance(task, dict):
        return None
    payload = o._task_payload_dict(task)
    job_dir_target = o._normalize_text(payload.get("job_dir"))
    index_root = (
        _workflow_internal_runs_root(job_dir_target, engine="crest")
        or _call_engine_aware(o._load_config_root, crest_auto_config, engine="crest")
        or (Path(job_dir_target).expanduser().resolve().parent if job_dir_target else Path(".").resolve().parent)
    )
    target = job_dir_target or o._submission_target(stage)
    if not target:
        return None
    try:
        return o.load_crest_artifact_contract(crest_index_root=index_root, target=target)
    except Exception:
        return None


def completed_orca_stage_impl(stage: dict[str, Any], *, orca_auto_config: str | None) -> Any | None:
    o = _orchestration_module()
    task = stage.get("task")
    if not isinstance(task, dict):
        return None
    payload = o._task_payload_dict(task)
    enqueue_payload = o._coerce_mapping(task.get("enqueue_payload"))
    stage_metadata = o._stage_metadata(stage)
    reaction_dir_hint = o._normalize_text(payload.get("reaction_dir") or enqueue_payload.get("reaction_dir"))
    target = (
        o._normalize_text(stage_metadata.get("run_id"))
        or reaction_dir_hint
        or o._normalize_text(stage_metadata.get("queue_id"))
    )
    if not target:
        return None
    try:
        return o.load_orca_artifact_contract(
            target=target,
            orca_allowed_root=_call_engine_aware(o._load_config_root, orca_auto_config, engine="orca"),
            orca_organized_root=(
                _workflow_internal_organized_root(reaction_dir_hint, engine="orca")
                or _call_engine_aware(o._load_config_organized_root, orca_auto_config, engine="orca")
            ),
            queue_id=o._normalize_text(stage_metadata.get("queue_id")),
            run_id=o._normalize_text(stage_metadata.get("run_id")),
            reaction_dir=reaction_dir_hint,
        )
    except Exception:
        return None


__all__ = [
    "append_unique_artifact_impl",
    "completed_crest_roles_impl",
    "completed_crest_stage_impl",
    "completed_orca_stage_impl",
    "ensure_crest_job_dir_impl",
    "ensure_xtb_job_dir_impl",
    "stage_has_xtb_candidates_impl",
    "sync_crest_stage_impl",
    "sync_orca_stage_impl",
    "sync_xtb_stage_impl",
    "write_xtb_path_job_impl",
    "xtb_attempt_record_impl",
    "xtb_attempt_rows_impl",
    "xtb_current_attempt_number_impl",
    "xtb_handoff_status_impl",
    "xtb_path_retry_limit_impl",
    "xtb_retry_recipe_impl",
]
