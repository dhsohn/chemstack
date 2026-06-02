from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.flow.orchestration.deps import OrchestrationDeps, orchestration_deps


def _orchestration_context(deps: OrchestrationDeps | None = None) -> OrchestrationDeps:
    return deps or orchestration_deps()


def _mapping_field(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    if isinstance(value, dict):
        return value
    value = {}
    raw[key] = value
    return value


def _existing_mapping_field(raw: dict[str, Any], key: str) -> dict[str, Any] | None:
    value = raw.get(key)
    return value if isinstance(value, dict) else None


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class WorkflowTaskView:
    raw: dict[str, Any]

    def existing_mapping(self, key: str) -> dict[str, Any] | None:
        return _existing_mapping_field(self.raw, key)

    def payload(self, o: Any) -> dict[str, Any]:
        if o is not None:
            return o.stages._task_payload_dict(self.raw)
        return _mapping_field(self.raw, "payload")

    def metadata(self, o: Any) -> dict[str, Any]:
        del o
        return _mapping_field(self.raw, "metadata")

    def enqueue_payload(self, o: Any | None = None) -> dict[str, Any]:
        del o
        return _mapping_field(self.raw, "enqueue_payload")

    def existing_enqueue_payload(self) -> dict[str, Any] | None:
        return self.existing_mapping("enqueue_payload")

    def existing_payload(self) -> dict[str, Any] | None:
        return self.existing_mapping("payload")

    def submission_result(self, o: Any | None = None) -> dict[str, Any]:
        del o
        return _mapping_field(self.raw, "submission_result")

    def existing_submission_result(self) -> dict[str, Any] | None:
        return self.existing_mapping("submission_result")

    def resource_request(self) -> dict[str, Any]:
        return _mapping_field(self.raw, "resource_request")

    def text_field(self, key: str, normalize_text: Callable[[Any], str]) -> str:
        return normalize_text(self.raw.get(key))

    def engine(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("engine")).lower()

    def kind(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("task_kind")).lower()

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()

    def status_with(self, normalize_text: Callable[[Any], str]) -> str:
        return self.text_field("status", normalize_text).lower()

    def has_submitted_result(self) -> bool:
        submission = self.existing_submission_result()
        return submission is not None and submission.get("status") == "submitted"

    def set_status(self, status: str) -> None:
        self.raw["status"] = status

    def set_submission_result(self, submission: dict[str, Any]) -> None:
        self.raw["submission_result"] = submission

    def set_cancel_result(self, result: dict[str, Any]) -> None:
        self.raw["cancel_result"] = result

    def clear_keys(self, *keys: str) -> None:
        for key in keys:
            self.raw.pop(key, None)

    def update_resource_request(self, resources: dict[str, int]) -> None:
        if resources:
            self.raw["resource_request"] = {**self.resource_request(), **resources}

    def set_payload_field(self, key: str, value: Any) -> None:
        self.payload(None)[key] = value

    def update_payload(self, fields: dict[str, Any]) -> None:
        self.payload(None).update(fields)

    def clear_payload_keys(self, *keys: str) -> None:
        payload = self.payload(None)
        for key in keys:
            payload.pop(key, None)

    def set_existing_payload_fields(self, keys: set[str] | frozenset[str], value: Any) -> None:
        payload = self.payload(None)
        for key in keys:
            if key in payload:
                payload[key] = value

    def update_enqueue_payload(self, fields: dict[str, Any]) -> None:
        self.enqueue_payload().update(fields)

    def clear_enqueue_payload_keys(self, *keys: str) -> None:
        payload = self.enqueue_payload()
        for key in keys:
            payload.pop(key, None)

    def set_existing_enqueue_payload_field(self, key: str, value: Any) -> None:
        payload = self.enqueue_payload()
        if key in payload:
            payload[key] = value

    def set_metadata_field(self, key: str, value: Any) -> None:
        self.metadata(None)[key] = value

    def update_metadata(self, fields: dict[str, Any]) -> None:
        self.metadata(None).update(fields)

    def clear_metadata_keys(self, *keys: str) -> None:
        metadata = self.metadata(None)
        for key in keys:
            metadata.pop(key, None)

    def update_orca_contract_payload(
        self,
        contract: Any,
        normalize_text: Callable[[Any], str],
    ) -> None:
        payload = self.payload(None)
        fields = {
            "selected_inp": contract.selected_inp or normalize_text(payload.get("selected_inp")),
        }
        for key in ("selected_input_xyz", "last_out_path", "optimized_xyz_path"):
            value = getattr(contract, key)
            if value:
                fields[key] = value
        self.update_payload(fields)

    def set_orca_latest_attempt_paths(
        self,
        attempt: dict[str, Any],
        normalize_text: Callable[[Any], str],
    ) -> None:
        self.update_payload(
            {
                "orca_latest_attempt_inp": normalize_text(attempt.get("inp_path")),
                "orca_latest_attempt_out": normalize_text(attempt.get("out_path")),
            }
        )

    def set_selected_input_xyz(self, value: Any) -> None:
        self.set_payload_field("selected_input_xyz", value)

    def record_crest_job_materialization(self, *, job_dir: Path | str, input_target: Path | str) -> None:
        self.update_payload(
            {
                "job_dir": str(job_dir),
                "selected_input_xyz": str(input_target),
            }
        )
        self.update_enqueue_payload({"job_dir": str(job_dir)})

    def update_crest_contract_payload(self, contract: Any) -> None:
        self.set_payload_field("selected_input_xyz", contract.selected_input_xyz)

    def record_xtb_path_job_payload(
        self,
        *,
        recipe: dict[str, Any],
        job_dir: Path | str,
        reactant_target: Path | str,
        product_target: Path | str,
        attempt_number: int,
        reaction_key: str,
        normalize_text: Callable[[Any], str],
    ) -> None:
        self.update_payload(
            {
                "job_dir": str(job_dir),
                "selected_input_xyz": str(reactant_target),
                "secondary_input_xyz": str(product_target),
                "xtb_active_attempt_number": int(attempt_number),
                "xtb_retry_recipe_id": normalize_text(recipe.get("recipe_id")),
            }
        )
        self.update_enqueue_payload({"job_dir": str(job_dir), "reaction_key": reaction_key})


@dataclass(frozen=True)
class WorkflowStageStatus:
    stage: str
    task: str

    def any_status(self, *statuses: str) -> bool:
        targets = set(statuses)
        return self.stage in targets or self.task in targets

    def any_matches(self, predicate: Callable[[str], bool]) -> bool:
        return predicate(self.stage) or predicate(self.task)


@dataclass(frozen=True)
class WorkflowStageView:
    raw: dict[str, Any]

    @classmethod
    def from_raw(cls, value: Any) -> WorkflowStageView | None:
        return cls(value) if isinstance(value, dict) else None

    @property
    def task(self) -> WorkflowTaskView:
        task = self.raw.get("task")
        return WorkflowTaskView(task if isinstance(task, dict) else {})

    @property
    def existing_task(self) -> WorkflowTaskView | None:
        task = self.raw.get("task")
        return WorkflowTaskView(task) if isinstance(task, dict) else None

    @property
    def has_task(self) -> bool:
        return isinstance(self.raw.get("task"), dict)

    def ensure_task(self) -> WorkflowTaskView:
        task = self.raw.get("task")
        if not isinstance(task, dict):
            task = {}
            self.raw["task"] = task
        return WorkflowTaskView(task)

    def metadata(self, o: Any) -> dict[str, Any]:
        if o is not None:
            return o.stages._stage_metadata(self.raw)
        return _mapping_field(self.raw, "metadata")

    def existing_metadata(self) -> dict[str, Any] | None:
        return _existing_mapping_field(self.raw, "metadata")

    def text_field(self, key: str, normalize_text: Callable[[Any], str]) -> str:
        return normalize_text(self.raw.get(key))

    def stage_id(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("stage_id"))

    def stage_id_with(self, normalize_text: Callable[[Any], str]) -> str:
        return self.text_field("stage_id", normalize_text)

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()

    def status_with(self, normalize_text: Callable[[Any], str]) -> str:
        return self.text_field("status", normalize_text).lower()

    def status_pair(self, o: Any) -> WorkflowStageStatus:
        return WorkflowStageStatus(stage=self.status(o), task=self.task_status(o))

    def status_pair_with(self, normalize_text: Callable[[Any], str]) -> WorkflowStageStatus:
        task = self.existing_task
        return WorkflowStageStatus(
            stage=self.status_with(normalize_text),
            task=task.status_with(normalize_text) if task is not None else "",
        )

    def set_status(self, status: str) -> None:
        self.raw["status"] = status

    def set_status_pair(self, *, stage_status: str, task_status: str) -> None:
        self.set_status(stage_status)
        if self.has_task:
            self.task.set_status(task_status)

    def set_output_artifacts(self, artifacts: list[dict[str, Any]]) -> None:
        self.raw["output_artifacts"] = artifacts

    def set_metadata_field(self, key: str, value: Any) -> None:
        self.metadata(None)[key] = value

    def update_metadata(self, fields: dict[str, Any]) -> None:
        self.metadata(None).update(fields)

    def clear_metadata_keys(self, *keys: str) -> None:
        metadata = self.metadata(None)
        for key in keys:
            metadata.pop(key, None)

    def update_orca_contract_metadata(
        self,
        contract: Any,
        normalize_text: Callable[[Any], str],
    ) -> None:
        metadata = self.metadata(None)
        self.update_metadata(
            {
                "queue_id": contract.queue_id or normalize_text(metadata.get("queue_id")),
                "run_id": contract.run_id or normalize_text(metadata.get("run_id")),
                "queue_status": contract.queue_status,
                "cancel_requested": bool(contract.cancel_requested),
                "latest_known_path": contract.latest_known_path,
                "organized_output_dir": contract.organized_output_dir,
                "optimized_xyz_path": contract.optimized_xyz_path,
                "analyzer_status": contract.analyzer_status,
                "reason": contract.reason,
                "completed_at": contract.completed_at,
                "state_status": contract.state_status,
                "attempt_count": contract.attempt_count,
                "max_retries": contract.max_retries,
                "orca_attempts": [dict(item) for item in contract.attempts],
                "orca_final_result": dict(contract.final_result),
            }
        )

    def update_orca_attempt_metadata(
        self,
        contract: Any,
        task_view: WorkflowTaskView,
        normalize_text: Callable[[Any], str],
    ) -> None:
        metadata = self.metadata(None)
        if contract.state_status in {"running", "retrying"}:
            metadata["orca_current_attempt_number"] = max(0, contract.attempt_count)
        elif contract.attempts:
            metadata["orca_current_attempt_number"] = contract.attempts[-1].get("attempt_number")
        else:
            metadata.pop("orca_current_attempt_number", None)

        if contract.attempts:
            last_attempt = contract.attempts[-1]
            metadata["orca_latest_attempt_number"] = last_attempt.get("attempt_number")
            metadata["orca_latest_attempt_status"] = last_attempt.get("analyzer_status")
            task_view.set_orca_latest_attempt_paths(last_attempt, normalize_text)
            return

        metadata.pop("orca_latest_attempt_number", None)
        metadata.pop("orca_latest_attempt_status", None)

    def update_xtb_contract_metadata(self, contract: Any) -> None:
        self.update_metadata(
            {
                "child_job_id": contract.job_id,
                "latest_known_path": contract.latest_known_path,
                "organized_output_dir": contract.organized_output_dir,
            }
        )

    def update_xtb_attempt_record(
        self,
        attempt_number: int,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        record = self.xtb_attempt_record(attempt_number)
        record.update(fields)
        return record

    def set_xtb_handoff_retry_state(
        self,
        *,
        retries_used: int,
        retry_limit: int,
    ) -> None:
        self.update_metadata(
            {
                "xtb_handoff_retries_used": retries_used,
                "xtb_handoff_retry_limit": retry_limit,
            }
        )

    def set_xtb_handoff_retrying(
        self,
        *,
        retry_limit: int,
        retries_used: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "reaction_handoff_status": "retrying",
            "xtb_handoff_retry_limit": retry_limit,
        }
        if retries_used is not None:
            fields["xtb_handoff_retries_used"] = retries_used
        self.update_metadata(fields)

    def xtb_attempt_rows(self) -> list[dict[str, Any]]:
        metadata = self.metadata(None)
        attempts = metadata.get("xtb_attempts")
        if isinstance(attempts, list):
            filtered = [item for item in attempts if isinstance(item, dict)]
            metadata["xtb_attempts"] = filtered
            return filtered
        metadata["xtb_attempts"] = []
        return metadata["xtb_attempts"]

    def xtb_attempt_record(self, attempt_number: int) -> dict[str, Any]:
        rows = self.xtb_attempt_rows()
        target_number = int(attempt_number)
        for row in rows:
            if _safe_int(row.get("attempt_number"), default=-1) == target_number:
                return row
        record = {"attempt_number": target_number}
        rows.append(record)
        rows.sort(key=lambda item: _safe_int(item.get("attempt_number"), default=0))
        return record

    def record_xtb_path_job_metadata(
        self,
        *,
        recipe: dict[str, Any],
        attempt_number: int,
        normalize_text: Callable[[Any], str],
    ) -> None:
        self.update_metadata(
            {
                "xtb_active_attempt_number": int(attempt_number),
                "xtb_retry_recipe_id": normalize_text(recipe.get("recipe_id")),
                "xtb_retry_recipe_label": normalize_text(recipe.get("recipe_label")),
            }
        )

    def record_xtb_path_attempt(
        self,
        *,
        recipe: dict[str, Any],
        job_dir: Path | str,
        manifest_path: Path | str,
        xcontrol_path: Path | str,
        namespace: str,
        reaction_key: str,
        attempt_number: int,
        normalize_text: Callable[[Any], str],
    ) -> None:
        self.update_xtb_attempt_record(
            attempt_number,
            {
                "attempt_number": int(attempt_number),
                "recipe_id": normalize_text(recipe.get("recipe_id")),
                "recipe_label": normalize_text(recipe.get("recipe_label")),
                "job_dir": str(job_dir),
                "manifest_path": str(manifest_path),
                "xcontrol_path": str(xcontrol_path),
                "namespace": namespace,
                "reaction_key": reaction_key,
            },
        )

    def xtb_current_attempt_number(self) -> int:
        metadata = self.metadata(None)
        current = _safe_int(metadata.get("xtb_active_attempt_number"), default=-1)
        if current >= 0:
            return current
        attempts = self.xtb_attempt_rows()
        if attempts:
            return max(_safe_int(item.get("attempt_number"), default=0) for item in attempts)
        return 0

    def set_reaction_handoff(self, handoff: dict[str, str]) -> None:
        if not handoff.get("status"):
            return
        metadata = self.metadata(None)
        metadata["reaction_handoff_status"] = handoff["status"]
        for source_key, metadata_key in (
            ("reason", "reaction_handoff_reason"),
            ("message", "reaction_handoff_message"),
            ("artifact_path", "reaction_handoff_artifact_path"),
        ):
            value = handoff.get(source_key, "")
            if value:
                metadata[metadata_key] = value
            else:
                metadata.pop(metadata_key, None)

    def update_crest_contract_metadata(self, contract: Any) -> None:
        self.update_metadata(
            {
                "child_job_id": contract.job_id,
                "latest_known_path": contract.latest_known_path,
                "organized_output_dir": contract.organized_output_dir,
            }
        )

    def set_crest_conformer_artifacts(self, contract: Any) -> None:
        self.set_output_artifacts(
            [
                {
                    "kind": "crest_conformer",
                    "path": path,
                    "selected": index == 1,
                    "metadata": {"rank": index, "mode": contract.mode},
                }
                for index, path in enumerate(contract.retained_conformer_paths, start=1)
            ]
        )

    def task_engine(self, o: Any) -> str:
        return self.task.engine(o)

    def task_kind(self, o: Any) -> str:
        return self.task.kind(o)

    def task_status(self, o: Any) -> str:
        return self.task.status(o)


@dataclass(frozen=True)
class WorkflowPayloadView:
    raw: dict[str, Any]

    @property
    def stage_views(self) -> list[WorkflowStageView]:
        return _stage_views(self.raw)

    def metadata(self) -> dict[str, Any] | None:
        metadata = self.raw.setdefault("metadata", {})
        return metadata if isinstance(metadata, dict) else None

    def workflow_id(self, normalize_text: Callable[[Any], str] | None = None) -> str:
        value = self.raw.get("workflow_id", "")
        return normalize_text(value) if normalize_text is not None else str(value)

    def status(self, normalize_text: Callable[[Any], str]) -> str:
        return normalize_text(self.raw.get("status")).lower()

    def set_status(self, status: str) -> None:
        self.raw["status"] = status


def _stage_views(payload: dict[str, Any]) -> list[WorkflowStageView]:
    return [
        view
        for raw_stage in payload.get("stages", [])
        if (view := WorkflowStageView.from_raw(raw_stage)) is not None
    ]


def _engine_stages(o: Any, payload: dict[str, Any], engine: str) -> list[dict[str, Any]]:
    return [view.raw for view in _stage_views(payload) if view.task_engine(o) == engine]


def _engine_stage_views(
    o: Any,
    payload: dict[str, Any],
    engine: str,
) -> list[WorkflowStageView]:
    return [view for view in _stage_views(payload) if view.task_engine(o) == engine]


def _request_params(o: Any, payload: dict[str, Any]) -> dict[str, Any]:
    metadata = o.stages._coerce_mapping(payload.get("metadata"))
    request = o.stages._coerce_mapping(metadata.get("request"))
    return o.stages._coerce_mapping(request.get("parameters"))


def _clear_workflow_error_scope(o: Any, payload_metadata: dict[str, Any], scopes: set[str]) -> None:
    workflow_error = payload_metadata.get("workflow_error")
    if (
        isinstance(workflow_error, dict)
        and o.stages._normalize_text(workflow_error.get("scope")) in scopes
    ):
        payload_metadata.pop("workflow_error", None)
