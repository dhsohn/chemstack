from __future__ import annotations

from dataclasses import dataclass
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


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class WorkflowTaskView:
    raw: dict[str, Any]

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

    def submission_result(self, o: Any | None = None) -> dict[str, Any]:
        del o
        return _mapping_field(self.raw, "submission_result")

    def resource_request(self) -> dict[str, Any]:
        return _mapping_field(self.raw, "resource_request")

    def engine(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("engine")).lower()

    def kind(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("task_kind")).lower()

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()

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

    def update_enqueue_payload(self, fields: dict[str, Any]) -> None:
        self.enqueue_payload().update(fields)

    def clear_enqueue_payload_keys(self, *keys: str) -> None:
        payload = self.enqueue_payload()
        for key in keys:
            payload.pop(key, None)

    def set_metadata_field(self, key: str, value: Any) -> None:
        self.metadata(None)[key] = value

    def update_metadata(self, fields: dict[str, Any]) -> None:
        self.metadata(None).update(fields)

    def clear_metadata_keys(self, *keys: str) -> None:
        metadata = self.metadata(None)
        for key in keys:
            metadata.pop(key, None)


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

    def stage_id(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("stage_id"))

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()

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

    def task_engine(self, o: Any) -> str:
        return self.task.engine(o)

    def task_kind(self, o: Any) -> str:
        return self.task.kind(o)

    def task_status(self, o: Any) -> str:
        return self.task.status(o)


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
