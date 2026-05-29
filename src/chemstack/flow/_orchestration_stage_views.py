from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ._orchestration_deps import OrchestrationDeps, orchestration_deps


def _orchestration_context(deps: OrchestrationDeps | None = None) -> OrchestrationDeps:
    return deps or orchestration_deps()


@dataclass(frozen=True)
class WorkflowTaskView:
    raw: dict[str, Any]

    def payload(self, o: Any) -> dict[str, Any]:
        return o.stages._coerce_mapping(self.raw.get("payload"))

    def metadata(self, o: Any) -> dict[str, Any]:
        metadata = self.raw.setdefault("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
            self.raw["metadata"] = metadata
        return metadata

    def engine(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("engine")).lower()

    def kind(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("task_kind")).lower()

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()

    def set_status(self, status: str) -> None:
        self.raw["status"] = status


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

    def metadata(self, o: Any) -> dict[str, Any]:
        return o.stages._stage_metadata(self.raw)

    def stage_id(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("stage_id"))

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()

    def set_status(self, status: str) -> None:
        self.raw["status"] = status

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
