from __future__ import annotations

from typing import Any

from ._orchestration_deps import OrchestrationDeps
from ._orchestration_stage_runtime_shared import _orchestration_context


def xtb_handoff_status_impl(
    contract: Any, *, deps: OrchestrationDeps | None = None
) -> dict[str, str]:
    o = _orchestration_context(deps)
    inputs = o.engines.select_xtb_downstream_inputs(
        contract,
        policy=o.contracts.XtbDownstreamPolicy.build(
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
            "artifact_path": o.stages._normalize_text(inputs[0].artifact_path),
        }
    error = o.stages._reaction_ts_guess_error(contract)
    return {
        "status": "failed",
        "reason": error["reason"],
        "message": error["message"],
        "artifact_path": "",
    }


def stage_has_xtb_candidates_impl(
    stage: dict[str, Any], *, deps: OrchestrationDeps | None = None
) -> bool:
    o = _orchestration_context(deps)
    artifacts = stage.get("output_artifacts")
    if not isinstance(artifacts, list):
        return False
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        if o.stages._normalize_text(artifact.get("kind")) != "xtb_candidate":
            continue
        if o.stages._normalize_text(artifact.get("path")):
            return True
    return False


def _empty_xtb_handoff() -> dict[str, str]:
    return {
        "status": "",
        "reason": "",
        "message": "",
        "artifact_path": "",
    }


def _update_xtb_handoff_metadata(stage_metadata: dict[str, Any], handoff: dict[str, str]) -> None:
    if not handoff["status"]:
        return
    stage_metadata["reaction_handoff_status"] = handoff["status"]
    for source_key, metadata_key in (
        ("reason", "reaction_handoff_reason"),
        ("message", "reaction_handoff_message"),
        ("artifact_path", "reaction_handoff_artifact_path"),
    ):
        value = handoff[source_key]
        if value:
            stage_metadata[metadata_key] = value
        else:
            stage_metadata.pop(metadata_key, None)


__all__ = [
    "_empty_xtb_handoff",
    "_update_xtb_handoff_metadata",
    "stage_has_xtb_candidates_impl",
    "xtb_handoff_status_impl",
]
