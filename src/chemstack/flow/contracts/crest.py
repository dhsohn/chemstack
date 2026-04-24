from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .xtb import WorkflowStageInput, _coerce_resource_dict
from ..xyz_utils import load_xyz_frames


def _normalize_text(value: Any) -> str:
    return str(value).strip()


@dataclass(frozen=True)
class CrestArtifactContract:
    job_id: str
    mode: str
    status: str
    reason: str
    job_dir: str
    latest_known_path: str
    organized_output_dir: str = ""
    molecule_key: str = ""
    selected_input_xyz: str = ""
    retained_conformer_count: int = 0
    retained_conformer_paths: tuple[str, ...] = ()
    resource_request: dict[str, int] = field(default_factory=dict)
    resource_actual: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "mode": self.mode,
            "status": self.status,
            "reason": self.reason,
            "job_dir": self.job_dir,
            "latest_known_path": self.latest_known_path,
            "organized_output_dir": self.organized_output_dir,
            "molecule_key": self.molecule_key,
            "selected_input_xyz": self.selected_input_xyz,
            "retained_conformer_count": self.retained_conformer_count,
            "retained_conformer_paths": list(self.retained_conformer_paths),
            "resource_request": dict(self.resource_request),
            "resource_actual": dict(self.resource_actual),
        }


@dataclass(frozen=True)
class CrestDownstreamPolicy:
    max_candidates: int = 3

    @classmethod
    def build(cls, *, max_candidates: int = 3) -> "CrestDownstreamPolicy":
        return cls(max_candidates=max(1, int(max_candidates)))


def to_workflow_stage_inputs(
    contract: CrestArtifactContract,
    *,
    policy: CrestDownstreamPolicy | None = None,
) -> tuple[WorkflowStageInput, ...]:
    active_policy = policy or CrestDownstreamPolicy.build()
    rows: list[WorkflowStageInput] = []
    next_rank = 1
    for path in contract.retained_conformer_paths:
        text = _normalize_text(path)
        if not text:
            continue
        frames = load_xyz_frames(text)
        if len(frames) <= 1:
            rows.append(
                WorkflowStageInput(
                    source_job_id=contract.job_id,
                    source_job_type=f"crest_{contract.mode}",
                    reaction_key=contract.molecule_key,
                    selected_input_xyz=contract.selected_input_xyz,
                    rank=next_rank,
                    kind="crest_conformer",
                    artifact_path=text,
                    selected=next_rank == 1,
                    metadata={"mode": contract.mode},
                )
            )
            next_rank += 1
            if len(rows) >= active_policy.max_candidates:
                break
            continue

        frame_count = len(frames)
        for frame in frames:
            metadata: dict[str, Any] = {
                "mode": contract.mode,
                "source_artifact_path": text,
                "source_frame_index": frame.index,
                "source_frame_count": frame_count,
            }
            if frame.energy is not None:
                metadata["source_frame_energy"] = frame.energy
            rows.append(
                WorkflowStageInput(
                    source_job_id=contract.job_id,
                    source_job_type=f"crest_{contract.mode}",
                    reaction_key=contract.molecule_key,
                    selected_input_xyz=contract.selected_input_xyz,
                    rank=next_rank,
                    kind="crest_conformer",
                    artifact_path=text,
                    selected=next_rank == 1,
                    metadata=metadata,
                )
            )
            next_rank += 1
            if len(rows) >= active_policy.max_candidates:
                break
        if len(rows) >= active_policy.max_candidates:
            break
    return tuple(rows)


__all__ = [
    "CrestArtifactContract",
    "CrestDownstreamPolicy",
    "WorkflowStageInput",
    "_coerce_resource_dict",
    "to_workflow_stage_inputs",
]
