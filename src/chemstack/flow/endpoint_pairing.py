from __future__ import annotations

from dataclasses import dataclass, field
from math import sqrt
from pathlib import Path
from typing import Any

from .contracts import WorkflowStageInput
from .xyz_utils import XYZFrame, load_xyz_frames


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = _normalize_text(value).lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on", "enabled"}


def _as_positive_int(value: Any, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return max(0, int(default))
    return max(0, parsed)


def _as_optional_float(value: Any) -> float | None:
    if value is None or _normalize_text(value) == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_atom_indices(value: Any) -> tuple[int, ...]:
    raw_items: list[Any]
    if isinstance(value, str):
        raw_items = [item for item in value.replace(",", " ").split() if item]
    elif isinstance(value, (list, tuple)):
        raw_items = list(value)
    else:
        return ()

    indices: list[int] = []
    seen: set[int] = set()
    for raw in raw_items:
        try:
            index = int(raw)
        except (TypeError, ValueError):
            continue
        if index <= 0 or index in seen:
            continue
        indices.append(index)
        seen.add(index)
    return tuple(indices)


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


@dataclass(frozen=True)
class EndpointPairingPolicy:
    enabled: bool = False
    comparison_atoms: tuple[int, ...] = ()
    max_distance_rmsd: float | None = None
    max_rank_gap: int = 0
    max_pairs: int = 0
    rank_weight: float = 0.01
    fallback_to_ranked: bool = False
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(
        cls,
        value: Any,
        *,
        default_max_pairs: int = 0,
    ) -> "EndpointPairingPolicy":
        if value is None or value == "":
            return cls(enabled=False)
        if isinstance(value, bool):
            return cls(enabled=value, max_pairs=max(0, int(default_max_pairs)))
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"", "0", "false", "no", "off", "disabled"}:
                return cls(enabled=False)
            return cls(enabled=True, max_pairs=max(0, int(default_max_pairs)), raw={"enabled": value})

        raw = _coerce_mapping(value)
        if not raw:
            return cls(enabled=False)
        mode = _normalize_text(raw.get("mode")).lower()
        if mode in {"off", "disabled", "none"}:
            return cls(enabled=False, raw=raw)
        enabled = _as_bool(raw.get("enabled"), default=True)
        atoms = (
            _coerce_atom_indices(raw.get("comparison_atoms"))
            or _coerce_atom_indices(raw.get("alignment_atoms"))
            or _coerce_atom_indices(raw.get("reaction_center_atoms"))
            or _coerce_atom_indices(raw.get("rmsd_atoms"))
            or _coerce_atom_indices(raw.get("atoms"))
        )
        max_distance_rmsd = (
            _as_optional_float(raw.get("max_distance_rmsd"))
            if "max_distance_rmsd" in raw
            else _as_optional_float(raw.get("max_reaction_center_rmsd"))
            if "max_reaction_center_rmsd" in raw
            else _as_optional_float(raw.get("max_rmsd"))
        )
        max_pairs = _as_positive_int(raw.get("max_pairs"), default=max(0, int(default_max_pairs)))
        rank_weight = _as_optional_float(raw.get("rank_weight"))
        fallback_default = not bool(atoms or max_distance_rmsd is not None)
        return cls(
            enabled=enabled,
            comparison_atoms=atoms,
            max_distance_rmsd=max_distance_rmsd,
            max_rank_gap=_as_positive_int(raw.get("max_rank_gap"), default=0),
            max_pairs=max_pairs,
            rank_weight=rank_weight if rank_weight is not None else 0.01,
            fallback_to_ranked=_as_bool(raw.get("fallback_to_ranked"), default=fallback_default),
            raw=raw,
        )

    def to_summary(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "comparison_atoms": list(self.comparison_atoms),
            "max_distance_rmsd": self.max_distance_rmsd,
            "max_rank_gap": self.max_rank_gap,
            "max_pairs": self.max_pairs,
            "rank_weight": self.rank_weight,
            "fallback_to_ranked": self.fallback_to_ranked,
        }


@dataclass(frozen=True)
class EndpointPair:
    reactant: WorkflowStageInput
    product: WorkflowStageInput
    score: float
    metadata: dict[str, Any]


def _frame_for_input(item: WorkflowStageInput) -> XYZFrame | None:
    path_text = _normalize_text(item.artifact_path)
    if not path_text:
        return None
    frames = load_xyz_frames(Path(path_text).expanduser())
    if not frames:
        return None
    try:
        requested_index = int(item.metadata.get("source_frame_index", 0) or 0)
    except (TypeError, ValueError):
        requested_index = 0
    if requested_index > 0:
        for frame in frames:
            if frame.index == requested_index:
                return frame
        return None
    return frames[0]


def _frame_coordinates(frame: XYZFrame) -> tuple[tuple[float, float, float], ...]:
    coords: list[tuple[float, float, float]] = []
    for line in frame.atom_lines:
        tokens = line.split()
        coords.append((float(tokens[1]), float(tokens[2]), float(tokens[3])))
    return tuple(coords)


def _validated_indices(indices: tuple[int, ...], *, natoms: int) -> tuple[int, ...]:
    if not indices:
        return tuple(range(1, natoms + 1))
    return tuple(index for index in indices if 1 <= index <= natoms)


def _distance(left: tuple[float, float, float], right: tuple[float, float, float]) -> float:
    return sqrt(
        (left[0] - right[0]) ** 2
        + (left[1] - right[1]) ** 2
        + (left[2] - right[2]) ** 2
    )


def _distance_fingerprint(
    coords: tuple[tuple[float, float, float], ...],
    indices: tuple[int, ...],
) -> tuple[float, ...]:
    distances: list[float] = []
    zero_based = [index - 1 for index in indices]
    for outer, left_index in enumerate(zero_based):
        for right_index in zero_based[outer + 1 :]:
            distances.append(_distance(coords[left_index], coords[right_index]))
    return tuple(distances)


def _distance_rmsd(
    reactant: WorkflowStageInput,
    product: WorkflowStageInput,
    *,
    atom_indices: tuple[int, ...],
) -> tuple[float | None, str]:
    reactant_frame = _frame_for_input(reactant)
    product_frame = _frame_for_input(product)
    if reactant_frame is None or product_frame is None:
        return None, "missing_or_invalid_xyz"
    if reactant_frame.natoms != product_frame.natoms:
        return None, "atom_count_mismatch"

    indices = _validated_indices(atom_indices, natoms=reactant_frame.natoms)
    if len(indices) < 2:
        return None, "too_few_comparison_atoms"

    reactant_fp = _distance_fingerprint(_frame_coordinates(reactant_frame), indices)
    product_fp = _distance_fingerprint(_frame_coordinates(product_frame), indices)
    if not reactant_fp or len(reactant_fp) != len(product_fp):
        return None, "empty_distance_fingerprint"
    squared = [(left - right) ** 2 for left, right in zip(reactant_fp, product_fp)]
    return sqrt(sum(squared) / len(squared)), "distance_fingerprint"


def _rank_gap(reactant: WorkflowStageInput, product: WorkflowStageInput) -> int:
    return abs(int(reactant.rank) - int(product.rank))


def select_endpoint_pairs(
    reactant_inputs: tuple[WorkflowStageInput, ...] | list[WorkflowStageInput],
    product_inputs: tuple[WorkflowStageInput, ...] | list[WorkflowStageInput],
    *,
    policy: EndpointPairingPolicy | None = None,
) -> tuple[EndpointPair, ...]:
    active_policy = policy or EndpointPairingPolicy()
    pairs: list[EndpointPair] = []
    sequence = 0

    for reactant in reactant_inputs:
        for product in product_inputs:
            sequence += 1
            rank_gap = _rank_gap(reactant, product)
            if active_policy.enabled and active_policy.max_rank_gap and rank_gap > active_policy.max_rank_gap:
                continue

            distance_rmsd: float | None = None
            metric_reason = "not_requested"
            if active_policy.enabled and (active_policy.comparison_atoms or active_policy.max_distance_rmsd is not None):
                distance_rmsd, metric_reason = _distance_rmsd(
                    reactant,
                    product,
                    atom_indices=active_policy.comparison_atoms,
                )
                if distance_rmsd is None and not active_policy.fallback_to_ranked:
                    continue
                if (
                    distance_rmsd is not None
                    and active_policy.max_distance_rmsd is not None
                    and distance_rmsd > active_policy.max_distance_rmsd
                ):
                    continue

            if distance_rmsd is None:
                score = float(rank_gap)
                strategy = "rank"
            else:
                score = float(distance_rmsd) + (float(rank_gap) * active_policy.rank_weight)
                strategy = metric_reason

            pairs.append(
                EndpointPair(
                    reactant=reactant,
                    product=product,
                    score=score,
                    metadata={
                        "enabled": active_policy.enabled,
                        "strategy": strategy,
                        "pairing_score": round(score, 6),
                        "distance_fingerprint_rmsd": round(distance_rmsd, 6)
                        if distance_rmsd is not None
                        else None,
                        "rank_gap": rank_gap,
                        "reactant_rank": int(reactant.rank),
                        "product_rank": int(product.rank),
                        "reactant_artifact_path": reactant.artifact_path,
                        "product_artifact_path": product.artifact_path,
                        "candidate_pair_order": sequence,
                    },
                )
            )

    if not active_policy.enabled:
        return tuple(pairs)

    pairs.sort(
        key=lambda item: (
            item.score,
            int(item.reactant.rank) + int(item.product.rank),
            int(item.reactant.rank),
            int(item.product.rank),
            int(item.metadata.get("candidate_pair_order", 0)),
        )
    )
    if active_policy.max_pairs:
        pairs = pairs[: active_policy.max_pairs]
    return tuple(pairs)


__all__ = [
    "EndpointPair",
    "EndpointPairingPolicy",
    "select_endpoint_pairs",
]
