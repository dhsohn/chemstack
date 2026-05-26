from __future__ import annotations

import json
from typing import Any

from .adapters import (
    load_crest_artifact_contract,
    load_xtb_artifact_contract,
    select_xtb_downstream_inputs,
)
from chemstack.cli_common import _dependency
from .contracts import XtbDownstreamPolicy


def cmd_xtb_inspect(args: Any, *, deps: Any | None = None) -> int:
    load_contract = _dependency(deps, "load_xtb_artifact_contract", load_xtb_artifact_contract)
    contract = load_contract(
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


def cmd_xtb_candidates(args: Any, *, deps: Any | None = None) -> int:
    load_contract = _dependency(deps, "load_xtb_artifact_contract", load_xtb_artifact_contract)
    select_inputs = _dependency(deps, "select_xtb_downstream_inputs", select_xtb_downstream_inputs)
    policy_cls = _dependency(deps, "XtbDownstreamPolicy", XtbDownstreamPolicy)

    contract = load_contract(
        xtb_index_root=getattr(args, "xtb_index_root"),
        target=getattr(args, "target"),
    )
    policy = policy_cls.build(
        preferred_kinds=getattr(args, "preferred_kinds", None),
        max_candidates=int(getattr(args, "max_candidates", 3) or 3),
        selected_only=not bool(getattr(args, "include_unselected", False)),
    )
    candidates = select_inputs(contract, policy=policy)
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


def cmd_crest_inspect(args: Any, *, deps: Any | None = None) -> int:
    load_contract = _dependency(deps, "load_crest_artifact_contract", load_crest_artifact_contract)
    contract = load_contract(
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
