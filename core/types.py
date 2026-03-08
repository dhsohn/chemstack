from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict


class AttemptRecord(TypedDict, total=False):
    index: int
    inp_path: str
    out_path: str
    return_code: int
    analyzer_status: str
    analyzer_reason: str
    markers: Dict[str, Any]
    patch_actions: List[str]
    started_at: str
    ended_at: str


class RunFinalResult(TypedDict, total=False):
    status: str
    analyzer_status: str
    reason: str
    completed_at: str
    last_out_path: Optional[str]
    resumed: bool
    skipped_execution: bool
    runner_error: str


class RunState(TypedDict, total=False):
    run_id: str
    reaction_dir: str
    selected_inp: str
    max_retries: int
    status: str
    started_at: str
    updated_at: str
    attempts: List[AttemptRecord]
    final_result: Optional[RunFinalResult]
