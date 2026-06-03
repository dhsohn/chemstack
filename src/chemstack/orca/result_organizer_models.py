from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REQUIRED_FILES = ["job_state.json", "job_report.json", "job_report.md"]


@dataclass
class SkipReason:
    reaction_dir: str
    reason: str


@dataclass
class OrganizePlan:
    reaction_dir: Path
    run_id: str
    job_type: str
    molecule_key: str
    selected_inp: str
    last_out_path: str
    attempt_count: int
    status: str
    analyzer_status: str
    reason: str
    completed_at: str
    source_dir: Path
    target_rel_path: str
    target_abs_path: Path
