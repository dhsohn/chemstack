from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.indexing import get_job_location
from chemstack.core.queue import list_queue
from chemstack.flow.adapters.xtb import load_xtb_artifact_contract
from chemstack.flow.submitters import xtb_auto as xtb_submitter


def _queue_status(entry: Any) -> str:
    return str(getattr(getattr(entry, "status", None), "value", "")).strip()


def test_xtb_submitter_roundtrip_smoke(
    smoke_workspace: Any,
    app_runner: Any,
    xtb_opt_job: Path,
) -> None:
    submission = xtb_submitter.submit_job_dir(
        job_dir=str(xtb_opt_job),
        priority=5,
        config_path=str(smoke_workspace.xtb_config_path),
        repo_root=str(smoke_workspace.repo_root),
    )

    assert submission["status"] == "submitted"
    assert submission["parsed_stdout"]["status"] == "queued"
    assert submission["job_id"]
    assert submission["queue_id"]

    queue_entries = list_queue(smoke_workspace.xtb_allowed_root)
    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == submission["job_id"]
    assert queue_entries[0].queue_id == submission["queue_id"]
    assert _queue_status(queue_entries[0]) == "pending"

    worker = app_runner(
        smoke_workspace.repo_root,
        "chemstack.xtb._internal_cli",
        "--config",
        str(smoke_workspace.xtb_config_path),
        "queue",
        "worker",
        "--once",
        "--auto-organize",
    )

    assert worker.returncode == 0, worker.stderr or worker.stdout
    assert f"queue_id: {submission['queue_id']}" in worker.stdout
    assert f"job_id: {submission['job_id']}" in worker.stdout
    assert "status: completed" in worker.stdout

    record = get_job_location(smoke_workspace.xtb_allowed_root, submission["job_id"])
    assert record is not None
    assert record.app_name == "xtb_auto"
    assert record.status == "completed"
    assert record.original_run_dir == str(xtb_opt_job.resolve())
    assert record.organized_output_dir
    assert record.latest_known_path == record.organized_output_dir

    organized_dir = Path(record.organized_output_dir)
    assert organized_dir.exists()
    assert (xtb_opt_job / "organized_ref.json").exists()
    assert (organized_dir / "job_state.json").exists()
    assert (organized_dir / "job_report.json").exists()
    assert (organized_dir / "job_report.md").exists()
    assert (organized_dir / "xtbopt.xyz").exists()
    assert (organized_dir / "xtbout.json").exists()

    report_payload = json.loads((organized_dir / "job_report.json").read_text(encoding="utf-8"))
    assert report_payload["status"] == "completed"
    assert report_payload["job_type"] == "opt"
    assert report_payload["candidate_count"] == 1
    assert report_payload["analysis_summary"]["optimization_ok"] is True

    contract = load_xtb_artifact_contract(
        xtb_index_root=smoke_workspace.xtb_allowed_root,
        target=submission["job_id"],
    )
    assert contract.status == "completed"
    assert contract.job_type == "opt"
    assert contract.organized_output_dir == str(organized_dir)
    assert contract.selected_candidate_paths == (str((organized_dir / "xtbopt.xyz").resolve()),)
    assert contract.analysis_summary["canonical_result_path"] == str((organized_dir / "xtbopt.xyz").resolve())

    queue_entries_after = list_queue(smoke_workspace.xtb_allowed_root)
    assert len(queue_entries_after) == 1
    assert _queue_status(queue_entries_after[0]) == "completed"

    admission_path = smoke_workspace.admission_root / "admission_slots.json"
    if admission_path.exists():
        assert json.loads(admission_path.read_text(encoding="utf-8")) == []
