from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.indexing import get_job_location
from chemstack.core.queue import list_queue
from chemstack.flow.adapters.crest import load_crest_artifact_contract
from chemstack.flow.submitters import crest_auto as crest_submitter


def _queue_status(entry: Any) -> str:
    return str(getattr(getattr(entry, "status", None), "value", "")).strip()


def test_crest_submitter_roundtrip_smoke(
    smoke_workspace: Any,
    app_runner: Any,
    crest_job: Path,
) -> None:
    submission = crest_submitter.submit_job_dir(
        job_dir=str(crest_job),
        priority=5,
        config_path=str(smoke_workspace.crest_config_path),
        repo_root=str(smoke_workspace.repo_root),
    )

    assert submission["status"] == "submitted"
    assert submission["parsed_stdout"]["status"] == "queued"
    assert submission["job_id"]
    assert submission["queue_id"]

    queue_entries = list_queue(smoke_workspace.crest_allowed_root)
    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == submission["job_id"]
    assert queue_entries[0].queue_id == submission["queue_id"]
    assert _queue_status(queue_entries[0]) == "pending"

    worker = app_runner(
        smoke_workspace.repo_root,
        "chemstack.crest.cli",
        "--config",
        str(smoke_workspace.crest_config_path),
        "queue",
        "worker",
        "--once",
        "--auto-organize",
    )

    assert worker.returncode == 0, worker.stderr or worker.stdout
    assert f"queue_id: {submission['queue_id']}" in worker.stdout
    assert f"job_id: {submission['job_id']}" in worker.stdout
    assert "status: completed" in worker.stdout

    record = get_job_location(smoke_workspace.crest_allowed_root, submission["job_id"])
    assert record is not None
    assert record.app_name == "crest_auto"
    assert record.status == "completed"
    assert record.original_run_dir == str(crest_job.resolve())
    assert record.organized_output_dir
    assert record.latest_known_path == record.organized_output_dir

    organized_dir = Path(record.organized_output_dir)
    assert organized_dir.exists()
    assert (crest_job / "organized_ref.json").exists()
    assert (organized_dir / "job_state.json").exists()
    assert (organized_dir / "job_report.json").exists()
    assert (organized_dir / "job_report.md").exists()
    assert (organized_dir / "crest_conformers.xyz").exists()
    assert (organized_dir / "crest_best.xyz").exists()

    report_payload = json.loads((organized_dir / "job_report.json").read_text(encoding="utf-8"))
    assert report_payload["status"] == "completed"
    assert report_payload["mode"] == "standard"
    assert report_payload["retained_conformer_count"] == 2
    assert sorted(Path(path).name for path in report_payload["retained_conformer_paths"]) == [
        "crest_best.xyz",
        "crest_conformers.xyz",
    ]

    contract = load_crest_artifact_contract(
        crest_index_root=smoke_workspace.crest_allowed_root,
        target=submission["job_id"],
    )
    assert contract.status == "completed"
    assert contract.mode == "standard"
    assert contract.organized_output_dir == str(organized_dir)
    assert contract.retained_conformer_count == 2
    assert sorted(Path(path).name for path in contract.retained_conformer_paths) == [
        "crest_best.xyz",
        "crest_conformers.xyz",
    ]

    queue_entries_after = list_queue(smoke_workspace.crest_allowed_root)
    assert len(queue_entries_after) == 1
    assert _queue_status(queue_entries_after[0]) == "completed"

    admission_path = smoke_workspace.admission_root / "admission_slots.json"
    if admission_path.exists():
        assert json.loads(admission_path.read_text(encoding="utf-8")) == []
