from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from chemstack.core.indexing import get_job_location
from chemstack.core.queue import list_queue

from chemstack.crest import queue_runtime as queue_cmd
from chemstack.crest import submission as crest_submission
from chemstack.crest.runner import CrestRunResult
from chemstack.crest.state import load_organized_ref, load_report_json, load_state
from chemstack.flow.submitters import crest as crest_submitter
from tests.engine_process_helpers import process_one_crest_for_test


def _write_config(tmp_path: Path) -> tuple[Path, Path, Path]:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "01_crest"
    organized_root = allowed_root
    allowed_root.mkdir(parents=True)
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow:",
                f"  root: {json.dumps(str(workflow_root))}",
                "resources:",
                "  max_cores_per_task: 6",
                "  max_memory_gb_per_task: 14",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path, allowed_root, organized_root


def _write_xyz(path: Path, label: str = "sample") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"1\n{label}\nH 0.0 0.0 0.0\n", encoding="utf-8")


def test_cmd_run_dir_queues_job_updates_state_and_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "job-queue"
    job_dir.mkdir(parents=True)
    _write_xyz(job_dir / "fallback.xyz", "fallback")
    _write_xyz(job_dir / "preferred.xyz", "preferred")
    (job_dir / "crest_job.yaml").write_text(
        "mode: nci\ninput_xyz: preferred.xyz\nresources:\n  max_cores: 9\n  max_memory_gb: 21\n",
        encoding="utf-8",
    )

    notifications: list[dict[str, Any]] = []
    monkeypatch.setattr(crest_submission, "new_job_id", lambda: "crest-fixed-id")

    def fake_notify_job_queued(cfg: Any, **kwargs: Any) -> bool:
        notifications.append(kwargs)
        return True

    monkeypatch.setattr(crest_submission, "notify_job_queued", fake_notify_job_queued)

    submission = crest_submitter.submit_job_dir(
        job_dir=str(job_dir),
        priority=4,
        config_path=str(config_path),
    )

    capsys.readouterr()
    queue_entries = list_queue(allowed_root)
    state = load_state(job_dir)
    record = get_job_location(allowed_root, "crest-fixed-id")

    assert submission["status"] == "submitted"
    assert submission["job_id"] == "crest-fixed-id"
    assert submission["parsed_stdout"]["status"] == "queued"
    assert submission["parsed_stdout"]["priority"] == "4"

    assert len(queue_entries) == 1
    entry = queue_entries[0]
    assert entry.task_id == "crest-fixed-id"
    assert entry.priority == 4
    assert entry.metadata == {
        "job_dir": str(job_dir.resolve()),
        "selected_input_xyz": str((job_dir / "preferred.xyz").resolve()),
        "mode": "nci",
        "molecule_key": "preferred",
        "manifest_present": "true",
        "resource_request": {"max_cores": 9, "max_memory_gb": 21},
        "resource_actual": {"max_cores": 9, "max_memory_gb": 21},
    }

    assert state is not None
    assert state["job_id"] == "crest-fixed-id"
    assert state["job_dir"] == str(job_dir.resolve())
    assert state["selected_input_xyz"] == str((job_dir / "preferred.xyz").resolve())
    assert state["status"] == "queued"
    assert state["mode"] == "nci"
    assert state["molecule_key"] == "preferred"
    assert state["resource_request"] == {"max_cores": 9, "max_memory_gb": 21}
    assert state["resource_actual"] == {"max_cores": 9, "max_memory_gb": 21}

    assert record is not None
    assert record.job_id == "crest-fixed-id"
    assert record.status == "queued"
    assert record.job_type == "crest_nci_conformer_search"
    assert record.original_run_dir == str(job_dir.resolve())
    assert record.latest_known_path == str(job_dir.resolve())
    assert record.selected_input_xyz == str((job_dir / "preferred.xyz").resolve())
    assert record.resource_request == {"max_cores": 9, "max_memory_gb": 21}
    assert record.resource_actual == {"max_cores": 9, "max_memory_gb": 21}

    assert notifications == [
        {
            "job_id": "crest-fixed-id",
            "queue_id": entry.queue_id,
            "job_dir": job_dir.resolve(),
            "mode": "nci",
            "selected_xyz": (job_dir / "preferred.xyz").resolve(),
        }
    ]


def test_cmd_run_dir_reports_duplicate_queue_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "job-duplicate"
    job_dir.mkdir(parents=True)
    _write_xyz(job_dir / "input.xyz", "input")

    notifications: list[dict[str, Any]] = []
    monkeypatch.setattr(crest_submission, "new_job_id", lambda: "crest-duplicate-id")

    def fake_notify_job_queued(cfg: Any, **kwargs: Any) -> bool:
        notifications.append(kwargs)
        return True

    monkeypatch.setattr(crest_submission, "notify_job_queued", fake_notify_job_queued)

    first_submission = crest_submitter.submit_job_dir(
        job_dir=str(job_dir),
        priority=10,
        config_path=str(config_path),
    )
    capsys.readouterr()

    second_submission = crest_submitter.submit_job_dir(
        job_dir=str(job_dir),
        priority=10,
        config_path=str(config_path),
    )
    capsys.readouterr()

    queue_entries = list_queue(allowed_root)
    state = load_state(job_dir)

    assert first_submission["status"] == "submitted"
    assert second_submission["status"] == "failed"
    assert (
        "Active queue entry already exists for app=chemstack_crest task_id=crest-duplicate-id"
        in second_submission["stderr"]
    )

    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == "crest-duplicate-id"
    assert state is not None
    assert state["job_id"] == "crest-duplicate-id"
    assert len(notifications) == 1


def test_cli_end_to_end_smoke_path_submission_worker_and_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "job-e2e"
    queued_notifications: list[dict[str, Any]] = []
    started_notifications: list[dict[str, Any]] = []
    finished_notifications: list[dict[str, Any]] = []

    monkeypatch.setattr(crest_submission, "new_job_id", lambda: "crest-e2e-001")

    def fake_notify_job_queued(cfg: Any, **kwargs: Any) -> bool:
        queued_notifications.append(kwargs)
        return True

    def fake_notify_job_started(cfg: Any, **kwargs: Any) -> bool:
        started_notifications.append(kwargs)
        return True

    def fake_notify_job_finished(cfg: Any, **kwargs: Any) -> bool:
        finished_notifications.append(kwargs)
        return True

    monkeypatch.setattr(crest_submission, "notify_job_queued", fake_notify_job_queued)
    monkeypatch.setattr(queue_cmd, "notify_job_started", fake_notify_job_started)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", fake_notify_job_finished)

    class _FakeProcess:
        def poll(self) -> int | None:
            return 0

    def fake_start_crest_job(cfg: Any, *, job_dir: Path, selected_xyz: Path) -> Any:
        return type("Running", (), {"process": _FakeProcess()})()

    def fake_finalize_crest_job(running: Any) -> CrestRunResult:
        selected_xyz = job_dir / "input.xyz"
        stdout_log = job_dir / "crest.stdout.log"
        stderr_log = job_dir / "crest.stderr.log"
        retained_path = job_dir / "crest_best.xyz"
        stdout_log.write_text("stdout\n", encoding="utf-8")
        stderr_log.write_text("stderr\n", encoding="utf-8")
        retained_path.write_text("1\nretained\nH 0.0 0.0 0.0\n", encoding="utf-8")
        return CrestRunResult(
            status="completed",
            reason="ok",
            command=("crest", selected_xyz.name, "--T", "6"),
            exit_code=0,
            started_at="2026-04-20T00:00:00+00:00",
            finished_at="2026-04-20T00:05:00+00:00",
            stdout_log=str(stdout_log.resolve()),
            stderr_log=str(stderr_log.resolve()),
            selected_input_xyz=str(selected_xyz.resolve()),
            mode="standard",
            retained_conformer_count=1,
            retained_conformer_paths=(str(retained_path.resolve()),),
            manifest_path=str((job_dir / "crest_job.yaml").resolve()),
            resource_request={"max_cores": 6, "max_memory_gb": 14},
            resource_actual={"assigned_cores": 6, "memory_limit_gb": 14},
        )

    monkeypatch.setattr(queue_cmd, "start_crest_job", fake_start_crest_job)
    monkeypatch.setattr(queue_cmd, "finalize_crest_job", fake_finalize_crest_job)

    job_dir.mkdir(parents=True)
    _write_xyz(job_dir / "input.xyz", "input")
    (job_dir / "crest_job.yaml").write_text("mode: standard\ninput_xyz: input.xyz\n", encoding="utf-8")

    submission = crest_submitter.submit_job_dir(
        job_dir=str(job_dir),
        priority=2,
        config_path=str(config_path),
    )
    capsys.readouterr()
    assert submission["status"] == "submitted"
    assert submission["job_id"] == "crest-e2e-001"

    assert process_one_crest_for_test(
        queue_cmd,
        queue_cmd.load_config(str(config_path)),
    ) == "processed"
    worker_output = capsys.readouterr().out
    assert "organized_output_dir:" not in worker_output
    assert "queue_id:" in worker_output
    assert "job_id: crest-e2e-001" in worker_output
    assert "status: completed" in worker_output

    queue_entries = list_queue(allowed_root)
    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == "crest-e2e-001"
    assert queue_entries[0].status.value == "completed"

    organized_ref = load_organized_ref(job_dir)
    assert organized_ref is None

    state = load_state(job_dir)
    report = load_report_json(job_dir)
    assert state is not None
    assert report is not None
    assert state["status"] == "completed"
    assert report["status"] == "completed"
    assert report["retained_conformer_count"] == 1

    record = get_job_location(allowed_root, "crest-e2e-001")
    assert record is not None
    assert record.original_run_dir == str(job_dir.resolve())
    assert record.organized_output_dir == ""
    assert record.latest_known_path == str(job_dir.resolve())

    assert len(queued_notifications) == 1
    assert queued_notifications[0]["job_id"] == "crest-e2e-001"
    assert len(started_notifications) == 1
    assert started_notifications[0]["job_id"] == "crest-e2e-001"
    assert started_notifications[0]["queue_id"].startswith("q_")
    assert Path(started_notifications[0]["job_dir"]).resolve() == job_dir.resolve()

    assert len(finished_notifications) == 1
    assert finished_notifications[0]["job_id"] == "crest-e2e-001"
    assert finished_notifications[0]["status"] == "completed"
    assert finished_notifications[0]["organized_output_dir"] is None
