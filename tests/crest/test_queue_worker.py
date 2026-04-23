from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from chemstack.core.config import CommonResourceConfig, CommonRuntimeConfig
from chemstack.core.indexing import get_job_location, list_job_locations
from chemstack.core.queue import enqueue, list_queue, request_cancel
from chemstack.core.queue.types import QueueStatus

from chemstack.crest.commands import queue as queue_cmd
from chemstack.crest.config import AppConfig
from chemstack.crest.runner import CrestRunResult
from chemstack.crest.state import REPORT_JSON_FILE_NAME, REPORT_MD_FILE_NAME, STATE_FILE_NAME


class FakeProcess:
    def __init__(self, *poll_values: int | None) -> None:
        self._poll_values = list(poll_values) or [None]
        self.pid = 4242

    def poll(self) -> int | None:
        if len(self._poll_values) > 1:
            return self._poll_values.pop(0)
        return self._poll_values[0]


class FakeChildProcess(FakeProcess):
    def __init__(self, pid: int, *poll_values: int | None) -> None:
        super().__init__(*poll_values)
        self.pid = pid
        self.terminate_calls = 0
        self.kill_calls = 0

    def terminate(self) -> None:
        self.terminate_calls += 1
        self._poll_values = [0]

    def kill(self) -> None:
        self.kill_calls += 1
        self._poll_values = [-9]


@pytest.fixture
def queue_env(tmp_path: Path) -> SimpleNamespace:
    allowed_root = tmp_path / "allowed_root"
    organized_root = tmp_path / "organized_root"
    allowed_root.mkdir()
    organized_root.mkdir()
    cfg = AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
            max_concurrent=2,
            admission_root=str(tmp_path / "admission_root"),
            admission_limit=1,
        ),
        resources=CommonResourceConfig(max_cores_per_task=4, max_memory_gb_per_task=16),
    )
    return SimpleNamespace(
        cfg=cfg,
        allowed_root=allowed_root,
        organized_root=organized_root,
        tmp_path=tmp_path,
    )


def _enqueue_job(
    env: SimpleNamespace,
    *,
    task_id: str,
    mode: str = "standard",
    molecule_key: str | None = None,
) -> SimpleNamespace:
    job_dir = env.tmp_path / "jobs" / task_id
    job_dir.mkdir(parents=True)
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    metadata = {
        "job_dir": str(job_dir),
        "selected_input_xyz": str(selected_xyz),
        "mode": mode,
    }
    if molecule_key is not None:
        metadata["molecule_key"] = molecule_key
    entry = enqueue(
        env.allowed_root,
        app_name="crest_auto",
        task_id=task_id,
        task_kind="conformer_search",
        engine="crest",
        metadata=metadata,
    )
    return SimpleNamespace(entry=entry, job_dir=job_dir, selected_xyz=selected_xyz)


def _make_result(
    job_dir: Path,
    selected_xyz: Path,
    *,
    status: str,
    reason: str,
    mode: str = "standard",
    exit_code: int = 0,
    retained_names: tuple[str, ...] = (),
) -> CrestRunResult:
    stdout_log = job_dir / "crest.stdout.log"
    stderr_log = job_dir / "crest.stderr.log"
    stdout_log.write_text("stdout\n", encoding="utf-8")
    stderr_log.write_text("stderr\n", encoding="utf-8")

    retained_paths: list[str] = []
    for name in retained_names:
        retained_path = job_dir / name
        retained_path.write_text("1\nretained\nH 0.0 0.0 0.0\n", encoding="utf-8")
        retained_paths.append(str(retained_path.resolve()))

    return CrestRunResult(
        status=status,
        reason=reason,
        command=("crest", selected_xyz.name, "--T", "4"),
        exit_code=exit_code,
        started_at="2026-04-19T00:00:00+00:00",
        finished_at="2026-04-19T00:05:00+00:00",
        stdout_log=str(stdout_log.resolve()),
        stderr_log=str(stderr_log.resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        mode=mode,
        retained_conformer_count=len(retained_paths),
        retained_conformer_paths=tuple(retained_paths),
        manifest_path=str((job_dir / "crest_job.yaml").resolve()),
        resource_request={"max_cores": 4, "max_memory_gb": 16},
        resource_actual={"assigned_cores": 4, "memory_limit_gb": 16},
    )


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _patch_common(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
    calls: dict[str, list[Any]] = {
        "started": [],
        "finished": [],
        "released": [],
    }

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda cfg: "slot-1")

    def fake_notify_started(cfg: AppConfig, **kwargs: object) -> bool:
        calls["started"].append(kwargs)
        return True

    def fake_notify_finished(cfg: AppConfig, **kwargs: object) -> bool:
        calls["finished"].append(kwargs)
        return True

    def fake_release_slot(root: str, token: str) -> None:
        calls["released"].append((root, token))

    monkeypatch.setattr(queue_cmd, "notify_job_started", fake_notify_started)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", fake_notify_finished)
    monkeypatch.setattr(queue_cmd, "release_slot", fake_release_slot)
    return calls


def test_process_one_completed_updates_queue_artifacts_index_and_organizes(
    queue_env: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job = _enqueue_job(queue_env, task_id="job-complete", mode="nci")
    manifest_path = job.job_dir / "crest_job.yaml"
    manifest_path.write_text("mode: nci\n", encoding="utf-8")
    organized_target = queue_env.organized_root / "job-complete"
    organized_target.mkdir()
    completed_result = _make_result(
        job.job_dir,
        job.selected_xyz,
        status="completed",
        reason="ok",
        mode="nci",
        retained_names=("crest_conformers.xyz", "crest_best.xyz"),
    )
    calls = _patch_common(monkeypatch)

    monkeypatch.setattr(
        queue_cmd,
        "start_crest_job",
        lambda cfg, *, job_dir, selected_xyz: SimpleNamespace(process=FakeProcess(0)),
    )
    monkeypatch.setattr(queue_cmd, "finalize_crest_job", lambda running: completed_result)
    organize_calls: list[tuple[Path, bool]] = []

    def fake_organize(cfg: AppConfig, job_dir: Path, *, notify_summary: bool) -> dict[str, str]:
        organize_calls.append((job_dir, notify_summary))
        return {"action": "organized", "target_dir": str(organized_target)}

    monkeypatch.setattr(queue_cmd, "organize_job_dir", fake_organize)

    outcome = queue_cmd._process_one(queue_env.cfg, auto_organize=True)

    assert outcome == "processed"
    entry = list_queue(queue_env.allowed_root)[0]
    assert entry.status == QueueStatus.COMPLETED
    assert entry.error == ""
    assert entry.metadata["retained_conformer_count"] == 2
    assert entry.metadata["mode"] == "nci"

    state_payload = _read_json(job.job_dir / STATE_FILE_NAME)
    assert state_payload["status"] == "completed"
    assert state_payload["reason"] == "ok"
    assert state_payload["molecule_key"] == ""
    assert state_payload["retained_conformer_count"] == 2

    report_payload = _read_json(job.job_dir / REPORT_JSON_FILE_NAME)
    assert report_payload["status"] == "completed"
    assert report_payload["reason"] == "ok"
    assert report_payload["mode"] == "nci"
    assert report_payload["manifest_path"] == str(manifest_path.resolve())
    assert report_payload["retained_conformer_paths"] == list(completed_result.retained_conformer_paths)

    report_md = (job.job_dir / REPORT_MD_FILE_NAME).read_text(encoding="utf-8")
    assert "Queue ID" in report_md
    assert "crest_conformers.xyz" in report_md

    records = list_job_locations(queue_env.allowed_root)
    assert len(records) == 1
    record = records[0]
    assert record.job_id == "job-complete"
    assert record.status == "completed"
    assert record.job_type == "crest_nci_conformer_search"
    assert record.original_run_dir == str(job.job_dir.resolve())
    assert record.organized_output_dir == str(organized_target.resolve())
    assert record.latest_known_path == str(organized_target.resolve())
    assert record.molecule_key == "selected_input"

    assert organize_calls == [(job.job_dir.resolve(), False)]
    assert len(calls["started"]) == 1
    assert calls["started"][0]["queue_id"] == job.entry.queue_id
    assert calls["started"][0]["job_dir"] == job.job_dir.resolve()
    assert len(calls["finished"]) == 1
    assert calls["finished"][0]["status"] == "completed"
    assert calls["finished"][0]["organized_output_dir"] == organized_target.resolve()
    assert calls["finished"][0]["retained_conformer_count"] == 2
    assert calls["released"] == [(queue_env.cfg.runtime.resolved_admission_root, "slot-1")]

    stdout = capsys.readouterr().out
    assert f"organized_output_dir: {organized_target.resolve()}" in stdout
    assert "status: completed" in stdout


def test_process_one_runner_failure_marks_failed_and_writes_failure_artifacts(
    queue_env: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job = _enqueue_job(queue_env, task_id="job-failed", molecule_key="fixed-key")
    manifest_path = job.job_dir / "crest_job.yaml"
    manifest_path.write_text("mode: standard\n", encoding="utf-8")
    calls = _patch_common(monkeypatch)

    def boom(cfg: AppConfig, *, job_dir: Path, selected_xyz: Path) -> SimpleNamespace:
        raise RuntimeError("boom")

    monkeypatch.setattr(queue_cmd, "start_crest_job", boom)

    outcome = queue_cmd._process_one(queue_env.cfg, auto_organize=False)

    assert outcome == "processed"
    entry = list_queue(queue_env.allowed_root)[0]
    assert entry.status == QueueStatus.FAILED
    assert entry.error == "runner_error:boom"
    assert entry.metadata["retained_conformer_count"] == 0
    assert entry.metadata["mode"] == "standard"

    state_payload = _read_json(job.job_dir / STATE_FILE_NAME)
    assert state_payload["status"] == "failed"
    assert state_payload["reason"] == "runner_error:boom"
    assert state_payload["molecule_key"] == "fixed-key"
    assert state_payload["resource_request"] == {"max_cores": 4, "max_memory_gb": 16}

    report_payload = _read_json(job.job_dir / REPORT_JSON_FILE_NAME)
    assert report_payload["status"] == "failed"
    assert report_payload["reason"] == "runner_error:boom"
    assert report_payload["command"] == []
    assert report_payload["manifest_path"] == str(manifest_path.resolve())
    assert report_payload["stdout_log"] == str((job.job_dir / "crest.stdout.log").resolve())
    assert report_payload["stderr_log"] == str((job.job_dir / "crest.stderr.log").resolve())

    record = get_job_location(queue_env.allowed_root, "job-failed")
    assert record is not None
    assert record.status == "failed"
    assert record.original_run_dir == str(job.job_dir.resolve())
    assert record.organized_output_dir == ""
    assert record.latest_known_path == str(job.job_dir.resolve())
    assert record.molecule_key == "fixed-key"

    assert len(calls["started"]) == 1
    assert len(calls["finished"]) == 1
    assert calls["finished"][0]["status"] == "failed"
    assert calls["finished"][0]["reason"] == "runner_error:boom"
    assert calls["finished"][0]["organized_output_dir"] is None
    assert calls["released"] == [(queue_env.cfg.runtime.resolved_admission_root, "slot-1")]

    stdout = capsys.readouterr().out
    assert "status: failed" in stdout


def test_process_one_cancel_requested_terminates_and_marks_cancelled(
    queue_env: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job = _enqueue_job(queue_env, task_id="job-cancelled")
    cancelled_result = _make_result(
        job.job_dir,
        job.selected_xyz,
        status="cancelled",
        reason="cancel_requested",
        exit_code=143,
    )
    calls = _patch_common(monkeypatch)
    process = FakeProcess(None)
    terminate_calls: list[FakeProcess] = []
    finalize_calls: list[tuple[str | None, str | None]] = []

    monkeypatch.setattr(
        queue_cmd,
        "start_crest_job",
        lambda cfg, *, job_dir, selected_xyz: SimpleNamespace(process=process),
    )

    def fake_get_cancel_requested(root: str, queue_id: str) -> bool:
        request_cancel(root, queue_id)
        return True

    def fake_finalize(
        running: SimpleNamespace,
        forced_status: str | None = None,
        forced_reason: str | None = None,
    ) -> CrestRunResult:
        finalize_calls.append((forced_status, forced_reason))
        return cancelled_result

    monkeypatch.setattr(queue_cmd, "get_cancel_requested", fake_get_cancel_requested)
    monkeypatch.setattr(queue_cmd, "_terminate_process", lambda proc: terminate_calls.append(proc))
    monkeypatch.setattr(queue_cmd, "finalize_crest_job", fake_finalize)

    outcome = queue_cmd._process_one(queue_env.cfg, auto_organize=False)

    assert outcome == "processed"
    entry = list_queue(queue_env.allowed_root)[0]
    assert entry.status == QueueStatus.CANCELLED
    assert entry.cancel_requested is True
    assert entry.error == "cancel_requested"
    assert entry.metadata["retained_conformer_count"] == 0
    assert entry.metadata["mode"] == "standard"

    state_payload = _read_json(job.job_dir / STATE_FILE_NAME)
    assert state_payload["status"] == "cancelled"
    assert state_payload["reason"] == "cancel_requested"

    report_payload = _read_json(job.job_dir / REPORT_JSON_FILE_NAME)
    assert report_payload["status"] == "cancelled"
    assert report_payload["reason"] == "cancel_requested"
    assert report_payload["exit_code"] == 143

    record = get_job_location(queue_env.allowed_root, "job-cancelled")
    assert record is not None
    assert record.status == "cancelled"
    assert record.latest_known_path == str(job.job_dir.resolve())

    assert terminate_calls == [process]
    assert finalize_calls == [("cancelled", "cancel_requested")]
    assert len(calls["started"]) == 1
    assert len(calls["finished"]) == 1
    assert calls["finished"][0]["status"] == "cancelled"
    assert calls["finished"][0]["organized_output_dir"] is None
    assert calls["released"] == [(queue_env.cfg.runtime.resolved_admission_root, "slot-1")]

    stdout = capsys.readouterr().out
    assert "status: cancelled" in stdout


def test_queue_worker_fill_slots_starts_multiple_child_processes(
    queue_env: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=str(queue_env.allowed_root),
            organized_root=str(queue_env.organized_root),
            max_concurrent=2,
            admission_root=str(queue_env.tmp_path / "admission_root"),
            admission_limit=2,
        ),
        resources=queue_env.cfg.resources,
    )
    job_one = _enqueue_job(queue_env, task_id="pool-job-001")
    job_two = _enqueue_job(queue_env, task_id="pool-job-002")
    tokens = iter(["slot-1", "slot-2"])
    started_commands: list[list[str]] = []
    activated_slots: list[tuple[Path, str, dict[str, object]]] = []
    child_processes = [
        FakeChildProcess(5101, None),
        FakeChildProcess(5102, None),
    ]

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda cfg_obj: next(tokens))

    def fake_popen(command: list[str], **kwargs: object) -> FakeChildProcess:
        started_commands.append(command)
        return child_processes.pop(0)

    def fake_activate_reserved_slot(root: Path | str, token: str, **kwargs: object) -> SimpleNamespace:
        activated_slots.append((Path(root), token, dict(kwargs)))
        return SimpleNamespace(token=token)

    monkeypatch.setattr(queue_cmd.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        queue_cmd,
        "activate_reserved_slot",
        fake_activate_reserved_slot,
    )

    worker = queue_cmd.QueueWorker(cfg, "/tmp/chemstack.yaml", max_concurrent=2, auto_organize=True)
    worker._fill_slots()

    assert sorted(worker._running) == sorted([job_one.entry.queue_id, job_two.entry.queue_id])
    assert len(started_commands) == 2
    assert all("chemstack.crest.worker_execution" in command for command in started_commands)
    assert all("--auto-organize" in command for command in started_commands)
    assert {command[command.index("--queue-id") + 1] for command in started_commands} == {
        job_one.entry.queue_id,
        job_two.entry.queue_id,
    }
    assert [slot[1] for slot in activated_slots] == ["slot-1", "slot-2"]
    assert [slot[2]["owner_pid"] for slot in activated_slots] == [5101, 5102]

    entries = sorted(list_queue(queue_env.allowed_root), key=lambda item: item.task_id)
    assert [entry.status for entry in entries] == [QueueStatus.RUNNING, QueueStatus.RUNNING]


def test_queue_worker_shutdown_requeues_running_children(
    queue_env: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _enqueue_job(queue_env, task_id="pool-shutdown-001")
    queue_root, entry = queue_cmd._dequeue_next_entry(queue_env.cfg) or pytest.fail("expected dequeued entry")
    released: list[tuple[Path, str]] = []
    child = FakeChildProcess(6201, None)

    monkeypatch.setattr(queue_cmd, "release_slot", lambda root, token: released.append((Path(root), token)))

    worker = queue_cmd.QueueWorker(queue_env.cfg, "/tmp/chemstack.yaml", max_concurrent=2, auto_organize=False)
    worker._shutdown_requested = True
    worker._running[entry.queue_id] = queue_cmd._RunningJob(
        queue_root=queue_root,
        entry=entry,
        process=cast(Any, child),
        admission_token="slot-1",
    )

    worker._shutdown_all()

    updated = queue_cmd._find_entry_by_target(list_queue(queue_env.allowed_root), job.entry.queue_id)
    assert updated is not None
    assert updated.status == QueueStatus.PENDING
    assert updated.cancel_requested is False
    assert child.terminate_calls == 1
    assert released == [(worker.admission_root, "slot-1")]


def test_queue_worker_reconcile_orphaned_running_requeues_entry_without_live_slot(
    queue_env: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orphan_job = _enqueue_job(queue_env, task_id="orphan-running")
    live_job = _enqueue_job(queue_env, task_id="live-running")
    orphan_root, orphan_entry = queue_cmd._dequeue_next_entry(queue_env.cfg) or pytest.fail("expected orphan entry")
    live_root, live_entry = queue_cmd._dequeue_next_entry(queue_env.cfg) or pytest.fail("expected live entry")

    monkeypatch.setattr(queue_cmd, "reconcile_stale_slots", lambda root: 0)
    monkeypatch.setattr(queue_cmd, "list_slots", lambda root: [SimpleNamespace(queue_id=live_entry.queue_id)])

    worker = queue_cmd.QueueWorker(queue_env.cfg, "/tmp/chemstack.yaml", max_concurrent=2, auto_organize=False)
    worker._reconcile_orphaned_running()

    orphan_updated = queue_cmd._find_entry_by_target(list_queue(queue_env.allowed_root), orphan_job.entry.queue_id)
    live_updated = queue_cmd._find_entry_by_target(list_queue(queue_env.allowed_root), live_job.entry.queue_id)
    assert orphan_root == live_root
    assert orphan_updated is not None
    assert live_updated is not None
    assert orphan_updated.status == QueueStatus.PENDING
    assert live_updated.status == QueueStatus.RUNNING


def test_queue_worker_reconcile_orphaned_cancel_requested_marks_cancelled(
    queue_env: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _enqueue_job(queue_env, task_id="orphan-cancelled")
    queue_root, entry = queue_cmd._dequeue_next_entry(queue_env.cfg) or pytest.fail("expected dequeued entry")
    request_cancel(queue_root, entry.queue_id)

    monkeypatch.setattr(queue_cmd, "reconcile_stale_slots", lambda root: 0)
    monkeypatch.setattr(queue_cmd, "list_slots", lambda root: [])

    worker = queue_cmd.QueueWorker(queue_env.cfg, "/tmp/chemstack.yaml", max_concurrent=2, auto_organize=False)
    worker._reconcile_orphaned_running()

    updated = queue_cmd._find_entry_by_target(list_queue(queue_env.allowed_root), job.entry.queue_id)
    assert updated is not None
    assert updated.status == QueueStatus.CANCELLED
    assert updated.error == "cancel_requested"
