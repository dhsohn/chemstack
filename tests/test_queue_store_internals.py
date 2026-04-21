from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

from chemstack.orca import queue_store
from chemstack.orca.statuses import QueueStatus
from chemstack.orca.types import QueueEntry


def _write_entries(root: Path, entries: list[dict[str, object]]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / queue_store.QUEUE_FILE_NAME).write_text(
        json.dumps(entries, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def test_lock_and_load_helpers_cover_error_paths(tmp_path: Path, caplog) -> None:
    lock_path = tmp_path / queue_store.QUEUE_LOCK_NAME
    active_error = queue_store._queue_lock_active_error(123, {}, lock_path)
    unreadable_error = queue_store._queue_lock_unreadable_error(lock_path)
    stale_error = queue_store._queue_lock_stale_remove_error(123, lock_path, OSError("boom"))

    assert "pid=123" in str(active_error)
    assert "unreadable" in str(unreadable_error)
    assert "boom" in str(stale_error)

    assert queue_store._load_entries(tmp_path) == []

    bad_queue = tmp_path / queue_store.QUEUE_FILE_NAME
    bad_queue.write_text("{bad json", encoding="utf-8")
    assert queue_store._load_entries(tmp_path) == []
    assert "starting fresh" in caplog.text

    bad_queue.write_text(json.dumps({"status": "pending"}), encoding="utf-8")
    assert queue_store._load_entries(tmp_path) == []


def test_report_reconciliation_helpers_cover_missing_nonterminal_and_failed_reason(tmp_path: Path, caplog) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()

    assert queue_store._load_report_payload(reaction_dir) is None
    assert queue_store._terminal_report_data(reaction_dir) is None

    report_path = reaction_dir / "run_report.json"
    report_path.write_text("{bad json", encoding="utf-8")
    assert queue_store._load_report_payload(reaction_dir) is None
    assert "Failed to parse run report" in caplog.text

    report_path.write_text(json.dumps([]), encoding="utf-8")
    assert queue_store._load_report_payload(reaction_dir) is None

    report_path.write_text(
        json.dumps({"status": "running", "final_result": {"status": "running"}}),
        encoding="utf-8",
    )
    assert queue_store._terminal_report_data(reaction_dir) is None

    report_path.write_text(
        json.dumps(
            {
                "run_id": "run_1",
                "updated_at": "2026-03-22T01:00:00+00:00",
                "final_result": {
                    "status": "failed",
                    "completed_at": "2026-03-22T02:00:00+00:00",
                    "reason": "scf_failed",
                },
            }
        ),
        encoding="utf-8",
    )
    assert queue_store._terminal_report_data(reaction_dir) == (
        QueueStatus.FAILED.value,
        "run_1",
        "2026-03-22T02:00:00+00:00",
        "scf_failed",
    )


def test_apply_terminal_reconciliation_and_duplicate_helpers_cover_branches(tmp_path: Path) -> None:
    entry: QueueEntry = {
        "queue_id": "q_1",
        "reaction_dir": str(tmp_path / "rxn"),
        "status": QueueStatus.RUNNING.value,
        "finished_at": None,
        "run_id": None,
        "error": "old",
    }
    queue_store._apply_terminal_reconciliation(
        entry,
        status=QueueStatus.COMPLETED.value,
        run_id="run_1",
        finished_at=None,
    )
    assert entry["status"] == QueueStatus.COMPLETED.value
    assert entry["run_id"] == "run_1"
    assert entry["error"] is None
    assert entry["finished_at"] is not None

    entries: list[QueueEntry] = [
        {"reaction_dir": "/tmp/a", "status": QueueStatus.PENDING.value},
        {"reaction_dir": "/tmp/a", "status": QueueStatus.COMPLETED.value},
        {"reaction_dir": "/tmp/a", "status": QueueStatus.FAILED.value},
    ]
    assert queue_store._find_active_entry(entries, "/tmp/a") == entries[0]
    assert queue_store._find_terminal_entry(entries, "/tmp/a") == entries[2]
    assert queue_store._find_active_entry(entries, "/tmp/missing") is None
    assert queue_store._find_terminal_entry(entries, "/tmp/missing") is None


def test_reconcile_orphaned_running_entries_covers_state_completion_failure_and_requeue(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    completed_dir = root / "completed"
    failed_dir = root / "failed"
    requeue_dir = root / "requeue"
    for path in (completed_dir, failed_dir, requeue_dir):
        path.mkdir(parents=True)

    _write_entries(
        root,
        [
            {"queue_id": "q_completed", "reaction_dir": str(completed_dir), "status": QueueStatus.RUNNING.value},
            {"queue_id": "q_failed", "reaction_dir": str(failed_dir), "status": QueueStatus.RUNNING.value},
            {"queue_id": "q_requeue", "reaction_dir": str(requeue_dir), "status": QueueStatus.RUNNING.value},
            {"queue_id": "q_blank", "reaction_dir": "   ", "status": QueueStatus.RUNNING.value},
        ],
    )

    def _load_state(reaction_dir: Path):
        if reaction_dir == completed_dir:
            return {
                "run_id": "run_completed",
                "status": "completed",
                "updated_at": "2026-03-22T01:00:00+00:00",
                "final_result": {"completed_at": "2026-03-22T01:30:00+00:00"},
            }
        if reaction_dir == failed_dir:
            return {
                "run_id": "run_failed",
                "status": "failed",
                "updated_at": "2026-03-22T02:00:00+00:00",
                "final_result": {"completed_at": "2026-03-22T02:15:00+00:00", "reason": "orca_crash"},
            }
        return None

    with patch("chemstack.orca.queue_store._acquire_queue_lock"), patch(
        "chemstack.orca.queue_store._active_lock_pid",
        return_value=None,
    ), patch("chemstack.orca.queue_store._read_worker_pid", return_value=None), patch(
        "chemstack.orca.queue_store.load_state",
        side_effect=_load_state,
    ):
        changed = queue_store.reconcile_orphaned_running_entries(root)

    assert changed == 3
    entries = {entry["queue_id"]: entry for entry in queue_store._load_entries(root)}
    assert entries["q_completed"]["status"] == QueueStatus.COMPLETED.value
    assert entries["q_completed"]["run_id"] == "run_completed"
    assert entries["q_failed"]["status"] == QueueStatus.FAILED.value
    assert entries["q_failed"]["error"] == "orca_crash"
    assert entries["q_requeue"]["status"] == QueueStatus.PENDING.value
    assert entries["q_requeue"]["started_at"] is None


def test_reconcile_orphaned_running_entries_skips_when_lock_or_worker_is_active(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    reaction_dir = root / "rxn"
    reaction_dir.mkdir(parents=True)
    _write_entries(
        root,
        [{"queue_id": "q_1", "reaction_dir": str(reaction_dir), "status": QueueStatus.RUNNING.value}],
    )

    with patch("chemstack.orca.queue_store._acquire_queue_lock"), patch(
        "chemstack.orca.queue_store._read_worker_pid",
        return_value=os.getpid(),
    ):
        assert queue_store.reconcile_orphaned_running_entries(root) == 0

    with patch("chemstack.orca.queue_store._acquire_queue_lock"), patch(
        "chemstack.orca.queue_store._read_worker_pid",
        return_value=None,
    ), patch("chemstack.orca.queue_store._active_lock_pid", return_value=999):
        assert queue_store.reconcile_orphaned_running_entries(root) == 0


def test_mark_cancelled_requeue_cancel_and_clear_terminal_cover_false_and_keep_last(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    _write_entries(
        root,
        [
            {
                "queue_id": "q_run",
                "reaction_dir": str(root / "a"),
                "status": QueueStatus.RUNNING.value,
                "finished_at": None,
                "cancel_requested": False,
            },
            {
                "queue_id": "q_done_old",
                "reaction_dir": str(root / "b"),
                "status": QueueStatus.COMPLETED.value,
                "finished_at": "2026-03-22T00:00:00+00:00",
                "cancel_requested": False,
            },
            {
                "queue_id": "q_done_new",
                "reaction_dir": str(root / "c"),
                "status": QueueStatus.FAILED.value,
                "finished_at": "2026-03-22T01:00:00+00:00",
                "cancel_requested": False,
            },
        ],
    )

    with patch("chemstack.orca.queue_store._acquire_queue_lock"):
        assert queue_store.mark_cancelled(root, "missing") is False
        assert queue_store.requeue_running_entry(root, "missing") is False

        assert queue_store.cancel(root, "missing") is None
        assert queue_store.get_cancel_requested(root, "missing") is False
        assert queue_store._update_terminal(root, "missing", QueueStatus.COMPLETED.value) is False

        cancelled = queue_store.cancel(root, "q_run")
        assert cancelled is not None
        assert cancelled["cancel_requested"] is True
        assert queue_store.requeue_running_entry(root, "q_run") is True
        assert queue_store.mark_cancelled(root, "q_run") is False

        removed = queue_store.clear_terminal(root, keep_last=1)

    assert removed == 1
    remaining = queue_store._load_entries(root)
    terminal_remaining = [entry for entry in remaining if entry["status"] in {QueueStatus.COMPLETED.value, QueueStatus.FAILED.value}]
    assert len(terminal_remaining) == 1
    assert terminal_remaining[0]["queue_id"] == "q_done_new"
