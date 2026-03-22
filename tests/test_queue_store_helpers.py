from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import core.queue_store as queue_store
from core.statuses import QueueStatus, RunStatus


def _entry(
    queue_id: str,
    reaction_dir: str,
    status: str,
    *,
    priority: int = 10,
    started_at: str | None = None,
    finished_at: str | None = None,
    cancel_requested: bool = False,
    run_id: str | None = None,
    error: str | None = None,
) -> dict[str, object]:
    return {
        "queue_id": queue_id,
        "reaction_dir": reaction_dir,
        "status": status,
        "priority": priority,
        "enqueued_at": "2026-03-10T00:00:00+00:00",
        "started_at": started_at,
        "finished_at": finished_at,
        "cancel_requested": cancel_requested,
        "run_id": run_id,
        "error": error,
        "force": False,
    }


def test_queue_lock_error_builders_and_load_entries_cover_edge_cases(tmp_path: Path) -> None:
    lock_path = tmp_path / queue_store.QUEUE_LOCK_NAME

    assert "active process" in str(queue_store._queue_lock_active_error(123, {}, lock_path))
    assert "unreadable" in str(queue_store._queue_lock_unreadable_error(lock_path))
    assert "stale queue lock" in str(
        queue_store._queue_lock_stale_remove_error(123, lock_path, OSError("boom"))
    )

    assert queue_store._load_entries(tmp_path) == []

    queue_path = tmp_path / queue_store.QUEUE_FILE_NAME
    queue_path.write_text("{not-json", encoding="utf-8")
    assert queue_store._load_entries(tmp_path) == []

    queue_path.write_text(json.dumps({"status": "bad"}), encoding="utf-8")
    assert queue_store._load_entries(tmp_path) == []

    queue_path.write_text(
        json.dumps([{"queue_id": "q_ok", "status": "pending"}, "bad", []]),
        encoding="utf-8",
    )
    assert queue_store._load_entries(tmp_path) == [{"queue_id": "q_ok", "status": "pending"}]


def test_report_payload_and_terminal_report_data_cover_missing_invalid_completed_and_failed(
    tmp_path: Path,
) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()

    assert queue_store._load_report_payload(reaction_dir) is None
    assert queue_store._terminal_report_data(reaction_dir) is None

    report_path = reaction_dir / "run_report.json"
    report_path.write_text("{not-json", encoding="utf-8")
    assert queue_store._load_report_payload(reaction_dir) is None

    report_path.write_text(json.dumps(["bad"]), encoding="utf-8")
    assert queue_store._load_report_payload(reaction_dir) is None

    report_path.write_text(
        json.dumps(
            {
                "run_id": "run_done",
                "status": "completed",
                "updated_at": "2026-03-10T05:00:00+00:00",
                "final_result": {
                    "status": "completed",
                    "completed_at": "2026-03-10T04:59:59+00:00",
                },
            }
        ),
        encoding="utf-8",
    )
    assert queue_store._terminal_report_data(reaction_dir) == (
        QueueStatus.COMPLETED.value,
        "run_done",
        "2026-03-10T04:59:59+00:00",
        None,
    )

    report_path.write_text(
        json.dumps(
            {
                "run_id": "run_fail",
                "updated_at": "2026-03-10T05:00:00+00:00",
                "final_result": {
                    "status": "failed",
                    "completed_at": "2026-03-10T04:58:00+00:00",
                    "reason": "orca_crash",
                },
            }
        ),
        encoding="utf-8",
    )
    assert queue_store._terminal_report_data(reaction_dir) == (
        QueueStatus.FAILED.value,
        "run_fail",
        "2026-03-10T04:58:00+00:00",
        "orca_crash",
    )

    report_path.write_text(
        json.dumps({"run_id": "run_live", "status": "running"}),
        encoding="utf-8",
    )
    assert queue_store._terminal_report_data(reaction_dir) is None


def test_apply_terminal_reconciliation_updates_fields_and_clears_completed_error() -> None:
    completed_entry = _entry(
        "q_done",
        "/tmp/rxn",
        QueueStatus.RUNNING.value,
        finished_at=None,
        error="stale_error",
    )
    with patch("core.queue_store._now_iso", return_value="2026-03-10T06:00:00+00:00"):
        queue_store._apply_terminal_reconciliation(
            completed_entry,
            status=QueueStatus.COMPLETED.value,
            run_id="run_done",
            finished_at=None,
        )

    assert completed_entry["status"] == QueueStatus.COMPLETED.value
    assert completed_entry["finished_at"] == "2026-03-10T06:00:00+00:00"
    assert completed_entry["run_id"] == "run_done"
    assert completed_entry["error"] is None

    failed_entry = _entry(
        "q_fail",
        "/tmp/rxn",
        QueueStatus.RUNNING.value,
        finished_at="2026-03-10T01:00:00+00:00",
    )
    queue_store._apply_terminal_reconciliation(
        failed_entry,
        status=QueueStatus.FAILED.value,
        run_id=None,
        finished_at=None,
        error="boom",
    )
    assert failed_entry["finished_at"] == "2026-03-10T01:00:00+00:00"
    assert failed_entry["error"] == "boom"


def test_find_helpers_cover_active_terminal_and_queue_id_lookup() -> None:
    entries = [
        _entry("q_pending", "/tmp/a", QueueStatus.PENDING.value),
        _entry("q_running", "/tmp/a", QueueStatus.RUNNING.value),
        _entry("q_done", "/tmp/a", QueueStatus.COMPLETED.value),
        _entry("q_cancelled", "/tmp/b", QueueStatus.CANCELLED.value),
    ]

    assert queue_store._find_active_entry(entries, "/tmp/a") == entries[0]
    assert queue_store._find_active_entry(entries, "/tmp/missing") is None
    assert queue_store._find_terminal_entry(entries, "/tmp/a") == entries[2]
    assert queue_store._find_terminal_entry(entries, "/tmp/b") == entries[3]
    assert queue_store._find_terminal_entry(entries, "/tmp/missing") is None
    assert queue_store._find_entry_by_queue_id(entries, "q_running") == entries[1]
    assert queue_store._find_entry_by_queue_id(entries, "q_missing") is None


def test_reconcile_orphaned_running_entries_covers_state_terminal_paths_and_pending_fallback(
    tmp_path: Path,
) -> None:
    root = tmp_path / "queue_root"
    root.mkdir()
    completed_dir = root / "completed"
    failed_dir = root / "failed"
    pending_dir = root / "pending"
    for path in (completed_dir, failed_dir, pending_dir):
        path.mkdir()

    queue_store._save_entries(
        root,
        [
            _entry("q_done", str(completed_dir), QueueStatus.RUNNING.value, started_at="2026-03-10T00:10:00+00:00"),
            _entry("q_fail", str(failed_dir), QueueStatus.RUNNING.value, started_at="2026-03-10T00:20:00+00:00"),
            _entry("q_requeue", str(pending_dir), QueueStatus.RUNNING.value, started_at="2026-03-10T00:30:00+00:00"),
        ],
    )

    def _load_state(reaction_dir: Path):
        if reaction_dir == completed_dir:
            return {
                "run_id": "run_done",
                "status": RunStatus.COMPLETED.value,
                "updated_at": "2026-03-10T02:00:00+00:00",
                "final_result": {"completed_at": "2026-03-10T01:59:00+00:00"},
            }
        if reaction_dir == failed_dir:
            return {
                "run_id": "run_fail",
                "status": RunStatus.FAILED.value,
                "updated_at": "2026-03-10T03:00:00+00:00",
                "final_result": {
                    "completed_at": "2026-03-10T02:59:00+00:00",
                    "reason": "orca_crash",
                },
            }
        return None

    with patch("core.queue_store._read_worker_pid", return_value=None), patch(
        "core.queue_store._active_lock_pid",
        return_value=None,
    ), patch(
        "core.queue_store.load_state",
        side_effect=_load_state,
    ), patch(
        "core.queue_store._terminal_report_data",
        return_value=None,
    ):
        changed = queue_store.reconcile_orphaned_running_entries(root)

    assert changed == 3
    entries = {entry["queue_id"]: entry for entry in queue_store.list_queue(root)}
    assert entries["q_done"]["status"] == QueueStatus.COMPLETED.value
    assert entries["q_done"]["run_id"] == "run_done"
    assert entries["q_fail"]["status"] == QueueStatus.FAILED.value
    assert entries["q_fail"]["error"] == "orca_crash"
    assert entries["q_requeue"]["status"] == QueueStatus.PENDING.value
    assert entries["q_requeue"]["started_at"] is None


def test_reconcile_orphaned_running_entries_skips_blank_dirs_and_active_locks(tmp_path: Path) -> None:
    root = tmp_path / "queue_root"
    root.mkdir()
    locked_dir = root / "locked"
    locked_dir.mkdir()

    queue_store._save_entries(
        root,
        [
            _entry("q_blank", "", QueueStatus.RUNNING.value),
            _entry("q_locked", str(locked_dir), QueueStatus.RUNNING.value),
        ],
    )

    with patch("core.queue_store._read_worker_pid", return_value=None), patch(
        "core.queue_store._active_lock_pid",
        side_effect=lambda reaction_dir: 999 if reaction_dir == locked_dir else None,
    ):
        changed = queue_store.reconcile_orphaned_running_entries(root)

    assert changed == 0
    entries = {entry["queue_id"]: entry for entry in queue_store.list_queue(root)}
    assert entries["q_blank"]["status"] == QueueStatus.RUNNING.value
    assert entries["q_locked"]["status"] == QueueStatus.RUNNING.value


def test_mark_cancelled_requeue_cancel_and_update_terminal_cover_missing_and_wrong_statuses(
    tmp_path: Path,
) -> None:
    root = tmp_path / "queue_root"
    root.mkdir()
    queue_store._save_entries(
        root,
        [
            _entry("q_pending", str(root / "pending"), QueueStatus.PENDING.value),
            _entry("q_running", str(root / "running"), QueueStatus.RUNNING.value),
            _entry("q_terminal", str(root / "terminal"), QueueStatus.COMPLETED.value),
        ],
    )

    assert queue_store.mark_cancelled(root, "q_missing") is False
    assert queue_store.mark_cancelled(root, "q_pending") is False
    assert queue_store.mark_cancelled(root, "q_running") is True

    entries = {entry["queue_id"]: entry for entry in queue_store.list_queue(root)}
    assert entries["q_running"]["status"] == QueueStatus.CANCELLED.value
    assert entries["q_running"]["cancel_requested"] is False

    queue_store._save_entries(
        root,
        [
            _entry("q_running", str(root / "running"), QueueStatus.RUNNING.value, cancel_requested=True),
            _entry("q_terminal", str(root / "terminal"), QueueStatus.COMPLETED.value),
        ],
    )
    assert queue_store.requeue_running_entry(root, "q_missing") is False
    assert queue_store.requeue_running_entry(root, "q_terminal") is False
    assert queue_store.requeue_running_entry(root, "q_running") is True

    entries = {entry["queue_id"]: entry for entry in queue_store.list_queue(root)}
    assert entries["q_running"]["status"] == QueueStatus.PENDING.value
    assert entries["q_running"]["started_at"] is None
    assert entries["q_running"]["cancel_requested"] is False

    queue_store._save_entries(
        root,
        [
            _entry("q_pending", str(root / "pending"), QueueStatus.PENDING.value),
            _entry("q_running", str(root / "running"), QueueStatus.RUNNING.value),
            _entry("q_terminal", str(root / "terminal"), QueueStatus.COMPLETED.value),
        ],
    )
    assert queue_store.cancel(root, "q_missing") is None
    assert queue_store.cancel(root, "q_terminal") is None
    assert queue_store.cancel(root, "q_pending") is not None
    running_entry = queue_store.cancel(root, "q_running")
    assert running_entry is not None
    assert running_entry["cancel_requested"] is True
    assert queue_store.get_cancel_requested(root, "q_running") is True
    assert queue_store.get_cancel_requested(root, "q_missing") is False

    assert queue_store._update_terminal(root, "q_missing", QueueStatus.COMPLETED.value) is False


def test_clear_terminal_keep_last_keeps_newest_terminal_entries(tmp_path: Path) -> None:
    root = tmp_path / "queue_root"
    root.mkdir()
    queue_store._save_entries(
        root,
        [
            _entry("q_pending", str(root / "pending"), QueueStatus.PENDING.value),
            _entry(
                "q_old",
                str(root / "old"),
                QueueStatus.COMPLETED.value,
                finished_at="2026-03-10T01:00:00+00:00",
            ),
            _entry(
                "q_new",
                str(root / "new"),
                QueueStatus.FAILED.value,
                finished_at="2026-03-10T03:00:00+00:00",
            ),
            _entry(
                "q_mid",
                str(root / "mid"),
                QueueStatus.CANCELLED.value,
                finished_at="2026-03-10T02:00:00+00:00",
            ),
        ],
    )

    removed = queue_store.clear_terminal(root, keep_last=2)

    assert removed == 1
    remaining = {entry["queue_id"]: entry for entry in queue_store.list_queue(root)}
    assert set(remaining) == {"q_pending", "q_new", "q_mid"}

