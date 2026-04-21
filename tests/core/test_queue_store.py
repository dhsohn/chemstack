from __future__ import annotations

import json
from contextlib import nullcontext
from itertools import count
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chemstack.core.queue import store
from chemstack.core.queue.types import QueueStatus


def _install_deterministic_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    token_counter = count(1)
    time_counter = count(1)

    monkeypatch.setattr(store, "file_lock", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(store, "timestamped_token", lambda prefix: f"{prefix}_{next(token_counter):04d}")
    monkeypatch.setattr(
        store,
        "now_utc_iso",
        lambda: f"2026-04-19T00:00:{next(time_counter):02d}+00:00",
    )


def _queue_file(root: Path) -> Path:
    return root / "queue.json"


def _entry(
    queue_id: str,
    *,
    app_name: str = "app",
    task_id: str = "task",
    task_kind: str = "kind",
    engine: str = "engine",
    status: QueueStatus = QueueStatus.PENDING,
    priority: int = 10,
    enqueued_at: str = "2026-04-19T00:00:00+00:00",
    started_at: str = "",
    finished_at: str = "",
    cancel_requested: bool = False,
    error: str = "",
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "queue_id": queue_id,
        "app_name": app_name,
        "task_id": task_id,
        "task_kind": task_kind,
        "engine": engine,
        "status": status.value,
        "priority": priority,
        "enqueued_at": enqueued_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "cancel_requested": cancel_requested,
        "error": error,
        "metadata": metadata or {},
    }


def test_list_queue_handles_missing_and_invalid_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_deterministic_helpers(monkeypatch)

    assert store.list_queue(tmp_path) == []

    _queue_file(tmp_path).write_text("{not valid json", encoding="utf-8")
    assert store.list_queue(tmp_path) == []

    _queue_file(tmp_path).write_text(json.dumps({"queue_id": "q-1"}), encoding="utf-8")
    assert store.list_queue(tmp_path) == []

    _queue_file(tmp_path).write_text(
        json.dumps(
            [
                {
                    **_entry("q-2"),
                    "status": "not-a-real-status",
                    "metadata": ["not", "a", "dict"],
                }
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    entries = store.list_queue(tmp_path)
    assert len(entries) == 1
    assert entries[0].status == QueueStatus.PENDING
    assert entries[0].metadata == {}


def test_enqueue_blocks_active_duplicates_and_allows_reenqueue_after_terminal_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_deterministic_helpers(monkeypatch)

    first = store.enqueue(
        tmp_path,
        app_name="app",
        task_id="task-1",
        task_kind="kind",
        engine="engine",
    )

    running = store.dequeue_next(tmp_path)
    assert running is not None
    assert running.queue_id == first.queue_id
    assert running.status == QueueStatus.RUNNING

    with pytest.raises(store.DuplicateQueueEntryError):
        store.enqueue(
            tmp_path,
            app_name="app",
            task_id="task-1",
            task_kind="kind",
            engine="engine",
        )

    completed = store.mark_completed(tmp_path, first.queue_id)
    assert completed is not None
    assert completed.status == QueueStatus.COMPLETED

    second = store.enqueue(
        tmp_path,
        app_name="app",
        task_id="task-1",
        task_kind="kind",
        engine="engine",
    )

    entries = store.list_queue(tmp_path)
    assert [entry.status for entry in entries] == [QueueStatus.COMPLETED, QueueStatus.PENDING]
    assert second.queue_id != first.queue_id


def test_dequeue_next_respects_priority_time_and_insertion_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_deterministic_helpers(monkeypatch)

    _queue_file(tmp_path).write_text(
        json.dumps(
            [
                _entry("q-1", task_id="a", priority=3, enqueued_at="2026-04-19T00:00:03+00:00"),
                _entry("q-2", task_id="b", priority=1, enqueued_at="2026-04-19T00:00:05+00:00"),
                _entry("q-3", task_id="c", priority=1, enqueued_at="2026-04-19T00:00:01+00:00"),
                _entry("q-4", task_id="d", priority=1, enqueued_at="2026-04-19T00:00:01+00:00"),
            ],
            indent=2,
        ),
        encoding="utf-8",
    )

    picked = [store.dequeue_next(tmp_path) for _ in range(4)]
    assert [entry.queue_id for entry in picked if entry is not None] == ["q-3", "q-4", "q-2", "q-1"]
    assert store.dequeue_next(tmp_path) is None


def test_request_cancel_handles_pending_running_and_terminal_entries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_deterministic_helpers(monkeypatch)

    pending = store.enqueue(
        tmp_path,
        app_name="app",
        task_id="pending",
        task_kind="kind",
        engine="engine",
    )
    pending_cancelled = store.request_cancel(tmp_path, pending.queue_id)
    assert pending_cancelled is not None
    assert pending_cancelled.status == QueueStatus.CANCELLED
    assert pending_cancelled.cancel_requested is True
    assert pending_cancelled.finished_at == "2026-04-19T00:00:02+00:00"

    running = store.enqueue(
        tmp_path,
        app_name="app",
        task_id="running",
        task_kind="kind",
        engine="engine",
    )
    dequeued = store.dequeue_next(tmp_path)
    assert dequeued is not None
    assert dequeued.queue_id == running.queue_id

    running_cancelled = store.request_cancel(tmp_path, running.queue_id)
    assert running_cancelled is not None
    assert running_cancelled.status == QueueStatus.RUNNING
    assert running_cancelled.cancel_requested is True
    assert running_cancelled.finished_at == ""
    assert store.get_cancel_requested(tmp_path, running.queue_id) is True
    assert store.get_cancel_requested(tmp_path, "missing-queue-id") is False
    assert store.request_cancel(tmp_path, "missing-queue-id") is None

    terminal = store.enqueue(
        tmp_path,
        app_name="app",
        task_id="terminal",
        task_kind="kind",
        engine="engine",
    )
    assert store.mark_completed(tmp_path, terminal.queue_id) is not None
    assert store.request_cancel(tmp_path, terminal.queue_id) is None


@pytest.mark.parametrize(
    ("helper_name", "helper_kwargs", "expected_status"),
    [
        ("mark_completed", {}, QueueStatus.COMPLETED),
        ("mark_failed", {"error": "  boom  "}, QueueStatus.FAILED),
        ("mark_cancelled", {"error": "  stop  "}, QueueStatus.CANCELLED),
    ],
)
def test_mark_helpers_merge_metadata_updates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    helper_name: str,
    helper_kwargs: dict[str, object],
    expected_status: QueueStatus,
) -> None:
    _install_deterministic_helpers(monkeypatch)

    entry = store.enqueue(
        tmp_path,
        app_name="app",
        task_id=f"task-{helper_name}",
        task_kind="kind",
        engine="engine",
        metadata={"keep": "yes", "shared": "old"},
    )

    helper = getattr(store, helper_name)
    updated = helper(
        tmp_path,
        entry.queue_id,
        metadata_update={"shared": "new", "added": 42},
        **helper_kwargs,
    )

    assert updated is not None
    assert updated.status == expected_status
    assert updated.metadata == {"keep": "yes", "shared": "new", "added": 42}
    if helper_name != "mark_completed":
        assert updated.error == str(helper_kwargs["error"]).strip()
    assert helper(tmp_path, "missing-queue-id", **helper_kwargs) is None
