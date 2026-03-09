import json
import tempfile
import unittest
from pathlib import Path

from core.queue_store import (
    DuplicateEntryError,
    cancel,
    cancel_all_pending,
    clear_terminal,
    count_running,
    dequeue_next,
    enqueue,
    find_entry,
    get_cancel_requested,
    list_queue,
    mark_completed,
    mark_failed,
)
from core.statuses import QueueStatus


class TestQueueStore(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    # -- enqueue / basic flow -------------------------------------------

    def test_enqueue_creates_entry(self) -> None:
        entry = enqueue(self.root, str(self.root / "mol_A"))
        self.assertEqual(entry["status"], QueueStatus.PENDING.value)
        self.assertTrue(entry["queue_id"].startswith("q_"))
        self.assertEqual(entry["priority"], 10)

    def test_enqueue_writes_queue_file(self) -> None:
        enqueue(self.root, str(self.root / "mol_A"))
        qp = self.root / "queue.json"
        self.assertTrue(qp.exists())
        entries = json.loads(qp.read_text(encoding="utf-8"))
        self.assertEqual(len(entries), 1)

    def test_list_queue_empty(self) -> None:
        self.assertEqual(list_queue(self.root), [])

    def test_list_queue_with_filter(self) -> None:
        enqueue(self.root, str(self.root / "mol_A"))
        enqueue(self.root, str(self.root / "mol_B"))
        dequeue_next(self.root)  # mol_A → running
        self.assertEqual(len(list_queue(self.root, status_filter="pending")), 1)
        self.assertEqual(len(list_queue(self.root, status_filter="running")), 1)

    # -- duplicate prevention -------------------------------------------

    def test_duplicate_active_entry_blocked(self) -> None:
        """Pending/running entries for the same dir are always blocked."""
        enqueue(self.root, str(self.root / "mol_A"))
        with self.assertRaises(DuplicateEntryError):
            enqueue(self.root, str(self.root / "mol_A"))

    def test_duplicate_running_entry_blocked(self) -> None:
        enqueue(self.root, str(self.root / "mol_A"))
        dequeue_next(self.root)  # → running
        with self.assertRaises(DuplicateEntryError):
            enqueue(self.root, str(self.root / "mol_A"))

    def test_duplicate_terminal_without_force_blocked(self) -> None:
        """Completed/failed entries block re-enqueue without --force (accidental)."""
        entry = enqueue(self.root, str(self.root / "mol_A"))
        mark_completed(self.root, entry["queue_id"])
        with self.assertRaises(DuplicateEntryError):
            enqueue(self.root, str(self.root / "mol_A"))

    def test_duplicate_terminal_with_force_allowed(self) -> None:
        """Completed/failed entries allow re-enqueue with --force (intentional retry)."""
        entry = enqueue(self.root, str(self.root / "mol_A"))
        mark_completed(self.root, entry["queue_id"])
        new_entry = enqueue(self.root, str(self.root / "mol_A"), force=True)
        self.assertNotEqual(entry["queue_id"], new_entry["queue_id"])
        self.assertTrue(new_entry.get("force"))

    def test_duplicate_active_blocked_even_with_force(self) -> None:
        """Active (pending/running) entries are always blocked, even with force."""
        enqueue(self.root, str(self.root / "mol_A"))
        with self.assertRaises(DuplicateEntryError):
            enqueue(self.root, str(self.root / "mol_A"), force=True)

    # -- dequeue --------------------------------------------------------

    def test_dequeue_returns_highest_priority(self) -> None:
        enqueue(self.root, str(self.root / "low"), priority=20)
        enqueue(self.root, str(self.root / "high"), priority=1)
        enqueue(self.root, str(self.root / "mid"), priority=10)

        entry = dequeue_next(self.root)
        self.assertIsNotNone(entry)
        self.assertIn("high", entry["reaction_dir"])
        self.assertEqual(entry["status"], QueueStatus.RUNNING.value)
        self.assertIsNotNone(entry["started_at"])

    def test_dequeue_empty_returns_none(self) -> None:
        self.assertIsNone(dequeue_next(self.root))

    # -- cancel ---------------------------------------------------------

    def test_cancel_pending(self) -> None:
        entry = enqueue(self.root, str(self.root / "mol_A"))
        result = cancel(self.root, entry["queue_id"])
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], QueueStatus.CANCELLED.value)

    def test_cancel_running_sets_flag(self) -> None:
        entry = enqueue(self.root, str(self.root / "mol_A"))
        dequeue_next(self.root)
        result = cancel(self.root, entry["queue_id"])
        self.assertIsNotNone(result)
        self.assertTrue(result["cancel_requested"])
        self.assertTrue(get_cancel_requested(self.root, entry["queue_id"]))

    def test_cancel_terminal_returns_none(self) -> None:
        entry = enqueue(self.root, str(self.root / "mol_A"))
        mark_completed(self.root, entry["queue_id"])
        self.assertIsNone(cancel(self.root, entry["queue_id"]))

    def test_cancel_all_pending(self) -> None:
        enqueue(self.root, str(self.root / "a"))
        enqueue(self.root, str(self.root / "b"))
        enqueue(self.root, str(self.root / "c"))
        dequeue_next(self.root)  # a → running
        count = cancel_all_pending(self.root)
        self.assertEqual(count, 2)
        # running entry should be unaffected
        self.assertEqual(count_running(self.root), 1)

    # -- mark_completed / mark_failed -----------------------------------

    def test_mark_completed(self) -> None:
        entry = enqueue(self.root, str(self.root / "mol_A"))
        dequeue_next(self.root)
        self.assertTrue(mark_completed(self.root, entry["queue_id"], run_id="run_test"))
        found = find_entry(self.root, entry["queue_id"])
        self.assertEqual(found["status"], QueueStatus.COMPLETED.value)
        self.assertEqual(found["run_id"], "run_test")

    def test_mark_failed_with_error(self) -> None:
        entry = enqueue(self.root, str(self.root / "mol_A"))
        dequeue_next(self.root)
        self.assertTrue(mark_failed(self.root, entry["queue_id"], error="exit_code=1"))
        found = find_entry(self.root, entry["queue_id"])
        self.assertEqual(found["status"], QueueStatus.FAILED.value)
        self.assertEqual(found["error"], "exit_code=1")

    # -- clear / count ---------------------------------------------------

    def test_clear_terminal(self) -> None:
        e1 = enqueue(self.root, str(self.root / "a"))
        e2 = enqueue(self.root, str(self.root / "b"))
        enqueue(self.root, str(self.root / "c"))  # stays pending
        mark_completed(self.root, e1["queue_id"])
        mark_failed(self.root, e2["queue_id"])
        removed = clear_terminal(self.root)
        self.assertEqual(removed, 2)
        remaining = list_queue(self.root)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["status"], QueueStatus.PENDING.value)

    def test_count_running(self) -> None:
        enqueue(self.root, str(self.root / "a"))
        enqueue(self.root, str(self.root / "b"))
        dequeue_next(self.root)
        dequeue_next(self.root)
        self.assertEqual(count_running(self.root), 2)

    # -- find_entry -----------------------------------------------------

    def test_find_entry_exists(self) -> None:
        entry = enqueue(self.root, str(self.root / "mol_A"))
        found = find_entry(self.root, entry["queue_id"])
        self.assertIsNotNone(found)
        self.assertEqual(found["queue_id"], entry["queue_id"])

    def test_find_entry_missing(self) -> None:
        self.assertIsNone(find_entry(self.root, "q_nonexistent"))

    # -- priority tie-breaking by enqueued_at ---------------------------

    def test_fifo_on_same_priority(self) -> None:
        e1 = enqueue(self.root, str(self.root / "first"))
        e2 = enqueue(self.root, str(self.root / "second"))
        dequeued = dequeue_next(self.root)
        self.assertEqual(dequeued["queue_id"], e1["queue_id"])


if __name__ == "__main__":
    unittest.main()
