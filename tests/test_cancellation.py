import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cancellation import CancelTargetError, cancel_target
from core.queue_store import dequeue_next, enqueue, get_cancel_requested
from core.state_store import STATE_FILE_NAME


def _write_running_state(reaction_dir: Path, *, run_id: str, pid: int) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "run_id": run_id,
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(reaction_dir / "rxn.inp"),
        "max_retries": 2,
        "status": "running",
        "started_at": "2026-03-01T00:00:00+00:00",
        "updated_at": "2026-03-01T00:05:00+00:00",
        "attempts": [{"index": 1}],
        "final_result": None,
    }
    (reaction_dir / STATE_FILE_NAME).write_text(json.dumps(state), encoding="utf-8")
    (reaction_dir / "run.lock").write_text(json.dumps({"pid": pid}), encoding="utf-8")


class TestCancellation(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_cancel_target_pending_queue_entry_by_queue_id(self) -> None:
        reaction_dir = self.root / "mol_A"
        reaction_dir.mkdir()
        entry = enqueue(self.root, str(reaction_dir))

        result = cancel_target(self.root, entry["queue_id"])

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.source, "queue")
        self.assertEqual(result.action, "cancelled")
        self.assertEqual(result.queue_id, entry["queue_id"])

    def test_cancel_target_running_queue_entry_by_reaction_dir_name(self) -> None:
        reaction_dir = self.root / "mol_A"
        reaction_dir.mkdir()
        entry = enqueue(self.root, str(reaction_dir))
        dequeue_next(self.root)

        result = cancel_target(self.root, "mol_A")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.source, "queue")
        self.assertEqual(result.action, "requested")
        self.assertTrue(get_cancel_requested(self.root, entry["queue_id"]))

    @patch("core.cancellation.os.kill")
    @patch("core.process_tracking.is_process_alive", return_value=True)
    def test_cancel_target_direct_run_by_relative_dir(
        self,
        mock_alive,
        mock_kill,
    ) -> None:
        reaction_dir = self.root / "group_a" / "mol_A"
        _write_running_state(reaction_dir, run_id="run_direct_1", pid=4321)

        result = cancel_target(self.root, "group_a/mol_A")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.source, "direct")
        self.assertEqual(result.action, "requested")
        self.assertEqual(result.run_id, "run_direct_1")
        self.assertEqual(result.pid, 4321)
        mock_alive.assert_called_once_with(4321)
        mock_kill.assert_called_once()

    @patch("core.cancellation.os.kill")
    @patch("core.process_tracking.is_process_alive", return_value=True)
    def test_cancel_target_direct_run_by_run_id(
        self,
        mock_alive,
        mock_kill,
    ) -> None:
        reaction_dir = self.root / "group_a" / "mol_A"
        _write_running_state(reaction_dir, run_id="run_direct_1", pid=4321)

        result = cancel_target(self.root, "run_direct_1")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.source, "direct")
        self.assertEqual(result.run_id, "run_direct_1")
        mock_alive.assert_called_once_with(4321)
        mock_kill.assert_called_once()

    @patch("core.cancellation.os.kill")
    @patch("core.process_tracking.is_process_alive", return_value=True)
    def test_cancel_target_ambiguous_direct_basename(
        self,
        mock_alive,
        mock_kill,
    ) -> None:
        _write_running_state(self.root / "group_a" / "mol_A", run_id="run_direct_1", pid=1111)
        _write_running_state(self.root / "group_b" / "mol_A", run_id="run_direct_2", pid=2222)

        with self.assertRaises(CancelTargetError) as ctx:
            cancel_target(self.root, "mol_A")

        self.assertIn("Ambiguous cancel target", str(ctx.exception))
        mock_alive.assert_any_call(1111)
        mock_alive.assert_any_call(2222)
        mock_kill.assert_not_called()

    @patch("core.cancellation.os.kill")
    @patch("core.process_tracking.is_process_alive", return_value=True)
    def test_cancel_target_prefers_queue_for_worker_managed_run(
        self,
        mock_alive,
        mock_kill,
    ) -> None:
        reaction_dir = self.root / "mol_A"
        reaction_dir.mkdir()
        entry = enqueue(self.root, str(reaction_dir))
        dequeue_next(self.root)
        _write_running_state(reaction_dir, run_id="run_worker_1", pid=4321)

        result = cancel_target(self.root, "mol_A")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.source, "queue")
        self.assertEqual(result.queue_id, entry["queue_id"])
        self.assertTrue(get_cancel_requested(self.root, entry["queue_id"]))
        mock_alive.assert_not_called()
        mock_kill.assert_not_called()


if __name__ == "__main__":
    unittest.main()
