import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.admission_store import (
    ADMISSION_FILE_NAME,
    activate_reserved_slot,
    acquire_direct_slot,
    active_slot_count,
    list_slots,
    reconcile_stale_slots,
    reserve_slot,
)


class TestAdmissionStore(unittest.TestCase):
    def test_acquire_direct_slot_tracks_and_releases_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reaction_dir = root / "rxn"
            reaction_dir.mkdir()

            with acquire_direct_slot(root, max_concurrent=2, reaction_dir=str(reaction_dir)) as token:
                slots = list_slots(root)
                self.assertEqual(len(slots), 1)
                self.assertEqual(slots[0]["token"], token)
                self.assertEqual(slots[0]["reaction_dir"], str(reaction_dir))

            self.assertEqual(active_slot_count(root), 0)

    def test_acquire_direct_slot_raises_when_limit_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "first"
            second = root / "second"
            first.mkdir()
            second.mkdir()

            with acquire_direct_slot(root, max_concurrent=1, reaction_dir=str(first)):
                with self.assertRaises(RuntimeError) as ctx:
                    with acquire_direct_slot(root, max_concurrent=1, reaction_dir=str(second)):
                        pass

            self.assertIn("Global admission limit reached", str(ctx.exception))

    def test_reserved_slot_activates_and_releases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reaction_dir = root / "queued"
            reaction_dir.mkdir()

            token = reserve_slot(
                root,
                1,
                queue_id="q_test",
                source="queue_worker",
            )
            self.assertIsNotNone(token)
            self.assertEqual(active_slot_count(root), 1)

            with activate_reserved_slot(
                root,
                token or "",
                reaction_dir=str(reaction_dir),
                source="queue_run",
                queue_id="q_test",
            ):
                slots = list_slots(root)
                self.assertEqual(len(slots), 1)
                self.assertEqual(slots[0]["state"], "active")
                self.assertEqual(slots[0]["reaction_dir"], str(reaction_dir))
                self.assertEqual(slots[0]["queue_id"], "q_test")
                self.assertEqual(slots[0]["source"], "queue_run")

            self.assertEqual(active_slot_count(root), 0)

    def test_reserve_slot_uses_slot_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            token = reserve_slot(root, 1, source="queue_worker")

            self.assertIsNotNone(token)
            self.assertTrue((token or "").startswith("slot_"))
            self.assertEqual(active_slot_count(root), 1)

    def test_reconcile_stale_slots_removes_dead_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = [
                {
                    "token": "slot_dead",
                    "state": "active",
                    "reaction_dir": str(root / "rxn"),
                    "queue_id": None,
                    "owner_pid": 987654321,
                    "process_start_ticks": None,
                    "source": "direct_run",
                    "acquired_at": "2026-03-20T00:00:00+00:00",
                }
            ]
            (root / ADMISSION_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

            removed = reconcile_stale_slots(root)

            self.assertEqual(removed, 1)
            self.assertEqual(active_slot_count(root), 0)

    @patch("core.admission_store.process_start_ticks", return_value=999)
    @patch("core.admission_store.is_process_alive", return_value=True)
    def test_list_slots_treats_pid_reuse_as_stale(
        self,
        mock_alive,
        mock_ticks,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = [
                {
                    "token": "slot_reused",
                    "state": "active",
                    "reaction_dir": str(root / "rxn"),
                    "queue_id": None,
                    "owner_pid": os.getpid(),
                    "process_start_ticks": 123,
                    "source": "direct_run",
                    "acquired_at": "2026-03-20T00:00:00+00:00",
                }
            ]
            (root / ADMISSION_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

            slots = list_slots(root)

            self.assertEqual(slots, [])
            mock_alive.assert_called()
            mock_ticks.assert_called()
