from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.process_tracking import active_run_lock_pid, current_process_lock_payload, read_pid_file


class TestProcessTracking(unittest.TestCase):
    def test_current_process_lock_payload_includes_ticks_when_available(self) -> None:
        with patch("core.process_tracking.current_process_start_ticks", return_value=123):
            payload = current_process_lock_payload()

        self.assertEqual(payload["pid"], os.getpid())
        self.assertEqual(payload["process_start_ticks"], 123)
        self.assertIsInstance(payload["started_at"], str)

    def test_active_run_lock_pid_returns_pid_for_matching_ticks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            (reaction_dir / "run.lock").write_text(
                json.dumps({"pid": 4321, "process_start_ticks": 111}),
                encoding="utf-8",
            )
            with patch("core.process_tracking.is_process_alive", return_value=True), patch(
                "core.process_tracking.process_start_ticks",
                return_value=111,
            ):
                pid = active_run_lock_pid(reaction_dir)

        self.assertEqual(pid, 4321)

    def test_active_run_lock_pid_returns_none_for_pid_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            (reaction_dir / "run.lock").write_text(
                json.dumps({"pid": 4321, "process_start_ticks": 111}),
                encoding="utf-8",
            )
            with patch("core.process_tracking.is_process_alive", return_value=True), patch(
                "core.process_tracking.process_start_ticks",
                return_value=222,
            ):
                pid = active_run_lock_pid(reaction_dir)

        self.assertIsNone(pid)

    def test_read_pid_file_cleans_stale_pid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "queue_worker.pid"
            pid_path.write_text("999999999", encoding="utf-8")

            pid = read_pid_file(pid_path)

        self.assertIsNone(pid)
        self.assertFalse(pid_path.exists())


if __name__ == "__main__":
    unittest.main()
