from __future__ import annotations

import json
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from core.process_tracking import active_run_lock_pid, current_process_lock_payload, read_pid_file


class TestProcessTracking(unittest.TestCase):
    def test_current_process_lock_payload_includes_ticks_when_available(self) -> None:
        with patch("core.process_tracking.current_process_start_ticks", return_value=123):
            payload = current_process_lock_payload()

        self.assertEqual(payload["pid"], os.getpid())
        self.assertEqual(payload["process_start_ticks"], 123)
        self.assertIsInstance(payload["started_at"], str)

    def test_current_process_lock_payload_omits_ticks_when_unavailable(self) -> None:
        with patch("core.process_tracking.current_process_start_ticks", return_value=None):
            payload = current_process_lock_payload()

        self.assertEqual(payload["pid"], os.getpid())
        self.assertNotIn("process_start_ticks", payload)
        self.assertIsInstance(payload["started_at"], str)

    def test_active_run_lock_pid_returns_none_for_invalid_pid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            (reaction_dir / "run.lock").write_text(json.dumps({"pid": 0}), encoding="utf-8")

            pid = active_run_lock_pid(reaction_dir)

        self.assertIsNone(pid)

    def test_active_run_lock_pid_returns_none_for_dead_pid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            (reaction_dir / "run.lock").write_text(json.dumps({"pid": 4321}), encoding="utf-8")

            with patch("core.process_tracking.is_process_alive", return_value=False):
                pid = active_run_lock_pid(reaction_dir)

        self.assertIsNone(pid)

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

    def test_active_run_lock_pid_invokes_callback_for_pid_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            (reaction_dir / "run.lock").write_text(
                json.dumps({"pid": 4321, "process_start_ticks": 111}),
                encoding="utf-8",
            )
            callback = Mock()
            with patch("core.process_tracking.is_process_alive", return_value=True), patch(
                "core.process_tracking.process_start_ticks",
                return_value=222,
            ):
                pid = active_run_lock_pid(reaction_dir, on_pid_reuse=callback)

        self.assertIsNone(pid)
        callback.assert_called_once_with(4321, 111, 222)

    def test_active_run_lock_pid_logs_pid_reuse_when_callback_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            (reaction_dir / "custom.lock").write_text(
                json.dumps({"pid": 4321, "process_start_ticks": 111}),
                encoding="utf-8",
            )
            logger = Mock(spec=logging.Logger)
            with patch("core.process_tracking.is_process_alive", return_value=True), patch(
                "core.process_tracking.process_start_ticks",
                return_value=None,
            ):
                pid = active_run_lock_pid(reaction_dir, logger=logger, lock_file_name="custom.lock")

        self.assertIsNone(pid)
        logger.info.assert_called_once()
        self.assertIn("Ignoring stale %s due to PID reuse", logger.info.call_args.args[0])
        self.assertEqual(logger.info.call_args.args[1], "custom.lock")

    def test_active_run_lock_pid_returns_pid_when_ticks_are_not_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction_dir = Path(td)
            (reaction_dir / "run.lock").write_text(json.dumps({"pid": 4321}), encoding="utf-8")
            with patch("core.process_tracking.is_process_alive", return_value=True), patch(
                "core.process_tracking.process_start_ticks"
            ) as process_start_ticks:
                pid = active_run_lock_pid(reaction_dir)

        self.assertEqual(pid, 4321)
        process_start_ticks.assert_not_called()

    def test_read_pid_file_returns_none_when_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "queue_worker.pid"

            pid = read_pid_file(pid_path)

        self.assertIsNone(pid)

    def test_read_pid_file_returns_none_for_invalid_contents(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "queue_worker.pid"
            pid_path.write_text("not-a-pid", encoding="utf-8")

            pid = read_pid_file(pid_path)
            exists_after = pid_path.exists()

        self.assertIsNone(pid)
        self.assertTrue(exists_after)

    def test_read_pid_file_cleans_stale_pid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "queue_worker.pid"
            pid_path.write_text("999999999", encoding="utf-8")

            pid = read_pid_file(pid_path)

        self.assertIsNone(pid)
        self.assertFalse(pid_path.exists())

    def test_read_pid_file_ignores_unlink_error_for_dead_pid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "queue_worker.pid"
            pid_path.write_text("999999999", encoding="utf-8")

            with patch("core.process_tracking.is_process_alive", return_value=False), patch.object(
                Path,
                "unlink",
                side_effect=OSError("busy"),
            ):
                pid = read_pid_file(pid_path)
                exists_after = pid_path.exists()

        self.assertIsNone(pid)
        self.assertTrue(exists_after)

    def test_read_pid_file_returns_live_pid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "queue_worker.pid"
            pid_path.write_text("1234", encoding="utf-8")

            with patch("core.process_tracking.is_process_alive", return_value=True):
                pid = read_pid_file(pid_path)
                exists_after = pid_path.exists()

        self.assertEqual(pid, 1234)
        self.assertTrue(exists_after)


if __name__ == "__main__":
    unittest.main()
