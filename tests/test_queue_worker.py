"""Tests for core.queue_worker — worker daemon managing concurrent job execution."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.config import AppConfig, RuntimeConfig
from core.queue_store import enqueue, dequeue_next
from core.queue_worker import (
    DEFAULT_MAX_CONCURRENT,
    QueueWorker,
    _RunningJob,
    _build_run_command,
    _get_run_id_from_state,
    _terminate_process,
    read_worker_pid,
)


def _make_cfg(tmp: str) -> AppConfig:
    return AppConfig(runtime=RuntimeConfig(allowed_root=tmp))


def _write_active_lock(reaction_dir: Path, *, pid: int) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    (reaction_dir / "run.lock").write_text(json.dumps({"pid": pid}), encoding="utf-8")


class TestBuildRunCommand(unittest.TestCase):
    def test_basic_command(self) -> None:
        cmd = _build_run_command("/tmp/rxn", "/tmp/config.yaml")
        self.assertEqual(cmd[:3], [sys.executable, "-m", "core.cli"])
        self.assertIn("--config", cmd)
        self.assertIn("/tmp/config.yaml", cmd)
        self.assertIn("run-inp", cmd)
        self.assertIn("--reaction-dir", cmd)
        self.assertIn("--foreground", cmd)

    def test_with_force(self) -> None:
        cmd = _build_run_command("/tmp/rxn", "/tmp/config.yaml", force=True)
        self.assertIn("--force", cmd)

    def test_without_force(self) -> None:
        cmd = _build_run_command("/tmp/rxn", "/tmp/config.yaml")
        self.assertNotIn("--force", cmd)


class TestTerminateProcess(unittest.TestCase):
    def test_already_terminated(self) -> None:
        proc = MagicMock()
        proc.poll.return_value = 0
        _terminate_process(proc)
        proc.terminate.assert_not_called()

    @patch("core.queue_worker.os.killpg")
    def test_killpg_success(self, mock_killpg: MagicMock) -> None:
        proc = MagicMock()
        proc.poll.return_value = None
        proc.pid = 1234
        proc.wait.return_value = 0
        _terminate_process(proc)
        mock_killpg.assert_called_once()

    @patch("core.queue_worker.os.killpg", side_effect=ProcessLookupError)
    def test_killpg_fallback_to_terminate(self, mock_killpg: MagicMock) -> None:
        proc = MagicMock()
        proc.poll.return_value = None
        proc.pid = 1234
        proc.wait.return_value = 0
        _terminate_process(proc)
        proc.terminate.assert_called_once()

    @patch("core.queue_worker.os.killpg")
    def test_escalate_to_sigkill(self, mock_killpg: MagicMock) -> None:
        proc = MagicMock()
        proc.poll.return_value = None
        proc.pid = 1234
        proc.wait.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=10)
        _terminate_process(proc)
        self.assertEqual(mock_killpg.call_count, 2)


class TestGetRunIdFromState(unittest.TestCase):
    def test_no_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = _get_run_id_from_state(tmp)
            self.assertIsNone(result)

    def test_with_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            import json
            state_file = Path(tmp) / "run_state.json"
            state_file.write_text(json.dumps({"run_id": "test_run_123"}))
            result = _get_run_id_from_state(tmp)
            self.assertEqual(result, "test_run_123")


class TestReadWorkerPid(unittest.TestCase):
    def test_no_pid_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(read_worker_pid(Path(tmp)))

    def test_stale_pid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pid_path = root / "queue_worker.pid"
            pid_path.write_text("999999999")  # non-existent pid
            result = read_worker_pid(root)
            self.assertIsNone(result)
            # PID file should be cleaned up
            self.assertFalse(pid_path.exists())

    def test_invalid_pid_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pid_path = root / "queue_worker.pid"
            pid_path.write_text("not_a_number")
            self.assertIsNone(read_worker_pid(root))


class TestQueueWorkerInit(unittest.TestCase):
    def test_max_concurrent_floor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(Path(tmp) / "config.yaml"), max_concurrent=0)
            self.assertEqual(worker.max_concurrent, 1)

    def test_default_init(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(Path(tmp) / "config.yaml"))
            self.assertEqual(worker.max_concurrent, DEFAULT_MAX_CONCURRENT)
            self.assertFalse(worker._shutdown_requested)
            self.assertEqual(len(worker._running), 0)


class TestQueueWorkerMethods(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cfg = _make_cfg(self._tmpdir.name)
        self.worker = QueueWorker(self.cfg, str(self.root / "config.yaml"), max_concurrent=2)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_pid_file_write_and_remove(self) -> None:
        self.worker._write_pid_file()
        pid_path = self.worker._pid_file_path()
        self.assertTrue(pid_path.exists())
        self.worker._remove_pid_file()
        self.assertFalse(pid_path.exists())

    def test_remove_pid_file_missing(self) -> None:
        # Should not raise
        self.worker._remove_pid_file()

    def test_pid_file_path(self) -> None:
        path = self.worker._pid_file_path()
        self.assertEqual(path.name, "queue_worker.pid")
        self.assertEqual(path.parent, self.root)

    def test_install_signal_handlers(self) -> None:
        # Should not raise
        self.worker._install_signal_handlers()

    def test_fill_slots_empty_queue(self) -> None:
        self.worker._fill_slots()
        self.assertEqual(len(self.worker._running), 0)

    @patch("core.queue_worker.subprocess.Popen")
    def test_start_job(self, mock_popen: MagicMock) -> None:
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc
        entry = {
            "queue_id": "q_test",
            "reaction_dir": str(self.root / "mol_A"),
            "force": False,
        }
        self.worker._start_job(entry)
        self.assertIn("q_test", self.worker._running)
        mock_popen.assert_called_once()

    @patch("core.queue_worker.subprocess.Popen", side_effect=OSError("spawn failed"))
    def test_start_job_oserror(self, mock_popen: MagicMock) -> None:
        rxn = self.root / "mol_err"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        dequeue_next(self.root)
        self.worker._start_job(entry)
        self.assertNotIn(entry["queue_id"], self.worker._running)

    def test_check_completed_jobs_success(self) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0
        rxn = self.root / "mol_done"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        dequeue_next(self.root)
        self.worker._running["q_done"] = _RunningJob(
            queue_id=entry["queue_id"],
            reaction_dir=str(rxn),
            process=mock_proc,
        )
        self.worker._check_completed_jobs()
        self.assertEqual(len(self.worker._running), 0)

    def test_check_completed_jobs_failure(self) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        rxn = self.root / "mol_fail"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        dequeue_next(self.root)
        self.worker._running["q_fail"] = _RunningJob(
            queue_id=entry["queue_id"],
            reaction_dir=str(rxn),
            process=mock_proc,
        )
        self.worker._check_completed_jobs()
        self.assertEqual(len(self.worker._running), 0)

    def test_check_completed_jobs_still_running(self) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        self.worker._running["q_run"] = _RunningJob(
            queue_id="q_run", reaction_dir="/tmp/r", process=mock_proc
        )
        self.worker._check_completed_jobs()
        self.assertEqual(len(self.worker._running), 1)

    def test_check_cancel_requests(self) -> None:
        from core.queue_store import cancel
        rxn = self.root / "mol_cancel"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        dequeue_next(self.root)
        cancel(self.root, entry["queue_id"])

        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.wait.return_value = 0
        self.worker._running[entry["queue_id"]] = _RunningJob(
            queue_id=entry["queue_id"],
            reaction_dir=str(rxn),
            process=mock_proc,
        )
        with patch("core.queue_worker._terminate_process"):
            self.worker._check_cancel_requests()
        self.assertNotIn(entry["queue_id"], self.worker._running)

    def test_shutdown_all_empty(self) -> None:
        self.worker._shutdown_all()
        self.assertEqual(len(self.worker._running), 0)

    def test_shutdown_all_with_running(self) -> None:
        rxn = self.root / "mol_shut"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        dequeue_next(self.root)

        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        self.worker._running[entry["queue_id"]] = _RunningJob(
            queue_id=entry["queue_id"],
            reaction_dir=str(rxn),
            process=mock_proc,
        )
        with patch("core.queue_worker._terminate_process"):
            self.worker._shutdown_all()
        self.assertEqual(len(self.worker._running), 0)

    @patch("core.queue_worker.time.sleep", side_effect=KeyboardInterrupt)
    def test_run_keyboard_interrupt(self, mock_sleep: MagicMock) -> None:
        rc = self.worker.run()
        self.assertEqual(rc, 0)
        # PID file should be cleaned up
        self.assertFalse(self.worker._pid_file_path().exists())

    @patch("core.queue_worker.time.sleep")
    def test_run_shutdown_flag(self, mock_sleep: MagicMock) -> None:
        def set_shutdown(*a):
            self.worker._shutdown_requested = True

        mock_sleep.side_effect = set_shutdown
        rc = self.worker.run()
        self.assertEqual(rc, 0)


class TestFillSlots(unittest.TestCase):
    def test_fill_slots_starts_pending_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=2)

            rxn = root / "mol_A"
            rxn.mkdir()
            enqueue(root, str(rxn))

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_popen.return_value = mock_proc
                worker._fill_slots()
                self.assertEqual(len(worker._running), 1)

    def test_fill_slots_respects_max_concurrent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=1)

            for name in ("a", "b"):
                d = root / name
                d.mkdir()
                enqueue(root, str(d))

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_popen.return_value = mock_proc
                worker._fill_slots()
                self.assertEqual(len(worker._running), 1)

    @patch("core.queue_worker.is_process_alive", return_value=True)
    def test_fill_slots_counts_external_active_runs(self, mock_alive: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=4)

            for idx in range(3):
                _write_active_lock(root / f"direct_{idx}", pid=4000 + idx)

            for name in ("queued_a", "queued_b"):
                d = root / name
                d.mkdir()
                enqueue(root, str(d))

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_popen.return_value = mock_proc
                worker._fill_slots()

            self.assertEqual(len(worker._running), 1)
            self.assertEqual(mock_popen.call_count, 1)
            self.assertGreaterEqual(mock_alive.call_count, 3)

    @patch("core.queue_worker.is_process_alive", return_value=True)
    def test_fill_slots_stops_when_global_limit_reached(self, mock_alive: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=3)

            for idx in range(3):
                _write_active_lock(root / f"direct_{idx}", pid=5000 + idx)

            queued = root / "queued_only"
            queued.mkdir()
            enqueue(root, str(queued))

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                worker._fill_slots()

            self.assertEqual(len(worker._running), 0)
            mock_popen.assert_not_called()
            self.assertEqual(mock_alive.call_count, 3)

    @patch("core.queue_worker.is_process_alive", return_value=True)
    def test_fill_slots_does_not_double_count_worker_jobs_with_lock(self, mock_alive: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=2)

            active_dir = root / "already_running"
            _write_active_lock(active_dir, pid=6001)
            worker._running["q_existing"] = _RunningJob(
                queue_id="q_existing",
                reaction_dir=str(active_dir),
                process=MagicMock(),
            )

            queued = root / "queued_only"
            queued.mkdir()
            enqueue(root, str(queued))

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_popen.return_value = mock_proc
                worker._fill_slots()

            self.assertEqual(len(worker._running), 2)
            mock_popen.assert_called_once()
            self.assertEqual(mock_alive.call_count, 0)


if __name__ == "__main__":
    unittest.main()
