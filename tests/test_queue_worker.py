"""Tests for core.queue_worker — worker daemon managing concurrent job execution."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.admission_store import (
    ADMISSION_TOKEN_ENV_VAR,
    acquire_direct_slot,
    active_slot_count,
    reserve_slot,
)
from core.config import AppConfig, RuntimeConfig
from core.queue_store import dequeue_next, enqueue, list_queue
from core.queue_worker import (
    DEFAULT_MAX_CONCURRENT,
    QueueWorker,
    _RunningJob,
    _build_run_command,
    _get_run_id_from_state,
    _terminate_process,
    ensure_worker_running,
    read_worker_pid,
    start_worker_daemon,
)
from core.types import QueueEntry


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
        self.assertIn("--execute-now", cmd)

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


class TestWorkerLaunchHelpers(unittest.TestCase):
    @patch("core.queue_worker.subprocess.Popen")
    @patch("core.queue_worker.time.sleep", return_value=None)
    def test_start_worker_daemon_success(self, mock_sleep: MagicMock, mock_popen: MagicMock) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 7777
        mock_popen.return_value = mock_proc

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config" / "settings.yaml"
            config_path.parent.mkdir()
            config_path.touch()
            result = start_worker_daemon(str(config_path))

        self.assertEqual(result.status, "started")
        self.assertEqual(result.pid, 7777)
        self.assertIsNotNone(result.log_file)
        cmd = mock_popen.call_args.args[0]
        self.assertNotIn("--max-concurrent", cmd)

    @patch("core.queue_worker.subprocess.Popen")
    @patch("core.queue_worker.time.sleep", return_value=None)
    def test_start_worker_daemon_failure(self, mock_sleep: MagicMock, mock_popen: MagicMock) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        mock_proc.pid = 8888
        mock_popen.return_value = mock_proc

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config" / "settings.yaml"
            config_path.parent.mkdir()
            config_path.touch()
            result = start_worker_daemon(str(config_path))

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.pid, 8888)
        self.assertEqual(result.detail, "worker_exited_early")

    @patch("core.queue_worker.read_worker_pid", return_value=4321)
    def test_ensure_worker_running_returns_existing_pid(self, mock_read_pid: MagicMock) -> None:
        result = ensure_worker_running("/tmp/config.yaml", Path("/tmp/allowed"))
        self.assertEqual(result.status, "already_running")
        self.assertEqual(result.pid, 4321)

    @patch("core.queue_worker.read_worker_pid", return_value=None)
    @patch("core.queue_worker.start_worker_daemon")
    def test_ensure_worker_running_starts_daemon_when_missing(
        self,
        mock_start_worker: MagicMock,
        mock_read_pid: MagicMock,
    ) -> None:
        mock_start_worker.return_value.status = "started"
        mock_start_worker.return_value.pid = 5678
        result = ensure_worker_running("/tmp/config.yaml", Path("/tmp/allowed"))
        self.assertEqual(result.status, "started")
        self.assertEqual(result.pid, 5678)
        mock_start_worker.assert_called_once_with("/tmp/config.yaml")


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
        mock_proc.pid = 4321
        mock_popen.return_value = mock_proc
        entry: QueueEntry = {
            "queue_id": "q_test",
            "reaction_dir": str(self.root / "mol_A"),
            "force": False,
        }
        self.worker._start_job(entry, admission_token="slot_test")
        self.assertIn("q_test", self.worker._running)
        mock_popen.assert_called_once()
        self.assertEqual(
            mock_popen.call_args.kwargs["env"][ADMISSION_TOKEN_ENV_VAR],
            "slot_test",
        )

    @patch("core.queue_worker.subprocess.Popen", side_effect=OSError("spawn failed"))
    def test_start_job_oserror(self, mock_popen: MagicMock) -> None:
        rxn = self.root / "mol_err"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        token = reserve_slot(
            self.root,
            self.worker.max_concurrent,
            reaction_dir=str(rxn),
            queue_id=entry["queue_id"],
            source="queue_worker",
        )
        self.assertIsNotNone(token)
        dequeue_next(self.root)
        self.worker._start_job(entry, admission_token=token or "")
        self.assertNotIn(entry["queue_id"], self.worker._running)
        self.assertEqual(active_slot_count(self.root), 0)

    def test_check_completed_jobs_success(self) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0
        rxn = self.root / "mol_done"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        token = reserve_slot(
            self.root,
            self.worker.max_concurrent,
            reaction_dir=str(rxn),
            queue_id=entry["queue_id"],
            source="queue_worker",
        )
        self.assertIsNotNone(token)
        dequeue_next(self.root)
        self.worker._running["q_done"] = _RunningJob(
            queue_id=entry["queue_id"],
            reaction_dir=str(rxn),
            process=mock_proc,
            admission_token=token or "",
        )
        self.worker._check_completed_jobs()
        self.assertEqual(len(self.worker._running), 0)
        self.assertEqual(active_slot_count(self.root), 0)

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
            admission_token="slot_fail",
        )
        self.worker._check_completed_jobs()
        self.assertEqual(len(self.worker._running), 0)

    def test_check_completed_jobs_still_running(self) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        self.worker._running["q_run"] = _RunningJob(
            queue_id="q_run", reaction_dir="/tmp/r", process=mock_proc, admission_token="slot_run"
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
            admission_token="slot_cancel",
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
            admission_token="slot_shutdown",
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

    def test_reconcile_orphaned_running_uses_run_report_even_with_worker_pid_file(self) -> None:
        rxn = self.root / "mol_done"
        rxn.mkdir()
        entry = enqueue(self.root, str(rxn))
        dequeue_next(self.root)
        self.worker._write_pid_file()

        (rxn / "run_report.json").write_text(
            json.dumps(
                {
                    "run_id": "run_done_1",
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

        self.worker._reconcile_orphaned_running()

        queue_data = json.loads((self.root / "queue.json").read_text(encoding="utf-8"))
        found = next(item for item in queue_data if item["queue_id"] == entry["queue_id"])
        self.assertEqual(found["status"], "completed")
        self.assertEqual(found["run_id"], "run_done_1")


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
                mock_proc.pid = 4101
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
                mock_proc.pid = 4102
                mock_popen.return_value = mock_proc
                worker._fill_slots()
                self.assertEqual(len(worker._running), 1)

    def test_fill_slots_fills_all_available_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=3)

            for name in ("p1", "p2", "p3", "p4"):
                reaction_dir = root / name
                reaction_dir.mkdir()
                enqueue(root, str(reaction_dir))

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                mock_popen.side_effect = [
                    MagicMock(pid=4103),
                    MagicMock(pid=4104),
                    MagicMock(pid=4105),
                ]
                worker._fill_slots()

            queue_by_name = {
                Path(entry["reaction_dir"]).name: entry["status"]
                for entry in list_queue(root)
            }
            self.assertEqual(len(worker._running), 3)
            self.assertEqual(mock_popen.call_count, 3)
            self.assertEqual(
                queue_by_name,
                {
                    "p1": "running",
                    "p2": "running",
                    "p3": "running",
                    "p4": "pending",
                },
            )

    def test_fill_slots_refills_immediately_after_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=1)

            first_dir = root / "first"
            second_dir = root / "second"
            first_dir.mkdir()
            second_dir.mkdir()

            completed_entry = enqueue(root, str(first_dir))
            pending_entry = enqueue(root, str(second_dir))
            dequeue_next(root)

            completed_proc = MagicMock()
            completed_proc.poll.return_value = 0
            completion_token = reserve_slot(
                root,
                worker.max_concurrent,
                reaction_dir=str(first_dir),
                queue_id=completed_entry["queue_id"],
                source="queue_worker",
            )
            self.assertIsNotNone(completion_token)
            worker._running[completed_entry["queue_id"]] = _RunningJob(
                queue_id=completed_entry["queue_id"],
                reaction_dir=str(first_dir),
                process=completed_proc,
                admission_token=completion_token or "",
            )

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                mock_popen.return_value = MagicMock(pid=4106)
                worker._check_completed_jobs()
                worker._fill_slots()

            queue_by_name = {
                Path(entry["reaction_dir"]).name: entry["status"]
                for entry in list_queue(root)
            }
            self.assertEqual(mock_popen.call_count, 1)
            self.assertEqual(len(worker._running), 1)
            self.assertIn(pending_entry["queue_id"], worker._running)
            self.assertNotIn(completed_entry["queue_id"], worker._running)
            self.assertEqual(
                queue_by_name,
                {
                    "first": "completed",
                    "second": "running",
                },
            )

    @patch("core.admission_store.is_process_alive", return_value=True)
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
                mock_proc.pid = 4107
                mock_popen.return_value = mock_proc
                worker._fill_slots()

            self.assertEqual(len(worker._running), 1)
            self.assertEqual(mock_popen.call_count, 1)
            self.assertGreaterEqual(mock_alive.call_count, 3)

    def test_fill_slots_respects_admission_slots_without_run_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=1)

            queued = root / "queued_only"
            queued.mkdir()
            enqueue(root, str(queued))

            with acquire_direct_slot(root, max_concurrent=1, reaction_dir=str(root / "direct_hold")):
                with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                    worker._fill_slots()

            self.assertEqual(len(worker._running), 0)
            mock_popen.assert_not_called()

    @patch("core.admission_store.is_process_alive", return_value=True)
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

    def test_fill_slots_does_not_double_count_worker_jobs_with_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            worker = QueueWorker(cfg, str(root / "config.yaml"), max_concurrent=2)

            active_dir = root / "already_running"
            _write_active_lock(active_dir, pid=6001)
            token = reserve_slot(
                root,
                worker.max_concurrent,
                reaction_dir=str(active_dir),
                queue_id="q_existing",
                source="queue_worker",
            )
            self.assertIsNotNone(token)
            worker._running["q_existing"] = _RunningJob(
                queue_id="q_existing",
                reaction_dir=str(active_dir),
                process=MagicMock(),
                admission_token=token or "",
            )

            queued = root / "queued_only"
            queued.mkdir()
            enqueue(root, str(queued))

            with patch("core.queue_worker.subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_proc.pid = 4108
                mock_popen.return_value = mock_proc
                worker._fill_slots()

            self.assertEqual(len(worker._running), 2)
            mock_popen.assert_called_once()


if __name__ == "__main__":
    unittest.main()
