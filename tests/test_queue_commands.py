"""Tests for core.commands.queue — CLI subcommands for queue management."""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from core.commands.queue import (
    _start_daemon,
    cmd_queue_add,
    cmd_queue_cancel,
    cmd_queue_stop,
    cmd_queue_worker,
)
from core.config import AppConfig, RuntimeConfig
from core.state_store import STATE_FILE_NAME


def _make_cfg(tmp: str) -> AppConfig:
    return AppConfig(runtime=RuntimeConfig(allowed_root=tmp))


def _make_args(tmp: str, **overrides):
    defaults = {"config": str(Path(tmp) / "config.yaml")}
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _write_running_state(reaction_dir: Path, *, run_id: str, pid: int) -> None:
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


class TestCmdQueueAdd(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cfg = _make_cfg(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    @patch("core.commands.queue.load_config")
    def test_add_success(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        rxn_dir = self.root / "mol_A"
        rxn_dir.mkdir()
        args = _make_args(self._tmpdir.name, reaction_dir=str(rxn_dir), priority=5, force=False)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_add(args)
        self.assertEqual(rc, 0)
        self.assertIn("Enqueued", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_add_with_force(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        rxn_dir = self.root / "mol_C"
        rxn_dir.mkdir()
        args = _make_args(self._tmpdir.name, reaction_dir=str(rxn_dir), priority=10, force=True)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_add(args)
        self.assertEqual(rc, 0)
        self.assertIn("force: true", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_add_duplicate_returns_1(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        rxn_dir = self.root / "mol_D"
        rxn_dir.mkdir()
        args = _make_args(self._tmpdir.name, reaction_dir=str(rxn_dir), priority=10, force=False)
        cmd_queue_add(args)
        rc = cmd_queue_add(args)
        self.assertEqual(rc, 1)

    @patch("core.commands.queue.load_config")
    def test_add_invalid_dir_returns_1(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        args = _make_args(self._tmpdir.name, reaction_dir="/nonexistent/dir", priority=10, force=False)
        rc = cmd_queue_add(args)
        self.assertEqual(rc, 1)


class TestCmdQueueCancel(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cfg = _make_cfg(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    @patch("core.commands.queue.load_config")
    def test_cancel_all_pending(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from core.queue_store import enqueue
        for name in ("a", "b", "c"):
            d = self.root / name
            d.mkdir()
            enqueue(self.root, str(d))
        args = _make_args(self._tmpdir.name, target="all-pending")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)
        self.assertEqual(rc, 0)
        self.assertIn("Cancelled 3 pending", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_cancel_specific_pending(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from core.queue_store import enqueue
        d = self.root / "mol_A"
        d.mkdir()
        entry = enqueue(self.root, str(d))
        args = _make_args(self._tmpdir.name, target=entry["queue_id"])
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)
        self.assertEqual(rc, 0)
        self.assertIn("Cancelled:", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_cancel_running_entry(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from core.queue_store import dequeue_next, enqueue
        d = self.root / "mol_A"
        d.mkdir()
        entry = enqueue(self.root, str(d))
        dequeue_next(self.root)
        args = _make_args(self._tmpdir.name, target=entry["queue_id"])
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)
        self.assertEqual(rc, 0)
        self.assertIn("Cancel requested", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_cancel_nonexistent_returns_1(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        args = _make_args(self._tmpdir.name, target="q_nonexistent")
        rc = cmd_queue_cancel(args)
        self.assertEqual(rc, 1)

    @patch("core.cancellation.os.kill")
    @patch("core.cancellation.is_process_alive", return_value=True)
    @patch("core.commands.queue.load_config")
    def test_cancel_direct_running_simulation(
        self,
        mock_load: MagicMock,
        mock_alive: MagicMock,
        mock_kill: MagicMock,
    ) -> None:
        mock_load.return_value = self.cfg
        d = self.root / "mol_direct"
        d.mkdir()
        _write_running_state(d, run_id="run_direct_1", pid=4321)
        args = _make_args(self._tmpdir.name, target="mol_direct")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)
        self.assertEqual(rc, 0)
        self.assertIn("Cancel requested for running simulation", buf.getvalue())
        self.assertIn("pid: 4321", buf.getvalue())
        mock_alive.assert_called_once_with(4321)
        mock_kill.assert_called_once()


class TestCmdQueueWorker(unittest.TestCase):
    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=12345)
    def test_worker_already_running(self, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp, daemon=False)
            rc = cmd_queue_worker(args)
            self.assertEqual(rc, 1)

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=None)
    @patch("core.commands.queue._start_daemon", return_value=0)
    def test_worker_daemon_mode(self, mock_daemon: MagicMock, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp, daemon=True)
            rc = cmd_queue_worker(args)
            self.assertEqual(rc, 0)
            mock_daemon.assert_called_once_with(args)

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=None)
    @patch("core.commands.queue.QueueWorker")
    def test_worker_foreground_mode(self, mock_worker_cls: MagicMock, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp, daemon=False)
            rc = cmd_queue_worker(args)
            self.assertEqual(rc, 0)
            mock_worker_cls.assert_called_once_with(
                mock_load.return_value,
                args.config,
                max_concurrent=4,
            )

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=None)
    @patch("core.commands.queue.QueueWorker")
    def test_worker_uses_config_max_concurrent_when_flag_omitted(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            cfg.runtime.max_concurrent = 6
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp, daemon=False)
            rc = cmd_queue_worker(args)
            self.assertEqual(rc, 0)
            mock_worker_cls.assert_called_once_with(
                cfg,
                args.config,
                max_concurrent=6,
            )


class TestCmdQueueStop(unittest.TestCase):
    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=None)
    def test_stop_no_worker(self, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_queue_stop(args)
            self.assertEqual(rc, 0)
            self.assertIn("No worker is running", buf.getvalue())

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=99999)
    @patch("os.kill", side_effect=ProcessLookupError)
    def test_stop_stale_pid(self, mock_kill: MagicMock, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            # Create a fake PID file so the unlink branch is exercised
            pid_file = Path(tmp) / "queue_worker.pid"
            pid_file.write_text("99999")
            args = _make_args(tmp)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_queue_stop(args)
            self.assertEqual(rc, 0)
            self.assertIn("not found", buf.getvalue())

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=99999)
    @patch("os.kill")
    def test_stop_sends_sigterm(self, mock_kill: MagicMock, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_queue_stop(args)
            self.assertEqual(rc, 0)
            self.assertIn("SIGTERM", buf.getvalue())

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=99999)
    @patch("os.kill", side_effect=PermissionError)
    def test_stop_permission_error(self, mock_kill: MagicMock, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp)
            rc = cmd_queue_stop(args)
            self.assertEqual(rc, 1)


class TestStartDaemon(unittest.TestCase):
    @patch("core.commands.queue.subprocess.Popen")
    @patch("core.commands.queue.time.sleep", return_value=None)
    def test_daemon_start_success(self, mock_sleep: MagicMock, mock_popen: MagicMock) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 7777
        mock_popen.return_value = mock_proc

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config" / "settings.yaml"
            config_path.parent.mkdir()
            config_path.touch()
            args = SimpleNamespace(config=str(config_path))
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _start_daemon(args)
            self.assertEqual(rc, 0)
            self.assertIn("Worker started", buf.getvalue())
            cmd = mock_popen.call_args.args[0]
            self.assertNotIn("--max-concurrent", cmd)

    @patch("core.commands.queue.subprocess.Popen")
    @patch("core.commands.queue.time.sleep", return_value=None)
    def test_daemon_start_failure(self, mock_sleep: MagicMock, mock_popen: MagicMock) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1  # exited immediately
        mock_popen.return_value = mock_proc

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config" / "settings.yaml"
            config_path.parent.mkdir()
            config_path.touch()
            args = SimpleNamespace(config=str(config_path))
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _start_daemon(args)
            self.assertEqual(rc, 1)
            self.assertIn("failed to start", buf.getvalue())

    @patch("core.commands.queue.subprocess.Popen")
    @patch("core.commands.queue.time.sleep", return_value=None)
    def test_daemon_non_config_dir(self, mock_sleep: MagicMock, mock_popen: MagicMock) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 8888
        mock_popen.return_value = mock_proc

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "my_settings.yaml"
            config_path.touch()
            args = SimpleNamespace(config=str(config_path))
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _start_daemon(args)
            self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
