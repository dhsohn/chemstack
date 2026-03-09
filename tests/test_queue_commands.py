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
    _emit_entry,
    _start_daemon,
    _status_icon,
    cmd_queue_add,
    cmd_queue_cancel,
    cmd_queue_clear,
    cmd_queue_list,
    cmd_queue_stop,
    cmd_queue_worker,
)
from core.config import AppConfig, RuntimeConfig
from core.statuses import QueueStatus


def _make_cfg(tmp: str) -> AppConfig:
    return AppConfig(runtime=RuntimeConfig(allowed_root=tmp))


def _make_args(tmp: str, **overrides):
    defaults = {"config": str(Path(tmp) / "config.yaml"), "json": False}
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestStatusIcon(unittest.TestCase):
    def test_known_statuses(self) -> None:
        self.assertEqual(_status_icon(QueueStatus.PENDING.value), "\u23f3")
        self.assertEqual(_status_icon(QueueStatus.RUNNING.value), "\u25b6")
        self.assertEqual(_status_icon(QueueStatus.COMPLETED.value), "\u2705")
        self.assertEqual(_status_icon(QueueStatus.FAILED.value), "\u274c")
        self.assertEqual(_status_icon(QueueStatus.CANCELLED.value), "\u26d4")

    def test_unknown_status(self) -> None:
        self.assertEqual(_status_icon("mystery"), "?")


class TestEmitEntry(unittest.TestCase):
    def test_emit_json(self) -> None:
        entry = {"queue_id": "q_1", "status": "pending", "priority": 10, "reaction_dir": "/tmp/mol_A"}
        buf = io.StringIO()
        with redirect_stdout(buf):
            _emit_entry(entry, as_json=True)
        parsed = json.loads(buf.getvalue())
        self.assertEqual(parsed["queue_id"], "q_1")

    def test_emit_text(self) -> None:
        entry = {"queue_id": "q_1", "status": "pending", "priority": 10, "reaction_dir": "/tmp/mol_A"}
        buf = io.StringIO()
        with redirect_stdout(buf):
            _emit_entry(entry, as_json=False)
        output = buf.getvalue()
        self.assertIn("q_1", output)
        self.assertIn("pri=10", output)


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
        args = _make_args(self._tmpdir.name, reaction_dir=str(rxn_dir), priority=5, force=False, max_retries=2)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_add(args)
        self.assertEqual(rc, 0)
        self.assertIn("Enqueued", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_add_json_output(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        rxn_dir = self.root / "mol_B"
        rxn_dir.mkdir()
        args = _make_args(self._tmpdir.name, reaction_dir=str(rxn_dir), priority=10, force=False, max_retries=2, json=True)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_add(args)
        self.assertEqual(rc, 0)
        parsed = json.loads(buf.getvalue())
        self.assertIn("queue_id", parsed)

    @patch("core.commands.queue.load_config")
    def test_add_with_force(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        rxn_dir = self.root / "mol_C"
        rxn_dir.mkdir()
        args = _make_args(self._tmpdir.name, reaction_dir=str(rxn_dir), priority=10, force=True, max_retries=2)
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
        args = _make_args(self._tmpdir.name, reaction_dir=str(rxn_dir), priority=10, force=False, max_retries=2)
        cmd_queue_add(args)
        rc = cmd_queue_add(args)
        self.assertEqual(rc, 1)

    @patch("core.commands.queue.load_config")
    def test_add_invalid_dir_returns_1(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        args = _make_args(self._tmpdir.name, reaction_dir="/nonexistent/dir", priority=10, force=False, max_retries=2)
        rc = cmd_queue_add(args)
        self.assertEqual(rc, 1)


class TestCmdQueueList(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cfg = _make_cfg(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    @patch("core.commands.queue.load_config")
    def test_list_empty(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        args = _make_args(self._tmpdir.name, filter=None)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_list(args)
        self.assertEqual(rc, 0)
        self.assertIn("Queue is empty", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_list_with_entries(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from core.queue_store import enqueue
        rxn_dir = self.root / "mol_A"
        rxn_dir.mkdir()
        enqueue(self.root, str(rxn_dir))
        args = _make_args(self._tmpdir.name, filter=None)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_list(args)
        self.assertEqual(rc, 0)
        self.assertIn("Queue:", buf.getvalue())
        self.assertIn("1 pending", buf.getvalue())

    @patch("core.commands.queue.load_config")
    def test_list_json(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from core.queue_store import enqueue
        rxn_dir = self.root / "mol_A"
        rxn_dir.mkdir()
        enqueue(self.root, str(rxn_dir))
        args = _make_args(self._tmpdir.name, filter=None, json=True)
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_list(args)
        self.assertEqual(rc, 0)
        parsed = json.loads(buf.getvalue())
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 1)

    @patch("core.commands.queue.load_config")
    def test_list_with_filter(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from core.queue_store import enqueue
        rxn_dir = self.root / "mol_A"
        rxn_dir.mkdir()
        enqueue(self.root, str(rxn_dir))
        args = _make_args(self._tmpdir.name, filter="running")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_list(args)
        self.assertEqual(rc, 0)
        self.assertIn("Queue is empty", buf.getvalue())


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


class TestCmdQueueClear(unittest.TestCase):
    @patch("core.commands.queue.load_config")
    def test_clear(self, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            mock_load.return_value = cfg
            from core.queue_store import enqueue, mark_completed
            root = Path(tmp)
            d = root / "mol_A"
            d.mkdir()
            entry = enqueue(root, str(d))
            mark_completed(root, entry["queue_id"])
            args = _make_args(tmp)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_queue_clear(args)
            self.assertEqual(rc, 0)
            self.assertIn("Cleared 1", buf.getvalue())


class TestCmdQueueWorker(unittest.TestCase):
    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=12345)
    def test_worker_already_running(self, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp, daemon=False, max_concurrent=4)
            rc = cmd_queue_worker(args)
            self.assertEqual(rc, 1)

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=None)
    @patch("core.commands.queue._start_daemon", return_value=0)
    def test_worker_daemon_mode(self, mock_daemon: MagicMock, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp, daemon=True, max_concurrent=4)
            rc = cmd_queue_worker(args)
            self.assertEqual(rc, 0)
            mock_daemon.assert_called_once()

    @patch("core.commands.queue.load_config")
    @patch("core.commands.queue.read_worker_pid", return_value=None)
    @patch("core.commands.queue.QueueWorker")
    def test_worker_foreground_mode(self, mock_worker_cls: MagicMock, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp, daemon=False, max_concurrent=2)
            rc = cmd_queue_worker(args)
            self.assertEqual(rc, 0)
            mock_worker_cls.assert_called_once()


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
            args = SimpleNamespace(config=str(config_path), max_concurrent=4)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _start_daemon(args)
            self.assertEqual(rc, 0)
            self.assertIn("Worker started", buf.getvalue())

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
            args = SimpleNamespace(config=str(config_path), max_concurrent=4)
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
            args = SimpleNamespace(config=str(config_path), max_concurrent=2)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _start_daemon(args)
            self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
