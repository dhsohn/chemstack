"""Tests for chemstack.orca.commands.queue foreground worker and cancel behavior."""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from chemstack.orca.cancellation import CancelResult, CancelTargetError
from chemstack.orca.commands.queue import cmd_queue_cancel, cmd_queue_worker
from chemstack.orca.config import AppConfig, RuntimeConfig
from chemstack.orca.state_store import STATE_FILE_NAME


def _make_cfg(tmp: str) -> AppConfig:
    return AppConfig(runtime=RuntimeConfig(allowed_root=tmp))


def _make_args(tmp: str, **overrides):
    defaults = {
        "config": str(Path(tmp) / "config.yaml"),
        "auto_organize": False,
        "no_auto_organize": False,
    }
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


class TestCmdQueueCancel(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.cfg = _make_cfg(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    @patch("chemstack.orca.commands.queue.load_config")
    def test_cancel_all_pending(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from chemstack.orca.queue_store import enqueue

        for name in ("a", "b", "c"):
            reaction_dir = self.root / name
            reaction_dir.mkdir()
            enqueue(self.root, str(reaction_dir))

        args = _make_args(self._tmpdir.name, target="all-pending")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)

        self.assertEqual(rc, 0)
        self.assertIn("Cancelled 3 pending", buf.getvalue())

    @patch("chemstack.orca.commands.queue.load_config")
    def test_cancel_specific_pending(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from chemstack.orca.queue_store import enqueue

        reaction_dir = self.root / "mol_A"
        reaction_dir.mkdir()
        entry = enqueue(self.root, str(reaction_dir))
        args = _make_args(self._tmpdir.name, target=entry["queue_id"])
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)

        self.assertEqual(rc, 0)
        self.assertIn("Cancelled:", buf.getvalue())

    @patch("chemstack.orca.commands.queue.load_config")
    def test_cancel_running_entry(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        from chemstack.orca.queue_store import dequeue_next, enqueue

        reaction_dir = self.root / "mol_A"
        reaction_dir.mkdir()
        entry = enqueue(self.root, str(reaction_dir))
        dequeue_next(self.root)
        args = _make_args(self._tmpdir.name, target=entry["queue_id"])
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)

        self.assertEqual(rc, 0)
        self.assertIn("Cancel requested", buf.getvalue())

    @patch("chemstack.orca.commands.queue.load_config")
    def test_cancel_nonexistent_returns_1(self, mock_load: MagicMock) -> None:
        mock_load.return_value = self.cfg
        args = _make_args(self._tmpdir.name, target="q_nonexistent")

        rc = cmd_queue_cancel(args)

        self.assertEqual(rc, 1)

    @patch("chemstack.orca.commands.queue.logger.error")
    @patch("chemstack.orca.commands.queue.cancel_target", side_effect=CancelTargetError("bad target"))
    @patch("chemstack.orca.commands.queue.load_config")
    def test_cancel_target_error_returns_1(
        self,
        mock_load: MagicMock,
        mock_cancel: MagicMock,
        mock_error: MagicMock,
    ) -> None:
        mock_load.return_value = self.cfg
        args = _make_args(self._tmpdir.name, target="bad")

        rc = cmd_queue_cancel(args)

        self.assertEqual(rc, 1)
        mock_cancel.assert_called_once()
        mock_error.assert_called_once()

    @patch("chemstack.orca.cancellation.os.kill")
    @patch("chemstack.orca.process_tracking.is_process_alive", return_value=True)
    @patch("chemstack.orca.commands.queue.load_config")
    def test_cancel_direct_running_simulation(
        self,
        mock_load: MagicMock,
        mock_alive: MagicMock,
        mock_kill: MagicMock,
    ) -> None:
        mock_load.return_value = self.cfg
        reaction_dir = self.root / "mol_direct"
        reaction_dir.mkdir()
        _write_running_state(reaction_dir, run_id="run_direct_1", pid=4321)
        args = _make_args(self._tmpdir.name, target="mol_direct")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)

        self.assertEqual(rc, 0)
        self.assertIn("Cancel requested for running simulation", buf.getvalue())
        self.assertIn("pid: 4321", buf.getvalue())
        mock_alive.assert_called_once_with(4321)
        mock_kill.assert_called_once()

    @patch("chemstack.orca.commands.queue.cancel_target")
    @patch("chemstack.orca.commands.queue.load_config")
    def test_cancel_direct_running_simulation_without_pid_text(
        self,
        mock_load: MagicMock,
        mock_cancel: MagicMock,
    ) -> None:
        mock_load.return_value = self.cfg
        reaction_dir = self.root / "mol_direct_no_pid"
        reaction_dir.mkdir()
        mock_cancel.return_value = CancelResult(
            source="direct",
            action="requested",
            reaction_dir=str(reaction_dir),
            run_id="run_direct_no_pid",
            pid=None,
        )
        args = _make_args(self._tmpdir.name, target="mol_direct_no_pid")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_queue_cancel(args)

        self.assertEqual(rc, 0)
        output = buf.getvalue()
        self.assertIn("Cancel requested for running simulation: mol_direct_no_pid", output)
        self.assertNotIn("pid:", output)
        mock_cancel.assert_called_once_with(self.root.resolve(), "mol_direct_no_pid")


class TestCmdQueueWorker(unittest.TestCase):
    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=12345)
    def test_worker_already_running(self, mock_pid: MagicMock, mock_load: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 1)

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_runs_in_foreground_only(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mock_load.return_value = _make_cfg(tmp)
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            mock_load.return_value,
            args.config,
            max_concurrent=4,
            auto_organize=False,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
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
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=6,
            auto_organize=False,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_uses_configured_auto_organize_by_default(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            cfg.behavior.auto_organize_on_terminal = True
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=4,
            auto_organize=True,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_cli_can_enable_auto_organize(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp, auto_organize=True)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=4,
            auto_organize=True,
        )

    @patch("chemstack.orca.commands.queue.load_config")
    @patch("chemstack.orca.commands.queue.read_worker_pid", return_value=None)
    @patch("chemstack.orca.commands.queue.QueueWorker")
    def test_worker_cli_can_disable_configured_auto_organize(
        self,
        mock_worker_cls: MagicMock,
        mock_pid: MagicMock,
        mock_load: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            cfg.behavior.auto_organize_on_terminal = True
            mock_load.return_value = cfg
            mock_worker_cls.return_value.run.return_value = 0
            args = _make_args(tmp, no_auto_organize=True)

            rc = cmd_queue_worker(args)

        self.assertEqual(rc, 0)
        mock_worker_cls.assert_called_once_with(
            cfg,
            args.config,
            max_concurrent=4,
            auto_organize=False,
        )


if __name__ == "__main__":
    unittest.main()
