import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from core.admission_store import AdmissionLimitReachedError
from core.commands.run_inp import _submit_as_queued, cmd_run_inp
from core.config import AppConfig, PathsConfig, RuntimeConfig
from core.queue_store import enqueue, list_queue


def _make_cfg(tmp: str) -> AppConfig:
    root = Path(tmp)
    fake_orca = root / "fake_orca"
    fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")
    fake_orca.chmod(0o755)
    cfg = AppConfig(
        runtime=RuntimeConfig(allowed_root=tmp),
        paths=PathsConfig(orca_executable=str(fake_orca)),
    )
    setattr(cfg.runtime, "max_concurrent", 1)
    return cfg


def _write_inp(reaction_dir: Path) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    (reaction_dir / "rxn.inp").write_text(
        "! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n",
        encoding="utf-8",
    )


def _make_args(root: Path, reaction_dir: Path, **overrides) -> SimpleNamespace:
    defaults = {
        "config": str(root / "orca_auto.yaml"),
        "reaction_dir": str(reaction_dir),
        "force": False,
        "foreground": True,
        "priority": 10,
        "queue_only": False,
        "require_slot": False,
        "execute_now": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestRunInpSubmit(unittest.TestCase):
    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    def test_submit_executes_directly_when_slot_available_and_no_pending_backlog(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir))

            self.assertEqual(rc, 0)
            mock_execute.assert_called_once()
            mock_submit_as_queued.assert_not_called()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", side_effect=AdmissionLimitReachedError("full"))
    def test_submit_enqueues_when_admission_limit_is_reached(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir))

            self.assertEqual(rc, 0)
            mock_execute.assert_called_once()
            mock_submit_as_queued.assert_called_once()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    @patch("core.commands.run_inp._has_pending_entries", return_value=True)
    def test_submit_enqueues_when_pending_backlog_exists_even_if_slot_is_available(
        self,
        mock_has_pending: MagicMock,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir))

            self.assertEqual(rc, 0)
            mock_has_pending.assert_called_once()
            mock_execute.assert_not_called()
            mock_submit_as_queued.assert_called_once()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    def test_submit_queue_only_enqueues_without_attempting_direct_run(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir, queue_only=True))

            self.assertEqual(rc, 0)
            mock_execute.assert_not_called()
            mock_submit_as_queued.assert_called_once()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", side_effect=AdmissionLimitReachedError("full"))
    def test_submit_require_slot_fails_instead_of_enqueuing(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir, require_slot=True))

            self.assertEqual(rc, 1)
            mock_execute.assert_called_once()
            mock_submit_as_queued.assert_not_called()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    @patch("core.commands.run_inp._has_pending_entries", return_value=True)
    def test_submit_require_slot_fails_when_queue_backlog_exists(
        self,
        mock_has_pending: MagicMock,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir, require_slot=True))

            self.assertEqual(rc, 1)
            mock_has_pending.assert_called_once()
            mock_execute.assert_not_called()
            mock_submit_as_queued.assert_not_called()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    def test_submit_rejects_when_active_queue_entry_exists_for_same_reaction_dir(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)
            enqueue(root, str(reaction_dir))

            rc = cmd_run_inp(_make_args(root, reaction_dir))

            self.assertEqual(rc, 1)
            mock_execute.assert_not_called()
            mock_submit_as_queued.assert_not_called()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    def test_submit_rejects_when_same_reaction_dir_is_already_running_directly(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)
            (reaction_dir / "run.lock").write_text(
                json.dumps({"pid": os.getpid(), "started_at": "2026-03-20T00:00:00+00:00"}),
                encoding="utf-8",
            )

            rc = cmd_run_inp(_make_args(root, reaction_dir))

            self.assertEqual(rc, 1)
            mock_execute.assert_not_called()
            mock_submit_as_queued.assert_not_called()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    def test_submit_uses_execute_path_for_completed_existing_output_even_when_queue_only(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)
            (reaction_dir / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

            rc = cmd_run_inp(_make_args(root, reaction_dir, queue_only=True))

            self.assertEqual(rc, 0)
            mock_execute.assert_called_once()
            mock_submit_as_queued.assert_not_called()

    @patch("core.commands.run_inp.notify_queue_enqueued_event", return_value=True)
    @patch("core.commands.run_inp._ensure_worker_for_submission")
    def test_submit_as_queued_autostarts_worker_after_enqueue(
        self,
        mock_ensure_worker: MagicMock,
        mock_notify_queue: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)
            mock_ensure_worker.return_value = {
                "status": "started",
                "pid": 4321,
                "log_file": "/tmp/queue_worker.log",
            }

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _submit_as_queued(cfg, _make_args(root, reaction_dir, priority=3), reaction_dir)

            self.assertEqual(rc, 0)
            entries = list_queue(root)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["priority"], 3)
            self.assertIn("worker: started", buf.getvalue())
            self.assertIn("worker_pid: 4321", buf.getvalue())
            self.assertIn("worker_log: /tmp/queue_worker.log", buf.getvalue())
            mock_notify_queue.assert_called_once()

    @patch("core.commands.run_inp.notify_queue_enqueued_event", return_value=True)
    @patch("core.commands.run_inp._ensure_worker_for_submission")
    def test_submit_as_queued_returns_success_when_worker_autostart_fails(
        self,
        mock_ensure_worker: MagicMock,
        mock_notify_queue: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)
            mock_ensure_worker.return_value = {
                "status": "failed",
                "detail": "boom",
            }

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _submit_as_queued(cfg, _make_args(root, reaction_dir), reaction_dir)

            self.assertEqual(rc, 0)
            self.assertEqual(len(list_queue(root)), 1)
            self.assertIn("worker: failed", buf.getvalue())
            self.assertIn("worker_detail: boom", buf.getvalue())
            mock_notify_queue.assert_called_once()


if __name__ == "__main__":
    unittest.main()
