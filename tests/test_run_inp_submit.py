import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
        "priority": 10,
        "queue_only": False,
        "require_slot": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestRunInpSubmit(unittest.TestCase):
    @patch("core.commands.run_inp.load_config")
    def test_submit_rejects_require_slot_before_loading_config(
        self,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reaction_dir = root / "rxn"

            rc = cmd_run_inp(_make_args(root, reaction_dir, require_slot=True))

        self.assertEqual(rc, 1)
        mock_load_config.assert_not_called()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    @patch("core.commands.run_inp._cmd_run_inp_execute", return_value=0)
    def test_submit_always_enqueues_without_attempting_direct_execution(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mock_load_config.return_value = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir))

        self.assertEqual(rc, 0)
        mock_execute.assert_not_called()
        mock_submit_as_queued.assert_called_once()

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp._submit_as_queued", return_value=0)
    def test_submit_queue_only_alias_still_enqueues(
        self,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mock_load_config.return_value = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            rc = cmd_run_inp(_make_args(root, reaction_dir, queue_only=True))

        self.assertEqual(rc, 0)
        mock_submit_as_queued.assert_called_once()

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
            mock_load_config.return_value = _make_cfg(tmp)
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
            mock_load_config.return_value = _make_cfg(tmp)
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
    def test_submit_uses_completed_output_shortcut_before_enqueue(
        self,
        mock_execute: MagicMock,
        mock_submit_as_queued: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mock_load_config.return_value = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)
            (reaction_dir / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

            rc = cmd_run_inp(_make_args(root, reaction_dir, queue_only=True))

        self.assertEqual(rc, 0)
        mock_execute.assert_called_once()
        mock_submit_as_queued.assert_not_called()

    @patch("core.commands.run_inp.notify_queue_enqueued_event", return_value=True)
    @patch("core.queue_worker.read_worker_pid", return_value=None)
    def test_submit_as_queued_reports_inactive_worker_without_autostart(
        self,
        mock_read_worker_pid: MagicMock,
        mock_notify_queue: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _submit_as_queued(cfg, _make_args(root, reaction_dir, priority=3), reaction_dir)

            entries = list_queue(root)

            self.assertEqual(rc, 0)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["priority"], 3)
            self.assertEqual(entries[0]["app_name"], "orca_auto")
            self.assertTrue(entries[0]["task_id"].startswith("orca_"))
            self.assertEqual(entries[0]["metadata"]["selected_inp"], str(reaction_dir / "rxn.inp"))
            self.assertEqual(entries[0]["metadata"]["max_retries"], 2)
            self.assertEqual(entries[0]["metadata"]["submitted_via"], "run_inp")
            tracking_records = json.loads((root / "job_locations.json").read_text(encoding="utf-8"))
            self.assertEqual(len(tracking_records), 1)
            self.assertEqual(tracking_records[0]["job_id"], entries[0]["task_id"])
            self.assertEqual(tracking_records[0]["status"], "queued")
            self.assertEqual(tracking_records[0]["original_run_dir"], str(reaction_dir.resolve()))
            self.assertEqual(tracking_records[0]["selected_input_xyz"], str((reaction_dir / "rxn.inp").resolve()))
            self.assertIn("status: queued", buf.getvalue())
            self.assertIn("job_id:", buf.getvalue())
            self.assertIn("worker: inactive", buf.getvalue())
            self.assertNotIn("worker_pid:", buf.getvalue())
            mock_read_worker_pid.assert_called_once()
            mock_notify_queue.assert_called_once()

    @patch("core.commands.run_inp.notify_queue_enqueued_event", return_value=True)
    @patch("core.queue_worker.read_worker_pid", return_value=4321)
    def test_submit_as_queued_reports_running_worker_pid(
        self,
        mock_read_worker_pid: MagicMock,
        mock_notify_queue: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _submit_as_queued(cfg, _make_args(root, reaction_dir), reaction_dir)

            self.assertEqual(rc, 0)
            self.assertEqual(len(list_queue(root)), 1)
            self.assertIn("worker: running", buf.getvalue())
            self.assertIn("worker_pid: 4321", buf.getvalue())
            mock_read_worker_pid.assert_called_once()
            mock_notify_queue.assert_called_once()


if __name__ == "__main__":
    unittest.main()
