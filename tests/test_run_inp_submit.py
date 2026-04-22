import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from chemstack.orca.commands.run_inp import _submit_as_queued, cmd_run_inp
from chemstack.orca.config import AppConfig, CommonResourceConfig, PathsConfig, RuntimeConfig
from chemstack.orca.queue_store import enqueue, list_queue


def _make_cfg(tmp: str, *, max_cores: int = 8, max_memory_gb: int = 32) -> AppConfig:
    root = Path(tmp)
    fake_orca = root / "fake_orca"
    fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")
    fake_orca.chmod(0o755)
    cfg = AppConfig(
        runtime=RuntimeConfig(allowed_root=tmp),
        paths=PathsConfig(orca_executable=str(fake_orca)),
        resources=CommonResourceConfig(
            max_cores_per_task=max_cores,
            max_memory_gb_per_task=max_memory_gb,
        ),
    )
    setattr(cfg.runtime, "max_concurrent", 1)
    return cfg


def _write_inp(reaction_dir: Path, content: str | None = None) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    (reaction_dir / "rxn.inp").write_text(
        content or "! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n",
        encoding="utf-8",
    )


def _make_args(root: Path, reaction_dir: Path, **overrides) -> SimpleNamespace:
    defaults = {
        "config": str(root / "chemstack.yaml"),
        "reaction_dir": str(reaction_dir),
        "force": False,
        "priority": 10,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestRunInpSubmit(unittest.TestCase):
    @patch("chemstack.orca.commands.run_inp.load_config")
    @patch("chemstack.orca.commands.run_inp._submit_as_queued", return_value=0)
    @patch("chemstack.orca.commands.run_inp._cmd_run_inp_execute", return_value=0)
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

    @patch("chemstack.orca.commands.run_inp.load_config")
    @patch("chemstack.orca.commands.run_inp._submit_as_queued", return_value=0)
    @patch("chemstack.orca.commands.run_inp._cmd_run_inp_execute", return_value=0)
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

    @patch("chemstack.orca.commands.run_inp.load_config")
    @patch("chemstack.orca.commands.run_inp._submit_as_queued", return_value=0)
    @patch("chemstack.orca.commands.run_inp._cmd_run_inp_execute", return_value=0)
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

    @patch("chemstack.orca.commands.run_inp.load_config")
    @patch("chemstack.orca.commands.run_inp._submit_as_queued", return_value=0)
    @patch("chemstack.orca.commands.run_inp._cmd_run_inp_execute", return_value=0)
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

            rc = cmd_run_inp(_make_args(root, reaction_dir))

        self.assertEqual(rc, 0)
        mock_execute.assert_called_once()
        mock_submit_as_queued.assert_not_called()

    @patch("chemstack.orca.commands.run_inp.notify_queue_enqueued_event", return_value=True)
    @patch("chemstack.orca.queue_worker.read_worker_pid", return_value=None)
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
            self.assertEqual(entries[0]["app_name"], "chemstack_orca")
            self.assertTrue(entries[0]["task_id"].startswith("orca_"))
            self.assertEqual(entries[0]["metadata"]["selected_inp"], str(reaction_dir / "rxn.inp"))
            self.assertEqual(entries[0]["metadata"]["selected_input_xyz"], str(reaction_dir / "rxn.inp"))
            self.assertEqual(entries[0]["metadata"]["max_retries"], 2)
            self.assertEqual(entries[0]["metadata"]["submitted_via"], "run_inp")
            self.assertEqual(entries[0]["metadata"]["job_type"], "opt")
            self.assertTrue(str(entries[0]["metadata"]["molecule_key"]).strip())
            self.assertEqual(entries[0]["metadata"]["resource_request"]["max_cores"], 8)
            self.assertEqual(entries[0]["metadata"]["resource_request"]["max_memory_gb"], 32)
            self.assertEqual(entries[0]["metadata"]["resource_actual"]["max_cores"], 8)
            self.assertEqual(entries[0]["metadata"]["resource_actual"]["max_memory_gb"], 32)
            inp_text = (reaction_dir / "rxn.inp").read_text(encoding="utf-8")
            self.assertIn("%pal", inp_text)
            self.assertIn("nprocs 8", inp_text)
            self.assertIn("%maxcore 4096", inp_text)
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

    @patch("chemstack.orca.commands.run_inp.notify_queue_enqueued_event", return_value=True)
    @patch("chemstack.orca.queue_worker.read_worker_pid", return_value=4321)
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

    @patch("chemstack.orca.commands.run_inp.notify_queue_enqueued_event", return_value=True)
    @patch("chemstack.orca.queue_worker.read_worker_pid", return_value=None)
    def test_submit_as_queued_reads_metadata_from_input_even_when_flags_are_present(
        self,
        mock_read_worker_pid: MagicMock,
        mock_notify_queue: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            reaction_dir = root / "rxn"
            _write_inp(
                reaction_dir,
                content=(
                    "! Opt\n"
                    "%pal\n"
                    "  nprocs 12\n"
                    "end\n"
                    "%maxcore 2048\n"
                    "* xyz 0 1\n"
                    "H 0 0 0\n"
                    "H 0 0 0.74\n"
                    "*\n"
                ),
            )

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = _submit_as_queued(
                    cfg,
                    _make_args(root, reaction_dir, max_cores=20, max_memory_gb=80),
                    reaction_dir,
                )

            entries = list_queue(root)

            self.assertEqual(rc, 0)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["metadata"]["resource_request"]["max_cores"], 12)
            self.assertEqual(entries[0]["metadata"]["resource_request"]["max_memory_gb"], 24)
            self.assertEqual(entries[0]["metadata"]["resource_actual"]["max_cores"], 12)
            self.assertEqual(entries[0]["metadata"]["resource_actual"]["max_memory_gb"], 24)
            inp_text = (reaction_dir / "rxn.inp").read_text(encoding="utf-8")
            self.assertIn("nprocs 12", inp_text)
            self.assertIn("%maxcore 2048", inp_text)
            mock_read_worker_pid.assert_called_once()
            mock_notify_queue.assert_called_once()


if __name__ == "__main__":
    unittest.main()
