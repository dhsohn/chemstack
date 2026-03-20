import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from core.admission_store import (
    ADMISSION_TOKEN_ENV_VAR,
    acquire_direct_slot,
    active_slot_count,
    reserve_slot,
)
from core.commands.run_inp import cmd_run_inp
from core.config import AppConfig, PathsConfig, RuntimeConfig


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


class TestRunInpAdmission(unittest.TestCase):
    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp.run_attempts", return_value=0)
    def test_direct_run_rejects_when_global_limit_reached(
        self,
        mock_run_attempts: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            other_dir = root / "other"
            _write_inp(reaction_dir)
            other_dir.mkdir()

            args = SimpleNamespace(
                config=str(root / "orca_auto.yaml"),
                reaction_dir=str(reaction_dir),
                force=False,
                foreground=True,
            )

            with acquire_direct_slot(root, max_concurrent=1, reaction_dir=other_dir):
                rc = cmd_run_inp(args)

            self.assertEqual(rc, 1)
            self.assertFalse(mock_run_attempts.called)
            self.assertEqual(active_slot_count(root), 0)
            self.assertFalse((reaction_dir / "run_state.json").exists())
            self.assertFalse((reaction_dir / "run.lock").exists())

    @patch("core.commands.run_inp.load_config")
    def test_direct_run_holds_slot_during_execution_and_releases_after(self, mock_load_config: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            observed_counts: list[int] = []

            def _fake_run_attempts(*args, **kwargs) -> int:
                observed_counts.append(active_slot_count(root))
                return 0

            args = SimpleNamespace(
                config=str(root / "orca_auto.yaml"),
                reaction_dir=str(reaction_dir),
                force=False,
                foreground=True,
            )

            with patch("core.commands.run_inp.run_attempts", new=_fake_run_attempts):
                rc = cmd_run_inp(args)

            self.assertEqual(rc, 0)
            self.assertEqual(observed_counts, [1])
            self.assertEqual(active_slot_count(root), 0)

    @patch("core.commands.run_inp.load_config")
    @patch("core.commands.run_inp.run_attempts", return_value=0)
    def test_reserved_slot_from_queue_is_activated_and_released(
        self,
        mock_run_attempts: MagicMock,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn"
            _write_inp(reaction_dir)

            token = reserve_slot(root, 1, queue_id="q_test", source="queue_worker")
            self.assertIsNotNone(token)

            args = SimpleNamespace(
                config=str(root / "orca_auto.yaml"),
                reaction_dir=str(reaction_dir),
                force=False,
                foreground=True,
            )

            with patch.dict(os.environ, {ADMISSION_TOKEN_ENV_VAR: token or ""}, clear=False):
                rc = cmd_run_inp(args)

            self.assertEqual(rc, 0)
            self.assertTrue(mock_run_attempts.called)
            self.assertEqual(active_slot_count(root), 0)

    @patch("core.commands.run_inp.load_config")
    def test_reserved_slot_is_released_when_existing_completed_out_skips_execution(
        self,
        mock_load_config: MagicMock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = _make_cfg(tmp)
            mock_load_config.return_value = cfg
            reaction_dir = root / "rxn_skip"
            _write_inp(reaction_dir)
            (reaction_dir / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

            token = reserve_slot(root, 1, queue_id="q_skip", source="queue_worker")
            self.assertIsNotNone(token)

            args = SimpleNamespace(
                config=str(root / "orca_auto.yaml"),
                reaction_dir=str(reaction_dir),
                force=False,
                foreground=True,
            )

            with patch.dict(os.environ, {ADMISSION_TOKEN_ENV_VAR: token or ""}, clear=False):
                rc = cmd_run_inp(args)

            self.assertEqual(rc, 0)
            self.assertEqual(active_slot_count(root), 0)
            state = json.loads((reaction_dir / "run_state.json").read_text(encoding="utf-8"))
            self.assertEqual(state["final_result"]["reason"], "existing_out_completed")
