from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import main


def _write_config(
    root: Path,
    allowed_root: Path,
    organized_root: Path,
    *,
    remove_overrides_keep: bool = False,
) -> Path:
    config_path = root / "orca_auto.yaml"
    config_path.write_text(
        json.dumps({
            "runtime": {
                "allowed_root": str(allowed_root),
                "organized_root": str(organized_root),
            },
            "cleanup": {
                "remove_overrides_keep": remove_overrides_keep,
            },
            "paths": {"orca_executable": "/usr/bin/true"},
        }),
        encoding="utf-8",
    )
    return config_path


def _make_organized_reaction(reaction_dir: Path) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    (reaction_dir / "rxn.inp").write_text("! Opt\n* xyz 0 1\nH 0 0 0\n*\n", encoding="utf-8")
    (reaction_dir / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
    (reaction_dir / "rxn.xyz").write_text("1\n\nH 0 0 0\n", encoding="utf-8")
    (reaction_dir / "rxn.gbw").write_bytes(b"\x00" * 100)
    (reaction_dir / "rxn.hess").write_text("hessian data\n", encoding="utf-8")
    (reaction_dir / "rxn.densities").write_bytes(b"\x00" * 500)
    (reaction_dir / "rxn.tmp").write_text("tmp\n", encoding="utf-8")
    (reaction_dir / "rxn.engrad").write_text("engrad\n", encoding="utf-8")
    (reaction_dir / "rxn.retry01.inp").write_text("! Opt\n", encoding="utf-8")
    (reaction_dir / "rxn.retry01.out").write_text("failed\n", encoding="utf-8")
    (reaction_dir / "rxn_trj.xyz").write_text("trj\n", encoding="utf-8")
    (reaction_dir / "run_report.json").write_text("{}", encoding="utf-8")
    (reaction_dir / "run_report.md").write_text("# Report\n", encoding="utf-8")
    state = {
        "run_id": f"run_20260222_101530_{reaction_dir.name[:8].ljust(8, '0')}",
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(reaction_dir / "rxn.inp"),
        "status": "completed",
        "attempts": [{"index": 1, "inp_path": str(reaction_dir / "rxn.inp"),
                      "out_path": str(reaction_dir / "rxn.out")}],
        "final_result": {
            "status": "completed",
            "analyzer_status": "completed",
            "reason": "normal_termination",
            "completed_at": "2026-02-22T10:15:45+00:00",
            "last_out_path": str(reaction_dir / "rxn.out"),
        },
    }
    (reaction_dir / "run_state.json").write_text(
        json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8",
    )


class TestCleanupDryRun(unittest.TestCase):

    def test_dry_run_does_not_delete_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = organized / "opt" / "H2" / "run_001"
            _make_organized_reaction(rxn)

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--reaction-dir", str(rxn),
                "--json",
            ])
            self.assertEqual(rc, 0)
            self.assertTrue((rxn / "rxn.densities").exists())
            self.assertTrue((rxn / "rxn.tmp").exists())
            self.assertTrue((rxn / "rxn.retry01.inp").exists())

    def test_dry_run_root_scan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            _make_organized_reaction(organized / "opt" / "H2" / "run_001")
            _make_organized_reaction(organized / "ts" / "H2O" / "run_002")

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--root", str(organized),
                "--json",
            ])
            self.assertEqual(rc, 0)

    def test_dry_run_default_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            _make_organized_reaction(organized / "opt" / "H2" / "run_001")

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--json",
            ])
            self.assertEqual(rc, 0)


class TestCleanupApply(unittest.TestCase):

    def test_apply_removes_junk_files_but_keeps_retry_patterns_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = organized / "opt" / "H2" / "run_001"
            _make_organized_reaction(rxn)

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--reaction-dir", str(rxn),
                "--apply",
            ])
            self.assertEqual(rc, 0)
            # junk removed
            self.assertFalse((rxn / "rxn.densities").exists())
            self.assertFalse((rxn / "rxn.tmp").exists())
            self.assertFalse((rxn / "rxn.engrad").exists())
            self.assertTrue((rxn / "rxn.retry01.inp").exists())
            self.assertTrue((rxn / "rxn.retry01.out").exists())
            self.assertTrue((rxn / "rxn_trj.xyz").exists())
            # essential kept
            self.assertTrue((rxn / "rxn.inp").exists())
            self.assertTrue((rxn / "rxn.out").exists())
            self.assertTrue((rxn / "rxn.xyz").exists())
            self.assertTrue((rxn / "rxn.gbw").exists())
            self.assertTrue((rxn / "rxn.hess").exists())
            self.assertTrue((rxn / "run_state.json").exists())
            self.assertTrue((rxn / "run_report.json").exists())
            self.assertTrue((rxn / "run_report.md").exists())

    def test_apply_removes_retry_patterns_when_override_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = organized / "opt" / "H2" / "run_001"
            _make_organized_reaction(rxn)

            config = _write_config(root, allowed, organized, remove_overrides_keep=True)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--reaction-dir", str(rxn),
                "--apply",
            ])
            self.assertEqual(rc, 0)
            self.assertFalse((rxn / "rxn.retry01.inp").exists())
            self.assertFalse((rxn / "rxn.retry01.out").exists())
            self.assertFalse((rxn / "rxn_trj.xyz").exists())

    def test_apply_preserves_state_referenced_retry_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = organized / "opt" / "H2" / "run_001"
            _make_organized_reaction(rxn)
            state_path = rxn / "run_state.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["selected_inp"] = "/legacy/path/rxn.retry01.inp"
            state["attempts"] = [
                {
                    "index": 1,
                    "inp_path": str(rxn / "rxn.inp"),
                    "out_path": str(rxn / "rxn.out"),
                },
                {
                    "index": 2,
                    "inp_path": "/legacy/path/rxn.retry01.inp",
                    "out_path": "/legacy/path/rxn.retry01.out",
                },
            ]
            state["final_result"]["last_out_path"] = "/legacy/path/rxn.retry01.out"
            state_path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--reaction-dir", str(rxn),
                "--apply",
            ])
            self.assertEqual(rc, 0)
            self.assertTrue((rxn / "rxn.retry01.inp").exists())
            self.assertTrue((rxn / "rxn.retry01.out").exists())
            self.assertFalse((rxn / "rxn.densities").exists())

    def test_apply_root_scan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            _make_organized_reaction(organized / "opt" / "H2" / "run_001")
            _make_organized_reaction(organized / "ts" / "H2O" / "run_002")

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--root", str(organized),
                "--apply",
                "--json",
            ])
            self.assertEqual(rc, 0)
            self.assertFalse((organized / "opt" / "H2" / "run_001" / "rxn.densities").exists())
            self.assertFalse((organized / "ts" / "H2O" / "run_002" / "rxn.densities").exists())

    @patch("core.commands._helpers._send_batch_summary")
    def test_apply_sends_summary_notification(self, mock_send_summary) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = organized / "opt" / "H2" / "run_001"
            _make_organized_reaction(rxn)

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--reaction-dir", str(rxn),
                "--apply",
            ])
            self.assertEqual(rc, 0)
            mock_send_summary.assert_called_once()
            summary_text = mock_send_summary.call_args[0][1]
            self.assertIn("[orca_auto] cleanup | action=apply", summary_text)
            self.assertIn("cleaned=1", summary_text)


class TestCleanupMutualExclusion(unittest.TestCase):

    def test_both_options_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--reaction-dir", str(organized / "rxn1"),
                "--root", str(organized),
            ])
            self.assertEqual(rc, 1)


class TestCleanupGuardrails(unittest.TestCase):

    def test_reaction_dir_outside_organized_root_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = allowed / "rxn1"
            rxn.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--reaction-dir", str(rxn),
            ])
            self.assertEqual(rc, 1)

    def test_root_mismatch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            other = root / "other_outputs"
            allowed.mkdir()
            organized.mkdir()
            other.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "cleanup",
                "--root", str(other),
            ])
            self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
