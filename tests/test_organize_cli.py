from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from chemstack.cli import main
from chemstack.orca.organize_index import records_path
from chemstack.orca.state import load_state, save_state, write_report_files


def _write_config(root: Path, allowed_root: Path, organized_root: Path) -> Path:
    config_path = root / "chemstack.yaml"
    config_path.write_text(
        json.dumps(
            {
                "orca": {
                    "runtime": {
                        "allowed_root": str(allowed_root),
                        "organized_root": str(organized_root),
                    },
                    "paths": {"orca_executable": "/usr/bin/true"},
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def _make_completed_reaction(reaction_dir: Path) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    inp = reaction_dir / "rxn.inp"
    inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\n*\n", encoding="utf-8")
    out = reaction_dir / "rxn.out"
    out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
    state = {
        "run_id": f"run_20260222_101530_{reaction_dir.name[:8].ljust(8, '0')}",
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(inp),
        "status": "completed",
        "started_at": "2026-02-22T10:15:30+00:00",
        "updated_at": "2026-02-22T10:15:45+00:00",
        "max_retries": 5,
        "attempts": [{"index": 1, "inp_path": str(inp), "out_path": str(out)}],
        "final_result": {
            "status": "completed",
            "analyzer_status": "completed",
            "reason": "normal_termination",
            "completed_at": "2026-02-22T10:15:45+00:00",
            "last_out_path": str(out),
        },
    }
    save_state(reaction_dir, state)
    write_report_files(reaction_dir, state)


class TestOrganizeDryRun(unittest.TestCase):
    def test_dry_run_single_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = allowed / "rxn1"
            _make_completed_reaction(rxn)

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--reaction-dir",
                    str(rxn),
                ]
            )
            self.assertEqual(rc, 0)

    def test_dry_run_does_not_move_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = allowed / "rxn1"
            _make_completed_reaction(rxn)

            config = _write_config(root, allowed, organized)
            main(["organize", "orca", "--config", str(config), "--reaction-dir", str(rxn)])
            self.assertTrue(rxn.exists(), "Source should still exist after dry-run")
            self.assertFalse(
                any(organized.iterdir()),
                "Organized root should be empty after dry-run",
            )

    def test_skips_non_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = allowed / "rxn_failed"
            rxn.mkdir()
            save_state(
                rxn,
                {
                    "run_id": "run_fail",
                    "reaction_dir": str(rxn),
                    "selected_inp": "",
                    "max_retries": 0,
                    "status": "failed",
                    "attempts": [],
                    "final_result": {},
                },
            )

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--reaction-dir",
                    str(rxn),
                ]
            )
            self.assertEqual(rc, 0)


class TestOrganizeApply(unittest.TestCase):
    def test_apply_moves_directory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = allowed / "rxn1"
            _make_completed_reaction(rxn)

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--reaction-dir",
                    str(rxn),
                    "--apply",
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue(
                rxn.exists(), "Original run directory should remain as an organized_ref stub"
            )
            self.assertTrue(
                (rxn / "organized_ref.json").exists(),
                "organized_ref should be written in original run directory",
            )

            rp = records_path(organized)
            self.assertTrue(rp.exists(), "Index should be created")
            records = [
                json.loads(line)
                for line in rp.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(records), 1)
            rec = records[0]
            target_dir = organized / rec["organized_path"]

            # Paths are normalized to moved directory and index stores organize-relative paths.
            self.assertEqual(rec["reaction_dir"], str(target_dir))
            self.assertEqual(rec["selected_inp"], "rxn.inp")
            self.assertEqual(rec["last_out_path"], "rxn.out")

            state = load_state(target_dir)
            assert state is not None
            self.assertEqual(state["reaction_dir"], str(target_dir))
            self.assertEqual(state["selected_inp"], str(target_dir / "rxn.inp"))
            final_result = state["final_result"]
            assert final_result is not None
            self.assertEqual(final_result["last_out_path"], str(target_dir / "rxn.out"))
            organized_ref = json.loads((rxn / "organized_ref.json").read_text(encoding="utf-8"))
            self.assertEqual(organized_ref["run_id"], rec["run_id"])
            self.assertEqual(organized_ref["organized_output_dir"], str(target_dir))
            tracking_records = json.loads(
                (allowed / "job_locations.json").read_text(encoding="utf-8")
            )
            self.assertEqual(len(tracking_records), 1)
            self.assertEqual(tracking_records[0]["job_id"], rec["run_id"])
            self.assertEqual(tracking_records[0]["original_run_dir"], str(rxn.resolve()))
            self.assertEqual(tracking_records[0]["organized_output_dir"], str(target_dir.resolve()))
            self.assertEqual(tracking_records[0]["latest_known_path"], str(target_dir.resolve()))

            self.assertEqual(state["attempts"][0]["inp_path"], str(target_dir / "rxn.inp"))
            self.assertEqual(state["attempts"][0]["out_path"], str(target_dir / "rxn.out"))

    def test_apply_rolls_back_when_index_append_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = allowed / "rxn_rollback"
            _make_completed_reaction(rxn)

            config = _write_config(root, allowed, organized)
            with patch(
                "chemstack.orca.commands.organize.append_record",
                side_effect=RuntimeError("index write failed"),
            ):
                rc = main(
                    [
                        "organize",
                        "orca",
                        "--config",
                        str(config),
                        "--reaction-dir",
                        str(rxn),
                        "--apply",
                    ]
                )

            self.assertEqual(rc, 1)
            self.assertTrue(rxn.exists(), "Source should be restored after rollback")
            self.assertFalse(
                (rxn / "organized_ref.json").exists(),
                "organized_ref stub should be cleaned up on rollback",
            )

            moved_state_files = [
                p for p in organized.rglob("job_state.json") if "index" not in p.parts
            ]
            self.assertEqual(moved_state_files, [])

            state = load_state(rxn)
            assert state is not None
            self.assertEqual(state["reaction_dir"], str(rxn))
            self.assertEqual(state["selected_inp"], str(rxn / "rxn.inp"))
            final_result = state["final_result"]
            assert final_result is not None
            self.assertEqual(final_result["last_out_path"], str(rxn / "rxn.out"))


class TestOrganizeRootScan(unittest.TestCase):
    def test_root_scan_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            _make_completed_reaction(allowed / "rxn1")
            _make_completed_reaction(allowed / "rxn2")

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--root",
                    str(allowed),
                ]
            )
            self.assertEqual(rc, 0)

    def test_root_scan_apply_includes_nested_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            nested_a = allowed / "batch1" / "nested_a1"
            nested_b = allowed / "batch2" / "nested_b2"
            _make_completed_reaction(nested_a)
            _make_completed_reaction(nested_b)

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--root",
                    str(allowed),
                    "--apply",
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue((nested_a / "organized_ref.json").exists())
            self.assertTrue((nested_b / "organized_ref.json").exists())

            rp = records_path(organized)
            self.assertTrue(rp.exists())
            records = [
                json.loads(line)
                for line in rp.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(records), 2)

    def test_root_scan_rejects_subdir_even_under_allowed_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()
            (allowed / "batch1").mkdir()

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--root",
                    str(allowed / "batch1"),
                ]
            )
            self.assertEqual(rc, 1)


class TestOrganizeMutualExclusion(unittest.TestCase):
    def test_both_reaction_dir_and_root_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--reaction-dir",
                    str(allowed / "rxn1"),
                    "--root",
                    str(allowed),
                ]
            )
            self.assertEqual(rc, 1)

    def test_neither_option_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                ]
            )
            self.assertEqual(rc, 1)


class TestOrganizeRebuildIndex(unittest.TestCase):
    def test_rebuild_index(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main(
                [
                    "organize",
                    "orca",
                    "--config",
                    str(config),
                    "--rebuild-index",
                ]
            )
            self.assertEqual(rc, 0)
