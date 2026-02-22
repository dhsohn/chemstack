from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import main
from core.organize_index import index_dir, records_path


def _write_config(root: Path, allowed_root: Path, organized_root: Path) -> Path:
    config_path = root / "orca_auto.yaml"
    config_path.write_text(
        json.dumps({
            "runtime": {
                "allowed_root": str(allowed_root),
                "organized_root": str(organized_root),
            },
            "paths": {"orca_executable": "/usr/bin/true"},
        }),
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
    (reaction_dir / "run_state.json").write_text(
        json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8",
    )
    (reaction_dir / "run_report.json").write_text("{}", encoding="utf-8")
    (reaction_dir / "run_report.md").write_text("# Report\n", encoding="utf-8")


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
            rc = main([
                "--config", str(config),
                "organize",
                "--reaction-dir", str(rxn),
                "--json",
            ])
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
            main(["--config", str(config), "organize", "--reaction-dir", str(rxn)])
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
            (rxn / "run_state.json").write_text(
                json.dumps({"run_id": "run_fail", "status": "failed", "final_result": {}}),
                encoding="utf-8",
            )

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "organize",
                "--reaction-dir", str(rxn),
                "--json",
            ])
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
            rc = main([
                "--config", str(config),
                "organize",
                "--reaction-dir", str(rxn),
                "--apply",
            ])
            self.assertEqual(rc, 0)
            self.assertFalse(rxn.exists(), "Source should be removed after apply")

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

            state = json.loads((target_dir / "run_state.json").read_text(encoding="utf-8"))
            self.assertEqual(state["reaction_dir"], str(target_dir))
            self.assertEqual(state["selected_inp"], str(target_dir / "rxn.inp"))
            self.assertEqual(state["final_result"]["last_out_path"], str(target_dir / "rxn.out"))

    def test_apply_recovers_legacy_windows_paths_in_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            rxn = allowed / "rxn_legacy"
            _make_completed_reaction(rxn)

            state_path = rxn / "run_state.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["reaction_dir"] = "/mnt/c/orca_runs/rxn_legacy"
            state["selected_inp"] = "/mnt/c/orca_runs/rxn_legacy/rxn.inp"
            state["attempts"] = [{"index": 1, "inp_path": "/mnt/c/orca_runs/rxn_legacy/rxn.inp", "out_path": "/mnt/c/orca_runs/rxn_legacy/rxn.out"}]
            state["final_result"]["last_out_path"] = "/mnt/c/orca_runs/rxn_legacy/rxn.out"
            state_path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "organize",
                "--reaction-dir", str(rxn),
                "--apply",
            ])
            self.assertEqual(rc, 0)

            rp = records_path(organized)
            recs = [json.loads(line) for line in rp.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(recs), 1)
            target_dir = organized / recs[0]["organized_path"]

            moved_state = json.loads((target_dir / "run_state.json").read_text(encoding="utf-8"))
            self.assertEqual(moved_state["reaction_dir"], str(target_dir))
            self.assertEqual(moved_state["selected_inp"], str(target_dir / "rxn.inp"))
            self.assertEqual(moved_state["attempts"][0]["inp_path"], str(target_dir / "rxn.inp"))
            self.assertEqual(moved_state["attempts"][0]["out_path"], str(target_dir / "rxn.out"))
            self.assertEqual(moved_state["final_result"]["last_out_path"], str(target_dir / "rxn.out"))

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
            with patch("core.orchestrator.append_record", side_effect=RuntimeError("index write failed")):
                rc = main([
                    "--config", str(config),
                    "organize",
                    "--reaction-dir", str(rxn),
                    "--apply",
                ])

            self.assertEqual(rc, 1)
            self.assertTrue(rxn.exists(), "Source should be restored after rollback")

            moved_state_files = [
                p for p in organized.rglob("run_state.json")
                if "index" not in p.parts
            ]
            self.assertEqual(moved_state_files, [])

            state = json.loads((rxn / "run_state.json").read_text(encoding="utf-8"))
            self.assertEqual(state["reaction_dir"], str(rxn))
            self.assertEqual(state["selected_inp"], str(rxn / "rxn.inp"))
            self.assertEqual(state["final_result"]["last_out_path"], str(rxn / "rxn.out"))


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
            rc = main([
                "--config", str(config),
                "organize",
                "--root", str(allowed),
                "--json",
            ])
            self.assertEqual(rc, 0)

    def test_root_scan_rejects_subdir_even_under_allowed_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()
            (allowed / "batch1").mkdir()

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "organize",
                "--root", str(allowed / "batch1"),
            ])
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
            rc = main([
                "--config", str(config),
                "organize",
                "--reaction-dir", str(allowed / "rxn1"),
                "--root", str(allowed),
            ])
            self.assertEqual(rc, 1)

    def test_neither_option_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "organize",
            ])
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
            rc = main([
                "--config", str(config),
                "organize",
                "--rebuild-index",
                "--json",
            ])
            self.assertEqual(rc, 0)


class TestOrganizeFind(unittest.TestCase):

    def test_find_requires_option(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = root / "outputs"
            allowed.mkdir()
            organized.mkdir()

            config = _write_config(root, allowed, organized)
            rc = main([
                "--config", str(config),
                "organize",
                "--find",
            ])
            self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
